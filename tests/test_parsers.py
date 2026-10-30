# -*- coding: utf-8 -*-
"""解析器与价格计算的回归测试。

每个用例都对应一个真实踩过的坑：
  - 倍率/单价单位换算（per-token vs per-1M vs new-api 倍率）
  - 分组倍率该不该乘（per-call 价格曾被整段丢掉）
  - 官方价兜底（站点自带价只补缺口，不覆盖权威源）
  - 脏数据闸门（负倍率、离谱单价不能入库）
运行：python -m pytest tests/ -q
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import relay_scraper as rs  # noqa: E402


# ---------------------------------------------------------------- new-api 系
def test_parse_pricing_basic():
    pricing = {"data": [{"model_name": "gpt-5.5", "model_ratio": 1.25,
                         "completion_ratio": 4.0, "cache_ratio": 0.1,
                         "enable_groups": ["default"]}],
               "group_ratio": {"default": 1.0}}
    models, groups, _ver = rs.parse_pricing(pricing)
    assert len(models) == 1
    m = models[0]
    # 倍率 1.25 → $2.5/M；归一化（model_canon/model_agg）是 save 阶段补的
    assert m["model_ratio"] == 1.25
    assert m["completion_ratio"] == 4.0
    assert m.get("model_agg") is None
    assert groups == {"default": 1.0}


def test_parse_pricing_tiered_expr_uses_coefficient():
    """MemeCode 类站点：model_ratio 是占位值，真实价在 billing_expr 的 tier 里。"""
    pricing = {"data": [{"model_name": "gpt-6-sol",
                         "model_ratio": 37.5,          # 占位，等价 $75/M 全错
                         "completion_ratio": 1.0,
                         "billing_mode": "tiered_expr",
                         "billing_expr":
                             'tier("standard", p * 2 + c * 8 + cr * 0.2 + cc * 2.5)'}]}
    models, _g, _v = rs.parse_pricing(pricing)
    m = models[0]
    # 系数 2（$/1M）→ 倍率 = 2 / 2
    assert m["model_ratio"] == 1.0
    assert m["completion_ratio"] == pytest.approx(4.0)


# ---------------------------------------------------------------- sub2api 系
def _plaza_payload():
    return {"data": {"groups": [
        {"name": "gemini", "rate_multiplier": 0.15, "platform": "gemini",
         "models": [
             {"name": "gemini-3.1-flash-image", "platform": "gemini",
              "pricing": {"billing_mode": "image", "input_price": 5e-07,
                          "output_price": 3e-06, "per_request_price": 0.008},
              "official_pricing": {"input_price": 5e-07, "output_price": 3e-06}},
             {"name": "gpt-5.5", "platform": "openai",
              "pricing": {"billing_mode": "token", "input_price": 2.5e-06,
                          "output_price": 1e-05},
              "official_pricing": {"input_price": 2.5e-06, "output_price": 1e-05}}]}]}}


def test_parse_model_plaza_token_ratio():
    models, groups, _ver = rs.parse_model_plaza(_plaza_payload())
    tok = [m for m in models if m["model_name"] == "gpt-5.5"][0]
    # 2.5e-06 $/token = $2.5/M → 倍率 1.25（new-api 约定倍率 1 = $2/M）
    assert tok["model_ratio"] == pytest.approx(1.25)
    assert tok["completion_ratio"] == pytest.approx(4.0)
    assert groups["gemini"] == 0.15


def test_parse_model_plaza_image_per_call_and_official():
    models, _g, _v = rs.parse_model_plaza(_plaza_payload())
    img = [m for m in models if m["model_name"] == "gemini-3.1-flash-image"][0]
    assert img["quota_type"] == 1
    assert img["billing_unit"] == "per_call"
    assert img["model_price"] == 0.008          # 折前基准价
    # official_pricing 是 USD/token → 换算成 USD/1M
    assert img["official_in_usd_m"] == pytest.approx(0.5)
    assert img["official_out_usd_m"] == pytest.approx(3.0)


def test_model_group_rows_multiplies_per_call_price():
    """核心回归：按次价必须乘分组倍率。

    hahacode 的 per_request_price=0.008 是折前价，分组倍率 0.15，
    站点页面展示 0.0012/张。曾经看板显示 0.008（贵 6.7 倍）。
    注意 enable_groups 是 JSON 字符串（与 DB 列一致）。
    """
    img = {"model_name": "gemini-3.1-flash-image", "model_ratio": 0.0,
           "completion_ratio": 1.0, "cache_ratio": None, "create_cache_ratio": None,
           "model_price": 0.008, "quota_type": 1, "billing_unit": "per_call",
           "enable_groups": '["gemini"]', "tiers": "[]"}
    rows = list(rs.model_group_rows(img, {"gemini": 0.15}))
    assert len(rows) == 1
    _g, gratio, _en, _i, _o, _cr, _cw, _sec, call, _res = rows[0]
    assert gratio == 0.15
    assert call == pytest.approx(0.0012)


def test_model_group_rows_token_price_unchanged():
    """token 模型的 $/M 价 = 倍率 × 2，不应受本次改动影响。"""
    tok = {"model_name": "gpt-5.5", "model_ratio": 1.25, "completion_ratio": 4.0,
           "cache_ratio": 0.1, "create_cache_ratio": None, "model_price": None,
           "quota_type": 0, "enable_groups": '["gemini"]', "tiers": "[]"}
    rows = list(rs.model_group_rows(tok, {"gemini": 0.15}))
    _g, gratio, _en, inp, out, _cr, _cw, _sec, call, _res = rows[0]
    # 基准价 = 倍率 × $2/M = 2.5；乘 0.15 倍率 = $0.375/M
    assert inp == pytest.approx(2.5)
    assert inp * gratio == pytest.approx(0.375)
    assert out * gratio == pytest.approx(1.5)
    assert call is None


# ---------------------------------------------------------------- 其它模式
def test_parse_public_models_synthesizes_default_group():
    """uptoken 直定价：无分组信息时必须补 default 组，否则价格列会空。"""
    payload = {"data": [{"name": "gpt-5.5", "input_price_per_1m": 2.5,
                         "output_price_per_1m": 10.0}]}
    models, groups = rs.parse_public_models(payload)
    assert len(models) == 1
    assert groups.get("default") == 1.0
    assert models[0]["model_ratio"] == pytest.approx(1.25)


def test_parse_models_feed_divides_out_multiplier():
    """top-one：价格已含分组折扣，基准价 = 价格 ÷ 倍率。"""
    payload = {"models": [{"modelId": "gpt-5.6-sol", "provider": "OpenAI",
                           "officialPricingUsd": {"input": 5.0, "output": 30.0},
                           "tiers": [{"group": "codex pro", "multiplier": "0.09x",
                                      "input": 0.45, "output": 3.6}]}]}
    models, groups = rs.parse_models_feed(payload)
    m = models[0]
    assert m["model_ratio"] == pytest.approx(2.5)      # 0.45 / 0.09 = $5/M
    assert m["completion_ratio"] == pytest.approx(8.0)
    assert groups["codex pro"] == 0.09
    assert m["official_in_usd_m"] == 5.0               # 已是 USD/1M


def test_off_usd_m_handles_both_units():
    assert rs._off_usd_m({"input": 5.0}, "input") == 5.0          # USD/1M
    assert rs._off_usd_m({"input": 5e-07}, "input") == pytest.approx(0.5)  # per-token
    assert rs._off_usd_m({"input": None}, "input") is None
    assert rs._off_usd_m(None, "input") is None


# ---------------------------------------------------------------- 数据闸门
def test_sanity_check_drops_impossible_prices():
    models = [
        {"model_name": "ok", "model_ratio": 1.0, "completion_ratio": 5.0},
        {"model_name": "neg", "model_ratio": -1, "completion_ratio": 5.0},
        {"model_name": "huge", "model_ratio": 99999, "completion_ratio": 5.0},
    ]
    ok, warns = rs.sanity_check_models(models)
    assert [m["model_name"] for m in ok] == ["ok"]
    assert any("倍率非法" in w for w in warns)
    assert any("超出合理区间" in w for w in warns)


def test_sanity_check_keeps_media_models_with_zero_completion():
    """图片/视频模型的 completion_ratio 恒为 0，不能误判为异常。"""
    models = [{"model_name": "gpt-image-2", "model_ratio": 0.5,
               "completion_ratio": 0, "quota_type": 1, "model_type": "image"},
              {"model_name": "gpt-5.5", "model_ratio": 1.0, "completion_ratio": 0}]
    ok, warns = rs.sanity_check_models(models)
    assert len(ok) == 2
    assert [w.split(":")[0] for w in warns] == ["gpt-5.5"]


# ---------------------------------------------------------------- 官方价
def test_lookup_official_matches_agg_key_not_prefix():
    official = ({"gpt-5.5": {"input": 1.75, "output": 14.0}}, {})
    # 分隔符差异要能匹配（点/下划线 → 连字符）
    assert rs.lookup_official(official, "gpt-5.5") is not None
    assert rs.lookup_official(official, "gpt_5.5") is not None
    # 不能前缀猜测：gpt-5.5-nano 不该用 gpt-5.5 的价
    assert rs.lookup_official(official, "gpt-5.5-nano") is None
    assert rs.lookup_official(official, "") is None


def test_feed_official_is_fallback_only():
    conn = rs.connect(":memory:")
    conn.execute("INSERT INTO feed_official(model_key,input_usd_m,output_usd_m,source,ts)"
                 " VALUES('gemini-3-1-flash-image',0.5,3.0,'hahacode','2026-10-23')")
    conn.commit()
    feed = rs.load_feed_official(conn)
    assert feed["gemini-3-1-flash-image"]["input"] == 0.5
    # 权威表有时不用兜底
    official = ({"gemini-3-1-flash-image": {"input": 0.4, "output": 2.4}}, {})
    assert rs.lookup_official(official, "gemini-3.1-flash-image") is not None


# ---------------------------------------------------------------- 数据库视图
def _seed_db():
    """造一个含站点/快照/价格/调价记录的内存库。

    时间戳一律相对「现在」生成：写死日期的测试会随真实时间过期
    （曾经硬编码 10-22，两天后就被 48 小时窗口过滤掉而失败）。
    """
    conn = rs.connect(":memory:")
    now = rs.utc_now()
    hours_ago = lambda h: rs._ts_to_epoch(now) - h * 3600  # noqa: E731
    import datetime as _dt

    def iso(h):
        return _dt.datetime.fromtimestamp(
            hours_ago(h), tz=_dt.timezone.utc).isoformat()

    conn.execute("INSERT INTO stations(name,domains,mode,created_at) "
                 "VALUES('t1','[\"https://a\"]','newapi',?)", (now,))
    sid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute("INSERT INTO snapshots(station_id,ts,ok) VALUES(?,?,?)",
                 (sid, iso(72), 1))
    snap = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute("UPDATE stations SET last_check=? WHERE id=?", (iso(2), sid))
    conn.execute("INSERT INTO model_prices(snapshot_id,model_name,model_key,model_canon,"
                 "model_agg,model_ratio,completion_ratio,enable_groups)"
                 " VALUES(?,?,?,?,?,?,?,?)",
                 (snap, "gpt-5.5", "gpt-5.5", "gpt-5.5", "gpt-5-5", 1.25, 4.0, '["default"]'))
    conn.execute("INSERT INTO group_ratios(snapshot_id,group_name,ratio) VALUES(?,?,?)",
                 (snap, "default", 1.0))
    conn.execute("INSERT INTO price_changes(station_id,ts,model_canon,grp,"
                 "old_input_usd_m,new_input_usd_m,pct) VALUES(?,?,?,?,?,?,?)",
                 (sid, iso(24), "gpt-5.5", "default", 2.5, 2.0, -20.0))
    conn.commit()
    return conn


def test_station_freshness_reports_check_and_change():
    f = rs.station_freshness(_seed_db())
    assert len(f) == 1
    assert f[0]["name"] == "t1"
    assert f[0]["models"] == 1
    assert f[0]["check_hours"] is not None
    assert f[0]["change_hours"] is not None
    # 探测统计字段必须存在（E5：站点总览要展示真实调用成本）
    assert f[0]["probes_24h"] == 0
    assert f[0]["probe_fail_24h"] == 0


def test_station_freshness_counts_model_probes():
    conn = _seed_db()
    sid = conn.execute("SELECT id FROM stations WHERE name='t1'").fetchone()[0]
    now = rs.utc_now()
    for st in ("probe_ok", "probe_fail", "probe_ok"):
        conn.execute("INSERT INTO model_availability(station_id, ts, model_agg, status)"
                     " VALUES(?,?,?,?)", (sid, now, "gpt-5-5", st))
    conn.commit()
    f = rs.station_freshness(conn)[0]
    assert f["probes_24h"] == 3
    assert f["probe_fail_24h"] == 1


def test_summary_cache_table_in_schema():
    """C2：汇总预计算缓存表必须随 SCHEMA 建出来。"""
    conn = rs.connect(":memory:")
    cols = {r[1] for r in conn.execute("PRAGMA table_info(summary_cache)")}
    assert {"ver", "payload", "built_at"} <= cols


def test_collect_price_changes_window_and_summary():
    d = rs.collect_price_changes(_seed_db(), hours=48, threshold=5)
    assert d["summary"]["total"] == 1
    assert d["summary"]["down"] == 1
    row = d["rows"][0]
    assert row["station"] == "t1"
    assert row["pct"] == -20.0
    # 超出窗口的时间应被过滤
    assert rs.collect_price_changes(_seed_db(), hours=1)["summary"]["total"] == 0


# ---------------------------------------------------------------- 展示格式
def test_fmt_never_uses_scientific_notation():
    assert rs.fmt(0.0000595905) == "0.0000595905"
    assert rs.fmt(0) == "0"
    assert rs.fmt(None) == ""
    assert rs.fmt(199998) == "199998"


# ---------------------------------------------------------------- 百炼模型市场
# 真实响应片段（免登录 POST 网关）：模型组 → items[] → prices[]
BAILIAN_SAMPLE = {
    "code": "200",
    "data": {"DataV2": {"data": {
        "codeEnum": "SUCCESS",
        "data": {"total": 1, "pageNo": 1, "pageSize": 50, "list": [
            {"model": "group-qwen3.8-max", "name": "Qwen3.8-Max",
             "items": [
                 {"model": "qwen3.8-max", "name": "Qwen3.8-Max",
                  "prices": [
                      {"priceUnit": "每百万tokens", "price": "12",
                       "type": "input_token", "priceName": "输入"},
                      {"priceUnit": "每百万tokens", "price": "36",
                       "type": "output_token", "priceName": "输出"},
                      {"priceUnit": "每百万tokens", "price": "1.5",
                       "type": "input_token_cache", "priceName": "输入（缓存命中）"},
                      {"priceUnit": "每百万tokens", "price": "15",
                       "type": "input_token_cache_creation_5m",
                       "priceName": "显式缓存创建"},
                      # 非 token 档必须被忽略：按张计价与 ¥/M 折扣列不同维度
                      {"priceUnit": "每张", "price": "0.14",
                       "type": "input_token", "priceName": "图片"},
                  ]},
                 # 历史快照版本：归一化后与上面同键，取首个
                 {"model": "qwen3.8-max-0902",
                  "prices": [{"priceUnit": "每百万tokens", "price": "99",
                              "type": "input_token"},
                             {"priceUnit": "每百万tokens", "price": "99",
                              "type": "output_token"}]},
             ]},
            # 按秒计费的视频模型：没有 token 档价格，应被跳过而不是记 0
            {"model": "group-wan3.0-video",
             "items": [{"model": "wan3.0-video",
                        "prices": [{"priceUnit": "每秒", "price": "1.2",
                                    "type": "input_token"}]}]},
        ]}}}}}


def test_parse_bailian_prices():
    out = rs._parse_bailian_payload(BAILIAN_SAMPLE)
    # 只有带完整 ¥/M 输入+输出的模型才入库
    assert set(out) == {"qwen3-8-max"}
    rec = out["qwen3-8-max"]
    assert rec["input"] == 12.0 and rec["output"] == 36.0
    # 隐式缓存命中当读价、显式创建当写价
    assert rec["cache_read"] == 1.5
    assert rec["cache_write"] == 15.0
    assert rec["currency"] == "cny" and rec["source"] == "bailian"


def test_parse_bailian_skips_non_token_units():
    """按张/按秒计价的模型没有 ¥/M 口径，不能进 token 折扣基准。"""
    out = rs._parse_bailian_payload(BAILIAN_SAMPLE)
    assert "wan3-0-video" not in out
