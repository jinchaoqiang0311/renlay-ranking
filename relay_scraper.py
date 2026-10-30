#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
relay_scraper.py — 中转站模型分组与价格抓取工具

按《中转站数据采集方法论》实现：
  1) 抓取各中转站公开的 /api/pricing 与 /api/status（one-api/new-api 零鉴权接口）
  2) 按 new-api 计价单位换算绝对价格：倍率 1 = $0.002/1K tokens = $2/1M tokens
  3) SQLite 时序快照 + pricing_version 变更检测 + 多域名故障转移
  4) 导出详细数据（CSV/JSON）：站点 × 模型 × 分组 的全倍率与折算价格

数据来源均为站点自行公开的无鉴权接口；仅做低频读取，请合理设置频率。

用法：
  python relay_scraper.py scrape                      # 抓取全部站点
  python relay_scraper.py list                        # 查看站点与最近抓取状态
  python relay_scraper.py export --format both        # 导出详细数据
  python relay_scraper.py cheapest                    # 每模型最低价
  python relay_scraper.py history --model claude-fable-5
  python relay_scraper.py models                      # 模型清单
  python relay_scraper.py availability --refresh      # 模型可用性检测（上架/下架比对）
  python relay_scraper.py availability --report       # 可用性报告（在售站数/上下架变更）
  python relay_scraper.py watch                       # 定时：可达性探测 10 分钟 + 价格抓取 12 小时
  python relay_scraper.py add --name X --domain x.com # 添加站点
  各子命令均可加 --help 查看参数。
"""

import argparse
import csv
import hashlib
import html as _htmlmod
import json
import os
import random
import re
import subprocess
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from functools import lru_cache
# 插件上报限速：每 IP 每分钟 30 次（插件自动上报是低频的，超限多半是滥用/脚本）
_INGEST_RATES = {}

try:
    import requests as _requests
except Exception:                                     # 无 requests 时退化为 urllib
    _requests = None

DB_PATH = os.environ.get("RELAY_DB", "relay_data.db")
STATIONS_PATH = os.environ.get("RELAY_STATIONS", "stations.json")
OFFICIAL_PATH = os.environ.get("RELAY_OFFICIAL", "official_prices.json")
# 自动备份：watch 每轮抓取后拷一份，只留最近 N 份（数据攒了几个月，丢了没法重抓）
BACKUP_DIR = os.environ.get("RELAY_BACKUP_DIR", "backups")
BACKUP_KEEP = int(os.environ.get("RELAY_BACKUP_KEEP", "14"))
# 浏览器 UA：多数中转站位于 Cloudflare 之后，会拦截 Python-urllib 等默认 UA
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
RATIO_UNIT_USD_PER_M = 2.0  # new-api 约定：倍率 1 = $0.002/1K tokens = $2/1M tokens
SCHEMA_VERSION = 9          # 归一化/聚合键/家族名/变体等派生规则的迁移版本（改规则需 +1）
# 站点私有计价规则：model_price 只是内部额度单价时，挂牌价还要乘站内硬编码倍率
STATION_RULES_PATH = os.environ.get("RELAY_STATION_RULES", "station_pricing_rules.json")
_STATION_RULES = None
# 「语言模型」筛选按钮的白名单（model_whitelist.json），按规范名前缀匹配
MODEL_WHITELIST_PATH = os.environ.get("RELAY_WHITELIST", "model_whitelist.json")
_WHITELIST = None
# 模型规范名 → 出品公司（vendor_map.json），供看板「全部公司」筛选与 CSV「公司」列
VENDOR_MAP_PATH = os.environ.get("RELAY_VENDOR_MAP", "vendor_map.json")
_VENDOR_MAP = None


def build_opener():
    """仅使用环境变量代理（http_proxy/https_proxy），忽略 Windows 注册表代理——
    本机代理软件未运行时，urllib 默认读取注册表代理会导致连接被拒(10061)。"""
    try:
        proxies = urllib.request.getproxies_environment()
    except Exception:
        proxies = {}
    return urllib.request.build_opener(urllib.request.ProxyHandler(proxies))


_OPENER = build_opener()

# requests 可用时按线程持有 Session（连接池复用，线程安全）；否则退回 urllib
_TLS = threading.local()


def _session():
    """按线程持有 requests.Session（连接池复用，线程安全）。
    注意：requests 默认会读 Windows 注册表代理，本机代理软件未运行时会连接被拒，
    因此关闭 trust_env，只用环境变量代理（与 urllib 分支行为一致）。"""
    if not _requests:
        return None
    s = getattr(_TLS, "session", None)
    if s is None:
        s = _requests.Session()
        s.headers.update({"User-Agent": UA, "Accept": "application/json,text/html"})
        try:
            env_proxies = urllib.request.getproxies_environment()
        except Exception:
            env_proxies = {}
        s.trust_env = False
        s.proxies = env_proxies
        _TLS.session = s
    return s


_RETRY_STATUS = {429, 500, 502, 503, 504}


def _one_request(url: str, timeout: float, headers: dict):
    """单次请求（Session 优先）。返回 (status, body_bytes, latency_ms, error)。"""
    t0 = time.monotonic()
    s = _session()
    try:
        if s is not None:
            r = s.request("GET", url, timeout=timeout, headers=headers or None,
                          allow_redirects=True)
            return r.status_code, r.content, int((time.monotonic() - t0) * 1000), None
        req = urllib.request.Request(url, headers=dict({"User-Agent": UA,
                                                        "Accept": "application/json,text/html"},
                                                       **(headers or {})))
        with _OPENER.open(req, timeout=timeout) as resp:
            return resp.status, resp.read(), int((time.monotonic() - t0) * 1000), None
    except urllib.error.HTTPError as e:
        return e.code, b"", int((time.monotonic() - t0) * 1000), f"HTTP {e.code}"
    except Exception as e:
        if _requests is not None and isinstance(e, _requests.exceptions.HTTPError):
            return e.response.status_code, b"", int((time.monotonic() - t0) * 1000), \
                f"HTTP {e.response.status_code}"
        return None, b"", int((time.monotonic() - t0) * 1000), f"{type(e).__name__}: {e}"


def _is_retryable(status, err) -> bool:
    """网络错误 / 超时 / 429 / 5xx 才值得重试；401/403/404 是确定性结果，重试无意义。"""
    if status is not None and status in _RETRY_STATUS:
        return True
    if status is not None:
        return False
    text = err or ""
    return any(k in text for k in ("timed out", "timeout", "Connection", "Chunked",
                                   "ProtocolError", "ConnectionError", "Timeout"))


def http_get(url: str, timeout: float, headers: dict = None,
             retries: int = 0, backoff: float = 1.0):
    """带指数退避重试的 GET。返回 (status, body_bytes, latency_ms, error, attempts)。"""
    attempts = 0
    while True:
        attempts += 1
        status, body, lat, err = _one_request(url, timeout, headers)
        # 统一错误文本（requests 分支不抛异常、只有 status），保证 classify_failure 可识别
        if status is not None and status >= 400 and not err:
            err = f"HTTP {status}"
        if attempts > retries or not _is_retryable(status, err):
            return status, body, lat, err, attempts
        time.sleep(backoff * (2 ** (attempts - 1)) + random.uniform(0, 0.3))

SCHEMA = """
CREATE TABLE IF NOT EXISTS stations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    domains TEXT NOT NULL,               -- JSON array
    api_base TEXT,                       -- 可选：自定义 API 基地址（如 https://api.xxx.com）
    plaza_path TEXT,                     -- 可选：model_plaza 模式的自定义接口路径
    cookie TEXT,                         -- 可选：登录态 Cookie（应对 requireAuth 的站）
    headers TEXT,                        -- 可选：额外请求头 JSON（如 {"HeyRoute-User":"23827"}）
    api_key TEXT,                        -- 可选：该站 API key（可用性探针用）
    mode TEXT DEFAULT 'newapi',          -- newapi | direct（直定价接口）| html_rsc（SSR 内嵌数据）
    notes TEXT DEFAULT '',
    enabled INTEGER DEFAULT 1,
    fail_streak INTEGER DEFAULT 0,       -- 连续失败次数（≥3 标记离线）
    offline INTEGER DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL,
    ts TEXT NOT NULL,                    -- ISO8601 UTC
    ok INTEGER NOT NULL,
    error TEXT,
    domain TEXT,
    pricing_version TEXT,
    exchange_rate REAL,                  -- 充值比例 price（¥ per $），None 表示未获取到
    usd_exchange_rate REAL,              -- 旧版遗留字段
    system_name TEXT,
    latency_ms INTEGER,
    page_prices TEXT,                   -- 价格页抓取的权威按组价 {model:{group:[in$,out$]}}，覆盖 ratio 推算
    raw_pricing BLOB,                   -- 整份定价响应 gzip 存档（raw_hash 未变时留 NULL 表示同上一条）
    raw_hash TEXT,                      -- 原始 JSON 的 sha256，用于跨快照去重与变更判定
    FOREIGN KEY(station_id) REFERENCES stations(id)
);
CREATE INDEX IF NOT EXISTS idx_snap_station ON snapshots(station_id, id);
CREATE TABLE IF NOT EXISTS model_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL,
    model_name TEXT NOT NULL,
    model_key TEXT NOT NULL,             -- 归一化名（小写去空白）
    model_canon TEXT,                    -- 规范名（剥前缀/日期/档位；保留厂商分隔符写法，用于显示）
    model_agg TEXT,                      -- 聚合键（抹平 . 与 - 差异，用于跨站聚合与查表）
    model_variant TEXT,                  -- 变体（thinking / high / low / medium / max）
    model_family TEXT,                   -- 家族名（厂商+系列，同系列聚合用）
    vendor_id INTEGER,
    quota_type INTEGER DEFAULT 0,        -- 0=按量 1=按次
    model_ratio REAL,
    completion_ratio REAL,
    cache_ratio REAL,
    create_cache_ratio REAL,
    model_price REAL,
    model_type TEXT,                     -- 站点标注的模型类型（视频/音视频/图像/对话…）
    model_tags TEXT,                     -- 站点标注的标签串（如 "异步,视频,首尾帧"）
    enable_groups TEXT,                  -- JSON array
    tiers TEXT,                          -- JSON array
    supported_endpoints TEXT,            -- JSON array
    billing_unit TEXT,                   -- 接口计费单位（token / per_second / per_call）
    video_resolution_price TEXT,         -- JSON {1080p: 0.12675, ...}，接口直定的按秒价（页面 404 的隐藏模型靠它）
    image_size_price TEXT,               -- JSON {1024x1024: 0.03, ...}，接口直定的「每张图×尺寸」价（image_size 计费）
    FOREIGN KEY(snapshot_id) REFERENCES snapshots(id)
);
CREATE INDEX IF NOT EXISTS idx_mp_snap ON model_prices(snapshot_id);
-- /api/history 与对比查询按「快照 + 模型」取数，没有这个复合索引时会全表扫
CREATE INDEX IF NOT EXISTS idx_mp_snap_agg ON model_prices(snapshot_id, model_agg);
CREATE INDEX IF NOT EXISTS idx_mp_key ON model_prices(model_key);
CREATE TABLE IF NOT EXISTS group_ratios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL,
    group_name TEXT NOT NULL,
    ratio REAL NOT NULL,
    UNIQUE(snapshot_id, group_name)
);
-- 站点自带的官方参考价（模型广场/自建价目里就有，如 sub2api 的 official_pricing、
-- top-one 的 officialPricingUsd）。优先级低于 manual / LiteLLM，仅用于补官方价缺口。
CREATE TABLE IF NOT EXISTS feed_official (
    model_key TEXT PRIMARY KEY,     -- 聚合键（agg_key），跨站合并同名模型
    input_usd_m REAL,
    output_usd_m REAL,
    source TEXT,                    -- 哪个站给的
    ts TEXT
);
-- 汇总行预计算缓存：build_summary_rows 要扫全库并折算价格，首请求约 2 秒。
-- 抓取后第一个请求（甚至换进程/重启后）直接读这里，不再卡一次。
CREATE TABLE IF NOT EXISTS summary_cache (
    ver TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    built_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS probes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL,                -- http（可达性） | api（真实调用）
    model TEXT,                        -- api 探针使用的模型
    ok INTEGER NOT NULL,
    status_code INTEGER,
    latency_ms INTEGER,
    ttft_ms INTEGER,                   -- 首 token 延迟（流式时）
    error TEXT,
    response_snippet TEXT,
    FOREIGN KEY(station_id) REFERENCES stations(id)
);
CREATE INDEX IF NOT EXISTS idx_probes_station ON probes(station_id, id);
CREATE TABLE IF NOT EXISTS model_availability (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL,
    model_agg TEXT NOT NULL,             -- 模型聚合键；空串表示站级检测失败
    model_name TEXT,
    ts TEXT NOT NULL,                    -- ISO8601 UTC
    status TEXT NOT NULL,                -- listed 在售 | gone 下架 | probe_ok 可调用 | probe_fail 不可调用 | unknown 检测失败
    detail TEXT,                         -- 说明（如「新上架」「已下架」/ 错误摘要）
    latency_ms INTEGER,
    FOREIGN KEY(station_id) REFERENCES stations(id)
);
CREATE INDEX IF NOT EXISTS idx_avail_sm ON model_availability(station_id, model_agg, id);
CREATE INDEX IF NOT EXISTS idx_avail_ts ON model_availability(ts);
CREATE TABLE IF NOT EXISTS price_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL,
    ts TEXT NOT NULL,
    model_canon TEXT NOT NULL,
    grp TEXT NOT NULL,
    old_input_usd_m REAL,
    new_input_usd_m REAL,
    pct REAL,                          -- 正=涨价，负=降价
    FOREIGN KEY(station_id) REFERENCES stations(id)
);
CREATE INDEX IF NOT EXISTS idx_pc_ts ON price_changes(ts);
-- My API Box 插件上报的用户侧探针/价格快照（脱敏 payload，key 绝不出插件本地）。
-- 与服务端自采数据分开存：来源不同，可信级别不同，展示时必须能区分。
CREATE TABLE IF NOT EXISTS user_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,                -- probe（七探针报告） | prices（价格快照）
    station_id INTEGER,                -- 按 origin 域名匹配到的在册站点；未匹配为 NULL
    origin TEXT NOT NULL,              -- 上报的站点 URL 原文
    origin_host TEXT NOT NULL,         -- 归一化 host，未匹配站点的聚合键
    ext_version TEXT,                  -- 插件版本
    body_hash TEXT UNIQUE NOT NULL,    -- 原始 body 哈希：重复 POST（重试）直接去重
    payload TEXT NOT NULL,             -- 完整 JSON
    client_ip TEXT,
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ur_station ON user_reports(station_id, received_at);
-- api-bench 性能实测（首字延迟/缓存命中/滚动缓存）。真实请求、消耗额度，
-- 因此只手动触发不进 watch 自动轮；与服务端探活/插件上报一样独立分层。
CREATE TABLE IF NOT EXISTS bench_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL,
    model_agg TEXT NOT NULL,           -- 被测模型聚合键
    model_raw TEXT,                    -- 被测模型原始名
    protocol TEXT NOT NULL,            -- messages（Claude /v1/messages） | responses（OpenAI /v1/responses）
    ts TEXT NOT NULL,
    ok INTEGER NOT NULL,
    requests INTEGER, fails INTEGER,
    ttft_p50_ms REAL, ttft_p90_ms REAL, ttft_p99_ms REAL,
    tpot_ms REAL,                      -- 每 token 生成耗时（可见文本口径）
    decode_tps REAL, req_per_s REAL,
    cache_hit_pct REAL,                -- 请求口径命中率（cache_read>0 占比，剔冷启动）
    cache_token_pct REAL,              -- token 口径命中率（Σcache_read/Σinput）
    roll_capture_pct REAL,             -- 滚动缓存：命中区捕获率
    roll_saved_pct REAL,               -- 滚动缓存：累计省下的 input 费用
    errors TEXT,                       -- JSON：错误分类计数
    payload TEXT                       -- 完整 --json 结果（证据回溯用）
);
CREATE INDEX IF NOT EXISTS idx_bench_station ON bench_results(station_id, ts);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""

CSV_HEADERS = [
    "站点", "域名", "数据时间(UTC)", "模型规范名", "原始模型名", "模型变体", "模型家族", "公司",
    "计费方式", "模型类型", "核心模型", "厂商ID",
    "分组", "分组倍率", "模型在该分组可用", "价格来源",
    "输入倍率model_ratio", "输出倍率completion_ratio", "缓存读倍率cache_ratio", "缓存写倍率create_cache_ratio",
    "按次价格model_price($/次)", "计价倍率", "分辨率",
    "按次价格($/次)", "按次价格(¥/次)", "按秒价格($/秒)", "按秒价格(¥/秒)", "阶梯计价tiers",
    "标价输入($/M)", "标价输出($/M)", "标价缓存读($/M)", "标价缓存写($/M)",
    "实付缓存读($/M)", "实付缓存写($/M)", "实付缓存读(¥/M)", "实付缓存写(¥/M)",
    "实付输入($/M)", "实付输出($/M)", "实付输入(¥/M)", "实付输出(¥/M)",
    "官方输入(¥/M)", "官方输出(¥/M)", "官方来源", "较官方价(%)", "对比官方价(%)",
    "样例请求成本($)", "阶梯计价明细",
    "充值比例(¥/$)", "充值比例来源", "支持接口", "抓取延迟(ms)", "站点system_name",
    "TTFT p50(ms)", "TTFT p90(ms)", "缓存命中%", "实测时间",
    "最近12次状态",
]

# 样例请求：默认 100K 输入 + 10K 输出（可用 --ref-in/--ref-out 调整）
REF_TOKENS_IN = 100_000
REF_TOKENS_OUT = 10_000


# ---------------------------------------------------------------- 基础工具

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_model(name) -> str:
    return " ".join(str(name).strip().lower().split())


# ============================== 归一化 ==============================
# 目标：把各站五花八门的写法收敛到同一"规范名"，并把推理档位/思考模式单独记为"变体"，
# 这样既能在规范名上跨站聚合（站数、最低价），又不会丢失档位差异（价格确实不同）。

CANON_ALIASES_PATH = os.environ.get("RELAY_CANON_ALIASES", "canon_aliases.json")

_PREFIX_RE = re.compile(r"^(?:[a-z0-9_-]+/)+")                      # openai/xxx、Pro/BAAI/xxx
# 日期：结尾或中间都剥（如 claude-haiku-4-5-20251001-thinking）
_DATE_RE = re.compile(r"[-_](?:19|20)\d{6}(?=[-_.]|$)|[-_](?:19|20)\d{2}-\d{1,2}-\d{1,2}(?=[-_.]|$)")
# 快照日期的短写形式：YYMMDD(250615)、YYMM(2509)、MMDD(0731/0813/0806)。剥序必须长先短后，
# 否则 260215 会被 MMDD 剥剩 2602；月份/日期做了合法性校验避免误伤版本号
_YYMMDD_RE = re.compile(r"[-_](?:1[6-9]|2[0-9])\d{4}(?=[-_.]|$)")
_YYMM_RE = re.compile(r"[-_](?:1[6-9]|2[0-9])(?:0[1-9]|1[0-2])(?=[-_.]|$)")
_MMDD_RE = re.compile(r"[-_](?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])(?=[-_.]|$)")
# 推理档位/思考模式 → 记为变体（而非独立模型）
# 注意 max 有歧义：qwen-max 是型号档位，claude-opus-4-7-max 才是推理档位，
# 因此 max 仅在前面紧跟纯版本号时（…4-7-max）才当作变体剥离。
_VARIANT_RE = re.compile(r"[-_](thinking|high|low|medium|xhigh|minimal|none)$", re.I)
_MAX_VARIANT_RE = re.compile(r"[-_](\d+(?:[.-]\d+)*)[-_]max$", re.I)
# 纯噪声后缀
_NOISE_RE = re.compile(r"[-_](?:latest|preview|exp|experimental|beta|alpha|canary)$", re.I)
# 冒号/点号后的渠道标记： :free :thinking（其中 :thinking 等档位需记为变体）
_COLON_RE = re.compile(r":[a-z0-9_-]+$", re.I)
_COLON_VARIANT_RE = re.compile(r":(thinking|high|low|medium|xhigh|minimal|none)$", re.I)
# 聚合键：把分隔符差异抹平（gpt-5.6-sol 与 gpt-5-6-sol 视为同一模型）
_DOTVER_RE = re.compile(r"(?<=[a-z])-(\d+)\.(\d+)(?=[-_.]|$)")


def load_canon_aliases() -> dict:
    """规范名别名表：把站方自定义写法映射到统一规范名（如 deepseek-chat -> deepseek-v3）。"""
    if not os.path.exists(CANON_ALIASES_PATH):
        return DEFAULT_CANON_ALIASES
    try:
        with open(CANON_ALIASES_PATH, encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return DEFAULT_CANON_ALIASES


DEFAULT_CANON_ALIASES = {
    "_comment": "键=站方原始/归一化写法，值=要统一到的规范名；可自行增删",
    "deepseek-chat": "deepseek-v3",
    "deepseek-v3-1": "deepseek-v3.1",
    "gpt-41": "gpt-4.1",
    "gpt-4o-0806": "gpt-4o",
    "claude-35-sonnet": "claude-3-5-sonnet",
    "claude-37-sonnet": "claude-3-7-sonnet",
}


def split_canonical(name) -> tuple:
    """→ (规范名, 变体)。变体如 thinking / high / low / medium / max。"""
    s = normalize_model(name)
    variant = None
    # 1) 剥供应商前缀、冒号标记、下划线统一为连字符
    s = _PREFIX_RE.sub("", s)
    m = _COLON_VARIANT_RE.search(s)            # claude-xxx:thinking → 变体 thinking
    if m:
        variant = m.group(1).lower()
        s = s[:m.start()]
    s = _COLON_RE.sub("", s)
    s = s.replace("_", "-").replace(" ", "-")
    s = re.sub(r"-+", "-", s).strip("-")
    # 2) 循环剥离：档位(记为变体) → 噪声 → 日期，直到不再变化
    for _ in range(4):
        m = _MAX_VARIANT_RE.search(s)          # …4-7-max → 保留版本号，max 记为变体
        if m:
            variant = "max"
            s = s[:m.start()] + "-" + m.group(1)
            continue
        m = _VARIANT_RE.search(s)
        if m:
            variant = m.group(1).lower()
            s = s[:m.start()]
            continue
        s2 = _NOISE_RE.sub("", s)
        s2 = _DATE_RE.sub("", s2)          # YYYYMMDD / YYYY-MM-DD
        s2 = _YYMMDD_RE.sub("", s2)        # YYMMDD（先于 MMDD，防 260215→2602）
        s2 = _YYMM_RE.sub("", s2)          # YYMM
        s2 = _MMDD_RE.sub("", s2)          # MMDD（0731/0813/0806…）
        s2 = re.sub(r"-+", "-", s2).strip("-")
        if s2 == s:
            break
        s = s2
    if not s:
        s = normalize_model(name)
    # 别名表（用户可扩展，如 deepseek-chat -> deepseek-v3）
    aliases = load_canon_aliases()
    target = aliases.get(s) or aliases.get(agg_key(s))
    if isinstance(target, str) and target:
        s = target
    return s, variant


@lru_cache(maxsize=None)
def agg_key(canon: str) -> str:
    """聚合键：规范名再抹平分隔符差异（点号版本号 → 连字符）。
    显示用 model_canon（保留厂商写法），跨站聚合/查表用 model_agg。"""
    s = _DOTVER_RE.sub(r"-\1-\2", canon or "")
    s = s.replace(".", "-").replace("_", "-")
    return re.sub(r"-+", "-", s).strip("-") or (canon or "")


@lru_cache(maxsize=None)
def canonical_model(name) -> str:
    """规范名：跨站聚合同一模型用（含日期版本、档位、噪声后缀均已剥离）。"""
    return split_canonical(name)[0]


def model_variant(name):
    """变体：thinking / high / low / medium / max 等推理档位。"""
    return split_canonical(name)[1]


def model_family(canon: str) -> str:
    """家族名：厂商 + 系列（claude-sonnet / gpt-6 / gemini-3.8 / deepseek-v4），
    去掉尾部型号细分，用于同系列聚合。"""
    parts = [p for p in re.split(r"[-_.:]", canon) if p]
    if not parts:
        return canon
    if len(parts) == 1:
        return parts[0]
    # 第二段是纯数字/版本号（gpt-6-astra -> gpt-6；claude-sonnet-5-5 -> claude-sonnet）
    if re.fullmatch(r"\d+(?:-\d+)*", parts[1]):
        return f"{parts[0]}-{parts[1]}"
    return f"{parts[0]}-{parts[1]}"


def to_float(v):
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def jdump(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# 顺序敏感的数组（顺序本身影响语义/展示），做规范化时保持原序
_ORDER_SENSITIVE = {"tiers", "request_rules", "conditions", "examples", "layout",
                    "ratios", "seconds", "resolutions", "sizes", "fields"}


def _canonical(obj, key=None):
    """把 JSON 变成「内容等价」的规范形：对象按键、集合型数组按值排序。
    站点每次返回的 enable_groups / vendors / supported_endpoint_types 顺序都会变，
    不规范化的话 raw_hash 每次都不同，去重就失效。"""
    if isinstance(obj, dict):
        return {k: _canonical(v, k) for k, v in obj.items()}
    if isinstance(obj, list):
        vals = [_canonical(v, key) for v in obj]
        if key in _ORDER_SENSITIVE:
            return vals
        try:
            return sorted(vals, key=lambda x: jdump(x))
        except (TypeError, ValueError):
            return vals
    return obj


def pricing_content_hash(pricing):
    """语义级内容指纹：忽略数组顺序、忽略模型条目上重复的 pricing_version
    （顶层的同名版本号已单独落在 snapshots.pricing_version）。"""
    import hashlib
    try:
        if isinstance(pricing, dict) and isinstance(pricing.get("data"), list):
            core = {k: v for k, v in pricing.items() if k != "data"}
            core["data"] = [{k: v for k, v in m.items() if k != "pricing_version"}
                            if isinstance(m, dict) else m for m in pricing["data"]]
        else:
            core = pricing
        return hashlib.sha256(jdump(_canonical(core)).encode("utf-8")).hexdigest()
    except (TypeError, ValueError, AttributeError):
        return None


def pack_raw_pricing(pricing):
    """整份定价响应 → (gzip BLOB, 内容指纹)。只存原始观测值，派生列可随时重算；
    站点字段只增不减，历史 raw 不落库就永久不可回补（换规则只能重抓）。"""
    import gzip
    try:
        raw = jdump(pricing).encode("utf-8")
    except (TypeError, ValueError):
        return None, None
    return gzip.compress(raw, 6), pricing_content_hash(pricing)


def load_raw_pricing(conn, snapshot_id: int):
    """读回某快照的原始定价（当前条 raw_pricing 为 NULL 时沿同站上一条继承）。
    返回 dict；无存档返回 None。"""
    import gzip
    row = conn.execute("SELECT station_id, id, raw_pricing, raw_hash FROM snapshots "
                       "WHERE id=?", (snapshot_id,)).fetchone()
    if not row:
        return None
    cur = row
    while cur is not None and not cur["raw_pricing"] and cur["raw_hash"]:
        cur = conn.execute("SELECT id, raw_pricing, raw_hash FROM snapshots "
                           "WHERE station_id=? AND id<? AND raw_pricing IS NOT NULL "
                           "ORDER BY id DESC LIMIT 1",
                           (row["station_id"], cur["id"])).fetchone()
    if cur is None or not cur["raw_pricing"]:
        return None
    try:
        return json.loads(gzip.decompress(cur["raw_pricing"]).decode("utf-8"))
    except Exception:
        return None


def _migrate(conn):
    """轻量迁移：为已存在的表补齐新增列（升级工具无需删库）。"""
    def cols(table):
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    for name, ddl in (("api_base", "TEXT"), ("cookie", "TEXT"), ("api_key", "TEXT"),
                      ("headers", "TEXT"), ("last_check", "TEXT"), ("plaza_path", "TEXT"),
                      ("mode", "TEXT DEFAULT 'newapi'"),
                      ("recharge_rate", "REAL"),
                      ("page_prices", "INTEGER DEFAULT 0"),
                      ("fail_streak", "INTEGER DEFAULT 0"), ("offline", "INTEGER DEFAULT 0"),
                      ("state", "TEXT DEFAULT 'active'")):
        if name not in cols("stations"):
            conn.execute(f"ALTER TABLE stations ADD COLUMN {name} {ddl}")
    for name in ("model_canon", "model_agg", "model_variant", "model_family",
                 "model_type", "model_tags"):
        if name not in cols("model_prices"):
            conn.execute(f"ALTER TABLE model_prices ADD COLUMN {name} TEXT")
    for name, ddl in (("page_prices", "TEXT"), ("raw_pricing", "BLOB"),
                      ("raw_hash", "TEXT")):
        if name not in cols("snapshots"):
            conn.execute(f"ALTER TABLE snapshots ADD COLUMN {name} {ddl}")
    for name, ddl in (("billing_unit", "TEXT"), ("video_resolution_price", "TEXT"),
                      ("image_size_price", "TEXT")):
        if name not in cols("model_prices"):
            conn.execute(f"ALTER TABLE model_prices ADD COLUMN {name} {ddl}")
    # 回填历史数据的规范名/家族名（索引必须在补列之后创建）
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mp_canon ON model_prices(model_canon)")
    pending = conn.execute(
        "SELECT id, model_name FROM model_prices WHERE model_canon IS NULL").fetchall()
    if pending:
        conn.executemany(
            "UPDATE model_prices SET model_canon=?, model_family=? WHERE id=?",
            [(canonical_model(r["model_name"]), model_family(canonical_model(r["model_name"])), r["id"])
             for r in pending])
    # 家族名规则随版本演进：仅在 schema 版本变化时重算一次（避免每次请求全表 UPDATE）
    cur = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    cur_v = int(cur["value"]) if cur and (cur["value"] or "").isdigit() else 0
    if cur_v < SCHEMA_VERSION:
        rows = conn.execute("SELECT id, model_name FROM model_prices").fetchall()
        if rows:
            data = []
            for r in rows:
                canon, variant = split_canonical(r["model_name"])
                data.append((canon, agg_key(canon), variant, model_family(canon), r["id"]))
            conn.executemany(
                "UPDATE model_prices SET model_canon=?, model_agg=?, model_variant=?,"
                " model_family=? WHERE id=?", data)
        conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (str(SCHEMA_VERSION),))
    conn.commit()


def connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # watch（写）与 serve（读）是两个进程：给锁等待留时间，避免 database is locked
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    _migrate(conn)
    return conn


def classify_failure(errors) -> str:
    """失败原因分类：决定状态与是否值得重试。
    auth_required=站方关闭公开定价(需 Cookie) / unsupported=无该路由 / offline=域名或网络问题"""
    text = " | ".join(errors or [])
    if "HTTP 401" in text or "HTTP 403" in text:
        return "auth_required"
    if "HTTP 404" in text:
        return "unsupported"
    if any(k in text for k in ("getaddrinfo", "Name or service not known", "nodename", "NameResolutionError")):
        return "offline"
    if "timed out" in text.lower() or "timeout" in text.lower() or "Connection aborted" in text:
        return "flaky"
    return "error"


def load_official_prices() -> tuple:
    """官方价基准表：(prices, aliases)，用于计算较官方价折扣。
    表的键会用与站方数据同一套归一化规则处理，因此 `gemini-3.8-flash` 与
    `gemini-3-8-flash` 这类分隔符差异不会导致匹配失败。"""
    if not os.path.exists(OFFICIAL_PATH):
        return {}, {}
    try:
        with open(OFFICIAL_PATH, encoding="utf-8") as f:
            d = json.load(f) or {}
        raw = d.get("prices") or {}
        # 先按规范键分组：同 canon 碰撞时（如 deepseek-v4-pro 与残留的
        # deepseek-v4-pro-0813 归一化后同键）取来源优先级更高的条目，
        # 避免过期快照键覆盖国内 ¥ 档
        by_canon = {}
        for k, v in raw.items():
            if k.startswith("_"):
                continue
            ck = canonical_model(k)
            cur = by_canon.get(ck)
            if cur is None or _base_rank(v) < _base_rank(cur):
                by_canon[ck] = v
        prices = {}
        for k, v in raw.items():
            if k.startswith("_"):
                continue
            ck = canonical_model(k)
            prices[ck] = by_canon[ck]
            prices.setdefault(k, by_canon[ck])   # 兼容未归一化的精确查表
        aliases = d.get("aliases") or {}
        aliases = {canonical_model(k): canonical_model(v)
                   for k, v in aliases.items() if not k.startswith("_")}
        return prices, aliases
    except Exception:
        return {}, {}


def load_feed_official(conn) -> dict:
    """站点自带官方价（feed_official）→ {聚合键: {input, output, source}}。

    仅当 official_prices.json / LiteLLM 都没有该模型时作为补位使用，
    所以把它单独放在一张表里、在查询侧做兜底，不污染官方价基准文件。
    """
    out = {}
    try:
        for r in conn.execute(
                "SELECT model_key, input_usd_m, output_usd_m, source FROM feed_official"):
            if r["input_usd_m"]:
                out[r["model_key"]] = {"input": r["input_usd_m"],
                                       "output": r["output_usd_m"],
                                       "source": "feed",
                                       "feed_source": r["source"]}
    except sqlite3.Error:
        pass
    return out


def lookup_official(official, canon: str):
    """按聚合键匹配（抹平 . 与 - 差异）+ 显式别名；不做前缀猜测，
    避免 gpt-5.4-nano 误用 gpt-5 基准价。"""
    prices, aliases = official if isinstance(official, tuple) else (official, {})
    if not canon or not prices:
        return None
    key = agg_key(canon)
    for k, v in prices.items():
        if k == canon or agg_key(k) == key:
            return v
    alias = aliases.get(canon) or aliases.get(key)
    if alias:
        for k, v in prices.items():
            if k == alias or agg_key(k) == agg_key(alias):
                return v
    return None


# ----------------------------------------------------- 官方价权威源（LiteLLM）
# 官方价渠道以 LiteLLM 社区定价库为权威源（被数千生产应用用于成本核算），
# 辅以"已与官方页人工核验"的 manual 条目；getcheapai 等第三方仅作可选二级补全。
LITELLM_URL = ("https://raw.githubusercontent.com/BerriAI/litellm/main/"
               "model_prices_and_context_window.json")
LITELLM_CACHE = os.environ.get("RELAY_LITELLM", "litellm_prices.json")
GROUND_TRUTH_PATH = os.environ.get("RELAY_GROUND_TRUTH", "official_ground_truth.json")
# 市场汇率（¥/$）：官方 USD 基准折人民币、以及站点未公布充值比例时估算真实成本用。
# 不能拿站点自己的「充值比例」当汇率——那常是营销定价（实测 0.3~6.75），不是汇率。
MARKET_FX = float(os.environ.get("RELAY_MARKET_FX", "7.2"))
_LITELLM_PROVIDERS = ("openai", "anthropic", "gemini", "google", "deepseek", "xai",
                      "azure", "azure_ai", "mistral", "mistralai", "qwen", "moonshot",
                      "zhipu", "vertex_ai", "bedrock", "bedrock_mantle", "databricks",
                      "groq", "cohere", "together", "fireworks_ai", "novita",
                      "global", "us", "eu", "ai21", "perplexity")


def load_litellm(path=LITELLM_CACHE):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_official(meta: dict, prices: dict, aliases: dict):
    """原子写官方价文件（临时文件 + os.replace），避免中途异常留下 0 字节损坏文件。"""
    import tempfile
    d = os.path.dirname(os.path.abspath(OFFICIAL_PATH)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".official_", suffix=".tmp", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"_meta": meta, "prices": prices, "aliases": aliases},
                      f, ensure_ascii=False, indent=2)
        os.replace(tmp, OFFICIAL_PATH)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


_MODEL_START_RE = re.compile(
    r"^(?:gpt|o[1-9]|claude|gemini|grok|deepseek|glm|kimi|qwen|minimax|mistral|llama"
    r"|command|embed|whisper|dall|flux|jamba|nova|phi|gemma|ernie|hunyuan|doubao"
    r"|seed|pixtral|codestral|voyage|rerank|baichuan|abab|pangu|falcon|gemma)", re.I)

# 模型家族 → 拥有该家族的厂商 provider（第一方与否取决于模型厂商，非 provider 本身：
# dashscope 对 qwen 是自营、对 glm/deepseek 是第三方托管）
_VENDOR_BY_FAMILY = {
    "gpt": {"openai"}, "o1": {"openai"}, "o2": {"openai"}, "o3": {"openai"},
    "o4": {"openai"}, "o5": {"openai"}, "codex": {"openai"},
    "claude": {"anthropic"},
    "gemini": {"gemini", "google"},
    "grok": {"xai"},
    "deepseek": {"deepseek"},
    "glm": {"zai", "zhipu", "bigmodel"},
    "kimi": {"moonshot"}, "moonshot": {"moonshot"},
    "qwen": {"qwen", "dashscope", "qwencloud", "qwen_ai_platform", "alibaba"},
    "minimax": {"minimax"}, "abab": {"minimax"},
    "mistral": {"mistral", "mistralai"},
    "command": {"cohere"}, "jamba": {"ai21"},
    "ernie": {"baidu"}, "hunyuan": {"tencent"}, "doubao": {"doubao", "volcengine"},
}


def _first_party_score(k):
    """裸名冲突时的选择优先级：0 裸名 > 1 模型厂商自营 > 2 github_copilot > 3 转售商。
    先按 / 分段（不能按 . 切，否则版本号 glm-5.3 会被切碎），再处理点前缀。"""
    bare, provider = k, ""
    if "/" in k:
        provider, bare = k.split("/", 1)
    else:
        first = k.split(".", 1)[0]
        if not _MODEL_START_RE.match(first):
            toks = k.split(".")
            provider = toks[0].lower()
            # 区域点前缀 us./eu./global. → 取真正的 provider 段
            if provider in ("us", "eu", "global", "apac", "uk") and len(toks) > 1:
                provider = toks[1].lower()
    provider = provider.lower()
    if not provider:
        return 0                                  # 裸名（无 provider 前缀）
    fam = re.split(r"[-_.]", bare, 1)[0].lower()
    providers = _VENDOR_BY_FAMILY.get(fam, {fam})
    if provider in providers:
        return 1
    if provider == "github_copilot":
        return 2
    return 3


def _litellm_index(L):
    """建索引：agg_key(裸名 / 带 provider / 剥预览后缀) ->
    {input, output, cache_read, cache_write, litellm_key, mode, first_party}。
    同一聚合键有多个 provider 条目时，优先厂商第一方（避免 azure/novita 转售价
    被当成官方价，如 glm-5.3 曾误取 coralbricks 1.12 而非 zai 1.40）。
    first_party=False 表示该聚合键在 LiteLLM 里只有转售商报价（无厂商自营条目）。"""
    idx = {}
    _strip = re.compile(r"[-_](?:preview|beta|alpha|exp|experimental)$")
    _date = re.compile(r"[-_](?:19|20)\d{6}$|[-_]\d{4}-\d{1,2}-\d{1,2}$")

    def per(v):
        return round(v * 1e6, 6) if isinstance(v, (int, float)) else None

    def register(a, rec):
        if not a:
            return
        cur = idx.get(a)
        if cur is None:
            idx[a] = rec
            return
        new_fp = 0 if rec["first_party"] else 1     # 第一方优先
        cur_fp = 0 if cur["first_party"] else 1
        if new_fp < cur_fp or (new_fp == cur_fp and rec["_score"] < cur["_score"]):
            idx[a] = rec

    for k, v in L.items():
        if k.startswith("_") or not isinstance(v, dict):
            continue
        inp = v.get("input_cost_per_token")
        outp = v.get("output_cost_per_token")
        if inp is None or outp is None:
            continue
        rec = {"input": per(inp), "output": per(outp),
               "cache_read": per(v.get("cache_read_input_token_cost")),
               "cache_write": per(v.get("cache_creation_input_token_cost")),
               "litellm_key": k, "mode": v.get("mode", "chat"),
               "_score": _first_party_score(k),
               "first_party": _first_party_score(k) <= 2}
        for form in (k, k.split("/")[-1]):
            a = agg_key(form)
            register(a, rec)
            register(agg_key(_strip.sub("", a)), rec)     # 剥 -preview
            register(agg_key(_date.sub("", a)), rec)      # 剥日期
        register(agg_key(_strip.sub("", _date.sub("", agg_key(k)))), rec)
    for rec in idx.values():
        rec.pop("_score", None)                  # first_party 保留供溯源/过滤
    return idx


def _family_vendors(canon: str):
    """按模型家族推导厂商 provider 列表（用于兜底匹配时优先厂商渠道）。"""
    fam = re.split(r"[-_.]", canon, 1)[0].lower()
    if fam.startswith("o") and len(fam) == 2 and fam[1].isdigit():
        fam = "gpt"
    return _VENDOR_BY_FAMILY.get(fam, {fam})


def match_litellm(canon, idx):
    """在我们的规范名与 LiteLLM 之间建立映射（抹平分隔符/provider/日期/预览后缀）。
    兜底按 provider 拼接匹配时，先试模型所属厂商的 provider，再 github_copilot，
    最后才是其它渠道——避免基准退化成转售商报价。"""
    key = agg_key(canon)
    if key in idx:
        return idx[key]
    # 剥日期
    stripped = re.sub(r"[-_](?:19|20)\d{6}$|[-_]\d{4}-\d{1,2}-\d{1,2}$", "", key)
    if stripped and stripped in idx:
        return idx[stripped]
    # 剥 -preview/-beta/-exp 等（LiteLLM 常带 gemini-3-flash-preview）
    no_prev = re.sub(r"[-_](?:preview|beta|alpha|exp|experimental)$", "", stripped or key)
    if no_prev and no_prev in idx:
        return idx[no_prev]
    ordered = list(_family_vendors(canon)) + ["github_copilot"] + \
        [p for p in _LITELLM_PROVIDERS
         if p not in _family_vendors(canon) and p != "github_copilot"]
    for p in ordered:
        pk = agg_key(f"{p}/{canon}")
        if pk in idx:
            return idx[pk]
    return None


def db_model_aggs(conn):
    """数据库里出现过的所有聚合规范名。"""
    return {r[0] for r in conn.execute("SELECT DISTINCT model_agg FROM model_prices"
                                      " WHERE model_agg IS NOT NULL")}


def rebuild_official_from_litellm(conn, keep_sources=("manual",), official_only=False):
    """以 LiteLLM 为权威源重建官方价；keep_sources 指定保留哪些非 litellm 来源。
    official_only=True 时仅接受第一方条目（裸名/厂商自营/github_copilot），
    LiteLLM 里只有转售商报价的模型宁可无基准也不拿转售价充官方价。"""
    L = load_litellm()
    idx = _litellm_index(L)
    targets = db_model_aggs(conn)
    prices = {}
    for canon in targets:
        m = match_litellm(canon, idx)
        if m:
            if official_only and not m.get("first_party", True):
                continue                      # 仅官方模式：跳过只有转售商报价的条目
            rec = dict(m)
            rec["source"] = "litellm"
            prices[canon] = rec
    # 保留 manual 来源且 LiteLLM 未覆盖的条目（已人工核验）
    cur, _ = load_official_prices()
    covered = {agg_key(x) for x in prices}
    for k, v in cur.items():
        if v.get("source") in keep_sources and agg_key(k) not in covered:
            prices[k] = v
    return prices, idx


# ----------------------------------------------------- 国内官方/平台价（¥ 计价）
# 官方价基准补充渠道：国内厂商官网与推理平台（硅基流动等）为 ¥/1M 计价，
# 抓取后以 currency=cny 入库，折扣与核验自动按真实人民币口径换算。
SILICONFLOW_URL = "https://siliconflow.cn/pricing"
MOONSHOT_PRICING_URL = "https://platform.moonshot.cn/docs/pricing/chat"
OPENROUTER_URL = "https://openrouter.ai/api/v1/models"
BIGMODEL_URL = "https://docs.bigmodel.cn/cn/guide/start/pricing"
DEEPSEEK_URL = "https://api-docs.deepseek.com/zh-cn/quick_start/pricing"
# 千问（通义）官方定价文档页：服务端渲染的价格表（元/百万 Token）。
# 用户指定千问官方价以此页为准（/pricing/api 是 lowcode SPA + 需登录网关，无法匿名抓取）
QWEN_PRICES_URL = "https://platform.qianwenai.com/docs/developer-guides/getting-started/model-prices"
# ¥ 列（国内基准）来源优先级：人工核验最高；千问平台官方定价文档页为国产大模型首要基准
# （用户指定「国产大模型以该页为主」，表中含 qwen/glm/deepseek/kimi/MiniMax 等全系列
# 国产模型的平台价格）；BigModel（GLM 官网）、DeepSeek 官网为该页未收录模型补充；
# 硅基流动/百炼 次之；厂商官网（Moonshot 等）再次；国际档兜底。
# Qwen 系列：用户指定以千问官方定价文档页为准（source=qwen）；硅基流动覆盖不到的模型
# 继续沿用 OpenRouter/litellm 的 usd 档。
# $ 列（国际参考）固定优先 OpenRouter。
# 用 0.4/0.5 将 qwen/bigmodel/deepseek 严格置于 manual(0) 与 siliconflow(1) 之间：
# 各自系列优先厂商官方，但仍可被 official_ground_truth.json 的人工核验覆盖。
_SOURCE_PRIO = {"manual": 0, "qwen": 0.4, "bigmodel": 0.5, "deepseek": 0.5,
                "bailian": 1, "siliconflow": 1, "platform_cn": 1, "vendor_cn": 2,
                "moonshot": 2, "litellm": 3, "getcheapai": 4}


def _base_rank(v):
    """基准条目排序键：(货币口径, 来源优先级)，越小越优先。
    cny 国内实价整体优先于 usd 档（¥ 列以国内渠道为准），同口径内按来源。"""
    return (0 if (v or {}).get("currency") == "cny" else 1,
            _SOURCE_PRIO.get((v or {}).get("source"), 9))


# 系列化定源顺序（用户要求：GLM / DeepSeek 仍以各自厂商官网为准，分档模型取空闲/最低档）：
#   GLM      → BigModel 官网 > 千问价格页 > 百炼 > 硅基流动
#   DeepSeek → DeepSeek 官网（空闲时段档）> 硅基流动 > 千问价格页
#   Kimi     → Moonshot 官网 > 千问价格页 > 百炼 > 硅基流动
#   其余国产 → 千问价格页 > 百炼 > 硅基流动 > 厂商官网；人工核验(manual) 永远最高
# 百炼（bailian）是阿里云官方售卖平台、免登录可自动抓，价格最全但排在各自
# 厂商官网之后——官网是「厂商直销」，百炼是「云平台代售」，口径以官网为先。
_SRC_ORDER_DEFAULT = ["manual", "qwen", "bigmodel", "deepseek", "bailian",
                      "siliconflow", "moonshot", "vendor_cn", "litellm",
                      "getcheapai"]
_SRC_ORDER_FAMILY = {
    "glm": ["manual", "bigmodel", "qwen", "bailian", "siliconflow", "moonshot",
            "litellm", "getcheapai"],
    "deepseek": ["manual", "deepseek", "siliconflow", "qwen", "bailian",
                 "moonshot", "litellm", "getcheapai"],
    "kimi": ["manual", "moonshot", "qwen", "bailian", "siliconflow", "litellm",
             "getcheapai"],
}
# 「官方来源」列展示用的中文名
_SOURCE_LABEL = {
    "manual": "人工核验", "qwen": "千问官网", "bigmodel": "GLM官网",
    "deepseek": "DeepSeek官网", "siliconflow": "硅基流动", "moonshot": "Moonshot官网",
    "platform_cn": "国内平台", "bailian": "百炼", "vendor_cn": "厂商官网",
    "litellm": "litellm", "getcheapai": "getcheapai", "openrouter": "OpenRouter",
    "feed": "站点官方价",
}


def _src_order(key: str) -> list:
    """按模型系列取来源优先顺序。"""
    k = (key or "").lower()
    for fam, order in _SRC_ORDER_FAMILY.items():
        if k.startswith(fam):
            return order
    return _SRC_ORDER_DEFAULT


def _rank_for(key: str, v: dict) -> tuple:
    """(货币口径, 系列内来源序位)：越小越优先。cny 国内实价整体优先于 usd 档。"""
    order = _src_order(key)
    src = (v or {}).get("source")
    pos = order.index(src) if src in order else len(order) + _SOURCE_PRIO.get(src, 9)
    return (0 if (v or {}).get("currency") == "cny" else 1, pos)


def _cny_num(x):
    """字符串安全转数值（to_float 只收数字类型，正则捕获需此转换）。"""
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None


def _http_text(url: str, timeout: float = 20, attempts: int = 2):
    """带浏览器 UA 的 GET（urllib，适用于服务端渲染的价格页）→ 文本或 None。
    Cloudflare 挑战页可能返回 200 但无正文数据，调用方解析为空时由重试兜底。"""
    import ssl as _ssl
    import time as _t
    for i in range(max(1, attempts)):
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=timeout,
                                        context=_ssl.create_default_context()) as r:
                return r.read().decode("utf-8", "replace")
        except Exception as e:
            print(f"  [抓取失败{i+1}] {url} {type(e).__name__}: {str(e)[:80]}")
            if i + 1 < attempts:
                _t.sleep(1.5)
    return None


def http_post_form(url: str, form: dict, timeout: float = 30,
                   extra_headers: dict = None, attempts: int = 2):
    """POST application/x-www-form-urlencoded → body bytes 或 None。

    阿里云控制台内部网关（百炼模型市场）要求这种编码，且必须带浏览器 UA 与
    Referer/Origin，否则会被挡回 SPA 的 index.html（200 但非 JSON）。
    """
    import time as _t
    data = urllib.parse.urlencode(form).encode("utf-8")
    headers = {"User-Agent": UA, "Accept": "application/json",
               "Content-Type": "application/x-www-form-urlencoded"}
    if extra_headers:
        headers.update(extra_headers)
    for i in range(max(1, attempts)):
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            print(f"  [POST 失败{i+1}] {url[:70]} {type(e).__name__}: {str(e)[:70]}")
            if i + 1 < attempts:
                _t.sleep(1.5)
    return None


def fetch_siliconflow_cn(timeout: float = 20) -> dict:
    """硅基流动定价页（服务端渲染）→ {聚合键: cny 价格}。取标准档（跳过 Pro/ 专用实例）。
    价格顺序 [输入, 输出, 缓存/分时段...]，取前两个。"""
    html = _http_text(SILICONFLOW_URL, timeout)
    if not html:
        return {}
    out = {}
    blocks = re.split(r'id="pricing-row-[^"]*"', html)[1:]
    for blk in blocks:
        mid = None
        for cand in re.findall(r"[a-z][a-z0-9-]*/[A-Z][A-Za-z0-9._-]+", blk[:1500]):
            if not cand.startswith("Pro/"):
                mid = cand
                break
        if not mid:
            continue
        prices = [_cny_num(x) for x in re.findall(r"[¥￥]\s*([\d.]+)", blk[:3000])]
        prices = [p for p in prices if p]
        if len(prices) < 2:
            continue
        canon = canonical_model(mid)
        key = agg_key(canon)
        if key:
            out[key] = {"input": prices[0], "output": prices[1],
                        "currency": "cny", "source": "siliconflow", "model": canon}
    return out


def fetch_moonshot_cn(timeout: float = 20) -> dict:
    """Moonshot 官方定价文档（Mintlify 内嵌表格）→ {聚合键: cny 价格}。
    行结构 [模型, 计价单位, 输入缓存命中, 输入未命中, 输出, 上下文]；
    列数不齐（kimi-k3 分档定价）的行跳过，避免猜错列。"""
    html = _http_text(MOONSHOT_PRICING_URL, timeout)
    if not html:
        return {}
    out = {}
    for m in re.finditer(r"rows:\s*(\[\[.*?\]\])", html):
        try:
            rows = json.loads(m.group(1).replace("`", '"'))
        except Exception:
            continue
        for row in rows:
            if not isinstance(row, list) or len(row) < 4:
                continue
            if not (isinstance(row[1], str) and "1M tokens" in row[1]):
                continue
            name = str(row[0]).strip()
            prices = [_cny_num(str(c).replace("¥", "").strip()) for c in row[2:]]
            prices = [p for p in prices if p is not None]
            if len(prices) != 3 or not name:      # 恰好 命中/未命中/输出 三价才可靠
                continue
            canon = canonical_model(name)
            key = agg_key(canon)
            if key:
                out[key] = {"input": prices[1], "output": prices[2],
                            "cache_read": prices[0],
                            "currency": "cny", "source": "moonshot", "model": canon}
    return out


def parse_bigmodel(html: str) -> dict:
    """BigModel 官方定价文档（Mintlify 内嵌表格）→ {聚合键: cny 价格}。
    表头含「输入单价（元/百万 Tokens）/ 输出单价 / 缓存命中」，按列索引取价，
    不依赖固定位置；缓存命中为「限时免费」等文案时视为 None。GLM 系列官方价权威源。"""
    out = {}
    for tbl in re.findall(r"<table[\s\S]*?</table>", html):
        head = re.search(r"<thead[\s\S]*?</thead>", tbl)
        hidx = {}
        if head:
            for i, th in enumerate(re.findall(r"<th[^>]*>([\s\S]*?)</th>", head.group(0))):
                t = re.sub(r"<[^>]+>", "", th)
                if "输入单价" in t:
                    hidx["input"] = i
                elif "输出单价" in t:
                    hidx["output"] = i
                elif "缓存命中" in t:
                    hidx["cache_read"] = i
        if "input" not in hidx or "output" not in hidx:
            continue
        for tr in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", tbl):
            cells = re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", tr)
            if len(cells) <= hidx["output"]:
                continue
            name = re.sub(r"<[^>]+>", "", cells[0]).strip()
            if not re.match(r"^GLM[-\w.]*$", name, re.I):
                continue
            canon = canonical_model(name)
            key = agg_key(canon)
            if not key:
                continue

            def num(c):
                x = re.sub(r"<[^>]+>", "", c).strip()
                x = re.sub(r"[^\d.]", "", x)   # 去掉 ¥/空格/百分号等，保留数字
                try:
                    return float(x) if x else None
                except Exception:
                    return None

            inp = num(cells[hidx["input"]])
            outp = num(cells[hidx["output"]])
            cr = num(cells[hidx["cache_read"]]) if "cache_read" in hidx else None
            if inp is None or outp is None:
                continue
            if key in out:
                continue                              # 分档模型只取首个（最低）档
            out[key] = {"input": inp, "output": outp, "cache_read": cr,
                        "currency": "cny", "source": "bigmodel", "model": canon}
    return out


def fetch_bigmodel_cn(timeout: float = 20) -> dict:
    """BigModel 官方定价文档页（服务端渲染 Mintlify）→ {聚合键: cny 价格}。
    GLM 系列官方价权威源（用户指定 GLM 从该站抓取）。"""
    html = _http_text(BIGMODEL_URL, timeout)
    if not html:
        return {}
    return parse_bigmodel(html)


def _ds_num(c):
    """DeepSeek 表格单元格 → 数值（「0.02元」「9.0元」等，剔除标签与单位）。"""
    x = re.sub(r"<[^>]+>", "", c)
    x = re.sub(r"[^\d.]", "", x)
    try:
        return float(x) if x else None
    except Exception:
        return None


def parse_deepseek(html: str) -> dict:
    """DeepSeek 官方定价页（Mintlify 特性表格）→ {聚合键: cny 价格}。
    模型列在第 0 行（deepseek-flash / deepseek-v4-pro）；
    价格行为 [百万tokens输入/输出（缓存命中/未命中）, 空闲时段, 各模型值] + 紧随的
    [高峰时段, 各模型值] 续行。全部模型取最低档（空闲时段，用户指定；仅当空闲档
    缺失时回退高峰档）；输入取缓存未命中价，缓存命中价存 cache_read。
    官方 flash 名为 deepseek-flash（版本 DeepSeek-V4.1-Flash-1017，与站方命名
    deepseek-v4-1-flash 同款模型），同时写入两个键以覆盖库内两种命名。"""
    out = {}
    for tbl in re.findall(r"<table[\s\S]*?</table>", html):
        if "百万tokens" not in tbl:
            continue
        heads = None
        cur_kind = None
        idle_vals = []
        for r in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", tbl):
            cells = [re.sub(r"<[^>]+>", "", c).strip()
                     for c in re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", r)]
            if not cells:
                continue
            if cells[0] == "模型":
                heads = [re.sub(r"\(\d+\)", "", c).strip() for c in cells[1:]]
                continue
            if heads is None:
                continue
            # 价格起始行：首行「价格」单元格带脚注（如 价格(2)），百万tokens 标签可能在第 1 格
            kind_cell = next((c for c in cells if c.startswith("百万tokens")), None)
            if kind_cell:
                cur_kind = kind_cell          # 空闲时段档起始行
                try:
                    pos = cells.index("空闲时段")
                except ValueError:
                    pos = -1
                idle_vals = ([_ds_num(c) for c in cells[pos + 1:pos + 1 + len(heads)]]
                             if pos >= 0 else [])
                continue
            if cur_kind and cells[0] == "高峰时段":
                if "缓存命中" in cur_kind:
                    slot = "cache_read"
                elif "输出" in cur_kind:
                    slot = "output"
                elif "输入" in cur_kind:
                    slot = "input"
                else:
                    slot = None
                vals = [_ds_num(c) for c in cells[1:1 + len(heads)]]
                if slot and len(vals) == len(heads):
                    for i, name in enumerate(heads):
                        canon = canonical_model(name)
                        key = agg_key(canon)
                        if not key:
                            continue
                        # 全部模型取最低档（空闲时段，用户指定），空闲档缺失回退高峰档
                        v = (idle_vals[i] if i < len(idle_vals)
                             and idle_vals[i] is not None else vals[i])
                        if v is None:
                            continue
                        e = out.setdefault(key, {"currency": "cny",
                                                 "source": "deepseek", "model": canon})
                        e[slot] = v
                cur_kind = None
                idle_vals = []
    # deepseek-flash（官方现名）= DeepSeek-V4.1-Flash-1017 = 库内 siliconflow 命名
    # deepseek-v4-1-flash：别名键同价写入，保证两个键都以官网为准
    if "deepseek-flash" in out:
        alias = dict(out["deepseek-flash"])
        alias["model"] = "deepseek-v4-1-flash"
        out["deepseek-v4-1-flash"] = alias
    return {k: v for k, v in out.items()
            if v.get("input") is not None and v.get("output") is not None}


def fetch_deepseek_cn(timeout: float = 20) -> dict:
    """DeepSeek 官方定价页 → {聚合键: cny 价格}。
    deepseek-v4-pro / deepseek-v4-1-flash 官方价权威源（用户指定以官网为准）。"""
    html = _http_text(DEEPSEEK_URL, timeout)
    if not html:
        return {}
    return parse_deepseek(html)


def _qw_num(c):
    """千问价表单元格 → 数值（「0.15」「-」等，剔除标签与千分位）。"""
    x = re.sub(r"<[^>]+>", "", c).replace(",", "").strip()
    x = re.sub(r"[^\d.]", "", x)
    try:
        return float(x) if x else None
    except Exception:
        return None


def parse_qwen(html: str) -> dict:
    """千问平台官方定价文档页（服务端渲染）→ {聚合键: cny 价格}。
    用户指定「国产大模型以该页为准」：表中收录的国内外厂商模型（qwen / glm / deepseek /
    kimi / MiniMax / xiaomi / stepfun 等）全部收录，作为 ¥ 列首要基准。
    文本模型价表表头：模型 | 模式 | 单次请求的输入 Token 数 |
    输入（元/百万 Token） | 输出（元/百万 Token） | 隐式缓存命中（元/百万 Token） | …
    同一模型按上下文区间分档多行（≤128K / ≤256K / ≤1M），取首个区间行（最低档）；
    厂商前缀（ZHIPU/、kimi/、xiaomi/ 等）由 canonical_model 统一剥离。"""
    out = {}
    for tbl in re.findall(r"<table[\s\S]*?</table>", html):
        head = re.search(r"<thead[\s\S]*?</thead>", tbl) or re.search(
            r"<tr[^>]*>[\s\S]*?</tr>", tbl)
        if not head or "输入（元/百万" not in head.group(0):
            continue
        for tr in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", tbl):
            cells = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c)).strip()
                     for c in re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", tr)]
            if len(cells) < 5:
                continue
            name = cells[0].split("（")[0].split("(")[0].strip()
            # 去掉行内促销标注（如「qwen3.7-plus 限时 8 折」）
            name = name.split()[0] if name.split() else name
            canon = canonical_model(name)
            key = agg_key(canon)
            if not key or key in out:
                continue                      # 已收录则保留首个（最低）区间档
            inp, outp = _qw_num(cells[3]), _qw_num(cells[4])
            cr = _qw_num(cells[5]) if len(cells) > 5 else None
            if inp is None or outp is None:
                continue
            out[key] = {"input": inp, "output": outp, "cache_read": cr,
                        "currency": "cny", "source": "qwen", "model": canon}
    return out


def fetch_qwen_cn(timeout: float = 25) -> dict:
    """千问官方定价文档页 → {聚合键: cny 价格}。千问系列官方价权威源。"""
    html = _http_text(QWEN_PRICES_URL, timeout)
    if not html:
        return {}
    return parse_qwen(html)


# 百炼模型市场（阿里云官方售卖平台）。控制台页面本身免登录，价格由前端 POST
# 一个内部网关取得，因此可以全自动抓取，不需要任何 Cookie。
BAILIAN_GW = ("https://bailian-cs.console.aliyun.com/data/api.json"
              "?action=BroadScopeAspnGateway&product=sfm_bailian"
              "&api=zeldaHttp.dashscopeModel./zelda/api/v1/modelCenter/"
              "listFoundationModels")
# prices[].type → 我们的字段名。阿里把缓存拆成「隐式命中/显式创建/显式命中」三种，
# 这里显式创建当写价、显式命中当读价（与 new-api 的 cache_ratio/create_cache_ratio 对齐）
_BAILIAN_PRICE_MAP = {
    "input_token": "input",
    "output_token": "output",
    "input_token_cache_read": "cache_read",
    "input_token_cache": "cache_read",           # 隐式缓存命中
    "input_token_cache_creation_5m": "cache_write",
    "input_token_cache_creation_1h": "cache_write",
}


def _parse_bailian_payload(d: dict) -> dict:
    """百炼网关响应 → {聚合键: cny 价格}。

    结构：data.DataV2.data.data.list[] → 每个模型组的 items[] → prices[]。
    每个 item 是一个可购买的具体型号（含历史快照版本），归一化后同键的取首个。
    只收 priceUnit=「每百万tokens」且输入输出齐全的条目——按张/按秒/按字符
    计价的模型与我们的 token 口径折扣列不同维度，混进来会算错折扣。
    """
    out = {}
    node = ((d.get("data") or {}).get("DataV2") or {}).get("data") or {}
    if node.get("codeEnum") != "SUCCESS":
        return out
    payload = node.get("data") or {}
    for grp in payload.get("list") or []:
        for it in grp.get("items") or []:
            name = it.get("model") or it.get("name")
            if not name:
                continue
            canon = canonical_model(name)
            key = agg_key(canon)
            if not key or key in out:
                continue          # 同名取首个（一般即默认档）
            rec = {"currency": "cny", "source": "bailian", "model": canon}
            for p in it.get("prices") or []:
                fld = _BAILIAN_PRICE_MAP.get(p.get("type") or p.get("ntmAmountType"))
                if not fld or fld in rec:
                    continue
                if (p.get("priceUnit") or "") != "每百万tokens":
                    continue
                # price 是字符串（"12"），to_float 只收数字类型，必须走 _cny_num
                v = _cny_num(p.get("price"))
                if v is not None:
                    rec[fld] = v
            if "input" in rec and "output" in rec:
                out[key] = rec
    return out


def fetch_bailian(timeout: float = 30, max_pages: int = 12) -> dict:
    """百炼模型市场 → {聚合键: cny 价格}（元/百万 tokens）。

    免登录：控制台页面公开，价格走 POST 网关。请求体是 form 编码的 params JSON，
    必须带 queryPrice=True 才返回价格；返回按「模型组 → items[]」两层组织。
    """
    import uuid as _uuid
    out = {}
    for page in range(1, max_pages + 1):
        body = {"Api": "zeldaHttp.dashscopeModel./zelda/api/v1/modelCenter/"
                       "listFoundationModels",
                "V": "1.0",
                "Data": {"input": {
                    "pageNo": page, "pageSize": 50, "search": "",
                    "providers": [], "inferenceProviders": [], "features": [],
                    "group": True, "capabilities": [], "contextWindows": [],
                    "queryPermissions": True, "queryApplyStatus": True,
                    "queryActivationStatus": True,
                    "supports": {"inference": True},
                    "queryPrice": True, "queryQpmInfo": True,
                    "queryWorkspaceLimit": True},
                    "cornerstoneParam": {
                        "feTraceId": str(_uuid.uuid4()),
                        "feURL": "https://bailian.console.aliyun.com/"
                                 "cn-beijing/model/market",
                        "protocol": "V2", "console": "ONE_CONSOLE"}}}
        try:
            r = http_post_form(BAILIAN_GW,
                               {"params": json.dumps(body, ensure_ascii=False)},
                               timeout,
                               {"Referer": "https://bailian.console.aliyun.com/"
                                           "cn-beijing/model/market",
                                "Origin": "https://bailian.console.aliyun.com"})
            d = json.loads(r.decode("utf-8", "replace")) if r else {}
        except Exception as e:
            print(f"  [百炼抓取失败] 第 {page} 页 {type(e).__name__}: {str(e)[:80]}")
            break
        if not d:
            break
        out.update(_parse_bailian_payload(d))
        node = ((d.get("data") or {}).get("DataV2") or {}).get("data") or {}
        payload = node.get("data") or {}
        if len(out) >= (payload.get("total") or 0) or not (payload.get("list")):
            break
    return out


def fetch_openrouter(timeout: float = 30) -> dict:
    """OpenRouter 公开模型价（USD/token 字符串）→ {聚合键: usd 价格}。
    urllib 会被其 TLS 指纹拦截，失败时退回 curl。"""
    st, body, _, err, _ = http_get(OPENROUTER_URL, timeout, {"Accept": "application/json"}, 1, 1.0)
    data = []
    if st == 200 and body:
        try:
            data = json.loads(body.decode("utf-8", "replace")).get("data", [])
        except Exception:
            data = []
    if not data:
        try:
            import subprocess
            r = subprocess.run(
                ["curl.exe", "-s", "-m", str(int(timeout)), "-A",
                 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0",
                 OPENROUTER_URL], capture_output=True, timeout=timeout + 10)
            data = json.loads(r.stdout.decode("utf-8", "replace")).get("data", [])
        except Exception as e:
            print(f"  [OpenRouter 抓取失败] {type(e).__name__}: {str(e)[:80]}")
            return {}
    out = {}
    for m in data:
        mid = m.get("id") or ""
        if mid.endswith(":free") or mid.endswith(":extended"):
            continue                          # 免费/扩展上下文变体价格不代表标准档
        p = m.get("pricing") or {}
        try:
            inp = float(p.get("prompt", 0)) * 1e6
            outp = float(p.get("completion", 0)) * 1e6
        except (TypeError, ValueError):
            continue
        if inp <= 0 or outp <= 0:
            continue
        canon = canonical_model(mid.split("/")[-1])
        key = agg_key(canon)
        if key and key not in out:
            out[key] = {"input": round(inp, 6), "output": round(outp, 6),
                        "currency": "usd", "source": "openrouter", "model": canon}
    return out


def merge_cn_official(prices: dict, timeout: float = 20) -> tuple:
    """抓国内渠道（¥ 基准）+ OpenRouter（$ 参考）合并进基准表。
    ¥ 列按系列选源（见 _SRC_ORDER_FAMILY）：GLM→BigModel 官网、DeepSeek→DeepSeek 官网
    （空闲档）、Kimi→Moonshot 官网、其余国产→千问官方价格页，硅基流动与厂商官网兜底；
    $ 列固定 OpenRouter。返回 (¥基准替换数, 明细摘要)。"""
    cand = {}                                   # key -> {source: 条目}

    def _put(src, entries):
        for k, v in entries.items():
            cand.setdefault(k, {})[src] = v

    bm = fetch_bigmodel_cn(timeout)
    print(f"  BigModel 官网：解析到 {len(bm)} 个 GLM 模型")
    _put("bigmodel", bm)
    ds = fetch_deepseek_cn(timeout)
    print(f"  DeepSeek 官网：解析到 {len(ds)} 个模型")
    _put("deepseek", ds)
    qw = fetch_qwen_cn(timeout)
    print(f"  千问官网：解析到 {len(qw)} 个模型")
    _put("qwen", qw)
    bl = fetch_bailian(timeout)
    print(f"  百炼模型市场：解析到 {len(bl)} 个模型")
    _put("bailian", bl)
    sf = fetch_siliconflow_cn(timeout)
    print(f"  硅基流动：解析到 {len(sf)} 个模型")
    _put("siliconflow", sf)
    ms = fetch_moonshot_cn(timeout)
    print(f"  Moonshot 官网：解析到 {len(ms)} 个模型")
    _put("moonshot", ms)
    cn = {}
    for k, bysrc in cand.items():
        order = _src_order(k)
        picked = None
        for s in order:
            if s in bysrc:
                picked = bysrc[s]
                break
        if picked is None:
            picked = min(bysrc.values(),
                         key=lambda v: _SOURCE_PRIO.get(v["source"], 9))
        cn[k] = picked
    changed = []
    for k, v in cn.items():
        v.pop("_prio", None)
        if k not in prices:
            # 国内官方渠道（厂商官网/百炼等）见到的新模型直接建档。
            # 这些源比 litellm 社区数据更权威，没有理由因为"库里还没有"就丢掉——
            # 百炼有 323 个在售型号，其中两成是 litellm 未收录的新模型。
            prices[k] = {"input": v["input"], "output": v["output"],
                         "cache_read": v.get("cache_read"),
                         "currency": "cny", "source": v["source"]}
            changed.append((k, v["input"], v["output"], v["source"]))
            continue
        cur_src = prices[k] or {}
        if _rank_for(k, {"input": v["input"], "output": v["output"],
                         "currency": "cny", "source": v["source"]}) <= _rank_for(k, cur_src):
            prices[k] = {"input": v["input"], "output": v["output"],
                         "cache_read": v.get("cache_read"),
                         "currency": "cny", "source": v["source"]}
            changed.append((k, v["input"], v["output"], v["source"]))
    # $ 列参考价：OpenRouter（覆盖到所有命中库内的模型，不动 ¥ 基准）
    orr = fetch_openrouter(timeout)
    print(f"  OpenRouter：解析到 {len(orr)} 个模型（$ 列参考）")
    n_usd = 0
    n_new_or = 0
    for k, v in orr.items():
        cur = prices.get(k) or {}
        if k not in prices:
            # $ 列以 OpenRouter 为准：无其它基准时直接建档（source=openrouter 明示非官方价，
            # ¥ 列按 USD×市场汇率折算，保证折扣列不空）
            prices[k] = {"input": v["input"], "output": v["output"],
                         "currency": "usd", "source": "openrouter",
                         "usd_input": v["input"], "usd_output": v["output"],
                         "usd_source": "openrouter"}
            n_new_or += 1
            continue
        if cur.get("usd_input") != v["input"] or cur.get("usd_output") != v["output"]:
            n_usd += 1
        cur["usd_input"], cur["usd_output"], cur["usd_source"] = \
            v["input"], v["output"], "openrouter"
    print(f"  $ 列参考价命中库内模型 {sum(1 for k in orr if k in prices)} 个"
          f"（更新 {n_usd}，新建档 {n_new_or}）")
    # 人工核验 GT 最高优先：official_ground_truth.json 的条目覆盖为主基准（source=manual），
    # 使手工核验的厂商价（含 cny 档）真正生效到折扣列，而不只是 verify 报警
    n_gt = 0
    if os.path.exists(GROUND_TRUTH_PATH):
        try:
            with open(GROUND_TRUTH_PATH, encoding="utf-8") as f:
                gt = (json.load(f) or {}).get("prices", {})
        except Exception:
            gt = {}
        for gk, gv in gt.items():
            k = agg_key(canonical_model(gk))
            if k not in prices or not isinstance(gv, dict) or gv.get("input") is None:
                continue
            old = prices[k] or {}
            # ¥ 列以国内实价为准：usd 的 GT（国际档）不覆盖已有的 cny 国内档；
            # cny 的 GT（人工核验的国内官网价）无条件覆盖
            if _rank_for(k, prices[k]) <= _rank_for(
                    k, {"input": gv["input"], "output": gv.get("output"),
                        "currency": gv.get("currency", "usd"), "source": "manual"}):
                continue
            prices[k] = {"input": gv["input"], "output": gv.get("output"),
                         "cache_read": gv.get("cache_read"),
                         "currency": gv.get("currency", "usd"), "source": "manual"}
            # 保留 OpenRouter $ 参考价（若之前已附着）
            for f_ in ("usd_input", "usd_output", "usd_source"):
                if old.get(f_) is not None:
                    prices[k][f_] = old[f_]
            n_gt += 1
    print(f"  人工核验 GT：覆盖 {n_gt} 个主基准（source=manual，最高优先级）")
    return len(changed), changed


# ---------------------------------------------------------------- 站点清单

DEFAULT_STATIONS = [
    {"name": "Xcode",       "domains": ["https://xcode.best"],         "notes": "已验证 /api/pricing 公开"},
    {"name": "PackyCode",   "domains": ["https://www.packyapi.ai"],    "notes": "已验证"},
    {"name": "Modelflare",  "domains": ["https://modelflare.dev"],     "notes": "已验证，billing v2 直定价"},
    {"name": "Kapibala",    "domains": ["https://kapibala.asia"],      "notes": "已验证"},
    {"name": "Portix",      "domains": ["https://www.portixapi.com"],  "notes": "已验证"},
    {"name": "ApiNebula",   "domains": ["https://apinebula.ai"],       "notes": "已验证"},
    {"name": "RunApi",      "domains": ["https://runapi.host"],        "notes": "已验证"},
    {"name": "LinksApi",    "domains": ["https://linksapi.cn"],        "notes": "已验证"},
    {"name": "RelayApiApp", "domains": ["https://relayapi.app"],       "notes": "已验证"},
    # --- 以下为曾经失败的站，按诊断结果配置了对应解法 ---
    # SPA 站，API 在 api.<域名> 子域（JS 中 Ame() 硬编码规则），直定价格式
    {"name": "SssAiApi",    "domains": ["https://sssaiapi.com"],
     "api_base": "https://api.sssaiapi.com", "mode": "direct",
     "notes": "SPA站；API在api子域，/api/config/models 直定价(input_price $/M)+channels分组"},
    # Next.js SSR 站，定价数据 RSC 内嵌在 /pricing HTML 中
    {"name": "Cubence",     "domains": ["https://cubence.ai"], "mode": "html_rsc",
     "notes": "Next.js站；解析 /pricing HTML 内嵌 RSC 的 input_price/output_price"},
    # /api/pricing 返回 401（status 显示 pricing.requireAuth=true）→ 需注册账号后填 cookie
    {"name": "ByteCat",     "domains": ["https://bytecatcode.org"],
     "notes": "定价接口需登录(requireAuth)；注册后 add --cookie 'session=...' 即可抓取"},
    {"name": "UUAPI",       "domains": ["https://uuapi.io"],
     "notes": "SPA站(Vue+axios)，API基地址动态拼接，需浏览器DevTools抓包确认路径"},
    {"name": "HapiOpen",    "domains": ["https://hapiopen.cc"],
     "notes": "/api/pricing 404，非标准路由；待抓包确认"},
    {"name": "Codex2Api",   "domains": ["https://codex2api.com"],
     "notes": "/api/pricing 404，非标准路由；待抓包确认"},
]


_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _expand_secrets(obj):
    """把配置里的 ${ENV_NAME} 展开成环境变量值（递归处理 dict/list/str）。

    stations.json 里目前是明文 key/cookie；把值改成 ${RELAY_KEY_XXX} 后，
    即使误提交/误发给别人也不泄露凭据。未设置的变量保持原样并在抓取时报错，
    避免"悄悄变成空字符串"这种更难查的情况。
    """
    if isinstance(obj, str):
        return _ENV_RE.sub(lambda m: os.environ.get(m.group(1), m.group(0)), obj)
    if isinstance(obj, list):
        return [_expand_secrets(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _expand_secrets(v) for k, v in obj.items()}
    return obj


def load_stations(conn) -> list:
    """stations.json 为唯一事实源，每次运行同步进 DB（支持 ${ENV} 占位）。"""
    if os.path.exists(STATIONS_PATH):
        with open(STATIONS_PATH, encoding="utf-8") as f:
            stations = json.load(f)
    else:
        stations = DEFAULT_STATIONS
        with open(STATIONS_PATH, "w", encoding="utf-8") as f:
            json.dump(stations, f, ensure_ascii=False, indent=2)
    stations = _expand_secrets(stations)
    for s in stations:
        conn.execute(
            "INSERT INTO stations(name, domains, api_base, cookie, headers, api_key, mode, notes, created_at, plaza_path) "
            "VALUES(?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(name) DO UPDATE SET domains=excluded.domains, api_base=excluded.api_base,"
            " cookie=excluded.cookie, headers=excluded.headers, api_key=excluded.api_key,"
            " mode=excluded.mode, notes=excluded.notes, plaza_path=excluded.plaza_path",
            (s["name"], jdump(s.get("domains", [])), s.get("api_base"), s.get("cookie"),
             jdump(s.get("headers") or {}), s.get("api_key"), s.get("mode") or "newapi",
             s.get("notes", ""), utc_now(), s.get("plaza_path")))
        # 充值比例手动覆盖（¥/$）：stations.json 里 recharge_rate 有值则写入，
        # 用于 /api/status 不返回 price 的站（如 SssAiApi）；无值则清空（用抓取值）
        rr = s.get("recharge_rate")
        conn.execute("UPDATE stations SET recharge_rate=? WHERE name=?",
                     (float(rr) if rr is not None else None, s["name"]))
        # 价格页权威价抓取开关：stations.json 里 page_prices=true 的站（如 Modelflare）
        # 在 save_snapshot 时额外抓 /pricing/{model} 取自定义组的真实价
        pp = 1 if s.get("page_prices") else 0
        conn.execute("UPDATE stations SET page_prices=? WHERE name=?", (pp, s["name"]))
    conn.commit()
    rows = conn.execute(
        "SELECT * FROM stations WHERE enabled=1 ORDER BY id").fetchall()
    return [{"id": r["id"], "name": r["name"],
             "domains": json.loads(r["domains"]), "api_base": r["api_base"],
             "cookie": r["cookie"], "api_key": r["api_key"],
             "headers": json.loads(r["headers"] or "{}") if r["headers"] else {},
             "mode": r["mode"] or "newapi",
             "plaza_path": r["plaza_path"],
             "recharge_rate": r["recharge_rate"],
             "notes": r["notes"], "offline": r["offline"],
             "page_prices": bool(r["page_prices"]),
             "fail_streak": r["fail_streak"] or 0, "state": r["state"] or "active"} for r in rows]


def normalize_domain(d: str) -> str:
    d = d.strip().rstrip("/")
    if not d.startswith(("http://", "https://")):
        d = "https://" + d
    return d


# ---------------------------------------------------------------- 抓取

def _try_status(base: str, timeout: float, headers: dict, retries: int = 0) -> dict:
    """尝试获取站点配置（汇率等）。直定价站的配置可能在 /api/status 或 /api/config/site-settings。"""
    for ep in ("/api/status", "/api/config/site-settings"):
        st, body, _, _, _ = http_get(base + ep, timeout, headers, retries)
        if st == 200:
            try:
                return json.loads(body.decode("utf-8", "replace"))
            except Exception:
                pass
    return None


def fetch_station(station: dict, timeout: float, retries: int = 0, backoff: float = 1.0) -> dict:
    """按 mode 抓取，多域名/多端点故障转移，网络类错误自动指数退避重试。

    mode:
      newapi   — one-api/new-api 系，GET {domain|api_base}/api/pricing（倍率格式）
      direct   — 自建 SPA 后端，GET {api_base}/api/config/models（input_price 直定价 $/M）
      html_rsc — Next.js SSR 站，GET {domain}/pricing，解析 HTML 内嵌 RSC 定价数据
      model_plaza — GET {domain}/api/v1/model-plaza（分组 → 模型 → 每 token 单价，
                    见 parse_model_plaza；UUAPI 用这个）
      public_models — GET {domain}/api/public/models（公开直定价 $/M，无分组；
                    uptoken 用，见 parse_public_models）
      models_feed — GET {domain}{models_path}（自建站公开价目 JSON，价格已含分组
                    折扣；top-one.cc 用，见 parse_models_feed）
    """
    mode = station.get("mode") or "newapi"
    api_base = normalize_domain(station["api_base"]) if station.get("api_base") else None
    cookie = station.get("cookie")
    headers = {}
    if cookie:
        headers["Cookie"] = cookie
    # 站点自定义请求头：有些站除了 Cookie 还要带专属头才能取到定价
    # （如 heyroute 必须带 HeyRoute-User: <用户id>，否则 401 "header not provided"）
    for k, v in (station.get("headers") or {}).items():
        if v is not None and v != "":
            headers[str(k)] = str(v)
    res = {"ok": False, "errors": [], "pricing": None, "status": None,
           "domain": None, "latency_ms": None, "format": mode, "attempts": 1}

    if mode == "html_rsc":
        for domain in station["domains"]:
            base = normalize_domain(domain)
            res["domain"] = base
            st, body, lat, err, att = http_get(base + "/pricing", timeout, headers,
                                               retries, backoff)
            res["latency_ms"], res["attempts"] = lat, att
            if st != 200:
                res["errors"].append(f"{base}/pricing -> {err or st}")
                continue
            models, groups = parse_rsc_html(body.decode("utf-8", "replace"))
            if not models:
                res["errors"].append(f"{base}/pricing 未解析到定价数据")
                continue
            res["ok"] = True
            res["pricing"] = {"data": models, "group_ratio": groups}
            res["status"] = _try_status(base, timeout, headers, retries)
            break
        return res

    if mode == "model_plaza":
        # UUAPI 类：/api/v1/model-plaza 返回「分组 → 模型 → 每 token 单价」
        # plaza_path 可覆盖路径（有些站的广场接口不在默认路径，或像 fluxionai
        # 那样前端域名与接口域名不同，用 api_base 指向接口域名）
        plaza_path = station.get("plaza_path") or "/api/v1/model-plaza"
        for domain in station["domains"]:
            base = api_base or normalize_domain(domain)
            url = base + plaza_path + ("" if "?" in plaza_path else "?timezone=Asia%2FShanghai")
            res["domain"] = url
            st, body, lat, err, att = http_get(url, timeout, headers, retries, backoff)
            res["latency_ms"], res["attempts"] = lat, att
            if st != 200 or not body:
                res["errors"].append(f"{url} -> {err or st}")
                continue
            try:
                payload = json.loads(body.decode("utf-8", "replace"))
            except Exception as e:
                res["errors"].append(f"{url} JSON 解析失败: {e}")
                continue
            # sidrune 型（fluxionai 用的平台）：groups 与 pricing 平级，
            # 用 group_ids 关联，且带 recharge_multiplier。转换成标准形态后
            # 复用同一个解析器，不必为一种变体再写一套。
            payload = _normalize_plaza_payload(payload)
            models, groups, ver = parse_model_plaza(payload)
            if not models:
                res["errors"].append(f"{url} 无有效模型数据")
                continue
            res.update(ok=True, format="model_plaza",
                       pricing={"data": models, "group_ratio": groups,
                                "pricing_version": ver})
            # 充值倍率：该平台在响应里直接给（1人民币=N美元额度），
            # 包成 status 结构让 parse_exchange_rate 取走
            rm = ((payload.get("data") or {}).get("recharge_multiplier")
                  if isinstance(payload.get("data"), dict) else None)
            if rm is not None:
                res["status"] = {"data": {"price": rm}}
            return res
        return res

    if mode == "public_models":
        # uptoken 类：GET /api/public/models 公开直定价（$/M），无分组
        for domain in station["domains"]:
            base = normalize_domain(domain)
            url = base + "/api/public/models"
            res["domain"] = url
            st, body, lat, err, att = http_get(url, timeout, headers, retries, backoff)
            res["latency_ms"], res["attempts"] = lat, att
            if st != 200:
                res["errors"].append(f"{url} -> {err or st}")
                continue
            try:
                payload = json.loads(body.decode("utf-8", "replace"))
            except Exception as e:
                res["errors"].append(f"{url} JSON 解析失败: {e}")
                continue
            models, groups = parse_public_models(payload)
            if not models:
                res["errors"].append(f"{url} 无有效模型数据")
                continue
            res.update(ok=True, format="public_models",
                       pricing={"data": models, "group_ratio": groups})
            return res
        return res

    if mode == "models_feed":
        # 自建站公开价目 JSON（路径由 station["models_path"] 指定，
        # 如 top-one.cc 的 /top-api-tools-api/models，价格已含分组折扣）
        path = station.get("models_path") or "/top-api-tools-api/models"
        for domain in station["domains"]:
            base = api_base or normalize_domain(domain)
            url = base + path
            res["domain"] = url
            st, body, lat, err, att = http_get(url, timeout, headers, retries, backoff)
            res["latency_ms"], res["attempts"] = lat, att
            if st != 200 or not body:
                res["errors"].append(f"{url} -> {err or st}")
                continue
            try:
                payload = json.loads(body.decode("utf-8", "replace"))
            except Exception as e:
                res["errors"].append(f"{url} JSON 解析失败: {e}")
                continue
            models, groups = parse_models_feed(payload)
            if not models:
                res["errors"].append(f"{url} 无有效模型数据")
                continue
            res.update(ok=True, format="models_feed",
                       pricing={"data": models, "group_ratio": groups},
                       status=_try_status(base, timeout, headers, retries))
            return res
        return res

    endpoints = ["/api/pricing"] if mode == "newapi" else ["/api/config/models", "/api/pricing"]
    for domain in station["domains"]:
        base = api_base or normalize_domain(domain)
        for ep in endpoints:
            res["domain"] = base + ep
            st, body, lat, err, att = http_get(base + ep, timeout, headers, retries, backoff)
            res["latency_ms"], res["attempts"] = lat, att
            if st != 200:
                res["errors"].append(f"{base}{ep} -> {err or st}"
                                     + (f"（重试{att - 1}次）" if att > 1 else ""))
                continue
            try:
                payload = json.loads(body.decode("utf-8", "replace"))
            except Exception as e:
                res["errors"].append(f"{base}{ep} JSON 解析失败: {e}")
                continue
            if mode == "direct":
                models, groups = parse_direct_pricing(payload)
                if not models:
                    res["errors"].append(f"{base}{ep} 无有效模型数据")
                    continue
                res.update(ok=True, pricing={"data": models, "group_ratio": groups},
                           status=_try_status(base, timeout, headers, retries))
                return res
            # newapi：兼容个别站直接用直定价字段的 /api/pricing
            if _looks_direct(payload):
                models, groups = parse_direct_pricing(payload)
                if models:
                    res.update(ok=True, format="direct",
                               pricing={"data": models, "group_ratio": groups},
                               status=_try_status(base, timeout, headers))
                    return res
            res.update(ok=True, format="newapi", pricing=payload)
            res["status"] = _try_status(base, timeout, headers, retries)
            return res
    return res


def _tags_text(v):
    """站点 tags 字段可能是字符串或数组，统一成逗号串（用于判定视频/图片/音乐）。"""
    if isinstance(v, (list, tuple)):
        return ",".join(str(x).strip() for x in v if str(x).strip()) or None
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def parse_pricing(pricing: dict):
    """→ (models, group_ratio_map, pricing_version)"""
    models, groups, version = [], {}, None
    if not isinstance(pricing, dict):
        return models, groups, version
    version = pricing.get("pricing_version")
    gr = pricing.get("group_ratio")
    if isinstance(gr, dict):
        for k, v in gr.items():
            fv = to_float(v)
            if fv is not None:
                groups[str(k)] = fv
    for item in pricing.get("data") or []:
        if not isinstance(item, dict) or not item.get("model_name"):
            continue
        models.append({
            "model_name": str(item["model_name"]).strip(),
            "model_key": normalize_model(item["model_name"]),
            "vendor_id": item.get("vendor_id"),
            "quota_type": item.get("quota_type", 0) or 0,
            "model_ratio": to_float(item.get("model_ratio")),
            "completion_ratio": to_float(item.get("completion_ratio")),
            "cache_ratio": to_float(item.get("cache_ratio")),
            "create_cache_ratio": to_float(item.get("create_cache_ratio")),
            "model_price": to_float(item.get("model_price")),
            "model_type": (str(item.get("model_type")).strip() or None) if item.get("model_type") else None,
            "model_tags": _tags_text(item.get("tags")),
            "enable_groups": item.get("enable_groups") if isinstance(item.get("enable_groups"), list) else [],
            "tiers": item.get("tiers") if isinstance(item.get("tiers"), list) else [],
            "supported_endpoint_types": item.get("supported_endpoint_types") if isinstance(item.get("supported_endpoint_types"), list) else [],
            # 媒体模型（视频/图片）接口直定价字段，供 api_media_prices 兜底页面缺失
            "billing_unit": (str(item.get("billing_unit")).strip().lower()
                             if item.get("billing_unit") else None),
            "video_resolution_price": item.get("video_resolution_price")
            if isinstance(item.get("video_resolution_price"), dict) else None,
            # 图片「按尺寸」价目表（Aixoras 等 billing_mode=image_size）：[{size, price}]
            "image_size_price": item.get("image_size_price")
            if isinstance(item.get("image_size_price"), (list, dict)) else None,
            # 站点自带官方参考价（USD/1M）：解析器挂在这里，落库进 feed_official
            "official_in_usd_m": to_float(item.get("official_in_usd_m")),
            "official_out_usd_m": to_float(item.get("official_out_usd_m")),
        })
        # —— 修正两类「倍率字段不可信」的模型（new-api 系常见坑）——
        rec = models[-1]
        ratio = rec.get("model_ratio") or 0.0
        # ① billing_mode=tiered_expr：真实价在 billing_expr 里，model_ratio 常被塞成
        #    占位值（MemeCode/ApiNebula/WanmoAPI 实测 37.5 → 等价 $75/M，全错）。
        #    系数本身即 $/1M 基准价（系数 × 分组倍率 = 官网展示价）。
        if str(item.get("billing_mode") or "").lower() == "tiered_expr":
            coef = parse_billing_expr(item.get("billing_expr") or "")
            if coef and coef.get("in"):
                base = coef["in"]
                rec["model_ratio"] = base / RATIO_UNIT_USD_PER_M
                rec["completion_ratio"] = (coef["out"] / base) if coef.get("out") else 1.0
                if coef.get("cr") is not None:
                    rec["cache_ratio"] = coef["cr"] / base
                if coef.get("cw") is not None:
                    rec["create_cache_ratio"] = coef["cw"] / base
        # ② 媒体模型只给「按分辨率/按张」价时，model_ratio 是占位值，不能当 token 价展示
        vres = rec.get("video_resolution_price")
        if vres:
            rec["billing_unit"] = rec.get("billing_unit") or "per_second"
            if not (rec.get("model_ratio") or 0) or rec.get("model_ratio") == 37.5:
                rec["model_ratio"] = 0.0
                rec["completion_ratio"] = 0.0
        elif str(item.get("billing_mode") or "").lower() == "image_size":
            # 按尺寸计价：image_size_price 是「每张图 × 尺寸」基准价，配 group_ratio 得实付
            levels = _size_levels(item.get("image_size_price"))
            if levels:
                rec["image_size_price"] = levels
                rec["billing_unit"] = rec.get("billing_unit") or "per_call"
            if not (rec.get("model_ratio") or 0) or rec.get("model_ratio") == 37.5:
                # 倍率是占位值（37.5 → $75/M），且这类模型不按 token 计价
                rec["model_ratio"] = 0.0
                rec["completion_ratio"] = 0.0
    # 分组倍率补全：group_ratio 只列「公开组」，首充/活动组（claude-first-topup 0.03、
    # openai-first-topup 0.015）的倍率在 group_definitions 里。只补「确实出现在
    # 某模型 enable_groups 中的 id」——group_definitions 还有 media/grok/claude-tmp
    # 这类与真实组名（Media/grok-stable/claude-tmp-cheap）不一致的别名条目。
    defs = {}
    for gd in pricing.get("group_definitions") or []:
        if isinstance(gd, dict) and gd.get("id"):
            r = to_float(gd.get("ratio"))
            if r is not None:
                defs[str(gd["id"])] = r
    if defs:
        in_use = set()
        for m in models:
            in_use.update(str(x) for x in (m.get("enable_groups") or []))
        for gid, r in defs.items():
            if gid not in groups and gid in in_use:
                groups[gid] = r
    return models, groups, version


def _looks_direct(payload) -> bool:
    """判断是否为 input_price/output_price 直定价格式（$ / M tokens）。"""
    data = payload.get("data") if isinstance(payload, dict) else payload
    return (isinstance(data, list) and data and isinstance(data[0], dict)
            and ("input_price" in data[0] or "output_price" in data[0])
            and "model_ratio" not in data[0])


def parse_direct_pricing(payload: dict):
    """直定价格式（input_price/output_price 单位 $/M）→ 合成倍率，统一内部表示。
    分组来自 channels[].group（同价多渠道）。"""
    models, groups = [], {}
    data = payload.get("data") if isinstance(payload, dict) else payload
    for item in data or []:
        if not isinstance(item, dict):
            continue
        name = item.get("model_name") or item.get("model_id")
        if not name:
            continue
        ip = to_float(item.get("input_price")) or 0.0
        op = to_float(item.get("output_price"))
        crp = to_float(item.get("cache_read_price"))
        cwp = to_float(item.get("cache_write_price"))
        ch_groups = [str(c.get("group")) for c in item.get("channels") or []
                     if isinstance(c, dict) and c.get("group")]
        models.append({
            "model_name": str(name).strip(),
            "model_key": normalize_model(name),
            "vendor_id": item.get("provider") or item.get("vendor_id"),
            "quota_type": 0,
            # 合成倍率：倍率 1 = $2/M，保证与 newapi 格式共用一套计价代码
            "model_ratio": ip / RATIO_UNIT_USD_PER_M if ip else 0.0,
            "completion_ratio": (op / ip) if (ip and op is not None) else 1.0,
            "cache_ratio": (crp / ip) if (ip and crp is not None) else None,
            "create_cache_ratio": (cwp / ip) if (ip and cwp is not None) else None,
            "model_price": None,
            "model_type": (str(item.get("model_type")).strip() or None) if item.get("model_type") else None,
            "model_tags": _tags_text(item.get("tags")),
            "enable_groups": ch_groups,
            "tiers": item.get("pricing_tiers") if isinstance(item.get("pricing_tiers"), list) else [],
            "supported_endpoint_types": item.get("supported_endpoint_types") if isinstance(item.get("supported_endpoint_types"), list) else [],
        })
        for g in ch_groups:
            groups.setdefault(g, 1.0)
    return models, groups


def parse_public_models(payload):
    """uptoken 类公开直定价（GET /api/public/models，input_price_per_1m 等，$/M，无分组）
    → 与 newapi 同构的 models/groups。合成一个 default 分组（倍率 1）：否则
    group_ratio 为空会让 iter_detail_rows 判为「倍率未知」而主动清空价格。"""
    models, groups = [], {"default": 1.0}
    data = payload if isinstance(payload, list) else (payload.get("data") or [])
    for item in data:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("model_name") or item.get("model_id")
        if not name:
            continue
        ip = to_float(item.get("input_price_per_1m")) or to_float(item.get("input_price"))
        if not ip:
            continue
        op = to_float(item.get("output_price_per_1m")) or to_float(item.get("output_price"))
        crp = (to_float(item.get("cache_read_price_per_1m"))
               or to_float(item.get("cache_read_price")))
        cwp = (to_float(item.get("cache_creation_price_per_1m"))
               or to_float(item.get("cache_write_price_per_1m"))
               or to_float(item.get("cache_write_price")))
        models.append({
            "model_name": str(name).strip(),
            "model_key": normalize_model(name),
            "vendor_id": item.get("provider") or item.get("vendor_id"),
            "quota_type": 0,
            "model_ratio": ip / RATIO_UNIT_USD_PER_M,
            "completion_ratio": (op / ip) if op is not None else 1.0,
            "cache_ratio": (crp / ip) if crp is not None else None,
            "create_cache_ratio": (cwp / ip) if cwp is not None else None,
            "model_price": None,
            "model_type": None,
            "model_tags": _tags_text(item.get("tags")),
            "enable_groups": ["default"],
            "tiers": [],
            "supported_endpoint_types": [],
        })
    return models, groups


def parse_rsc_html(html: str):
    """从 Next.js RSC 内嵌数据（self.__next_f，引号被转义）提取 model_name/input_price/output_price。"""
    models, groups, seen = [], {}, set()
    for m in re.finditer(r'\\"model_name\\":\\"([^"\\]+)\\"', html):
        name = m.group(1)
        if name in seen:
            continue
        window = html[m.start():m.start() + 3000]
        ip = re.search(r'\\"input_price\\":([0-9.]+)', window)
        if not ip:
            continue
        op = re.search(r'\\"output_price\\":([0-9.]+)', window)
        crp = re.search(r'\\"cache_read_price\\":([0-9.]+)', window)
        cwp = re.search(r'\\"cache_write_price\\":([0-9.]+)', window)
        ipv, opv = float(ip.group(1)), float(op.group(1)) if op else None
        seen.add(name)
        models.append({
            "model_name": name,
            "model_key": normalize_model(name),
            "vendor_id": None,
            "quota_type": 0,
            "model_ratio": ipv / RATIO_UNIT_USD_PER_M if ipv else 0.0,
            "completion_ratio": (opv / ipv) if (ipv and opv is not None) else 1.0,
            "cache_ratio": (float(crp.group(1)) / ipv) if (crp and ipv) else None,
            "create_cache_ratio": (float(cwp.group(1)) / ipv) if (cwp and ipv) else None,
            "model_price": None,
            "model_type": None,
            "model_tags": None,
            "enable_groups": [],
            "tiers": [],
            "supported_endpoint_types": [],
        })
    return models, groups


_EXPR_VARS = {"p": "in", "c": "out", "cr": "cr", "cc": "cw", "cc1h": "cw1h"}


def parse_billing_expr(expr: str):
    """解析 new-api 的 `billing_mode=tiered_expr` 表达式 → {$/M 系数}（基础档）。

    站点把 model_ratio 塞成占位值（MemeCode 全是 37.5，等价 $75/M），真实价格只在
    billing_expr 里，形如：
        tier("standard", p * 5 + cr * 0.5 + cc * 6.25 + cc1h * 10 + c * 25)
        len <= 272000 ? tier("0_272k", p * 2 + ...) : tier("272k_plus", p * 4 + ...)
        ((service_tier == "fast") && len <= 353400) ? tier("fast", p*0.4...) : tier("standard", p*0.2...)
    变量：p=输入、c=输出、cr=缓存读、cc=缓存写(5m)、cc1h=缓存写(1h)。
    **系数本身就是 $/1M 基准价**（实测 系数 × 分组倍率 = 官网展示价）。
    多档时取「基准档」：优先标签含 standard 的那档，否则取输入系数最小的一档
    （WanmoAPI 的 fast/priority 档排在最前，不能直接取第一档）。
    返回 {"in","out","cr","cw"}；解析不了返回 None。
    """
    if not expr or "tier(" not in expr:
        return None
    tiers = []
    for m in re.finditer(r'\btier\(\s*"([^"]*)"\s*,', expr):
        label = m.group(1)
        i = expr.find("(", m.start())          # tier( 的左括号
        depth, j, body_start = 0, i, None
        while j < len(expr):
            ch = expr[j]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    break
            elif ch == "," and depth == 1 and body_start is None:
                body_start = j + 1
            j += 1
        body = expr[body_start if body_start is not None else i:j]
        coefs = {"in": None, "out": None, "cr": None, "cw": None, "cw1h": None}
        for var, coef in re.findall(r"([a-z][a-z0-9]*)\s*\*\s*([0-9.]+)", body):
            key = _EXPR_VARS.get(var)
            fv = to_float(float(coef))
            if key and fv is not None:
                coefs[key] = fv
        if coefs["cw"] is None and coefs["cw1h"] is not None:
            coefs["cw"] = coefs["cw1h"]
        if coefs["in"] is not None:
            tiers.append((label, coefs))
    if not tiers:
        return None
    std = [t for t in tiers if "standard" in t[0].lower()]
    _, best = (std[0] if std else min(tiers, key=lambda t: t[1]["in"]))
    return {k: v for k, v in best.items() if k != "cw1h"}


def _normalize_plaza_payload(payload: dict) -> dict:
    """把 sidrune 型模型广场响应转成 UUAPI 那种标准形态。

    sidrune（fluxionai.space 背后的平台）：::

        {"data": {"recharge_multiplier": 1,
                  "groups":   [{id, name, rate_multiplier, ...}],
                  "pricing":  [{name, billing_mode, input_price, group_ids, ...}]}}

    UUAPI 标准形态：::

        {"data": {"groups": [{name, rate_multiplier,
                              "models": [{name, pricing: {...}}]}]}}

    两者差异只在「分组与价格平级、用 group_ids 关联」。转换后即可复用
    `parse_model_plaza`，避免为一种变体维护第二套解析。
    同一模型可属于多个分组，转换后会在每个分组里各出现一次——
    这与 UUAPI 的原始数据同构，下游按「模型 × 分组」展开的语义不变。
    """
    if not isinstance(payload, dict):
        return payload
    data = payload.get("data")
    if not isinstance(data, dict):
        return payload
    pricing = data.get("pricing")
    groups = data.get("groups")
    if not isinstance(pricing, list) or not isinstance(groups, list):
        return payload                     # 已是标准形态，原样返回
    by_id = {}
    for g in groups:
        if not isinstance(g, dict) or not g.get("id"):
            continue
        rate = to_float(g.get("effective_rate_multiplier"))
        if rate is None:
            rate = to_float(g.get("rate_multiplier"))
        by_id[g["id"]] = {"name": str(g.get("name") or "").strip(),
                          "rate_multiplier": rate if rate is not None else 1.0,
                          "models": []}
    if not by_id:
        return payload
    by_model = {}
    for p in pricing:
        if not isinstance(p, dict) or not p.get("name"):
            continue
        by_model.setdefault(p["name"], []).append(p)

    for name, entries in by_model.items():
        # 同一模型在该平台有多条记录，差价来自分组倍率（如 1e-05 属于倍率 1.0
        # 的组、1.2e-05 属于倍率 1.2 的组）。取最低价作基准价、分组倍率交给
        # groups，下游 base × 倍率即可还原各组的实付价。
        priced = [e for e in entries if to_float(e.get("input_price"))]
        base = (min(priced, key=lambda e: to_float(e["input_price"]))
                if priced else entries[0])
        gids = []
        for e in entries:
            for g in (e.get("group_ids") or []):
                if g in by_id and g not in gids:
                    gids.append(g)
        if not gids:
            gids = [next(iter(by_id))]
        # parse_model_plaza 期望价格嵌在 pricing 键里，sidrune 是扁平的，包一层
        wrapped = {"name": name, "pricing": base}
        for gid in gids:
            by_id[gid]["models"].append(wrapped)
    out = dict(payload)
    out["data"] = dict(data)
    out["data"]["groups"] = list(by_id.values())
    return out


def parse_model_plaza(payload: dict):
    """UUAPI 类「模型广场」接口（/api/v1/model-plaza）→ 与 new-api 同构的 models/groups。

    与 new-api 的差别：
      1. 价格是**每单个 token** 的单价（2e-06 = $2/1M），而 new-api 倍率 1 = $2/1M
         → model_ratio = 单价 × 1e6 ÷ 2 = 单价 × 500000；
      2. 分组倍率在 group.rate_multiplier，模型本身的 pricing 不含分组折扣；
      3. billing_mode：token / image / per_request / video——
         per_request → quota_type=1 + model_price；
         video → billing_unit=per_second + video_resolution_price（intervals 的
         tier_label 就是 480p/720p/1080p，per_request_price 是每秒价）。
    返回 (models, groups, pricing_version)。"""
    import hashlib
    data = (payload or {}).get("data") or {}
    groups_raw = data.get("groups") or []
    groups, merged, order = {}, {}, []
    for g in groups_raw:
        gname = str(g.get("name") or "").strip()
        if not gname:
            continue
        ratio = to_float(g.get("rate_multiplier"))
        groups[gname] = ratio if ratio is not None else 1.0
        model_type_hint = g.get("platform") or None
        for m in g.get("models") or []:
            name = str(m.get("name") or "").strip()
            if not name:
                continue
            p = m.get("pricing") or {}
            mode = (p.get("billing_mode") or "token").lower()
            ip = to_float(p.get("input_price"))
            op = to_float(p.get("output_price"))
            crp = to_float(p.get("cache_read_price"))
            cwp = to_float(p.get("cache_write_price"))
            per_req = to_float(p.get("per_request_price"))
            rec = merged.get(name)
            if rec is None:
                rec = {
                    "model_name": name,
                    "model_key": normalize_model(name),
                    "vendor_id": None,
                    "quota_type": 0,
                    "model_ratio": 0.0,
                    "completion_ratio": 1.0,
                    "cache_ratio": None,
                    "create_cache_ratio": None,
                    "model_price": None,
                    "model_type": None,
                    "model_tags": None,
                    "enable_groups": [],
                    "tiers": [],
                    "supported_endpoint_types": [],
                    "billing_unit": None,
                    "video_resolution_price": None,
                }
                merged[name] = rec
                order.append(name)
            rec["enable_groups"].append(gname)
            # 站点自带的官方参考价（official_pricing 是 USD/token）→ USD/M，
            # 只在首次出现时取一次；用于补足 official_prices.json 没覆盖的模型
            if not rec.get("official_in_usd_m"):
                op_off = m.get("official_pricing") or {}
                oi = to_float(op_off.get("input_price"))
                oo = to_float(op_off.get("output_price"))
                if oi:
                    rec["official_in_usd_m"] = oi * 1_000_000
                    rec["official_out_usd_m"] = (oo * 1_000_000) if oo else None
            # 同一模型出现在多个分组时，价格字段是同一份，取首次即可
            if rec["model_ratio"] or rec["model_price"] or rec["video_resolution_price"]:
                continue
            if mode == "video":
                levels = {}
                for it in p.get("intervals") or []:
                    label = str(it.get("tier_label") or "").strip()
                    pv = to_float(it.get("per_request_price"))
                    if label and pv is not None:
                        levels[label] = pv
                if levels:
                    rec["billing_unit"] = "per_second"
                    rec["video_resolution_price"] = levels
                    rec["quota_type"] = 1
                    rec["model_type"] = "video"
                continue
            if mode in ("per_request", "image") and per_req:
                rec["quota_type"] = 1
                rec["model_price"] = per_req
                rec["billing_unit"] = "per_call"
                rec["model_type"] = "image" if mode == "image" else None
                continue
            if ip:
                rec["model_ratio"] = ip * 1_000_000 / RATIO_UNIT_USD_PER_M
                if op is not None and ip:
                    rec["completion_ratio"] = op / ip
                if crp is not None:
                    rec["cache_ratio"] = crp / ip
                if cwp is not None:
                    rec["create_cache_ratio"] = cwp / ip
                rec["billing_unit"] = "token"
                if mode == "image" and rec["model_type"] is None:
                    rec["model_type"] = "image"
        _ = model_type_hint
    models = [merged[n] for n in order if merged[n]["model_ratio"] or
              merged[n]["model_price"] or merged[n]["video_resolution_price"]]
    try:
        ver = hashlib.sha256(jdump({"g": groups, "m": [
            (m["model_name"], m["model_ratio"], m["completion_ratio"],
             m["cache_ratio"], m["create_cache_ratio"], m["model_price"],
             m["video_resolution_price"]) for m in models]}).encode()).hexdigest()[:16]
    except (TypeError, ValueError):
        ver = None
    return models, groups, ver


def _parse_multiplier(v):
    """把 "0.09x" / "1.2X" / 0.09 统一成 float；解析不了返回 None。"""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v) or None
    m = re.match(r"\s*([0-9]*\.?[0-9]+)", str(v or ""))
    return float(m.group(1)) or None if m else None


def _median(vals):
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def parse_models_feed(payload: dict):
    """自建站「公开价目 JSON」接口 → 与 new-api 同构的 models/groups。
    （top-one.cc 的 GET /top-api-tools-api/models 属这类，见 mode=models_feed）

    结构：{models: [{modelId, provider, tiers: [{group, multiplier: "0.09x",
            input, output, cacheRead, cacheWrite}]}]}

    要点：
      1. 价格单位是 **USD / 1M tokens**，且**已乘分组折扣**（实测
         gpt-5.6-sol 特惠档 0.45 = 官方 $5 × 0.09x）。所以 base = 价格 ÷ 倍率，
         groups 记录分组倍率，下游再乘回去，与新 api 的「基准价 × 分组倍率」同构；
      2. 同一模型各分组共用同一基准价，源站保留 2 位小数时个别档有 ~2% 偏差，
         取各档基准价的中位数抹平；
      3. 同一模型可能只出现在部分分组，enable_groups 记录实际分组。
    """
    models, groups = [], {}
    for m in (payload or {}).get("models") or []:
        if not isinstance(m, dict):
            continue
        name = m.get("modelId") or m.get("slug") or m.get("name")
        if not name:
            continue
        rows = []
        for t in m.get("tiers") or []:
            if not isinstance(t, dict):
                continue
            mult = _parse_multiplier(t.get("multiplier"))
            ip = to_float(t.get("input"))
            if not mult or not ip:
                continue
            gname = str(t.get("group") or t.get("name") or "").strip()
            if gname:
                groups[gname] = mult
            rows.append((mult, ip, to_float(t.get("output")),
                         to_float(t.get("cacheRead")), to_float(t.get("cacheWrite")),
                         gname))
        if not rows:
            continue
        base_in = _median([r[1] / r[0] for r in rows])
        if not base_in:
            continue
        comps = [r[2] / r[1] for r in rows if r[2]]
        crs = [r[3] / r[1] for r in rows if r[3]]
        cws = [r[4] / r[1] for r in rows if r[4]]
        models.append({
            "model_name": str(name).strip(),
            "model_key": normalize_model(name),
            "vendor_id": m.get("provider"),
            "quota_type": 0,
            "model_ratio": base_in / RATIO_UNIT_USD_PER_M,
            "completion_ratio": _median(comps) if comps else 1.0,
            "cache_ratio": _median(crs) if crs else None,
            "create_cache_ratio": _median(cws) if cws else None,
            "model_price": None,
            "model_type": None,
            "model_tags": _tags_text([c for c in (m.get("category") or [])
                                      if c not in ("all",)] or None),
            "enable_groups": [r[5] for r in rows if r[5]],
            "tiers": [],
            "supported_endpoint_types": m.get("endpoints") or [],
            # 官方参考价：top-one 明确标注 USD/1M；若站点给的是 per-token 小数也兼容
            "official_in_usd_m": _off_usd_m(m.get("officialPricingUsd"), "input"),
            "official_out_usd_m": _off_usd_m(m.get("officialPricingUsd"), "output"),
        })
    return models, groups


def _off_usd_m(block: dict, field: str):
    """从站点自带官方价里取 USD/1M。两种单位都可能：值很小(<0.01)视为 per-token。"""
    if not isinstance(block, dict):
        return None
    v = to_float(block.get(field))
    if v is None:
        return None
    return v * 1_000_000 if v < 0.01 else v


def _split_page_map(pm):
    """价格页数据拆成 (token 权威价, 每秒权威价, 每次权威价)。
    新格式 {"per_1m":..., "per_second":..., "per_call":...}；
    兼容旧格式 {group: [in,out]}。"""
    if not isinstance(pm, dict):
        return {}, {}, {}
    if any(k in pm for k in ("per_1m", "per_second", "per_call")):
        return pm.get("per_1m") or {}, pm.get("per_second") or {}, pm.get("per_call") or {}
    return pm, {}, {}


def _page_second_price(rows: dict):
    """从「Per second」行里挑默认展示价：取页面表格的第一行
    （官网一般把主推档位放最前，如 kling-o1 的 1080p、MiniMax-H3 的 2k）；
    其余分辨率仍完整存在 JSON 里可查。"""
    if not isinstance(rows, dict):
        return None
    for cond, price in rows.items():
        if cond == "_default":
            continue
        v = to_float(price)
        if v is not None:
            return v
    return to_float(rows.get("_default")) if "_default" in rows else None


def api_media_prices(models):
    """用 /api/pricing 里的媒体直定价字段补齐**价格页缺失**的按秒/按次价。

    背景：Modelflare 的 nsfw 系列（seedance-2.0-nsfw / wan2.7-t2v-nsfw …）在
    /pricing/{model} 返回 404，页面一个字价都没有，ratio 也全是 0，导致面板
    按秒价长期为空。这些模型其实在接口里给了 authority 字段：
      billing_unit == "per_second"  →  video_resolution_price {1080p: 0.12675, ...}
      billing_unit == "per_call" 且 model_price >0  →  按次单价
    返回与 scrape_page_prices 同构的 {model: {"per_second"/"per_call": {group: {...}}}}，
    由调用方只在页面无该计费方式时并入（页面权威价优先，绝不覆盖）。"""
    out = {}
    for m in models or []:
        unit = (m.get("billing_unit") or "").lower()
        groups = [str(g) for g in (m.get("enable_groups") or [])] or [""]
        entry = {}
        vres = m.get("video_resolution_price") or {}
        if unit in ("per_second", "per_sec", "second") and isinstance(vres, dict) and vres:
            vals = {}
            for k, v in vres.items():
                fv = to_float(v)
                if fv is not None:
                    vals[str(k)] = fv
            if vals:
                # 默认档取接口返回的第一个分辨率（与页面「第一行」口径一致）
                vals["_default"] = next(iter(vals.values()))
                entry["per_second"] = {g: dict(vals) for g in groups}
        mp = to_float(m.get("model_price"))
        # 仅媒体模型的接口计次价（billing_unit=per_call/per_request/per_image）才并入；
        # 传统 new-api 的 quota_type=1 模型走 model_price×分组倍率，不在此处改写
        if "per_second" not in entry and mp \
                and unit in ("per_call", "per_request", "per_image", "per_generation"):
            entry["per_call"] = {g: {"price": mp} for g in groups}
        if entry and m.get("model_name"):
            out[str(m["model_name"])] = entry
    return out


def merge_media_prices(pm, models):
    """把接口直定的媒体价并入 page_prices（页面已有该计费方式则保留页面值）。"""
    extra = api_media_prices(models)
    if not extra:
        return pm or {}
    out = dict(pm or {})
    for name, ent in extra.items():
        cur = dict(out.get(name) or {})
        for k, v in ent.items():
            if not cur.get(k):
                cur[k] = v
        out[name] = cur
    return out


_CTX_COND = re.compile(r"(?:context length|上下文长度)\s*<=\s*(\d+)", re.I)


def page_tier_rank(cond):
    """价格页同一组同一类型常有多档（上下文分档 / Standard），这里给「基准档」排序：
    上下文阈值越小越基础（0, N）→ Standard（1,0）→ 其它（2,0）。

    注意不能反过来按字面优先 Standard：阶梯模型（gpt-6-* / gpt-5.6-* /
    claude-haiku-5-5）的页面把**长上下文档**标成 Standard、把基准档标成
    "Context length <= 272000"，按 Standard 优先会把 2~5 倍的长上下文价
    当成实付价（实测 gpt-6-luna openai-award 0.015 vs 正确 0.0075）。"""
    c = _htmlmod.unescape(str(cond or "")).strip()
    m = _CTX_COND.search(c)
    if m:
        return (0, int(m.group(1)))
    if c.lower() in ("standard", "标准"):
        return (1, 0)
    return (2, 0)


def scrape_page_prices(domain: str, model_names, timeout: float = 15, max_workers: int = 6):
    """对支持价格页的站（Modelflare 等），逐个抓 /pricing/{model} 取**权威价**，
    覆盖 /api/pricing 缺失的自定义组倍率（svip / openai-first-topup / 国产套餐…）。
    返回 {model_name: {"per_1m": {group: {in,out,cr,cw}},
                       "per_second": {group: {"_default": x, "1080p": ...}}}}；
    失败/无数据跳过。

    页面是 SPA，但服务端渲染了一段隐藏的 SEO 内容，价格就在 HTML 表格里：
      <tr><td>claude-stable</td><td>Input</td><td>Standard</td>
          <td>Per 1M tokens</td><td>USD 0.06</td></tr>
    视频模型同一张表，只是单位换成 "Per second"、Conditions 是分辨率：
      <tr><td>Media</td><td>Output</td><td>1080p</td>
          <td>Per second</td><td>USD 0.10192</td></tr>
    注意：早先版本按 "Unit Price ..." 文本匹配，页面改版后一条都匹配不到，
    导致自定义组长期缺失、只能按 group_ratio=1 推算（首充组价被高估几十倍）。
    """
    from concurrent.futures import ThreadPoolExecutor
    import urllib.parse as _up
    domain = (domain or "").rstrip("/")
    if not domain:
        return {}
    out = {}
    row_re = re.compile(
        r"<tr><td>([^<]+)</td><td>([^<]+)</td><td>([^<]+)</td><td>([^<]+)</td>"
        r"<td>\s*(?:USD|US\$|\$)\s*([\d.]+)\s*</td></tr>", re.I)
    kind_map = {"input": "in", "output": "out", "cache read": "cr", "cache write": "cw",
                "输入": "in", "输出": "out", "缓存读取": "cr", "缓存写入": "cw"}

    def one(name):
        url = f"{domain}/pricing/{_up.quote(str(name), safe='')}"
        try:
            st, body, _, err, _ = http_get(url, timeout, {}, 1, 0.5)
        except Exception:
            return name, None
        if st != 200 or not body:
            return name, None
        html = body.decode("utf-8", "replace")
        per_1m, per_second, per_call = {}, {}, {}
        ranks = {}      # (group, kind) → 该行档位排名，用于挑基准档
        for g, kind, cond, unit, price in row_re.findall(html):
            g = g.strip()
            unit_l = unit.strip().lower()
            rk = page_tier_rank(cond)
            if unit_l in ("per 1m tokens", "per 1m token"):
                k = kind_map.get(kind.strip().lower())
                if not k:
                    continue
                key = (g, k)
                # 同组同类型多档（上下文分档 / Standard）取基准档，别把长上下文价当实付价
                if key not in ranks or rk < ranks[key]:
                    per_1m.setdefault(g, {})[k] = float(price)
                    ranks[key] = rk
            elif unit_l in ("per second", "per sec", "per s"):
                # 视频：Conditions 是分辨率（1080p / 720p · Input video…），顺序有意义
                slot = per_second.setdefault(g, {})
                slot[cond.strip()] = float(price)
            elif unit_l in ("per request", "per image", "per call", "per generation"):
                # 图片等：按次计费，Conditions 多为 Standard
                key = (g, "__call")
                if key not in ranks or rk < ranks[key]:
                    per_call.setdefault(g, {})["price"] = float(price)
                    ranks[key] = rk
        for g, rows in per_second.items():
            d = _page_second_price(rows)
            if d is not None:
                rows["_default"] = d
        if not per_1m and not per_second and not per_call:
            return name, None
        return name, {"per_1m": per_1m, "per_second": per_second, "per_call": per_call}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for name, res in ex.map(one, list(model_names)):
            if res:
                out[name] = res
    return out


def parse_exchange_rate(status):
    """现行版取 price（¥ per $），旧版取 usd_exchange_rate，直定价站可能是 exchange_rate。
    price=1 是 one-api 内部虚拟美元占位；真正 ¥1=$1 的站 usd_exchange_rate 也为 1。
    若 price=1 却公布了真实 usd_exchange_rate(≠1)，说明实际充值按国际汇率走，
    优先用它（Portix/PackyCode/RelayApiApp/Apiwarrior 均属此类）。"""
    if not isinstance(status, dict):
        return None, None, None
    data = status.get("data") if isinstance(status.get("data"), dict) else status
    price = to_float(data.get("price"))
    usd = to_float(data.get("usd_exchange_rate"))
    ex = to_float(data.get("exchange_rate"))
    name = data.get("system_name")
    if price == 1.0 and usd is not None and usd != 1.0:
        return usd, usd, name
    return (price if price is not None else (usd if usd is not None else ex)), usd, name


# ---------------------------------------------------------------- 存储

MIN_INPUT_USD_M, MAX_INPUT_USD_M = 1e-4, 5000.0


def sanity_check_models(models):
    """入库前的合理性闸门 → (可用模型, 告警列表)。

    中转站偶尔返回 0、负数，或把 ¥ 当 $ 的整份数据；一旦入库，
    "全网最低价"会被脏值污染且看不出错。这里只丢弃**明显不可能**的值
    （负倍率、输入价超出 1e-4~5000 $/M），可疑但不致命的保留并告警，
    避免因为规则太严把正常模型挡掉。
    """
    ok, warns = [], []
    for m in models:
        name = m.get("model_name") or "?"
        mr = m.get("model_ratio")
        cr = m.get("completion_ratio")
        if not isinstance(mr, (int, float)) or isinstance(mr, bool) or mr < 0:
            warns.append(f"{name}: 倍率非法({mr}) 已丢弃")
            continue
        inp = mr * RATIO_UNIT_USD_PER_M
        if mr and not (MIN_INPUT_USD_M <= inp <= MAX_INPUT_USD_M):
            warns.append(f"{name}: 输入价 {inp:g}$/M 超出合理区间 已丢弃")
            continue
        # 图片/视频按次或按秒计价时 completion_ratio 恒为 0，属正常，不报警。
        # 用 model_kind() 而不是 model_type：new-api 站常把图片模型记成 text。
        media = (m.get("quota_type") == 1
                 or m.get("billing_unit") in ("per_call", "per_second")
                 or model_kind(m) in ("image", "video"))
        if not media and cr is not None and isinstance(cr, (int, float)) and 0 <= cr < 0.05:
            warns.append(f"{name}: 输出倍率偏低(×{cr:g}) 已保留")
        ok.append(m)
    return ok, warns


def _save_feed_official(conn, models, station) -> int:
    """把站点自带的官方价写入 feed_official（按聚合键合并，后写覆盖先写）。

    与快照解耦：价格没变时也要更新，否则新接入的官方价永远补不进去。
    """
    rows = [(m["model_agg"], m.get("official_in_usd_m"), m.get("official_out_usd_m"),
             station["name"], utc_now())
            for m in models if m.get("official_in_usd_m")]
    if rows:
        conn.executemany(
            "INSERT INTO feed_official(model_key, input_usd_m, output_usd_m, source, ts)"
            " VALUES(?,?,?,?,?) ON CONFLICT(model_key) DO UPDATE SET"
            " input_usd_m=excluded.input_usd_m, output_usd_m=excluded.output_usd_m,"
            " source=excluded.source, ts=excluded.ts", rows)
    return len(rows)


def save_snapshot(conn, station: dict, res: dict, force: bool = False):
    """落库一次抓取结果。返回 snapshot_id / 'unchanged' / None(失败)。
    连续失败 ≥3 次自动标记离线；成功则重置。"""
    if not res.get("ok"):
        errors = res.get("errors", [])
        state = classify_failure(errors)
        retryable = state in ("offline", "flaky", "error")
        streak = (station.get("fail_streak") or 0) + 1 if retryable else 0
        offline = 1 if (state == "offline" and streak >= 3) else 0
        conn.execute(
            "INSERT INTO snapshots(station_id, ts, ok, error, domain, latency_ms) "
            "VALUES(?,?,?,?,?,?)",
            (station["id"], utc_now(), 0, " | ".join(errors)[:500],
             res.get("domain"), res.get("latency_ms")))
        conn.execute("UPDATE stations SET fail_streak=?, state=?, offline=? WHERE id=?",
                     (streak, state, offline, station["id"]))
        conn.commit()
        return None

    models, groups, version = parse_pricing(res["pricing"])
    # 合理性闸门：脏数据不入库；模型数骤降也告警（接口改版/被限流的典型信号）
    prev_n = conn.execute(
        "SELECT COUNT(*) c FROM model_prices WHERE snapshot_id="
        "(SELECT MAX(id) FROM snapshots WHERE station_id=? AND ok=1)",
        (station["id"],)).fetchone()["c"]
    models, gate_warns = sanity_check_models(models)
    if prev_n and len(models) < prev_n * 0.5:
        gate_warns.append(f"模型数从 {prev_n} 骤降到 {len(models)}（接口可能改版或被限流）")
    if gate_warns:
        print("  [数据校验] " + station["name"] + "：" + "；".join(gate_warns[:6])
              + (f" …共 {len(gate_warns)} 条" if len(gate_warns) > 6 else ""))
    if not models:
        conn.execute("INSERT INTO snapshots(station_id, ts, ok, error, domain) VALUES(?,?,?,?,?)",
                     (station["id"], utc_now(), 0, "全部模型未通过合理性校验", res.get("domain")))
        conn.commit()
        return None
    ex_rate, ex_usd, sys_name = parse_exchange_rate(res.get("status"))
    conn.execute("UPDATE stations SET fail_streak=0, offline=0, state='active' WHERE id=?", (station["id"],))
    # 价格页基准域名：res["domain"] 是完整接口 URL（含 /api/pricing 路径），
    # 必须剥成 scheme://host 才能拼 /pricing/{model}；兜底用配置的根域名
    def _page_base():
        import urllib.parse as _up
        d = res.get("domain") or ""
        p = _up.urlsplit(d)
        if p.netloc:
            return f"{p.scheme}://{p.netloc}"
        return (station.get("domains") or [""])[0]
    # 统一补齐规范名/变体/家族名（所有格式共用）
    for m in models:
        canon, variant = split_canonical(m["model_name"])
        m["model_canon"] = canon
        m["model_agg"] = agg_key(canon)
        m["model_variant"] = variant
        m["model_family"] = model_family(canon)

    # 整份原始响应存档：内容未变则不重复存（只留 hash，读时沿上一条继承）
    raw_blob, raw_hash = pack_raw_pricing(res.get("pricing"))

    if not force and version:
        last = conn.execute(
            "SELECT id, pricing_version, page_prices, raw_pricing, raw_hash FROM snapshots"
            " WHERE station_id=? AND ok=1 ORDER BY id DESC LIMIT 1", (station["id"],)).fetchone()
        if last and last["pricing_version"] == version:
            # 未变但仍补抓价格页（仅当该快照尚无 page_prices），避免自定义组价长期缺失
            if station.get("page_prices") and not last["page_prices"] and models:
                try:
                    pm = scrape_page_prices(_page_base(),
                                            [m["model_name"] for m in models], timeout=12)
                    if pm:
                        pm = merge_media_prices(pm, models)
                        conn.execute("UPDATE snapshots SET page_prices=? WHERE id=?",
                                     (jdump(pm), last["id"]))
                        conn.commit()
                except Exception:
                    pass
            # 老快照没有 raw 存档时补一次，否则历史 raw 永久缺失
            if raw_blob and not last["raw_pricing"] and last["raw_hash"] != raw_hash:
                conn.execute("UPDATE snapshots SET raw_pricing=?, raw_hash=? WHERE id=?",
                             (raw_blob, raw_hash, last["id"]))
                conn.commit()
            # 官方价是独立于调价的元数据：即便价格没变也要更新一次，
            # 否则首次接入 feed_official 时永远写不进去
            _save_feed_official(conn, models, station)
            conn.commit()
            return "unchanged"

    # 价格页权威组价（覆盖 ratio 推算缺失的自定义组）；仅对标记 page_prices 的站触发
    page_prices_json = None
    if station.get("page_prices") and models:
        pm = {}
        try:
            pm = scrape_page_prices(_page_base(),
                                    [m["model_name"] for m in models], timeout=12) or {}
        except Exception:
            pm = {}
        # 页面 404 的隐藏模型（nsfw 等）：用接口的媒体直定价补齐按秒/按次价
        pm = merge_media_prices(pm, models)
        if pm:
            page_prices_json = jdump(pm)
    # 与上一条内容相同则不重复存 BLOB（raw_hash 相同即可判定，读时继承）
    prev = conn.execute("SELECT raw_hash FROM snapshots WHERE station_id=? AND ok=1"
                        " ORDER BY id DESC LIMIT 1", (station["id"],)).fetchone()
    store_blob = raw_blob if (raw_blob and (not prev or prev["raw_hash"] != raw_hash)) else None
    cur = conn.execute(
        "INSERT INTO snapshots(station_id, ts, ok, exchange_rate,"
        " usd_exchange_rate, system_name, latency_ms, domain, pricing_version, page_prices,"
        " raw_pricing, raw_hash)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (station["id"], utc_now(), 1, ex_rate, ex_usd,
         sys_name, res.get("latency_ms"), res.get("domain"), version, page_prices_json,
         store_blob, raw_hash))
    sid = cur.lastrowid
    conn.executemany(
        "INSERT INTO model_prices(snapshot_id, model_name, model_key, model_canon, model_agg,"
        " model_variant, model_family, vendor_id, quota_type, model_ratio, completion_ratio,"
        " cache_ratio, create_cache_ratio, model_price, model_type, model_tags,"
        " enable_groups, tiers, supported_endpoints, billing_unit, video_resolution_price,"
        " image_size_price)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(sid, m["model_name"], m["model_key"], m["model_canon"], m["model_agg"],
          m["model_variant"], m["model_family"],
          m["vendor_id"], m["quota_type"], m["model_ratio"], m["completion_ratio"],
          m["cache_ratio"], m["create_cache_ratio"], m["model_price"],
          m.get("model_type"), m.get("model_tags"),
          jdump(m["enable_groups"]), jdump(m["tiers"]),
          jdump(m["supported_endpoint_types"]),
          m.get("billing_unit"),
          jdump(m["video_resolution_price"]) if m.get("video_resolution_price") else None,
          jdump(m["image_size_price"]) if m.get("image_size_price") else None)
         for m in models])
    conn.executemany(
        "INSERT INTO group_ratios(snapshot_id, group_name, ratio) VALUES(?,?,?) "
        "ON CONFLICT(snapshot_id, group_name) DO UPDATE SET ratio=excluded.ratio",
        [(sid, g, r) for g, r in groups.items()])
    _save_feed_official(conn, models, station)
    conn.commit()
    # 与上一次快照比对，记录价格异动（阈值 1%）
    try:
        ts = conn.execute("SELECT ts FROM snapshots WHERE id=?", (sid,)).fetchone()["ts"]
        res["_changes"] = record_price_changes(conn, station["id"], sid, ts, threshold=1.0)
    except Exception:
        res["_changes"] = 0
    return sid


# ---------------------------------------------------------------- 价格计算

def base_prices(m: dict):
    """new-api 计价单位：$/M tokens。输出价 = 输入价 × completion_ratio。"""
    mr = m["model_ratio"] or 0.0
    cr = m["completion_ratio"] if m["completion_ratio"] is not None else 1.0
    inp = mr * RATIO_UNIT_USD_PER_M
    out = mr * cr * RATIO_UNIT_USD_PER_M
    return inp, out, inp * (m["cache_ratio"] or 0.0), inp * (m["create_cache_ratio"] or 0.0)


def tiered_prices(m: dict, gratio: float):
    """阶梯计价：输入 tokens ≥ threshold 时，该档 ratio 替换基础倍率（对整次请求生效）。
    返回 [{threshold, ratio, input_usd_m, output_usd_m}]（已含分组折扣）。"""
    out = []
    for t in json.loads(m.get("tiers") or "[]") or []:
        if not isinstance(t, dict):
            continue
        th = to_float(t.get("threshold") or t.get("min_input_tokens"))
        tr = to_float(t.get("ratio"))
        if th is None or tr is None:
            continue
        out.append({
            "threshold": int(th), "ratio": tr,
            "input_usd_m": tr * RATIO_UNIT_USD_PER_M * gratio,
            "output_usd_m": tr * (m["completion_ratio"] or 1.0) * RATIO_UNIT_USD_PER_M * gratio,
        })
    return sorted(out, key=lambda x: x["threshold"])


def tier_for(tiers, tokens_in: int, base_ratio, completion_ratio, gratio):
    """给定输入长度，返回该次请求的实付单价（$/M）。"""
    unit = RATIO_UNIT_USD_PER_M * gratio
    ratio = base_ratio or 0.0
    for t in tiers or []:
        if tokens_in >= t["threshold"]:
            ratio = t["ratio"]
    return ratio * unit, ratio * (completion_ratio or 1.0) * unit


# 视频类模型按「秒」计价：站点把 model_price 定义为每秒基础单价，
# 实际扣费 = 生成秒数 × 单价 × 分辨率/步数倍率；图片、音乐等才是真正的「每次」。
_VIDEO_HINTS = ("video", "t2v", "i2v", "r2v", "veo", "seedance", "kling", "hailuo",
                "sora", "vidu", "pika", "runway", "mochi", "cogvideo", "pixverse",
                "happyhorse", "luma", "minimax-h", "seedance")
_IMAGE_HINTS = ("image", "dall-e", "seedream", "flux", "midjourney", "mj-", "nanobanana",
                "imagen", "stable-diffusion", "kolors", "recraft", "ideogram", "rmbg", "upscale")
_AUDIO_HINTS = ("suno", "music", "tts", "audio", "speech")
# 名称里出现即确定是「音频模型」（含语音合成/识别，如 gpt-4o-mini-transcribe）
_AUDIO_NAME_HINTS = _AUDIO_HINTS + ("voice", "whisper", "transcribe")
# 名字里出现即确定是「生成视频」（压过 image 判据，如 kling-avatar-image2video）
_VIDEO_STRONG = ("video", "2video", "t2v", "i2v", "r2v")


def billing_of(m: dict) -> str:
    """判定计费方式：per_token（按 token）/ per_call（按次，图片·音乐）/ per_second（按秒，视频）。

    站点若给了 model_type/tags（new-api 系常见），优先采信它的显式标注；
    没有标注时按模型名关键词兜底。只有「非 token 计价」的模型才需要区分按次/按秒，
    带 model_ratio 的对话模型一律 per_token。"""
    if m["quota_type"] != 1 and not (m["model_price"] or 0) > 0:
        return "per_token"
    marks = f"{m.get('model_tags') or ''},{m.get('model_type') or ''}".lower()
    # 显式标注优先：音乐/绘画/图像 → 按次；视频 → 按秒（"音视频"含视频，但音乐先判）
    if any(k in marks for k in ("音乐", "music", "绘画", "图像")):
        return "per_call"
    if "视频" in marks or "video" in marks:
        return "per_second"
    name = normalize_model(m["model_name"])
    key = f"{name}/{m.get('model_canon') or ''}/{m.get('model_family') or ''}".lower()
    # 生成视频的强信号优先于 image：如 kling-avatar-image2video、xxx-video-edit
    if any(h in key for h in _VIDEO_STRONG):
        return "per_second"
    if any(h in key for h in _IMAGE_HINTS) or any(h in key for h in _AUDIO_HINTS):
        return "per_call"
    if any(h in key for h in _VIDEO_HINTS):
        return "per_second"
    return "per_call"


def model_kind(m: dict) -> str:
    """模型大类：text（语言）/ image（图片）/ video（视频）/ audio（音频）。
    看板四个筛选按钮用它分组；口径与 billing_of 保持一致：
    先看站点给的 model_type/tags，再用模型名关键词兜底。

    判定优先级经过真实数据校对（站点标注并不总是可靠，例如 Portix 把
    seedance-2-0-mini 标成 audio）：
      1 名称含 video/2video/t2v/i2v/r2v  → video（生成视频的硬信号）
      2 名称含 audio/tts/speech/voice…    → audio（kling-audio、gpt-4o-audio）
      3 名称含 image/dall-e/seedream…     → image（aigc-image-kling 不能被 kling 抢走）
      4 站点标注含「视频」                 → video
      5 名称含视频品牌词（seedance/kling/veo/sora…）→ video
      6 站点标注含「音乐/音频」            → audio（"音视频" 里的音频标注靠这一步）
      7 站点标注含「绘画/图像」            → image
      8 其余                              → text"""
    marks = f"{m.get('model_tags') or ''},{m.get('model_type') or ''}".lower()
    name = f"{normalize_model(m['model_name'])}/{m.get('model_canon') or ''}" \
           f"/{m.get('model_family') or ''}".lower()
    if any(h in name for h in _VIDEO_STRONG):
        return "video"
    if any(h in name for h in _AUDIO_NAME_HINTS):
        return "audio"
    if any(h in name for h in _IMAGE_HINTS):
        return "image"
    if "视频" in marks or "video" in marks:
        return "video"
    if any(h in name for h in _VIDEO_HINTS):
        return "video"
    if any(k in marks for k in ("音乐", "音频", "语音", "music", "audio", "tts", "speech")):
        return "audio"
    if "绘画" in marks or "图像" in marks:
        return "image"
    return "text"


def load_model_whitelist():
    """「语言模型」筛选按钮的白名单（小写条目列表），见 model_whitelist.json。"""
    global _WHITELIST
    if _WHITELIST is None:
        try:
            with open(MODEL_WHITELIST_PATH, encoding="utf-8") as f:
                data = json.load(f)
            items = (data.get("text") if isinstance(data, dict) else data) or []
            _WHITELIST = [str(x).strip().lower() for x in items if x]
        except Exception:
            _WHITELIST = []
    return _WHITELIST


def load_vendor_map():
    """vendor_map.json：{default, normalize, exact, prefix, contains}。缺失时返回空表
    （vendor_of 一律退回「其他/未识别」），不影响抓取主流程。"""
    global _VENDOR_MAP
    if _VENDOR_MAP is None:
        try:
            with open(VENDOR_MAP_PATH, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
        _VENDOR_MAP = {
            "default": str(data.get("default") or "其他/未识别"),
            "normalize": {str(k): str(v) for k, v in (data.get("normalize") or {}).items()},
            "exact": {str(k).lower(): str(v) for k, v in (data.get("exact") or {}).items()},
            "prefix": [(str(p).lower(), str(c)) for p, c in (data.get("prefix") or [])],
            "contains": [(str(p).lower(), str(c)) for p, c in (data.get("contains") or [])],
        }
    return _VENDOR_MAP


@lru_cache(maxsize=None)
def vendor_of(canon: str) -> str:
    """模型规范名 → 出品公司。顺序：exact > prefix（按表内顺序，具体在前）
    > contains > default。规则见 vendor_map.json，改规则不用动代码。"""
    vm = load_vendor_map()
    c = str(canon or "").strip().lower()
    if not c:
        return vm["default"]
    hit = vm["exact"].get(c)
    if hit:
        return vm["normalize"].get(hit, hit)
    for pat, comp in vm["prefix"]:
        if c.startswith(pat):
            return vm["normalize"].get(comp, comp)
    for pat, comp in vm["contains"]:
        if pat in c:
            return vm["normalize"].get(comp, comp)
    return vm["default"]


@lru_cache(maxsize=None)
def in_model_whitelist(canon: str) -> bool:
    """是否在「语言模型」白名单内。按前缀匹配：gpt-5.6 覆盖 gpt-5.6-cyber、
    qwen3-coder 覆盖 qwen3-coder-plus / -next。白名单为空时退化为按 model_kind==text。"""
    if not canon:
        return False
    c = str(canon).lower()
    wl = load_model_whitelist()
    if wl:
        return any(c.startswith(w) for w in wl)
    return True


def load_station_rules():
    """站点私有计价规则（model_price 的前端倍率），见 station_pricing_rules.json。"""
    global _STATION_RULES
    if _STATION_RULES is None:
        try:
            with open(STATION_RULES_PATH, encoding="utf-8") as f:
                _STATION_RULES = json.load(f)
        except Exception:
            _STATION_RULES = {}
    return _STATION_RULES


def station_price_rule(station: str, m: dict):
    """命中站点私有规则时返回 (multiplier, billing)，否则 (None, None)。

    部分站的 /api/pricing 里 model_price 只是「内部额度单价」，真实挂牌价
    还要乘站内硬编码倍率（如 Portix 的 happyhorse：×90 且按秒，见
    station_pricing_rules.json 的核对记录）。规则未命中时返回 None，保持原值。"""
    rules = (load_station_rules().get(station) or {}).get("rules") or []
    name = str(m.get("model_name") or "")
    low = name.lower()
    for r in rules:
        hit = name in (r.get("exact") or {})
        if not hit:
            hit = any(low.startswith(str(p).lower()) for p in (r.get("prefix") or []))
        if hit:
            return float(r.get("multiplier") or 1), (r.get("billing") or None)
    return None, None


def _size_area(size) -> int:
    """'1024x768' / '1920×1080' → 像素面积，用于按尺寸从小到大排序；解析不了返回 0。"""
    m = re.match(r"\s*(\d+)\s*[x×*]\s*(\d+)\s*$", str(size or ""), re.I)
    return int(m.group(1)) * int(m.group(2)) if m else 0


def _size_levels(raw):
    """接口的「按尺寸」价目表 → 有序 {size: 每张价}。
    兼容 [{size, price}, ...]（Aixoras image_size_price）与 {size: price}。"""
    pairs = []
    if isinstance(raw, list):
        for x in raw:
            if isinstance(x, dict):
                pairs.append((x.get("size"), x.get("price")))
    elif isinstance(raw, dict):
        pairs = list(raw.items())
    out = {}
    for size, price in pairs:
        fv = to_float(price)
        if size and fv is not None:
            out[str(size).strip()] = fv
    return dict(sorted(out.items(), key=lambda kv: (_size_area(kv[0]), kv[0])))


def _sec_levels(rows):
    """把 per_second 的 {分辨率: 价, "_default": x} 展开成 [(分辨率, 价)]，保留原顺序。"""
    out = []
    if isinstance(rows, dict):
        for k, v in rows.items():
            if k == "_default":
                continue
            fv = to_float(v)
            if fv is not None:
                out.append((str(k), fv))
    return out


def model_group_rows(m: dict, groups: dict, page_map: dict = None):
    """展开为 模型 × 分组 行；分组取 enable_groups 与 group_ratio 的并集。
    enable_groups 为空 = 该模型在所有分组可用（new-api 语义：无分组限制）；
    非空 = 仅在列出分组可用，其余分组标「否」（保留作审计，但不参与最低价计算）。
    page_map: 价格页抓取的权威按组价 {model_name:{group:[in$,out$]}}，命中则直接
    用绝对价覆盖 ratio 推算（自定义组倍率 API 不返回时的正确来源）。"""
    page_map = page_map or {}
    pm_raw = page_map.get(m["model_name"]) or {}
    pm, pm_sec, pm_call = _split_page_map(pm_raw)
    enabled = [str(g) for g in json.loads(m["enable_groups"] or "[]")]
    names = sorted(set(enabled) | set(groups.keys()) | set(pm.keys())
                   | set(pm_sec.keys()) | set(pm_call.keys()))
    if not names:
        names = [""]
    inp, out, crd, cwr = base_prices(m)
    # 接口直定的按秒价（model_prices.video_resolution_price）：价格页 404 时的兜底，
    # 页面有该组数据时仍以页面为准（下面分支优先命中 pm_sec）
    api_levels = []
    api_sizes = {}
    mkeys = m.keys() if hasattr(m, "keys") else ()
    if "video_resolution_price" in mkeys and m["video_resolution_price"]:
        try:
            vres = json.loads(m["video_resolution_price"])
        except (TypeError, ValueError):
            vres = None
        if isinstance(vres, dict) and vres:
            api_levels = _sec_levels(vres)
    # 图片「按尺寸」价（Aixoras billing_mode=image_size）：每张图一个价，按像素面积升序
    if "image_size_price" in mkeys and m["image_size_price"]:
        raw_sizes = m["image_size_price"]
        if isinstance(raw_sizes, str):
            try:
                raw_sizes = json.loads(raw_sizes)
            except (TypeError, ValueError):
                raw_sizes = None
        if isinstance(raw_sizes, (dict, list)):
            api_sizes = _size_levels(raw_sizes)
    for g in names:
        gratio = groups.get(g, 1.0)
        if g in pm_sec:
            # 视频：按分辨率逐档展开（每档一行）。不能只取「第一行」——站点把旗舰档
            # （1080p/2k/4k）放最前，只取第一行会让面板永远显示最贵的分辨率。
            levels = sorted(_sec_levels(pm_sec[g]), key=lambda kv: kv[1])
            if levels:
                for res, pv in levels:
                    yield g, 1.0, True, 0.0, 0.0, 0.0, 0.0, pv, None, res
            else:
                yield g, 1.0, True, 0.0, 0.0, 0.0, 0.0, \
                    _page_second_price(pm_sec[g]), None, ""
        elif g in pm_call:
            # 图片等：页面权威「每次」价（Conditions 多为 Standard）
            yield g, 1.0, True, 0.0, 0.0, 0.0, 0.0, None, to_float(pm_call[g]), ""
        elif api_levels and (not enabled or g in enabled):
            # 接口权威「每秒」价兜底（页面无该模型/该组），同样按分辨率逐档展开。
            # 接口给的是**基准价**，站点前端展示价 = 基准价 × 分组倍率，这里一并乘上
            for res, pv in sorted(api_levels, key=lambda kv: kv[1]):
                yield g, 1.0, True, 0.0, 0.0, 0.0, 0.0, pv * gratio, None, res
        elif api_sizes and (not enabled or g in enabled) and not api_levels:
            # 图片按尺寸价：每档一行（分辨率列放尺寸），实付 = 每张价 × 分组倍率
            for size, pv in api_sizes.items():
                yield g, 1.0, True, 0.0, 0.0, 0.0, 0.0, None, pv * gratio, size
        elif g in pm:
            # 权威绝对价（价格页表格）。页面价 = 基准价 × 分组倍率，所以能用
            # 「基准价 + 真实倍率」表达时优先这样返回：面板才会显示 openai-award 0.03
            # 而不是 1，且实付仍等于页面价。对不上（倍率未知/站点另有调价）才退回
            # 「页面绝对价 + 倍率 1」，避免把实付价再乘一次。
            row = pm[g]
            if isinstance(row, dict):
                pi, po = to_float(row.get("in")), to_float(row.get("out"))
                pcr, pcw = to_float(row.get("cr")), to_float(row.get("cw"))
            else:  # 兼容旧格式 [in, out]
                pi, po, pcr, pcw = to_float(row[0]), to_float(row[1]), None, None
            if pi is None and po is None:
                continue        # 页面该组没有任何绝对价，跳过（不产出误导行）
            pi = pi if pi is not None else inp
            po = po if po is not None else out
            ratio = groups.get(g)
            if ratio is not None and inp is not None and out is not None:

                def _same(a, b):
                    # b 可能是 0（如 jev-1.13 completion_ratio=0，输出价本就是 0），
                    # 不能用 `b` 的真假值判断
                    if a is None or b is None:
                        return False
                    return abs(a - b) <= max(1e-9, abs(b) * 1e-6)

                if _same(pi, inp * ratio) and _same(po, out * ratio):
                    yield g, ratio, True, inp, out, crd, cwr, None, None, ""
                    continue
            cr = pcr if pcr is not None else pi * (m["cache_ratio"] or 0.0)
            cw = pcw if pcw is not None else pi * (m["create_cache_ratio"] or 0.0)
            yield g, 1.0, True, pi, po, cr, cw, None, None, ""
        else:
            # 按次计费模型：model_price 是**折前**基准价，实付 = 基准 × 分组倍率。
            # 依据有两条：
            #   ① model_plaza（hahacode）：0.008 × 0.15 = 0.0012/张，与站点展示一致；
            #   ② 传统 new-api：Portix 已用真实调用实测核对过同一条公式
            #      （0.0014706 × 0.715 × 90 ≈ 站点显示的 $0.0946/秒）。
            # 以前这里不产出 call，上层只能拿 model_price 原值，倍率被整段丢掉。
            call_price = None
            mp = to_float(m.get("model_price"))
            if mp:
                call_price = mp * gratio
            yield g, gratio, (not enabled or g in enabled), inp, out, crd, cwr, None, call_price, ""


def fmt(v, nd=10):
    """输出浮点：不用科学计数法，且不再截断（0.0000595905 这类极小价格必须保精度）。"""
    if v is None:
        return ""
    if isinstance(v, float):
        if v == 0:
            return "0"
        s = f"{v:.{nd}f}".rstrip("0").rstrip(".")
        return s if s not in ("", "-") else "0"
    return str(v)


# ---------------------------------------------------------------- 子命令

def run_scrape_round(conn, args) -> dict:
    """执行一轮抓取并落库，返回轮摘要：
    {new, unchanged, failed, total, changes: [本轮异动行]}。供 cmd_scrape / cmd_watch 共用。"""
    round_start = utc_now()
    stations = load_stations(conn)
    targets = [s for s in stations if not args.station or s["name"] in args.station]
    if not args.include_offline:
        targets = [s for s in targets if (s.get("state") or "active") == "active"]
    if not targets:
        print("没有匹配的站点（被跳过的站可用 --include-offline 强制重试）")
        return {"new": 0, "unchanged": 0, "failed": 0, "total": 0, "changes": [],
                "empty": True}
    blocked = [s for s in stations if (s.get("state") or "active") != "active"]
    if blocked and not args.include_offline:
        detail = "、".join(f"{s['name']}({s['state']})" for s in blocked)
        print(f"跳过 {len(blocked)} 个非活跃站（--include-offline 重试）：{detail}")
    print(f"并发 {args.concurrency}，超时 {args.timeout}s，抓取 {len(targets)} 个站点 …")
    results = {}
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = {ex.submit(fetch_station, s, args.timeout, args.retries, args.backoff): s
                for s in targets}
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                results[s["name"]] = fut.result()
            except Exception as e:
                results[s["name"]] = {"ok": False, "errors": [str(e)], "domain": None,
                                      "latency_ms": None, "pricing": None, "status": None}
    n_new = n_unch = n_fail = n_changes = 0
    avail_pending = []
    for s in targets:
        res = results[s["name"]]
        # 可用性检测：抓取结果已含该站当前在售模型，与落库前的上一份快照比对，
        # 零额外请求即可得到上架/下架结论（要在 save_snapshot 之前取基线）
        if res.get("ok"):
            prev_models = _avail_snapshot_models(conn, s["id"])
            cur_models = _avail_models_of(res)
            for key, name in cur_models.items():
                if key not in prev_models:
                    avail_pending.append((s["id"], key, name, "listed", "新上架"))
            for key, name in prev_models.items():
                if key not in cur_models:
                    avail_pending.append((s["id"], key, name, "gone", "已下架"))
        outcome = save_snapshot(conn, s, res, force=args.force)
        # 只要这次成功取到数据就记一次「检查时间」。快照只在内容变化时写入，
        # 长期不调价的站会被误判成"两天没抓过"——检查时间才是采集是否还在跑的证据。
        if outcome is not None:
            conn.execute("UPDATE stations SET last_check=? WHERE id=?", (utc_now(), s["id"]))
        if outcome is None:
            n_fail += 1
            state = classify_failure(res.get("errors"))
            hint = {"auth_required": " → 需注册后 add --cookie",
                    "unsupported": " → 非标准路由，需抓包定位接口",
                    "offline": " → 域名/网络问题",
                    "flaky": " → 网络抖动，可重试"}.get(state, "")
            print(f"  [失败:{state}] {s['name']:<12} "
                  f"{(res.get('errors') or ['?'])[0][:60]}{hint}")
        elif outcome == "unchanged":
            n_unch += 1
            print(f"  [未变] {s['name']:<14} pricing_version 未变化，跳过落库")
        else:
            n_new += 1
            models, groups, version = parse_pricing(res["pricing"])
            fmt_tag = {"newapi": "倍率", "direct": "直定价", "html_rsc": "RSC"}.get(res.get("format"), res.get("format") or "")
            chg = res.get("_changes") or 0
            chg_txt = f"  ⚠ 价格异动 {chg} 处" if chg else ""
            print(f"  [成功] {s['name']:<14} {res['domain']}  模型 {len(models):>3}  "
                  f"分组 {len(groups):>2}  延迟 {res['latency_ms']}ms  {fmt_tag}  "
                  f"版本 {(version or '-')[:12]}{chg_txt}")
            if chg:
                n_changes += chg
    # 可用性变更落库（上架/下架）
    n_up = sum(1 for p in avail_pending if p[3] == "listed")
    n_down = sum(1 for p in avail_pending if p[3] == "gone")
    if avail_pending:
        ts = utc_now()
        conn.executemany(
            "INSERT INTO model_availability(station_id, model_agg, model_name, ts, status,"
            " detail, latency_ms) VALUES(?,?,?,?,?,?,?)",
            [(sid, key, name, ts, status, detail, None)
             for sid, key, name, status, detail in avail_pending])
        conn.commit()
        print(f"  [可用性] 新上架 {n_up}，已下架 {n_down}（明细见 `availability --report`）")
    # 每轮抓取后自动备份：数据是几个月攒下来的，丢了没法重抓
    if not getattr(args, "no_backup", False):
        try:
            path = backup_database(conn, BACKUP_DIR, BACKUP_KEEP)
            print(f"  [备份] {os.path.basename(path)}")
        except Exception as e:
            print(f"  [备份失败] {type(e).__name__}: {str(e)[:80]}")
    print(f"\n汇总：新增快照 {n_new}，未变 {n_unch}，失败 {n_fail}，共 {len(targets)}")
    if n_changes:
        print(f"检测到价格异动 {n_changes} 处（明细见 `alerts` 命令）")
    changes = conn.execute("""
        SELECT pc.model_canon AS model, pc.grp AS grp, pc.pct AS pct,
               pc.old_input_usd_m AS old, pc.new_input_usd_m AS new, s.name AS station
        FROM price_changes pc JOIN stations s ON s.id = pc.station_id
        WHERE pc.ts >= ? ORDER BY ABS(pc.pct) DESC""", (round_start,)).fetchall()
    return {"new": n_new, "unchanged": n_unch, "failed": n_fail, "total": len(targets),
            "changes": [dict(r) for r in changes], "empty": False}


def cmd_scrape(args):
    conn = connect()
    s = run_scrape_round(conn, args)
    return 1 if s.get("empty") else 0


def cmd_list(args):
    conn = connect()
    rows = conn.execute("""
        SELECT s.name, s.mode, s.state, s.offline, s.fail_streak,
               (SELECT ts FROM snapshots WHERE station_id=s.id ORDER BY id DESC LIMIT 1) AS last_ts,
               (SELECT ok FROM snapshots WHERE station_id=s.id ORDER BY id DESC LIMIT 1) AS last_ok,
               (SELECT error FROM snapshots WHERE station_id=s.id ORDER BY id DESC LIMIT 1) AS last_err,
               (SELECT COUNT(*) FROM snapshots WHERE station_id=s.id AND ok=1) AS n_ok,
               (SELECT COUNT(*) FROM model_prices mp JOIN snapshots k ON mp.snapshot_id=k.id
                 WHERE k.station_id=s.id AND k.ok=1
                 AND k.id=(SELECT MAX(id) FROM snapshots WHERE station_id=s.id AND ok=1)) AS n_models,
               (SELECT ts FROM probes WHERE station_id=s.id ORDER BY id DESC LIMIT 1) AS probe_ts,
               (SELECT ok FROM probes WHERE station_id=s.id ORDER BY id DESC LIMIT 1) AS probe_ok,
               (SELECT latency_ms FROM probes WHERE station_id=s.id ORDER BY id DESC LIMIT 1) AS probe_lat
        FROM stations s WHERE s.enabled=1 ORDER BY s.id""").fetchall()
    print(f"{'站点':<16}{'状态':<14}{'模式':<10}{'模型数':>5}{'成功快照':>8}  最近抓取(UTC)"
          f"   可达性(最近探测)")
    STATE_CN = {"active": "正常", "auth_required": "需登录Cookie", "unsupported": "接口不支持",
                "offline": "离线", "flaky": "网络抖动", "error": "异常"}
    for r in rows:
        # 状态优先级：最近一次成功 > 已分类的非活跃状态 > 由最近错误推断
        if r["last_ok"] == 1:
            status = f"抖动×{r['fail_streak']}" if r["fail_streak"] else "正常"
        elif (r["state"] or "active") != "active":
            status = STATE_CN.get(r["state"], r["state"])
        elif r["last_ok"] == 0:
            status = STATE_CN.get(classify_failure([r["last_err"] or ""]), "失败")
        else:
            status = "未抓取"
        if r["probe_ts"]:
            reach = ("可达" if r["probe_ok"] else "不可达") + \
                    (f" {r['probe_lat']}ms" if r["probe_lat"] else "") + \
                    f" @{(r['probe_ts'] or '')[:16].replace('T', ' ')}"
        else:
            reach = "未探测"
        print(f"{r['name']:<16}{status:<14}{(r['mode'] or 'newapi'):<10}{(r['n_models'] or 0):>5}"
              f"{r['n_ok']:>8}  {r['last_ts'] or '-'}   {reach}")
        if r["last_ok"] == 0 and r["last_err"]:
            print(f"    ↳ {r['last_err'][:100]}")
    return 0


_STATIONS_SYNC_MTIME = None


def _sync_stations_if_changed(conn):
    """stations.json 变更后才同步进 DB。
    原来每次读接口都 load_stations()，会对 41 个站各写 3 条 SQL（约 123 次写），
    在看板每次请求里白跑一遍；按 mtime 判断可完全跳过。"""
    global _STATIONS_SYNC_MTIME
    try:
        mt = os.path.getmtime(STATIONS_PATH)
    except OSError:
        mt = None
    if mt is not None and mt == _STATIONS_SYNC_MTIME:
        return
    load_stations(conn)
    _STATIONS_SYNC_MTIME = mt


def latest_snapshots(conn, station_id=None):
    # 先同步 stations.json（充值比例手动覆盖等字段），保证读到的站点元数据最新
    _sync_stations_if_changed(conn)
    # 只取 enabled=1 的站：禁用（enabled=0）的站不再出现在汇总/对比/导出里，
    # 但历史快照仍留在库里，随时 enabled=1 就能恢复。
    # station_id 传值时只取该站最新快照——单站页不必再把全站 7 万行生成一遍再过滤。
    if station_id is not None:
        return conn.execute("""
            SELECT s.* FROM snapshots s
            JOIN (SELECT station_id, MAX(id) mid FROM snapshots
                  WHERE ok=1 AND station_id=? GROUP BY station_id) t ON s.id = t.mid
            JOIN stations st ON st.id = s.station_id AND st.enabled = 1
            ORDER BY s.station_id""", (station_id,)).fetchall()
    return conn.execute("""
        SELECT s.* FROM snapshots s
        JOIN (SELECT station_id, MAX(id) mid FROM snapshots WHERE ok=1 GROUP BY station_id) t
          ON s.id = t.mid
        JOIN stations st ON st.id = s.station_id AND st.enabled = 1
        ORDER BY s.station_id""").fetchall()


def eff_input_map(conn, snapshot_id: int) -> dict:
    """某快照的 {(规范名, 分组): 实付输入价 $/M}，用于跨快照比对。"""
    out = {}
    groups = {r["group_name"]: r["ratio"]
              for r in conn.execute("SELECT group_name, ratio FROM group_ratios WHERE snapshot_id=?",
                                    (snapshot_id,))}
    for m in conn.execute("SELECT * FROM model_prices WHERE snapshot_id=?", (snapshot_id,)):
        m = dict(m)
        canon = m.get("model_canon") or canonical_model(m["model_name"])
        base = (m["model_ratio"] or 0.0) * RATIO_UNIT_USD_PER_M
        enabled = set(json.loads(m["enable_groups"] or "[]"))
        names = sorted(enabled | set(groups.keys())) or [""]
        for g in names:
            out[(canon, g)] = base * groups.get(g, 1.0)
    return out


def record_price_changes(conn, station_id: int, sid: int, ts: str, threshold: float = 1.0):
    """与上一次成功快照比对，记录超过阈值的价格异动。"""
    prev = conn.execute(
        "SELECT id FROM snapshots WHERE station_id=? AND ok=1 AND id<? ORDER BY id DESC LIMIT 1",
        (station_id, sid)).fetchone()
    if not prev:
        return 0
    old, new = eff_input_map(conn, prev["id"]), eff_input_map(conn, sid)
    rows = []
    for key, nv in new.items():
        ov = old.get(key)
        if ov is None or ov <= 0 or nv <= 0:
            continue
        pct = (nv - ov) / ov * 100
        if abs(pct) < threshold:
            continue
        rows.append((station_id, ts, key[0], key[1], ov, nv, round(pct, 2)))
    if rows:
        conn.executemany(
            "INSERT INTO price_changes(station_id, ts, model_canon, grp, old_input_usd_m,"
            " new_input_usd_m, pct) VALUES(?,?,?,?,?,?,?)", rows)
        conn.commit()
    return len(rows)


def _probe_history_map(conn, n: int = 12) -> dict:
    """每站最近 n 次可达性探测结果：{station_id: [{ok, ts}, ...]}（旧 → 新）。
    站点 id → 名称的映射一并返回，便于按站名直接取。"""
    by_st = {}
    try:
        rows = conn.execute(
            "SELECT station_id, ok, ts FROM ("
            "  SELECT id, station_id, ok, ts,"
            "         ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY id DESC) rn"
            "  FROM probes) WHERE rn<=? ORDER BY station_id, id", (n,)).fetchall()
    except Exception:                       # 老版本 SQLite 不支持窗口函数时退回
        rows = []
        for r in conn.execute("SELECT station_id, ok, ts FROM probes ORDER BY id"):
            rows.append(r)
        keep = {}
        for r in reversed(rows):
            keep.setdefault(r["station_id"], [])
            if len(keep[r["station_id"]]) < n:
                keep[r["station_id"]].append(r)
        rows = [r for lst in keep.values() for r in reversed(lst)]
    for r in rows:
        by_st.setdefault(r["station_id"], []).append({"ok": bool(r["ok"]), "ts": r["ts"]})
    names = {r["id"]: r["name"] for r in conn.execute("SELECT id, name FROM stations")}
    return by_st, names


def _probe_window_map(conn, hours: int = 72) -> dict:
    """每站最近 hours 小时内的可达性探测：{station_id: [{ok, ts}, ...]}（旧→新）。
    供「72 小时可用率」统计；probes.ts 为 UTC ISO 字符串，按字典序比较即可。"""
    by_st = {}
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(timespec="seconds")
        rows = conn.execute(
            "SELECT station_id, ok, ts FROM probes WHERE ts>=? ORDER BY station_id, id",
            (cutoff,)).fetchall()
        for r in rows:
            by_st.setdefault(r["station_id"], []).append({"ok": bool(r["ok"]), "ts": r["ts"]})
    except Exception:                       # probes 表不存在或尚未探测过时返回空
        pass
    return by_st


def _model_probe_map(conn, n: int = 12, hours: int = 72) -> dict:
    """按 (station_id, model_agg) 汇总「真实调用探测」的时间窗数据：
    {(sid, model): {'hist': [{'ok','ts','status'}] 旧→新 ≤n, 'avail': {'p','ok','tot','src'}|None}}。
    只统计 probe_ok/probe_fail —— 这两类是每次探测都写入的周期采样；
    listed/gone 是变更日志（只在状态改变时写），不能用来算比率。"""
    out: dict = {}
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(timespec="seconds")
        rows = conn.execute(
            "SELECT station_id, model_agg, status, ts FROM model_availability"
            " WHERE status IN ('probe_ok','probe_fail') ORDER BY id").fetchall()
    except Exception:                       # model_availability 表缺失/无真实探测
        return out
    for r in rows:
        key = (r["station_id"], r["model_agg"])
        d = out.setdefault(key, {"hist": [], "avail": None, "_ok": 0, "_tot": 0})
        ok = (r["status"] == "probe_ok")
        d["hist"].append({"ok": ok, "ts": r["ts"], "status": r["status"]})
        if r["ts"] >= cutoff:
            d["_tot"] += 1
            if ok:
                d["_ok"] += 1
    for d in out.values():
        d["hist"] = d["hist"][-n:]
        if d["_tot"]:
            d["avail"] = {"p": round(d["_ok"] / d["_tot"] * 100, 1),
                          "ok": d["_ok"], "tot": d["_tot"], "src": "probe"}
        d.pop("_ok", None)
        d.pop("_tot", None)
    return out


def _model_avail_by_key(mmap: dict) -> dict:
    """把 (站, 模型) 的探测可用率按模型聚合：{model_agg: {'p','ok','tot','src'}}。
    供首页「72 小时可用率」用模型级口径（无模型探测的模型不在结果里，调用方再回退站点级）。"""
    agg: dict = {}
    for (_sid, m), v in mmap.items():
        a = v.get("avail")
        if not a:
            continue
        d = agg.setdefault(m, [0, 0])
        d[0] += a["ok"]
        d[1] += a["tot"]
    return {m: {"p": round(o / t * 100, 1), "ok": o, "tot": t, "src": "probe"}
            for m, (o, t) in agg.items() if t}


def _cmp_probe_stats(conn, model, n: int = 12, hours: int = 72) -> dict:
    """对比页用：各站最近探测情况 {station_id: {'hist':[{ok,ts}...旧→新,≤n], 'avail':{p,ok,tot}|None}}。
    探测按站点粒度记录（probes.model 仅 api 探测写入），故可用率取「该站」最近探测，
    与首页「72 小时可用率」同源（首页再按模型在售站聚合）；无探测的站不出现在结果里。
    这是「模型级探测缺失时」的兜底口径，avail.src='station' 便于前端标注来源。"""
    probe_hist, _ = _probe_history_map(conn, n)
    probe72 = _probe_window_map(conn, hours)
    out: dict = {}
    for sid, hist in probe_hist.items():
        arr = probe72.get(sid, [])
        avail = ({"p": round(sum(x["ok"] for x in arr) / len(arr) * 100, 1),
                 "ok": sum(x["ok"] for x in arr), "tot": len(arr), "src": "station"}
                 if arr else None)
        out[sid] = {"hist": hist, "avail": avail}
    return out


def iter_detail_rows(conn, model_agg=None, station_id=None):
    """流式生成 站点 × 模型 × 分组 的详细行（含折算价格与较官方价折扣）。
    model_agg 为聚合键（抹平 . 与 - 差异），传 None 表示全部。
    station_id 传值时只算该站（单站页用，避免生成全站行再过滤）。"""
    official = load_official_prices()
    feed_off = load_feed_official(conn)   # 站点自带官方价，作为基准表没覆盖时的补位
    probe_hist, _probe_names = _probe_history_map(conn, 12)
    for snap in latest_snapshots(conn, station_id=station_id):
        st = conn.execute("SELECT * FROM stations WHERE id=?", (snap["station_id"],)).fetchone()
        # 充值比例：手动覆盖（stations.recharge_rate）优先于 /api/status 抓取值
        ex_rate = st["recharge_rate"] if st["recharge_rate"] is not None else snap["exchange_rate"]
        rate_src = "手动" if st["recharge_rate"] is not None else ("status" if ex_rate else "")
        # price=1 是 one-api 内部虚拟美元占位；真正 ¥1=$1 的站 usd_exchange_rate 也为 1。
        # 若公布了真实 usd_exchange_rate(≠1)，说明实际充值按国际汇率走，优先用它
        # （如 Portix 6.718 / PackyCode 7 / RelayApiApp 7.3 / Apiwarrior 6.8）。
        if st["recharge_rate"] is None and ex_rate == 1.0 \
                and snap["usd_exchange_rate"] not in (None, 1.0):
            ex_rate = snap["usd_exchange_rate"]
            rate_src = "status(汇率)"
        groups = {r["group_name"]: r["ratio"]
                  for r in conn.execute("SELECT group_name, ratio FROM group_ratios WHERE snapshot_id=?",
                                        (snap["id"],))}
        page_map = json.loads(snap["page_prices"]) if snap["page_prices"] else {}
        for m in conn.execute("SELECT * FROM model_prices WHERE snapshot_id=? ORDER BY model_canon",
                              (snap["id"],)):
            m = dict(m)
            canon = m.get("model_canon") or canonical_model(m["model_name"])
            akey = m.get("model_agg") or agg_key(canon)
            if model_agg and akey != model_agg:
                continue
            # 站点私有倍率：model_price 只是内部额度单价时，挂牌价还要乘站内硬编码倍率
            rule_mult, rule_billing = station_price_rule(st["name"], m)
            # 价格页权威价优先：每秒 → 按秒；每次 → 按次；$ / M → 按量
            pm_1m_all, pm_sec_all, pm_call_all = _split_page_map(
                page_map.get(m["model_name"]) or {})
            if pm_sec_all:
                billing = "per_second"
            elif pm_call_all:
                billing = "per_call"
            elif m.get("video_resolution_price"):
                # 接口给的按分辨率每秒价（Aixoras / UUAPI 等无价格页的站）
                billing = "per_second"
            elif m.get("image_size_price"):
                # 接口给的按尺寸每张价（billing_mode=image_size，如 Aixoras）
                billing = "per_call"
            else:
                billing = rule_billing or billing_of(m)
            mkind = model_kind(m)
            off = lookup_official(official, canon) or feed_off.get(agg_key(canon)) or {}
            # 官方价统一 ¥ 口径：国内渠道条目（currency=cny）直用；
            # 国外模型 = 条目自身 $ 价（litellm 第一方 / GT 人工核验，官方语义）
            #           × 市场汇率。注意不能用 OpenRouter 价折算——OR /models 返回的是
            # 集市快照价（多 provider 取一，如 kimi-k3 官方 $3/15 vs OR $0.304/11.936，
            # 差 10 倍），只能作 $ 参考列，不能当折扣基准。
            off_cny = off.get("currency") == "cny"
            off_in = to_float(off.get("input"))
            off_out = to_float(off.get("output"))
            off_in_cny = off_in if off_cny else (off_in * MARKET_FX if off_in else None)
            off_out_cny = off_out if off_cny else (off_out * MARKET_FX if off_out else None)
            if not off_cny and off_in:
                off_src = f"{_SOURCE_LABEL.get(off.get('source'), off.get('source', ''))}×{MARKET_FX:g}"
            elif off_cny:
                off_src = _SOURCE_LABEL.get(off.get("source"), off.get("source", ""))
            else:
                off_src = ""
            # 实付真实人民币 = 实付$ × 站点充值比例（1元=1额度的虚拟美元站由此还原真实成本）；
            # 站点未公布比例（直定价站）时假设 1 额度 = 真美元，按市场汇率折算
            paid_fx = ex_rate if ex_rate is not None else MARKET_FX
            for g, gratio, enabled, inp, out, crd, cwr, sec, call, res in \
                    model_group_rows(m, groups, page_map):
                ei, eo = inp * gratio, out * gratio
                # 价格来源：价格页表格 > 接口媒体直定价（video_resolution_price）
                # > 分组倍率推算 > 倍率未知（如 codex-pro-root / 企业国模专用，
                #   站点既不在 group_ratio 公布、价格页也没有该组，价格不可核验）
                if g in pm_1m_all or g in pm_sec_all or g in pm_call_all:
                    price_src = "价格页"
                elif sec is not None:
                    price_src = "接口"
                elif call is not None and (m.get("image_size_price")
                                           or m.get("video_resolution_price")):
                    # 按尺寸的每张价同样来自接口直定价
                    price_src = "接口"
                elif g in groups:
                    price_src = "倍率推算"
                else:
                    price_src = "倍率未知"
                if price_src == "倍率未知" and billing == "per_token":
                    # 倍率未知时「实付」无法计算，宁可留空也不拿 1.0 兜底冒充
                    ei = eo = ""
                pay_cny = (ei * paid_fx) if ei != "" else ""
                disc = None
                if off_in_cny and pay_cny:
                    disc = round((1 - pay_cny / off_in_cny) * 100, 2)
                tiers = tiered_prices(m, gratio)
                # 样例请求成本：命中阶梯时按阶梯倍率计价（输入长度即样例输入）
                r_in, r_out = tier_for(tiers, REF_TOKENS_IN, m["model_ratio"],
                                       m["completion_ratio"], gratio)
                sample_cost = (REF_TOKENS_IN * r_in + REF_TOKENS_OUT * r_out) / 1_000_000
                # 按次/按秒单价：页面权威价（每秒/每次）> 站点私有倍率 > model_price 原值
                if sec is not None:
                    unit_price = sec * (rule_mult or 1)
                elif call is not None:
                    unit_price = call * (rule_mult or 1)
                elif m["model_price"] and rule_mult:
                    unit_price = round(m["model_price"] * gratio * rule_mult, 10)
                else:
                    unit_price = m["model_price"] or ""
                yield {
                    "站点": st["name"], "站点id": snap["station_id"], "域名": snap["domain"], "数据时间(UTC)": snap["ts"],
                    "模型规范名": canon, "原始模型名": m["model_name"],
                    "模型变体": m.get("model_variant") or "",
                    "模型家族": m.get("model_family") or model_family(canon),
                    "公司": vendor_of(canon),
                    "计费方式": billing, "模型类型": mkind,
                    # 「语言模型」按钮只看白名单内的模型（前缀匹配）；白名单为空则不限
                    "核心模型": "是" if in_model_whitelist(canon) else "",
                    "厂商ID": m["vendor_id"] or "",
                    "分组": g or "(无分组信息)", "分组倍率": gratio, "模型在该分组可用": "是" if enabled else "否",
                    # 该站最近 12 次可达性探测结果（前端渲染小格子）
                    "最近12次状态": probe_hist.get(snap["station_id"], []),
                    "价格来源": price_src, "分辨率": res or "",
                    "输入倍率model_ratio": m["model_ratio"], "输出倍率completion_ratio": m["completion_ratio"],
                    "缓存读倍率cache_ratio": m["cache_ratio"], "缓存写倍率create_cache_ratio": m["create_cache_ratio"],
                    "按次价格model_price($/次)": m["model_price"] or "",
                    "计价倍率": rule_mult if rule_mult else "",
                    # 统一展示列：按次/按秒各自只有对应计费方式的行才有值，token 行留空
                    "按次价格($/次)": unit_price if billing == "per_call" else "",
                    "按秒价格($/秒)": unit_price if billing == "per_second" else "",
                    # 真实人民币单价：内部$ × 充值比例，跨站横比只认这个
                    "按次价格(¥/次)": ((unit_price * paid_fx)
                                       if billing == "per_call" and unit_price else ""),
                    "按秒价格(¥/秒)": ((unit_price * paid_fx)
                                       if billing == "per_second" and unit_price else ""),
                    "阶梯计价tiers": jdump(json.loads(m["tiers"] or "[]")),
                    "标价输入($/M)": inp, "标价输出($/M)": out,
                    "标价缓存读($/M)": crd, "标价缓存写($/M)": cwr,
                    # 缓存同样要乘分组倍率才是实付：官网价格页展示的是实付缓存价，
                    # 只给标价会让 claude_kiro(×0.2) 这类组看起来贵 1/0.2=5 倍
                    "实付缓存读($/M)": (crd * gratio) if crd is not None else "",
                    "实付缓存写($/M)": (cwr * gratio) if cwr is not None else "",
                    # 缓存价的人民币口径（内部$ × 充值比例），与 Token 价格同尺度便于并排展示
                    "实付缓存读(¥/M)": (round(crd * gratio * paid_fx, 6) if crd is not None else ""),
                    "实付缓存写(¥/M)": (round(cwr * gratio * paid_fx, 6) if cwr is not None else ""),
                    "实付输入($/M)": ei, "实付输出($/M)": eo,
                    "实付输入(¥/M)": (round(pay_cny, 6) if isinstance(pay_cny, (int, float)) else pay_cny),
                    "实付输出(¥/M)": (round(eo * paid_fx, 6) if eo != "" else ""),
                    "官方输入(¥/M)": (round(off_in_cny, 6) if isinstance(off_in_cny, (int, float))
                                      else off_in_cny if off_in_cny is not None else ""),
                    "官方输出(¥/M)": (round(off_out_cny, 6) if isinstance(off_out_cny, (int, float))
                                      else off_out_cny if off_out_cny is not None else ""),
                    "官方来源": off_src,
                    "较官方价(%)": disc if disc is not None else "",
                    "对比官方价(%)": (round(pay_cny / off_in_cny * 100, 4)
                                    if (off_in_cny and pay_cny) else ""),
                    "样例请求成本($)": sample_cost if billing == "per_token" else "",
                    "阶梯计价明细": jdump(tiers) if tiers else "",
                    "充值比例(¥/$)": ex_rate if ex_rate is not None else "",
                    "充值比例来源": rate_src,
                    "支持接口": jdump(json.loads(m["supported_endpoints"] or "[]")),
                    "抓取延迟(ms)": snap["latency_ms"] or "",
                    "站点system_name": snap["system_name"] or "",
                }


def build_detail_rows(conn, model_key=None):
    """兼容旧调用：返回列表（大数据量请用 iter_detail_rows）。"""
    key = agg_key(canonical_model(model_key)) if model_key else None
    return list(iter_detail_rows(conn, key))


def write_csv(rows, path):
    """流式写 CSV，避免大结果集占满内存。"""
    n = 0
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        w.writeheader()
        for r in rows:
            w.writerow({k: fmt(r[k]) if isinstance(r[k], float) else _csv_cell(r[k])
                        for k in CSV_HEADERS})
            n += 1
    return n


SUMMARY_HEADERS = [
    "模型规范名", "模型家族", "公司", "模型类型", "核心模型", "计费方式", "在售站数", "在售站点",
    "最低实付输入($/M)", "最低实付输出($/M)", "最低按次价格($/次)", "最低按秒价格($/秒)",
    "最低实付输入(¥/M)", "最低实付输出(¥/M)", "最低实付缓存读(¥/M)", "最低实付缓存写(¥/M)",
    "最低按秒价格(¥/秒)", "最低按次价格(¥/次)",
    "模型在该分组可用", "官方输入(¥/M)", "官方输出(¥/M)", "官方来源", "较官方价(%)", "对比官方价(%)",
    f"样例成本$({REF_TOKENS_IN // 1000}K入/{REF_TOKENS_OUT // 1000}K出)",
    "最低价来自站点", "充值比例(¥/$)", "充值比例来源", "最低价分组", "分组倍率", "最低价分辨率",
    "72小时可用率", "最近12次状态",
]


def _csv_cell(v):
    """CSV 单元格：探测历史数组 → '12/12 可达'；72h 可用率字典 → '92.3% (ok/tot)'。"""
    if isinstance(v, list):
        arr = v
        bad = sum(1 for x in arr if not x.get("ok"))
        return f"{len(arr) - bad}/{len(arr)} 可达" if arr else ""
    if isinstance(v, dict) and "p" in v:
        return f"{v['p']}% ({v['ok']}/{v['tot']})"
    return "" if v is None else v


def _detail_unit_price(r):
    """明细行按计费方式取单价（token 用 $/M，视频用 $/秒，其他用 $/次）。
    注意这是「站点内部额度美元」——price=1 的站（¥1=$1）会把它当人民币用。"""
    billing = r["计费方式"]
    if billing == "per_token":
        return r["实付输入($/M)"]
    if billing == "per_second":
        return r["按秒价格($/秒)"]
    return r["按次价格($/次)"]


def _detail_fx(r):
    """该行的充值比例（¥ per 内部$）；站点未公布时按市场汇率估算。"""
    v = r.get("充值比例(¥/$)")
    return v if v else MARKET_FX


def _detail_real_price(r):
    """真实人民币单价（跨站可比的唯一口径）：内部$ × 充值比例。
    token → ¥/M；按秒 → ¥/秒；按次 → ¥/次。量纲仍按计费方式分段，不可混排。"""
    p = _detail_unit_price(r)
    if not p:
        return None
    return p * _detail_fx(r)


def _summary_unit_price(x):
    """汇总行排序用的单价：走真实人民币口径（¥/M、¥/秒、¥/次）。
    站点 $ 列是内部额度美元，按它排序会让「¥1=$1」的站虚高/虚低 7 倍。"""
    billing = x["计费方式"]
    if billing == "per_token":
        return x["最低实付输入(¥/M)"] or 0
    if billing == "per_second":
        return x["最低按秒价格(¥/秒)"] or 0
    return x["最低按次价格(¥/次)"] or 0


def _avail_summary_text(d) -> str:
    """可用性检测摘要：'在售 3/5'（+ 真实调用探针时附 '· 可调用 1/1'）。"""
    if not d or not d.get("checked"):
        return ""
    txt = f"在售 {d.get('listed', 0)}/{d.get('checked', 0)}"
    p_ok, p_fail = d.get("probe_ok", 0), d.get("probe_fail", 0)
    if p_ok or p_fail:
        txt += f" · 可调用 {p_ok}/{p_ok + p_fail}"
    return txt


def _avail_ts_text(d) -> str:
    """最近检测时间（YYYY-MM-DD HH:MM）。"""
    ts = (d or {}).get("ts") or ""
    return ts[:16].replace("T", " ") if ts else ""


def _avail_72h(d, probe72: dict) -> dict | None:
    """该模型在售站点最近 72 小时可用率：{'p': 百分比, 'ok': 可达次数, 'tot': 总次数, 'lst': 在售文案}。
    无探测数据时返回 None（前端显示「-」）。"""
    d = d or {}
    sids = d.get("stations") or set()
    ok = tot = 0
    for sid in sids:
        for p in probe72.get(sid, []):
            tot += 1
            if p["ok"]:
                ok += 1
    if not tot:
        return None
    return {"p": round(ok / tot * 100, 1), "ok": ok, "tot": tot,
            "src": "station", "lst": _avail_summary_text(d)}


def build_summary_rows(conn, model_agg=None):
    """每模型一行汇总：最低价 + 在售站数 + 折扣（getcheapai 式核心表）。
    按聚合键分组（抹平 . 与 - 差异），显示名取最低价那条的规范名。

    三种计费量纲不同，不能同轴比价，因此分段输出：
      per_token（$ / M tokens，对话类）、per_second（$ / 秒，视频类）、
      per_call（$ / 次，图片·音乐等）。各段内部按自己的单价升序。
    同一模型若在不同站用不同计费方式，会出现多行（由「计费方式」列区分），
    这样按秒/按次的低价不会被 token 行掩盖。非 token 模型没有官方基准，
    折扣列一律留空。"""
    best = {}
    stations = {}
    avail = _avail_by_model(conn)          # 模型可用性检测结果（见 availability 子命令）
    probe_hist, probe_names = _probe_history_map(conn, 12)
    probe72 = _probe_window_map(conn, 72)   # 最近 72 小时可达性探测，用于可用率统计
    model_probe = _model_probe_map(conn)    # 按 (站, 模型) 的真实调用探测（模型级可用率）
    model_avail = _model_avail_by_key(model_probe)
    station_ids = {name: sid for sid, name in probe_names.items()}
    bench_map = bench_metrics_by_station(conn)   # 站级性能实测（TTFT/缓存命中），未测为缺
    for r in iter_detail_rows(conn, model_agg):
        billing = r["计费方式"]
        pair = (agg_key(r["模型规范名"]), billing)
        stations.setdefault(pair, set())
        # 只统计可用分组：不可用分组（模型未开通）的价格用户买不到，
        # 早期版本混入导致最低价系统性偏低（站点自己的工具只显示可用分组）
        if r["模型在该分组可用"] != "是":
            continue
        # 分组倍率未公布的行（价格来源=倍率未知）不能参与最低价：
        # 它按 1.0 兜底，会被当成「最便宜」，把真实最低价拉低（如 MemeCode 的
        # Claude-Curosr、Modelflare 的 codex-pro-root / 企业国模专用）
        if r.get("价格来源") == "倍率未知":
            continue
        # 比价必须用「真实人民币」：站点 $ 列是内部额度美元，
        # 充值比例 0.3(¥0.3=$1) 与 7.3(¥7.3=$1) 的站混在一起按 $ 排会差出几十倍
        price = _detail_real_price(r)
        if not price:
            continue
        stations[pair].add(r["站点"])
        cur = best.get(pair)
        if cur is None or price < (_detail_real_price(cur) or float("inf")):
            best[pair] = r
    rows = []
    for (key, billing), r in best.items():
        is_tok = billing == "per_token"
        rows.append({
            "模型规范名": r["模型规范名"], "模型家族": r["模型家族"],
            "公司": r.get("公司") or vendor_of(agg_key(r["模型规范名"])),
            "模型类型": r["模型类型"], "核心模型": r["核心模型"], "计费方式": billing,
            "在售站数": len(stations.get((key, billing), ())),
            "在售站点": "、".join(sorted(stations.get((key, billing), ()))),
            "最低实付输入($/M)": r["实付输入($/M)"] if is_tok else "",
            "最低实付输出($/M)": r["实付输出($/M)"] if is_tok else "",
            "最低按次价格($/次)": r["按次价格($/次)"] if billing == "per_call" else "",
            "最低按秒价格($/秒)": r["按秒价格($/秒)"] if billing == "per_second" else "",
            # 真实人民币单价（排序/跨站比价就用这个口径）
            "最低按次价格(¥/次)": (_detail_real_price(r) if billing == "per_call" else ""),
            "最低按秒价格(¥/秒)": (_detail_real_price(r) if billing == "per_second" else ""),
            "模型在该分组可用": "是",   # 汇总最低价只取可用组，故恒为「是」
            "最低实付输入(¥/M)": (r["实付输入(¥/M)"] if r["实付输入(¥/M)"] is not None
                                  else "") if is_tok else "",
            "最低实付输出(¥/M)": (r["实付输出(¥/M)"] if r["实付输出(¥/M)"] is not None
                                  else "") if is_tok else "",
            "最低实付缓存读(¥/M)": (r.get("实付缓存读(¥/M)") or "") if is_tok else "",
            "最低实付缓存写(¥/M)": (r.get("实付缓存写(¥/M)") or "") if is_tok else "",
            "官方输入(¥/M)": r["官方输入(¥/M)"] if is_tok else "",
            "官方输出(¥/M)": r.get("官方输出(¥/M)") if is_tok else "",
            "官方来源": r["官方来源"] if is_tok else "",
            "较官方价(%)": r["较官方价(%)"] if is_tok else "",
            "对比官方价(%)": r["对比官方价(%)"] if is_tok else "",
            f"样例成本$({REF_TOKENS_IN // 1000}K入/{REF_TOKENS_OUT // 1000}K出)":
                r["样例请求成本($)"] if is_tok else "",
            "最低价来自站点": r["站点"],
            # 该站充值比例（¥ per 内部$）：解释「输入¥/M」是怎么折算出来的
            "充值比例(¥/$)": r.get("充值比例(¥/$)"),
            "充值比例来源": r.get("充值比例来源") or "",
            "最低价分组": r["分组"], "分组倍率": r["分组倍率"],
            # 视频按分辨率逐档展开后，这里标注最低价对应哪一档
            "最低价分辨率": r.get("分辨率") or "",
            # 72 小时可用率：优先「该模型在各站的真实调用探测」，无模型探测时回退站点可达性
            "72小时可用率": ((dict(model_avail[key], lst=_avail_summary_text(avail.get(key)))
                              if key in model_avail else _avail_72h(avail.get(key), probe72))),
            # 最近 12 次状态：优先该模型在最低价站的真实调用探测，回退站点可达性
            "最近12次状态": (model_probe.get((station_ids.get(r["站点"], -1), key), {}).get("hist")
                              or probe_hist.get(station_ids.get(r["站点"], -1), [])),
            # 性能实测（api-bench，站级属性随最低价站带入；未测站点为 None → 前端显示「未测」）
            "TTFT p50(ms)": (bench_map.get(r["站点"]) or {}).get("ttft_p50_ms"),
            "TTFT p90(ms)": (bench_map.get(r["站点"]) or {}).get("ttft_p90_ms"),
            "缓存命中%": (bench_map.get(r["站点"]) or {}).get("cache_hit_pct"),
            "实测时间": (bench_map.get(r["站点"]) or {}).get("ts"),
        })
    order = {"per_token": 0, "per_second": 1, "per_call": 2}
    rows.sort(key=lambda x: (order[x["计费方式"]], _summary_unit_price(x)))
    return rows


def cmd_export(args):
    conn = connect()
    canon = agg_key(canonical_model(args.model)) if args.model else None
    fmts = ["csv", "json", "summary"] if args.format == "all" else (
        ["csv", "json"] if args.format == "both" else [args.format])
    outs = []
    for fmt_name in fmts:
        path = args.out
        if not path.lower().endswith("." + fmt_name):
            path = f"{path}.{fmt_name}"
        if fmt_name == "csv":
            n = write_csv(iter_detail_rows(conn, canon), path)
        elif fmt_name == "summary":
            rows = build_summary_rows(conn, canon)
            if not rows:
                continue
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=SUMMARY_HEADERS)
                w.writeheader()
                for r in rows:
                    w.writerow({k: fmt(r[k]) if isinstance(r[k], float) else _csv_cell(r[k])
                                for k in SUMMARY_HEADERS})
            n = len(rows)
        else:
            payload = {
                "generated_at": utc_now(),
                "unit_note": "倍率 1 = $2/1M tokens（new-api 约定）；实付价 = 标价 × 分组倍率；"
                             "实付人民币 = 实付$ × 站点充值比例；官方价统一人民币口径："
                             "国内渠道条目直用 ¥，国外模型 = 官方$ × 市场汇率(默认7.2)；"
                             "较官方价(%) = (1-实付输入/官方输入)*100",
                "ratio_unit_usd_per_m": RATIO_UNIT_USD_PER_M,
                "official_prices_source": OFFICIAL_PATH,
                "stations": [],
            }
            by_station = {}
            n = 0
            for r in iter_detail_rows(conn, canon):
                n += 1
                by_station.setdefault(r["站点"], {"meta": r, "models": {}})
                by_station[r["站点"]]["models"].setdefault(
                    (r["模型规范名"], r["原始模型名"]), []).append(r)
            for sname, bundle in by_station.items():
                meta = bundle["meta"]
                payload["stations"].append({
                    "name": sname, "domain": meta["域名"], "snapshot_ts": meta["数据时间(UTC)"],
                    "exchange_rate_rmb_per_usd": meta["充值比例(¥/$)"] or None,
                    "latency_ms": meta["抓取延迟(ms)"] or None,
                    "models": [{
                        "model_canon": canon_m, "model_raw": raw_m,
                        "variant": mrows[0]["模型变体"] or None,
                        "family": mrows[0]["模型家族"], "billing": mrows[0]["计费方式"],
                        "model_kind": mrows[0]["模型类型"],
                        # 计价单位：per_token → $/M tokens；per_second → $/秒（视频）；per_call → $/次
                        "billing_unit": {"per_token": "$/M", "per_second": "$/秒",
                                         "per_call": "$/次"}[mrows[0]["计费方式"]],
                        "vendor_id": mrows[0]["厂商ID"] or None,
                        "official_rmb_per_m": {"input": mrows[0]["官方输入(¥/M)"] or None,
                                               "output": mrows[0]["官方输出(¥/M)"] or None,
                                               "source": mrows[0]["官方来源"] or None},
                        "ratios": {
                            "model_ratio": mrows[0]["输入倍率model_ratio"],
                            "completion_ratio": mrows[0]["输出倍率completion_ratio"],
                            "cache_ratio": mrows[0]["缓存读倍率cache_ratio"],
                            "create_cache_ratio": mrows[0]["缓存写倍率create_cache_ratio"],
                            "model_price": mrows[0]["按次价格model_price($/次)"] or None,
                        },
                        "base_prices_usd_per_m": {
                            "input": mrows[0]["标价输入($/M)"], "output": mrows[0]["标价输出($/M)"],
                            "cache_read": mrows[0]["标价缓存读($/M)"], "cache_write": mrows[0]["标价缓存写($/M)"],
                        },
                        "tiers": json.loads(mrows[0]["阶梯计价tiers"] or "[]"),
                        "supported_endpoints": json.loads(mrows[0]["支持接口"] or "[]"),
                        "groups": [{
                            "group": mr["分组"], "group_ratio": mr["分组倍率"],
                            "model_enabled_in_group": mr["模型在该分组可用"] == "是",
                            "effective_usd_per_m": {"input": mr["实付输入($/M)"], "output": mr["实付输出($/M)"]},
                            "effective_rmb_per_m": {"input": mr["实付输入(¥/M)"] or None,
                                                    "output": mr["实付输出(¥/M)"] or None},
                            "discount_vs_official_pct": mr["较官方价(%)"] if mr["较官方价(%)"] != "" else None,
                        } for mr in mrows],
                    } for (canon_m, raw_m), mrows in bundle["models"].items()],
                })
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        outs.append((path, n))
    if not outs:
        print("没有可导出的数据，请先运行 scrape")
        return 1
    for path, n in outs:
        print(f"已导出 {n} 行 → {path}")
    return 0


def cmd_cheapest(args):
    conn = connect()
    rows = build_summary_rows(conn, agg_key(canonical_model(args.model)) if args.model else None)
    if not rows:
        print("没有数据，请先运行 scrape")
        return 1
    rows = rows[: args.top]
    # 排序与最低价都是真实人民币口径，所以主列给 ¥；内部额度$ 另列备查
    print(f"{'模型规范名':<34}{'计费':>6}{'最低¥输入/M':>14}{'最低¥输出/M':>14}"
          f"{'内部$输入/M':>13}{'站数':>5}{'较官方价%':>11}  最低价来自 / 分组")
    for r in rows:
        disc = f"{r['较官方价(%)']:+.1f}%" if r["较官方价(%)"] != "" else "—"
        b = r["计费方式"]
        if b == "per_token":
            label = "按量"
            p_in, p_out = fmt(r["最低实付输入(¥/M)"]), fmt(r["最低实付输出(¥/M)"])
            p_usd = fmt(r["最低实付输入($/M)"])
        elif b == "per_second":
            label = "按秒"
            p_in, p_out = f"{fmt(r['最低按秒价格(¥/秒)'])}/秒", ""
            p_usd = f"{fmt(r['最低按秒价格($/秒)'])}/秒"
        else:
            label = "按次"
            p_in, p_out = f"{fmt(r['最低按次价格(¥/次)'])}/次", ""
            p_usd = f"{fmt(r['最低按次价格($/次)'])}/次"
        print(f"{r['模型规范名'][:33]:<34}{label:>6}"
              f"{p_in:>14}{p_out:>14}{p_usd:>13}{r['在售站数']:>5}{disc:>11}  "
              f"{r['最低价来自站点']} / {r['最低价分组']}")
    return 0


def cmd_models(args):
    conn = connect()
    rows = conn.execute("""
        SELECT mp.model_canon AS canon, mp.model_agg AS agg, k.station_id AS sid
        FROM model_prices mp JOIN snapshots k ON mp.snapshot_id=k.id
        WHERE k.ok=1 AND k.id IN (SELECT MAX(id) FROM snapshots WHERE ok=1 GROUP BY station_id)
        AND mp.model_canon IS NOT NULL""").fetchall()
    official = load_official_prices()
    # 按聚合键分组：显示名取该组出现次数最多的规范名
    groups = {}
    for r in rows:
        key = r["agg"] or agg_key(r["canon"])
        g = groups.setdefault(key, {"stations": set(), "names": {}})
        g["stations"].add(r["sid"])
        g["names"][r["canon"]] = g["names"].get(r["canon"], 0) + 1
    ordered = sorted(groups.items(), key=lambda kv: (-len(kv[1]["stations"]), kv[0]))
    print(f"共 {len(ordered)} 个模型（按聚合键去重，按收录站点数排序）：")
    for key, g in ordered:
        display = max(g["names"].items(), key=lambda kv: (kv[1], -len(kv[0])))[0]
        mark = " *" if lookup_official(official, display) else ""
        print(f"  {display:<40} {len(g['stations']):>2} 站{mark}")
    print("  （* 已在 official_prices.json 中收录，可计算较官方价折扣）")
    return 0


def cmd_history(args):
    conn = connect()
    key = agg_key(canonical_model(args.model))
    snaps = conn.execute("""
        SELECT k.id, k.ts, k.exchange_rate, st.name AS station
        FROM snapshots k JOIN stations st ON st.id=k.station_id
        WHERE k.ok=1 ORDER BY k.id""").fetchall()
    series = []
    for k in snaps:
        models = [dict(m) for m in conn.execute(
            "SELECT * FROM model_prices WHERE snapshot_id=? AND (model_canon=? OR model_agg=?)",
            (k["id"], key, key))]
        if not models:  # 兼容历史数据（未回填 canon 的旧快照）
            models = [dict(m) for m in conn.execute(
                "SELECT * FROM model_prices WHERE snapshot_id=? AND model_key=?",
                (k["id"], normalize_model(args.model)))]
        groups = {r["group_name"]: r["ratio"]
                  for r in conn.execute("SELECT group_name, ratio FROM group_ratios WHERE snapshot_id=?", (k["id"],))}
        for m in models:
            best = None
            for g, gratio, enabled, inp, out, crd, cwr, *_ in \
                    model_group_rows(m, groups):
                if args.group and g != args.group:
                    continue
                if args.station and k["station"] != args.station:
                    continue
                if not enabled:
                    continue
                cand = (inp * gratio, out * gratio, g, gratio)
                if best is None or cand[0] < best[0]:
                    best = cand
            if best:
                series.append({"ts": k["ts"], "station": k["station"], "group": best[2],
                               "group_ratio": best[3],
                               "input_usd_per_m": best[0], "output_usd_per_m": best[1],
                               "input_rmb_per_m": best[0] * k["exchange_rate"] if k["exchange_rate"] is not None else None})
    if not series:
        print(f"没有找到模型 {args.model} 的历史数据")
        return 1
    if args.format == "csv":
        path = args.out or f"history_{key}.csv"
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(series[0].keys()))
            w.writeheader()
            w.writerows([{k: (fmt(v) if isinstance(v, float) else v) for k, v in s.items()} for s in series])
        print(f"已导出 {len(series)} 条历史记录 → {path}")
    else:
        path = args.out or f"history_{key}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(series, f, ensure_ascii=False, indent=2)
        print(f"已导出 {len(series)} 条历史记录 → {path}")
    for s in series[-15:]:
        rmb = f"  ¥{fmt(s['input_rmb_per_m'])}/M" if s["input_rmb_per_m"] is not None else ""
        print(f"  {s['ts']}  {s['station']:<14}{s['group']:<18}${fmt(s['input_usd_per_m'])}/M{rmb}")
    return 0


def compare_rows(conn, canon: str):
    """同模型跨站比价：所有 站点×分组 按实付输入价排序。
    只算「模型在该分组可用」的行——站点自己的工具也只展示可用分组，
    早期版本混入不可用分组导致最低价系统性偏低（曾 97.8% 汇总行受影响）。

    按秒（视频）/按次（图片·音乐）行不参与 $/M 排序，单独排在末尾：
    按秒行先、按次行后，各自按单价升序，避免 model_ratio=0 的它们
    以 $0/M 混进最便宜榜首。"""
    token_rows, unit_rows = [], []
    for r in iter_detail_rows(conn, canon):
        if r["模型在该分组可用"] != "是":
            continue
        if not _detail_unit_price(r):
            continue
        (token_rows if r["计费方式"] == "per_token" else unit_rows).append(r)
    # 跨站比价按真实人民币排序（站内 $ 是内部额度美元，¥1=$1 的站会和真美元站差 7 倍）
    token_rows.sort(key=lambda r: _detail_real_price(r) or 0.0)
    # 视频行按分辨率分块：块顺序 = 各分辨率的最低价升序（便宜档在前），块内按站价升序
    res_min = {}
    for r in unit_rows:
        if r["计费方式"] != "per_second":
            continue
        res, p = r.get("分辨率") or "", _detail_real_price(r)
        if p and (res not in res_min or p < res_min[res]):
            res_min[res] = p
    unit_rows.sort(key=lambda r: (0 if r["计费方式"] == "per_second" else 1,
                                  res_min.get(r.get("分辨率") or "", 0.0) if
                                  r["计费方式"] == "per_second" else 0.0,
                                  _detail_real_price(r) or 0.0))
    # 非 token 行去重：同站同计费同分辨率只留最低价分组（首个即最低）。
    # 视频/图片模型常一次开通十几个分组且单价相同，不去重会把对比表刷屏；
    # 但分辨率必须保留——否则只剩「最便宜那档」，看不到各档位在各站的报价。
    seen, deduped = set(), []
    for r in unit_rows:
        k = (r["站点"], r["计费方式"],
             (r.get("分辨率") or "") if r["计费方式"] == "per_second" else "")
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    rows = token_rows + deduped
    # 每站该模型的 72h 可用率与最近 12 次状态：
    # 优先「该站该模型」的真实调用探测（model_availability 的 probe_ok/probe_fail），
    # 没有模型探测时回退该站的 http 可达性（avail.src 让前端标注来源）。
    mstats = _model_probe_map(conn)
    sstats = _cmp_probe_stats(conn, canon)
    for r in rows:
        sid = r.get("站点id")
        m = mstats.get((sid, canon))
        s = sstats.get(sid)
        r["72小时可用率"] = (m.get("avail") if m else None) or (s.get("avail") if s else None)
        r["最近12次状态"] = (m.get("hist") if m else None) or (s.get("hist") if s else [])
    return rows


def cmd_compare(args):
    """同模型跨站比价视图：按站数取前 N 个模型，或指定单个模型。"""
    conn = connect()
    if args.model:
        models = [agg_key(canonical_model(args.model))]
    else:
        rows = conn.execute("""
            SELECT mp.model_canon AS canon, COUNT(DISTINCT k.station_id) AS n
            FROM model_prices mp JOIN snapshots k ON mp.snapshot_id=k.id
            WHERE k.ok=1 AND k.id IN (SELECT MAX(id) FROM snapshots WHERE ok=1 GROUP BY station_id)
            AND mp.model_canon IS NOT NULL
            GROUP BY mp.model_canon ORDER BY n DESC, mp.model_canon""").fetchall()
        models = [r["canon"] for r in rows][: args.top]
    if not models:
        print("没有匹配的模型")
        return 1
    out_rows = []
    for canon in models:
        rs = compare_rows(conn, canon)
        if not rs:
            continue
        tok = [r for r in rs if r["计费方式"] == "per_token"]
        sec = [r for r in rs if r["计费方式"] == "per_second"]
        cal = [r for r in rs if r["计费方式"] == "per_call"]
        print(f"\n=== {canon} ===  {len(set(r['站点'] for r in rs))} 站 / {len(rs)} 个分组"
              f"（按量 {len(tok)} · 按秒 {len(sec)} · 按次 {len(cal)}）")
        if tok:
            prices = [_detail_real_price(r) for r in tok]
            lo, hi = min(prices), max(prices)
            spread = f"  价差 {fmt(hi / lo, 1)}×" if lo else ""
            print(f"    按量最低 ¥{fmt(lo)}/M（{tok[0]['站点']} / {tok[0]['分组']}）"
                  f"  最高 ¥{fmt(hi)}/M{spread}   ← 已按各站充值比例折算真实人民币")
            if tok[0].get("官方输入(¥/M)"):
                print(f"    官方 ¥{fmt(tok[0]['官方输入(¥/M)'])}/M（{tok[0]['官方来源'] or '未注明'}）"
                      f"  最低价折扣 {tok[0]['较官方价(%)']}%")
        for grp, label, unit in ((sec, "按秒", "/秒"), (cal, "按次", "/次")):
            if not grp:
                continue
            ps = [_detail_real_price(r) for r in grp]
            spread = f"  价差 {fmt(max(ps) / min(ps), 1)}×" if min(ps) else ""
            print(f"    {label}最低 ¥{fmt(min(ps))}{unit}（{grp[0]['站点']} / {grp[0]['分组']}）"
                  f"  最高 ¥{fmt(max(ps))}{unit}{spread}")
        print(f"    {'站点':<14}{'分组':<20}{'分辨率':<22}{'计费':>6}{'分组倍率':>8}"
              f"{'实付输入$/M':>14}{'实付输出$/M':>14}{'单价':>12}{'单价¥':>12}{'¥/M':>12}")
        for r in rs[: args.rows]:
            b = r["计费方式"]
            is_tok = b == "per_token"
            label = "按量" if is_tok else ("按秒" if b == "per_second" else "按次")
            unit_txt = "-" if is_tok else f"{fmt(_detail_unit_price(r))}{'/秒' if b == 'per_second' else '/次'}"
            unit_txt_rmb = "-" if is_tok else f"{fmt(_detail_real_price(r))}{'/秒' if b == 'per_second' else '/次'}"
            print(f"    {r['站点']:<14}{r['分组'][:19]:<20}{(r.get('分辨率') or '')[:21]:<22}{label:>6}"
                  f"{fmt(r['分组倍率'], 4):>8}"
                  f"{fmt(r['实付输入($/M)']) if is_tok else '-':>14}"
                  f"{fmt(r['实付输出($/M)']) if is_tok else '-':>14}"
                  f"{unit_txt:>12}{unit_txt_rmb:>12}"
                  f"{'-' if (not is_tok or r['实付输入(¥/M)'] is None) else fmt(r['实付输入(¥/M)']):>12}")
        for r in rs[: args.rows]:
            out_rows.append({"模型规范名": canon, "站点": r["站点"], "分组": r["分组"],
                             "分辨率": r.get("分辨率") or "",
                             "计费方式": r["计费方式"], "分组倍率": r["分组倍率"],
                             "单价": _detail_unit_price(r),
                             "单价(¥)": _detail_real_price(r),
                             "按次价格($/次)": r["按次价格($/次)"],
                             "按秒价格($/秒)": r["按秒价格($/秒)"],
                             "实付输入($/M)": r["实付输入($/M)"], "实付输出($/M)": r["实付输出($/M)"],
                             "实付输入(¥/M)": r["实付输入(¥/M)"] or "",
                             "官方输入(¥/M)": r["官方输入(¥/M)"], "官方来源": r["官方来源"],
                             "较官方价(%)": r["较官方价(%)"]})
    if args.out and out_rows:
        headers = list(out_rows[0].keys())
        with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for r in out_rows:
                w.writerow({k: fmt(v) if isinstance(v, float) else v for k, v in r.items()})
        print(f"\n已导出跨站比价 {len(out_rows)} 行 → {args.out}")
    return 0


def cmd_alerts(args):
    """价格异动告警：列出超过阈值的涨降价。"""
    conn = connect()
    since = args.hours
    rows = conn.execute("""
        SELECT pc.*, s.name AS station FROM price_changes pc
        JOIN stations s ON s.id = pc.station_id
        ORDER BY pc.id DESC""").fetchall()
    if since:
        cutoff = time.time() - since * 3600
        rows = [r for r in rows if _ts_to_epoch(r["ts"]) >= cutoff] if rows else []
    rows = [r for r in rows if abs(r["pct"] or 0) >= args.threshold]
    if args.min_price:
        rows = [r for r in rows if (r["new_input_usd_m"] or 0) >= args.min_price]
    rows = rows[: args.top]
    if not rows:
        print(f"没有超过 {args.threshold}% 的价格异动（可用更低的 --threshold 查看）")
        return 0
    print(f"{'时间(UTC)':<22}{'站点':<14}{'模型':<26}{'分组':<18}{'旧$/M':>10}{'新$/M':>10}{'变动%':>10}")
    for r in rows:
        arrow = "↑涨" if r["pct"] > 0 else "↓降"
        print(f"{r['ts'][:19]:<22}{r['station']:<14}{(r['model_canon'] or '')[:25]:<26}"
              f"{(r['grp'] or '')[:17]:<18}{fmt(r['old_input_usd_m']):>10}"
              f"{fmt(r['new_input_usd_m']):>10}{arrow}{fmt(abs(r['pct']), 2):>7}")
    if args.out:
        with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["时间", "站点", "模型规范名", "分组", "旧输入$/M", "新输入$/M", "变动%"])
            for r in rows:
                w.writerow([r["ts"], r["station"], r["model_canon"], r["grp"],
                            fmt(r["old_input_usd_m"]), fmt(r["new_input_usd_m"]), r["pct"]])
        print(f"\n已导出 {len(rows)} 条异动 → {args.out}")
    return 0


DASHBOARD_HTML = """<!DOCTYPE html>
<!-- DASH-V2 2026-10-28:与 RelayLens 整合版(返回链接 + 口径条) -->
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>中转站价格看板</title>
<style>
:root{
  color-scheme:light;
  /* ── 马卡龙四色 + 深梅色文字 ── */
  --mint:#a8e6cf; --mint-deep:#57c9a4;
  --sakura:#ffd6e3; --sakura-deep:#ff9dba;
  --butter:#ffefb8; --butter-deep:#f5cf74;
  --lilac:#ddd2ff; --lilac-deep:#a992f0;
  --sky:#cfeaff;
  /* ink-3 定为 #8a7d99：浅色底上再浅就跌破 4.5:1，表头小字会糊 */
  --ink:#4a3d5c; --ink-2:#7c6d90; --ink-3:#8a7d99;
  --line:rgba(140,115,170,.14); --line-2:rgba(140,115,170,.22);
  --white:rgba(255,255,255,.9);
  --good:#1f9d74; --warn:#d9863a; --bad:#d9566a;
  --r-s:14px; --r-m:20px; --r-l:26px; --r-xl:34px;
  --ease:cubic-bezier(.22,.61,.36,1);
  --spring:cubic-bezier(.34,1.56,.64,1);
  --t1:140ms; --t2:220ms; --t3:320ms;
  /* 果冻面：顶部白色高光 + 底部回光，让边缘看起来是鼓起来的 */
  --jelly:inset 0 1.5px 0 rgba(255,255,255,.98),
          inset 0 -10px 18px -12px rgba(255,255,255,.9);
  --lift-1:0 2px 5px rgba(146,116,178,.10),0 12px 26px -14px rgba(146,116,178,.45);
  --lift-2:0 4px 10px rgba(146,116,178,.12),0 26px 48px -22px rgba(146,116,178,.55);
  --font-ui:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;
  --font-round:ui-rounded,"SF Pro Rounded","Hiragino Maru Gothic ProN",-apple-system,"Segoe UI",sans-serif;
}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{margin:0;min-height:100vh;color:var(--ink);
  background:linear-gradient(180deg,#fdf1f7 0%,#f6f5ff 42%,#f2fbf8 100%);
  font:13px/1.6 var(--font-ui);font-variant-numeric:tabular-nums;
  -webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;
  scrollbar-gutter:stable}
/* 背景的糖霜光斑：缓慢漂移，给静态页面一点呼吸 */
body::before{content:"";position:fixed;inset:-15% -15% -15% -15%;z-index:0;pointer-events:none;
  background:
    radial-gradient(closest-side at 16% 18%,rgba(168,230,207,.68),transparent 70%),
    radial-gradient(closest-side at 82% 10%,rgba(255,183,201,.66),transparent 70%),
    radial-gradient(closest-side at 74% 82%,rgba(221,210,255,.66),transparent 70%),
    radial-gradient(closest-side at 10% 88%,rgba(255,239,184,.6),transparent 70%);
  animation:drift 30s var(--ease) infinite alternate}
@keyframes drift{from{transform:translate3d(-1%,-1%,0) scale(1)}to{transform:translate3d(1.5%,1.5%,0) scale(1.05)}}
::selection{background:rgba(255,157,186,.35);color:var(--ink)}
::-webkit-scrollbar{width:11px;height:11px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:rgba(169,146,240,.35);border-radius:99px;border:3px solid transparent;
  background-clip:content-box}
::-webkit-scrollbar-thumb:hover{background:rgba(169,146,240,.55);background-clip:content-box}
:focus-visible{outline:3px solid rgba(255,157,186,.5);outline-offset:2px;border-radius:12px}
.wrap{position:relative;z-index:1;max-width:1480px;margin:0 auto;padding:26px 24px 80px}

/* ───────── 页头：果冻糖牌 ───────── */
.top{display:flex;align-items:center;justify-content:space-between;gap:18px;flex-wrap:wrap;
  padding:14px 20px 14px 16px;margin-bottom:18px;border-radius:var(--r-xl);
  background:linear-gradient(180deg,rgba(255,255,255,.92),rgba(255,255,255,.66));
  border:1.5px solid rgba(255,255,255,.95);
  box-shadow:var(--jelly),var(--lift-1);
  backdrop-filter:blur(16px) saturate(1.25)}
.brand{display:flex;align-items:center;gap:14px;min-width:0}
/* 立体糖块 logo：左上高光 + 右下回光 */
.mark{width:44px;height:44px;flex:none;display:grid;place-items:center;border-radius:16px;
  font-size:17px;font-weight:800;color:#fff;letter-spacing:.01em;
  background:linear-gradient(160deg,#ffc2d6 0%,#ff9dba 46%,#e57ee0 100%);
  box-shadow:inset 0 2px 2px rgba(255,255,255,.9),
             inset 0 -6px 12px rgba(180,90,170,.35),
             0 10px 20px -8px rgba(255,141,176,.85),
             0 3px 6px rgba(146,116,178,.2);
  text-shadow:0 1px 2px rgba(180,70,130,.35)}
h1{margin:0;font-size:21px;font-weight:750;letter-spacing:-.01em;line-height:1.2;
  background:linear-gradient(100deg,#ff8fb1,#b98cf0 65%,#6fc9d8);
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
.sub{color:var(--ink-3);font-size:12px;margin-top:3px;font-weight:500}
/* 数据新鲜度：每站一颗状态糖，抓取一旦停摆立刻看得见 */
/* 口径说明页脚：常驻但克制，回答「这些数字怎么算的」而不必翻文档 */
.legend{display:flex;gap:5px 18px;flex-wrap:wrap;align-items:baseline;
  margin:22px 2px 6px;padding-top:14px;font-size:11px;color:var(--ink-3);
  border-top:1px solid rgba(169,146,240,.22)}
.legend b{font-size:10.5px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;
  color:#b0557f}
.legend i{font-style:normal;font-weight:700;color:var(--ink-2)}
.legend span{white-space:nowrap}
@media(max-width:720px){.legend span{white-space:normal}}
.fresh{display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin:-4px 0 16px}
.fchip{display:inline-flex;align-items:center;gap:6px;padding:3px 11px;border-radius:999px;
  font-size:11px;font-weight:650;color:var(--ink-2);white-space:nowrap;
  border:1.5px solid rgba(255,255,255,.95);
  background:linear-gradient(180deg,#fff,#fdf7ff);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 2px 5px -2px rgba(146,116,178,.5);
  cursor:default;transition:transform var(--t1) var(--spring)}
.fchip:hover{transform:translateY(-1px)}
.fchip i{width:7px;height:7px;border-radius:50%;flex:none;box-shadow:0 0 6px currentColor}
.fchip.ok i{background:#3fb894;color:#3fb894}
.fchip.warn i{background:#eaa22e;color:#eaa22e}
.fchip.bad i{background:#e8617a;color:#e8617a}
.fchip b{font-weight:750;color:var(--ink);margin-left:1px}
.fchip.hd{background:linear-gradient(180deg,#fff,#ffeff6);color:#b0557f}
.fnote{font-size:11px;font-weight:650;color:#c2455f;margin-left:2px}
/* 页头右侧视图入口 */
.topacts{display:flex;gap:8px;align-items:center}
.tbtn{position:relative;height:32px;padding:0 14px;border-radius:999px;font-size:12px;font-weight:650;
  color:var(--ink-2);cursor:pointer;display:inline-flex;align-items:center;gap:6px;white-space:nowrap;
  background:linear-gradient(180deg,#fff,#f6effc);border:1.5px solid rgba(255,255,255,.95);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 3px 8px -4px rgba(146,116,178,.7);
  transition:transform var(--t1) var(--spring),color var(--t1) var(--ease),box-shadow var(--t1) var(--ease)}
.tbtn:hover{transform:translateY(-1px);color:var(--ink)}
.tbtn:active{transform:translateY(1px);box-shadow:inset 0 2px 6px rgba(146,116,178,.3)}
.tbtn.on{color:#b0557f;background:linear-gradient(180deg,#fff,#ffe9f2)}
.tbadge{font-size:10px;padding:0 6px;border-radius:99px;background:#ffe1ec;color:#b0557f;font-weight:750}
/* 返回 RelayLens 与口径声明(2026-10-28 与主站整合):返回链接复用 .tbtn 糖丸样式;
   口径条用 --sky 浅蓝调,与粉系状态糖区分开,克制常驻 */
.topacts a.tbtn{text-decoration:none}
.caliber{display:flex;gap:6px;align-items:baseline;flex-wrap:wrap;
  margin:-6px 0 16px;padding:9px 14px;border-radius:var(--r-m);
  font-size:11.5px;line-height:1.7;color:var(--ink-2);
  border:1.5px solid rgba(255,255,255,.95);
  background:linear-gradient(180deg,#fff,#eef7fd);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 2px 5px -2px rgba(146,116,178,.4)}
.caliber b{font-weight:800;color:#3d7fa6;white-space:nowrap}
.caliber a{color:#7c5cd6;font-weight:650;text-decoration:none}
.caliber a:hover{text-decoration:underline}
/* 视图容器：调价监控 / 站点总览 */
.view{display:none;margin-top:18px}
body.v-changes #viewChanges,body.v-stations #viewStations,body.v-selector #viewSelector{display:block}
body.v-changes .bar,body.v-changes .kinds,body.v-changes #mainTable,body.v-changes .stats,
body.v-stations .bar,body.v-stations .kinds,body.v-stations #mainTable,body.v-stations .stats,
body.v-selector .bar,body.v-selector .kinds,body.v-selector #mainTable,body.v-selector .stats{display:none}
/* ── 选型顾问：左表单右结果，同看板果冻糖面板 ── */
.sel-grid{display:grid;grid-template-columns:330px 1fr;gap:16px;align-items:start}
@media(max-width:900px){.sel-grid{grid-template-columns:1fr}}
.sel-form{display:flex;flex-direction:column;gap:11px;padding:16px;border-radius:var(--r-l);
  border:1.5px solid rgba(255,255,255,.95);background:linear-gradient(180deg,#fffdfe,#fbf5ff)}
.sel-form label{display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:700;
  color:var(--ink-2)}
.sel-form .hint{font-weight:550;font-size:10.5px;color:var(--ink-3)}
.sel-form select,.sel-form input{padding:7px 10px;border-radius:10px;font-size:12.5px;
  border:1.5px solid rgba(169,146,240,.35);background:#fff;color:var(--ink)}
.sel-form .row2{display:grid;grid-template-columns:1fr 1fr;gap:9px}
.sel-form input[type=range]{accent-color:var(--sakura-deep);padding:0}
.sel-out{display:flex;flex-direction:column;gap:12px;min-width:0}
.sel-top{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}
.sel-card{padding:13px 15px;border-radius:var(--r-l);border:1.5px solid rgba(255,255,255,.95);
  background:linear-gradient(180deg,#fff,#ffeef5);box-shadow:var(--jelly)}
.sel-card.win{background:linear-gradient(180deg,#fff,#e9fbf2);box-shadow:var(--jelly),
  0 10px 26px -14px rgba(40,172,125,.5)}
.sel-card .t{font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;
  color:#b0557f}
.sel-card.win .t{color:#1c8f66}
.sel-card .v{font-family:var(--font-round);font-size:17px;font-weight:800;color:var(--ink);
  margin-top:3px}
.sel-card .s{font-size:11px;color:var(--ink-3);margin-top:4px;line-height:1.55}
.sel-table{width:100%;border-collapse:separate;border-spacing:0;font-size:12px;
  background:rgba(255,255,255,.7);border-radius:var(--r-m);overflow:hidden}
.sel-table th{padding:7px 10px;font-size:10px;font-weight:750;letter-spacing:.06em;
  text-transform:uppercase;color:var(--ink-3);text-align:left;background:rgba(246,238,255,.6);
  border-bottom:1.5px solid rgba(169,146,240,.3);white-space:nowrap}
.sel-table td{padding:7px 10px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}
.sel-table td.num{text-align:right;font-family:var(--font-mono)}
.sel-rej{font-size:11.5px;color:var(--ink-3);line-height:1.8}
.sel-rej b{color:var(--ink-2)}
.sel-note{font-size:10.5px;color:var(--ink-3);line-height:1.7;padding:10px 14px;
  border-radius:var(--r-m);background:rgba(246,238,255,.45);
  border:1px dashed rgba(169,146,240,.4)}
.panel{border:1.5px solid rgba(255,255,255,.95);border-radius:var(--r-l);
  background:linear-gradient(180deg,rgba(255,255,255,.95),rgba(255,255,255,.62));
  box-shadow:var(--jelly),var(--lift-2);padding:18px 20px;backdrop-filter:blur(14px)}
.phead{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;
  margin-bottom:14px}
.phead b{font-size:15px;font-weight:750;letter-spacing:-.01em}
.psub{font-size:11.5px;color:var(--ink-3);margin-left:9px;font-weight:500}
.pacts{display:flex;gap:8px;align-items:center}
.kpis{display:flex;gap:22px;flex-wrap:wrap;margin-bottom:16px}
.kpi{display:flex;flex-direction:column;gap:2px}
.kpi .kl{font-size:10.5px;letter-spacing:.11em;text-transform:uppercase;color:var(--ink-3);font-weight:700}
.kpi .kv{font-family:var(--font-round);font-size:24px;font-weight:800;letter-spacing:-.02em}
.kpi .kv.up{color:#d9566a} .kpi .kv.down{color:#1f9d74}
.kpi .kv small{font-family:var(--font-ui);font-size:11.5px;font-weight:500;color:var(--ink-3);margin-left:3px}
.dtab{border-collapse:separate;border-spacing:0;width:100%;font-size:12.5px;margin-top:4px}
.dtab th{position:static;height:34px;padding:7px 10px;font-size:10.5px;font-weight:750;
  letter-spacing:.09em;text-transform:uppercase;color:var(--ink-3);text-align:left;cursor:pointer;
  border-bottom:1.5px solid rgba(169,146,240,.32);background:transparent}
.dtab td{padding:7px 10px;border-bottom:1px solid var(--line);text-align:left}
.dtab td.num,.dtab th.num{text-align:right;font-variant-numeric:tabular-nums}
.dtab tbody tr{transition:background var(--t1) var(--ease)}
.dtab tbody tr:hover td{background:rgba(255,214,227,.4)}
.dtab td.mono{font-family:var(--font-round);font-weight:650}
.up{color:#d9566a} .down{color:#1f9d74}
@media(max-width:720px){.panel{padding:14px}.kpis{gap:16px}.kpi .kv{font-size:20px}}

/* ───────── 指标：四颗马卡龙 ───────── */
.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-bottom:18px}
/* 首屏/切站时四张卡级联上浮：只播一次，之后不再重复 */
.stats.land .stat{animation:statIn 320ms var(--spring) backwards}
.stats.land .stat:nth-child(2){animation-delay:40ms}
.stats.land .stat:nth-child(3){animation-delay:80ms}
.stats.land .stat:nth-child(4){animation-delay:120ms}
@keyframes statIn{from{opacity:0;transform:translateY(12px) scale(.96)}to{opacity:1;transform:none}}
.stat{position:relative;padding:16px 18px 15px;border-radius:var(--r-l);
  background:
    linear-gradient(180deg,rgba(255,255,255,.76),rgba(255,255,255,.32)),
    var(--tint,var(--sakura));
  border:1.5px solid rgba(255,255,255,.95);
  box-shadow:var(--jelly),var(--lift-1),0 18px 34px -20px var(--glow,transparent);
  transform:perspective(900px) rotateX(var(--rx,0deg)) rotateY(var(--ry,0deg));
  transition:transform var(--t2) var(--spring),box-shadow var(--t2) var(--ease);
  overflow:hidden;will-change:transform}
/* 糖面高光：左上角一块椭圆反光，果冻质感主要来自这里 */
.stat::after{content:"";position:absolute;right:14px;top:8px;width:62px;height:26px;border-radius:50%;
  background:radial-gradient(ellipse at 50% 40%,rgba(255,255,255,.98),rgba(255,255,255,0) 72%);
  opacity:.9;pointer-events:none}
/* 每张卡带一层同色投影：糖果落在桌面上会染出一点自己的颜色 */
.stat:nth-child(1){--tint:linear-gradient(180deg,rgba(168,230,207,.85),rgba(168,230,207,.42));
  --glow:rgba(87,201,164,.55)}
.stat:nth-child(2){--tint:linear-gradient(180deg,rgba(255,214,227,.9),rgba(255,214,227,.45));
  --glow:rgba(255,157,186,.6)}
.stat:nth-child(3){--tint:linear-gradient(180deg,rgba(255,239,184,.9),rgba(255,239,184,.5));
  --glow:rgba(245,207,116,.6)}
.stat:nth-child(4){--tint:linear-gradient(180deg,rgba(221,210,255,.9),rgba(221,210,255,.45));
  --glow:rgba(169,146,240,.55)}
.stat:hover{box-shadow:var(--jelly),var(--lift-2),0 26px 46px -22px var(--glow,transparent)}
.stat .lab{position:relative;z-index:1;font-size:10.5px;letter-spacing:.12em;text-transform:uppercase;
  color:var(--ink-2);font-weight:700}
.stat .val{position:relative;z-index:1;margin-top:9px;font-family:var(--font-round);
  font-size:26px;font-weight:800;letter-spacing:-.02em;line-height:1.1;color:var(--ink);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.stat .val small{font-family:var(--font-ui);font-size:12px;font-weight:600;color:var(--ink-3);
  margin-left:3px;letter-spacing:0}
.stat .hint{position:relative;z-index:1;margin-top:5px;font-size:11.5px;color:var(--ink-2);
  font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

/* ───────── 工具栏：奶油底座 ───────── */
.bar{display:flex;gap:9px;align-items:center;flex-wrap:wrap;padding:10px;
  border-radius:var(--r-l);border:1.5px solid rgba(255,255,255,.98);
  background:linear-gradient(180deg,#fffdfe 0%,#fbf5ff 62%,#f7efff 100%);
  box-shadow:var(--jelly),var(--lift-2),inset 0 -2px 0 rgba(221,210,255,.55);
  backdrop-filter:blur(16px) saturate(1.2)}
.search{position:relative;flex:1 1 260px;min-width:180px}
/* 放大镜做成嵌在凹槽里的小糖钮，给搜索框一个可见的"实体边界" */
.search svg{position:absolute;left:8px;top:50%;transform:translateY(-50%);
  width:26px;height:26px;padding:6px;box-sizing:border-box;border-radius:50%;
  color:#a992f0;pointer-events:none;
  background:linear-gradient(180deg,#fff,#eee2ff);
  border:1px solid rgba(255,255,255,.95);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),
             0 2px 5px rgba(146,116,178,.35),
             0 1px 1px rgba(146,116,178,.18);
  transition:color var(--t1) var(--ease),box-shadow var(--t2) var(--ease),transform var(--t1) var(--spring)}
.search:focus-within svg{color:#e2618f;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),
             0 2px 6px rgba(255,141,176,.55),
             0 0 0 4px rgba(255,157,186,.18)}
.search input{width:100%;padding-left:46px}
input,select,button{font:inherit;color:var(--ink);border-radius:999px;
  padding:0 14px;height:36px;outline:none;
  transition:box-shadow var(--t2) var(--ease),background var(--t2) var(--ease),
             transform var(--t1) var(--spring),color var(--t1) var(--ease),border-color var(--t1)}
/* 输入框＝真正压进面里的凹槽：底色用 background-color 打底，
   纵向渐变单独一层，这样下拉框再叠箭头/圆钮也不会把凹槽底色冲掉 */
input,select{
  background-color:#f7f0fd;
  background-image:linear-gradient(180deg,rgba(232,215,250,.95),rgba(255,255,255,0) 58%);
  border:1.5px solid rgba(255,255,255,.92);
  box-shadow:
    inset 0 6px 10px -6px rgba(110,78,150,.62),
    inset 0 1px 0 rgba(146,116,178,.26),
    inset 0 -3px 6px -3px rgba(255,255,255,.95),
    0 1px 0 rgba(255,255,255,.95),
    0 -1px 0 rgba(146,116,178,.08)}
input::placeholder{color:#b3a4c4}
input:hover,select:hover{background-color:#faf5ff;
  box-shadow:
    inset 0 6px 10px -6px rgba(110,78,150,.5),
    inset 0 1px 0 rgba(146,116,178,.2),
    inset 0 -3px 6px -3px rgba(255,255,255,.95),
    0 1px 0 #fff,
    0 4px 10px -6px rgba(146,116,178,.45)}
input:focus,select:focus{background-color:#fff;background-image:none;
  border-color:rgba(255,157,186,.9);
  box-shadow:
    inset 0 6px 10px -6px rgba(110,78,150,.3),
    inset 0 -3px 6px -3px rgba(255,255,255,.95),
    0 0 0 4px rgba(255,157,186,.2),
    0 2px 6px rgba(255,141,176,.25)}
select{appearance:none;-webkit-appearance:none;cursor:pointer;padding-right:42px;font-weight:500;
  background-image:
    url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='11' height='11' viewBox='0 0 24 24' fill='none' stroke='%23a992f0' stroke-width='3.2' stroke-linecap='round'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E"),
    radial-gradient(circle at 50% 50%,#ffffff 0 38%,#eadcff 39% 100%),
    linear-gradient(180deg,rgba(232,215,250,.95),rgba(255,255,255,0) 58%);
  background-repeat:no-repeat,no-repeat,no-repeat;
  background-position:right 13px center,right 6px center,0 0;
  background-size:11px 11px,29px 29px,100% 100%}
select:hover{background-image:
    url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='11' height='11' viewBox='0 0 24 24' fill='none' stroke='%23c96f9b' stroke-width='3.2' stroke-linecap='round'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E"),
    radial-gradient(circle at 50% 50%,#ffffff 0 38%,#ffe4ef 39% 100%),
    linear-gradient(180deg,rgba(232,215,250,.95),rgba(255,255,255,0) 58%)}
select:focus{background-image:
    url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='11' height='11' viewBox='0 0 24 24' fill='none' stroke='%23e2618f' stroke-width='3.2' stroke-linecap='round'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E"),
    radial-gradient(circle at 50% 50%,#ffffff 0 38%,#ffe4ef 39% 100%)}
select option{background:#fff;color:var(--ink)}
/* 按钮＝果冻糖：常态鼓起，悬停弹起，按下回陷 */
button{cursor:pointer;white-space:nowrap;display:inline-flex;align-items:center;gap:6px;
  font-weight:650;color:var(--ink);
  background:linear-gradient(180deg,#ffffff,#f4ecff);
  border:1.5px solid rgba(255,255,255,.95);
  box-shadow:var(--jelly),0 3px 6px rgba(146,116,178,.14),0 10px 20px -12px rgba(146,116,178,.75)}
button:hover{transform:translateY(-2px) scale(1.02);color:#3c3050;
  background:linear-gradient(180deg,#fff,#fbf2ff);
  box-shadow:var(--jelly),0 6px 12px rgba(146,116,178,.16),0 18px 30px -14px rgba(146,116,178,.8)}
button:active{transform:translateY(1px) scale(.985);
  box-shadow:inset 0 3px 8px rgba(146,116,178,.28),inset 0 -1px 0 rgba(255,255,255,.9),0 2px 4px rgba(146,116,178,.12)}
button.primary{color:#fff;border-color:rgba(255,255,255,.9);
  /* 比马卡龙粉再深一档：白字在浅粉上对比不足，这里优先可读性 */
  background:linear-gradient(180deg,#ff9ab8 0%,#f5769f 48%,#dd5083 100%);
  text-shadow:0 1px 2px rgba(150,40,90,.45);
  box-shadow:inset 0 2px 1px rgba(255,255,255,.85),
             inset 0 -8px 14px -8px rgba(200,60,130,.5),
             0 4px 10px rgba(255,141,176,.35),
             0 14px 26px -12px rgba(255,141,176,.95)}
/* 只在左上角点一小块反光，避免整条白条压住按钮文字 */
button.primary::after{content:"";position:absolute;top:1px;left:7px;width:46%;height:52%;
  border-radius:999px;pointer-events:none;
  background:radial-gradient(ellipse at 32% 0%,rgba(255,255,255,.8),rgba(255,255,255,0) 72%)}
button.primary{position:relative;overflow:hidden}
button.primary:hover{transform:translateY(-2px) scale(1.03);
  background:linear-gradient(180deg,#ffabc4 0%,#fb87ad 48%,#e85f90 100%);
  box-shadow:inset 0 2px 1px rgba(255,255,255,.95),
             inset 0 -8px 14px -8px rgba(200,60,130,.45),
             0 8px 16px rgba(255,141,176,.4),
             0 22px 38px -16px rgba(255,141,176,1)}
button.primary:active{transform:translateY(1px) scale(.99);
  box-shadow:inset 0 4px 10px rgba(190,60,120,.45),inset 0 -1px 0 rgba(255,255,255,.6)}
.bar-actions{display:flex;gap:9px;align-items:center;margin-left:auto}
.chk{display:inline-flex;align-items:center;gap:7px;font-size:12px;color:var(--ink-2);font-weight:550;
  padding:0 8px;cursor:pointer;user-select:none;white-space:nowrap;transition:color var(--t1) var(--ease)}
.chk:hover{color:var(--ink)}
.chk input{width:auto;height:auto;margin:0;padding:0;accent-color:var(--sakura-deep);cursor:pointer;
  background:none;box-shadow:none}
/* ───────── 列显隐：果冻弹出面板 ───────── */
.colwrap{position:relative}
/* 列选择弹层挂在 body 下（position:fixed + JS 定位）：
   放在工具栏里会被 .bar 的 backdrop-filter 圈进它的层叠上下文，
   整体被 z-index 更高的吸顶表头盖住——这正是"弹层被挡"的根因 */
.colpop{position:fixed;z-index:90;width:212px;max-height:520px;
  overflow-y:auto;padding:12px;border-radius:var(--r-m);
  border:1.5px solid rgba(255,255,255,.95);
  background:linear-gradient(180deg,#fffdfe,#fdf6ff);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.98),0 18px 40px -20px rgba(146,116,178,.85);
  display:none;opacity:0;transform:translateY(-2px);
  transition:opacity var(--t1) var(--ease),transform var(--t2) var(--spring)}
.colpop.on{display:block;opacity:1;transform:none}
.colpop .cp-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:9px}
.colpop .cp-head b{font-size:11px;letter-spacing:.09em;text-transform:uppercase;color:var(--ink-3)}
.colpop .cp-reset{font-size:11px;color:#b0557f;cursor:pointer;font-weight:650;padding:2px 8px;
  border-radius:999px;transition:background var(--t1) var(--ease)}
.colpop .cp-reset:hover{background:#ffe9f2}
.colpop label{display:flex;align-items:center;gap:8px;padding:5px 7px;border-radius:10px;
  font-size:12px;color:var(--ink-2);cursor:pointer;user-select:none;font-weight:550;
  transition:background var(--t1) var(--ease),color var(--t1) var(--ease)}
.colpop label:hover{background:#f6eeff;color:var(--ink)}
.colpop input{width:auto;height:auto;margin:0;padding:0;cursor:pointer;accent-color:var(--sakura-deep);
  background:none;box-shadow:none}
.colpop .cp-grp{font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--ink-3);
  font-weight:700;margin:9px 0 3px;padding:0 7px}
.colpop .cp-grp:first-of-type{margin-top:2px}

/* ───────── 分段控件：胶囊里的果冻滑块 ───────── */
.kinds{position:relative;display:inline-flex;gap:3px;margin:16px 0 2px;padding:5px;
  border-radius:999px;border:1.5px solid rgba(255,255,255,.95);
  background:rgba(255,255,255,.62);
  box-shadow:inset 0 2px 6px rgba(146,116,178,.14),inset 0 -1px 0 rgba(255,255,255,.95);
  backdrop-filter:blur(12px);max-width:100%}
.kind-ind{position:absolute;top:5px;bottom:5px;left:0;width:0;border-radius:999px;opacity:0;
  background:linear-gradient(180deg,#fff,#ffeaf2);
  box-shadow:inset 0 2px 1px rgba(255,255,255,.95),
             0 3px 8px rgba(255,141,176,.3),
             0 8px 16px -8px rgba(146,116,178,.7);
  transition:transform var(--t3) var(--spring),width var(--t3) var(--spring),opacity var(--t2) var(--ease);
  pointer-events:none}
.kind{position:relative;z-index:1;height:32px;padding:0 15px;border-radius:999px;font-size:12.5px;
  font-weight:650;color:var(--ink-2);background:transparent;border:1.5px solid transparent;box-shadow:none}
.kind:hover{transform:none;background:transparent;color:var(--ink);box-shadow:none}
.kind:active{transform:none;box-shadow:none;background:transparent}
.kind.on{color:#e2618f;background:transparent}
.kind .n{opacity:.55;font-size:10.5px;margin-left:5px;transition:opacity var(--t1) var(--ease)}
.kind.on .n{opacity:.95;color:#c07bd8}

/* ───────── 面板（走势 / 对比） ───────── */
/* 窄屏下表格比容器宽：让表格在容器内横向滚动，而不是把整页撑出横向滚动条。
   2026-10-28:overflow-x:auto 会把 sticky 表头囚禁在本容器里(容器不垂直滚动,
   thead 永远吸不到页面顶部,下滚表头整体消失)——实测宽屏 14 列并不溢出,
   故只在 ≤1100px 真需要横滚的窄屏启用,宽屏放开给吸顶表头让路 */
.tablescroll{margin:0}
@media(max-width:1100px){
  .tablescroll{overflow-x:auto;scrollbar-width:none}
  .tablescroll::-webkit-scrollbar{display:none}
}
#chart:not(:empty){margin:18px 0;padding:20px 22px;border-radius:var(--r-l);
  border:1.5px solid rgba(255,255,255,.95);
  background:linear-gradient(180deg,rgba(255,255,255,.95),rgba(255,255,255,.62));
  box-shadow:var(--jelly),var(--lift-2);backdrop-filter:blur(14px);
  animation:pop var(--t3) var(--spring)}
@keyframes pop{from{opacity:0;transform:translateY(10px) scale(.985)}to{opacity:1;transform:none}}
#chart b{font-weight:750;letter-spacing:-.01em}
.ch-head{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap}
.ch-head b{font-size:14.5px}
.ch-legend{display:flex;flex-wrap:wrap;gap:7px 16px;margin-top:12px;font-size:11.5px;
  color:var(--ink-2);font-weight:550}
.ch-legend .lg{display:inline-flex;align-items:center;gap:6px;white-space:nowrap}
.ch-legend i{width:9px;height:9px;border-radius:50%;display:inline-block;flex:none;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.9),0 2px 5px rgba(146,116,178,.35)}
body.cmpmode .bar,body.cmpmode .kinds,body.cmpmode .stats,body.cmpmode #mainTable{display:none}
body.cmpmode #chart:not(:empty){background:none;border:0;padding:0;margin:0;box-shadow:none;
  backdrop-filter:none;animation:none}

/* ───────── 表格：一整片奶冻 ───────── */
/* 注意：不能给 table 加 overflow:hidden，否则吸顶表头失效；圆角改写在各角单元格上 */
table{border-collapse:separate;border-spacing:0;width:100%;margin-top:18px;
  font-size:12.5px;border-radius:var(--r-l);background:rgba(255,255,255,.58);
  box-shadow:0 1px 0 rgba(255,255,255,.95) inset,0 18px 40px -26px rgba(146,116,178,.75)}
thead tr:first-child th:first-child{border-top-left-radius:var(--r-l)}
thead tr:first-child th:last-child{border-top-right-radius:var(--r-l)}
tbody tr:last-child td:first-child{border-bottom-left-radius:var(--r-l)}
tbody tr:last-child td:last-child{border-bottom-right-radius:var(--r-l)}
th,td{padding:8px 11px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}
td:first-child,td:nth-child(2){text-align:left}
thead tr:first-child th:first-child,thead tr:first-child th:nth-child(2){text-align:left}
/* 表头做成一整块"悬浮板"，压在内容之上；浅色主题下必须自带色阶，
   否则吸顶后和白底表体糊成一片 */
thead{position:sticky;top:0;z-index:20;
  background:linear-gradient(180deg,#fffdfe 0%,#fdf6ff 70%,#f8eeff 100%);
  box-shadow:0 8px 16px -14px rgba(146,116,178,.6);
  transition:box-shadow var(--t2) var(--ease)}
body.stuck #thead{box-shadow:0 12px 26px -16px rgba(146,116,178,.95)}
th{position:static;height:38px;padding:8px 11px;cursor:pointer;user-select:none;
  font-size:10.5px;font-weight:750;letter-spacing:.09em;text-transform:uppercase;
  color:var(--ink-3);background:transparent;
  border-bottom:1.5px solid rgba(169,146,240,.32);
  transition:color var(--t1) var(--ease)}
th:hover{color:#d9668f}
th.on .sa{display:inline-block;margin-left:3px;font-size:9px;line-height:1;
  animation:arrowPop var(--t3) var(--spring)}
@keyframes arrowPop{0%{opacity:0;transform:scale(.4) rotate(-35deg)}100%{opacity:1;transform:none}}
/* 吸附后表头微收缩，给内容让出两行 */
body.stuck th{padding-top:5px;padding-bottom:5px}
body.stuck #thead{box-shadow:0 12px 24px -16px rgba(146,116,178,.95)}
/* 组标题：不再做彩色色块岛（会夹在透明 rowspan 格之间显得割裂），
   与表体同处一块连续面板上，只用文字色阶 + 组间发丝线 + 短下划线点缀区分层级；
   字号与叶子表头同族（10px/10.5px），配色取页面已有的粉→紫强调色 */
thead tr:first-child th[colspan]{text-align:center;cursor:default;
  color:#9a7fc0;font-size:10px;font-weight:800;letter-spacing:.12em;
  border-left:1px solid rgba(169,146,240,.13);border-right:1px solid rgba(169,146,240,.13);
  background-image:linear-gradient(90deg,#ffb7d0,#c9a8f5);
  background-repeat:no-repeat;background-size:34px 2px;
  background-position:50% calc(100% - 4px)}
thead tr:first-child th[colspan]:last-child{border-right:none}
thead tr:first-child th[colspan]:hover{color:#8a5fb5}
tbody tr{transition:background var(--t1) var(--ease)}
td:first-child{transition:background-color var(--t1) var(--ease)}
/* 行透镜：整行文字加深一档 + 左缘 3px 渐变指示条（粉→紫，硬断点） */
tr:hover td{background:rgba(255,214,227,.4);color:#3a2e4c}
tr:hover td:first-child{
  background-image:linear-gradient(90deg,#ff9dba 0,#c9a8f5 3px,rgba(0,0,0,0) 3px);
  background-repeat:no-repeat}
tr.grp:hover td{color:#6a5590;background-image:none}
/* 厂商分组：马卡龙糖条 */
tbody tr.grp td{background:linear-gradient(90deg,#f0e8ff,#e8f6ff 52%,#e6fbf3);
  color:#6a5590;font-weight:800;font-size:10.5px;letter-spacing:.12em;text-transform:uppercase;
  text-align:left;padding:8px 14px;border-bottom:1px solid rgba(255,255,255,.95);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),inset 3px 0 0 rgba(255,157,186,.9)}
tbody tr.grp:hover td{background:linear-gradient(90deg,#e9ddff,#dff2ff 52%,#d9f8ee)}
.price{color:var(--ink);font-weight:600}
.disc{color:var(--ink-2)}
.disc.hi{color:var(--bad);font-weight:700}
.tok{color:var(--ink-3);font-size:11px;font-weight:600}
.call{color:#a267d8;font-size:11px;font-weight:700}
.muted{color:var(--ink-3);font-size:12px}
td .muted,th .muted{font-size:11.5px}
td.nowrap{white-space:nowrap}
/* 「未测」琥珀色：与空值「-」区分——这是没跑过，不是跑不了（三态空值语义） */
.un{color:#c08a2e;font-weight:600;font-size:11px}
/* 最右列（最近12次状态）钉在右侧：横向滚动时始终完整可见，不再被裁切。
   必须给不透明背景，否则其后方滚过的单元格会透出来 */
#mainTable th.stick-r,#mainTable td.stick-r{
  position:sticky;right:0;z-index:8;
  background:linear-gradient(180deg,#fffdfe,#fdf6ff 70%,#f8eeff);
  box-shadow:-9px 0 12px -7px rgba(146,116,178,.4)}
#mainTable tbody tr:hover td.stick-r{background:rgba(255,214,227,.4)}
#mainTable tbody tr.grp td.stick-r{background:linear-gradient(90deg,#f0e8ff,#e8f6ff 52%,#e6fbf3)}
/* 排序翻页感：旧内容先淡出一半再回满，制造"换了一批"的节奏，不做位移 */
.swap{animation:dip 200ms var(--ease)}
@keyframes dip{0%{opacity:1}40%{opacity:.28}100%{opacity:1}}

/* ───────── 链接 / 小控件 ───────── */
.mlink{color:var(--ink);font-weight:650;text-decoration:none;border-bottom:1.5px solid transparent;
  transition:color var(--t1) var(--ease),border-color var(--t1) var(--ease)}
.mlink:hover{color:#e2618f;border-bottom-color:rgba(255,157,186,.6)}
.lbtn{color:#c2457a;cursor:pointer;font-size:11.5px;font-weight:700;padding:3px 11px;border-radius:999px;
  white-space:nowrap;border:1.5px solid rgba(255,255,255,.95);
  background:linear-gradient(180deg,#fff,#ffe4ef);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 3px 8px -3px rgba(255,141,176,.7);
  transition:transform var(--t1) var(--spring),box-shadow var(--t1) var(--ease)}
.lbtn:hover{transform:translateY(-1px) scale(1.03);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 8px 14px -6px rgba(255,141,176,.8)}
.lbtn:active{transform:translateY(1px) scale(.99);box-shadow:inset 0 2px 6px rgba(200,100,150,.3)}
.exp{display:inline-flex;align-items:center;margin-left:6px;padding:0 9px;border-radius:999px;
  font-size:10.5px;line-height:17px;font-weight:700;color:#8a6fb8;cursor:pointer;user-select:none;
  white-space:nowrap;vertical-align:1px;border:1.5px solid rgba(255,255,255,.95);
  background:linear-gradient(180deg,#fff,#f0e9ff);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 2px 5px rgba(146,116,178,.2);
  transition:all var(--t1) var(--spring)}
.exp:hover{transform:translateY(-1px);color:#7a5aad;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 6px 12px -4px rgba(146,116,178,.55)}
.exp:active{transform:translateY(1px);box-shadow:inset 0 2px 5px rgba(146,116,178,.3)}

/* ───────── 可用性格：小糖果 ───────── */
.spark{display:inline-flex;gap:3px;align-items:center;vertical-align:middle}
.spark i{width:6px;height:13px;border-radius:4px;display:inline-block;
  background:rgba(146,116,178,.16);box-shadow:inset 0 1px 0 rgba(255,255,255,.7);
  transition:transform var(--t1) var(--spring)}
.spark i.ok{background:linear-gradient(180deg,#8ef0c8,#3fc59a);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.85),0 2px 5px rgba(63,197,154,.5)}
.spark i.bad{background:linear-gradient(180deg,#ffe08a,#f0b64a);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.85),0 2px 5px rgba(240,182,74,.5)}
.spark i.bad2{background:linear-gradient(180deg,#ffa8b6,#e8617a);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.8),0 2px 5px rgba(232,97,122,.45)}
.spark i:hover{transform:scaleY(1.25) scaleX(1.15)}

/* ───────── 对比页 ───────── */
.cmphero{position:sticky;top:0;z-index:30;margin:0 0 16px;padding:18px 22px 16px;
  border-radius:var(--r-xl);border:1.5px solid rgba(255,255,255,.96);
  background:linear-gradient(180deg,rgba(255,255,255,.95),rgba(255,247,252,.85));
  backdrop-filter:blur(20px) saturate(1.3);
  box-shadow:inset 0 2px 1px rgba(255,255,255,.98),
             0 8px 18px -10px rgba(146,116,178,.5),
             0 34px 60px -30px rgba(146,116,178,.75)}
.cmphero-row{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
.cmphero-title{font-size:19px;font-weight:800;letter-spacing:-.015em}
.cmpbadge{margin-left:10px;padding:3px 11px;border-radius:999px;font-size:11px;font-weight:750;
  color:#a04b7c;background:linear-gradient(180deg,#fff,#ffe1ee);
  border:1.5px solid rgba(255,255,255,.95);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 3px 8px -3px rgba(255,141,176,.6);
  vertical-align:2px}
.cmphero-actions{display:flex;align-items:center;gap:12px}
.cmptoggle{display:inline-flex;align-items:center;gap:7px;font-size:12px;color:var(--ink-2);
  font-weight:550;cursor:pointer;user-select:none;white-space:nowrap;transition:color var(--t1) var(--ease)}
.cmptoggle:hover{color:var(--ink)}
.cmptoggle input{width:auto;height:auto;margin:0;padding:0;cursor:pointer;
  accent-color:var(--sakura-deep);background:none;box-shadow:none}
.cmphero-kpi{display:flex;gap:26px;align-items:baseline;flex-wrap:wrap;margin-top:13px}
.cmphero-kpi>span{display:inline-flex;align-items:baseline;gap:6px;font-size:10.5px;
  color:var(--ink-3);letter-spacing:.1em;text-transform:uppercase;font-weight:750;white-space:nowrap}
.cmphero-kpi b{font-family:var(--font-round);font-size:16.5px;font-weight:800;color:var(--ink);
  letter-spacing:-.015em;text-transform:none}
.cmphero-kpi .muted{text-transform:none;letter-spacing:0;font-size:11.5px}
.cmphero-kpi>.cmphero-note{flex:1 1 100%;font-size:11px;color:#b0a3c2;font-weight:500;
  margin-top:1px;letter-spacing:0;text-transform:none;white-space:normal}
.backlink{display:inline-flex;align-items:center;gap:6px;color:#a04b7c;cursor:pointer;
  font-size:12px;font-weight:700;height:31px;padding:0 15px;border-radius:999px;
  border:1.5px solid rgba(255,255,255,.95);
  background:linear-gradient(180deg,#fff,#ffe4ef);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 3px 8px -3px rgba(255,141,176,.65);
  transition:all var(--t1) var(--spring)}
.backlink:hover{transform:translateY(-2px) scale(1.02);color:#8f3f6c;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 10px 18px -8px rgba(255,141,176,.85)}
.backlink:active{transform:translateY(1px);box-shadow:inset 0 2px 6px rgba(200,100,150,.28)}
.cmpwrap{overflow-x:auto;scrollbar-width:none;max-width:100%}
.cmpwrap::-webkit-scrollbar{display:none}
.cmp{background:rgba(255,255,255,.58);border-radius:var(--r-l);overflow:hidden}
.cmp thead{position:static;box-shadow:none}
.cmp th{position:static}
/* 前三低价不再整行染色：只保留名次徽标，行底色与普通行一致 */
.cmp tr.cheap td{background:transparent;box-shadow:none}
.rb{display:inline-block;margin-right:7px;padding:1px 9px;border-radius:999px;font-size:10px;
  line-height:16px;font-weight:800;vertical-align:1px;white-space:nowrap;color:#fff;
  text-shadow:0 1px 1px rgba(120,60,100,.25);
  box-shadow:inset 0 1.5px 0 rgba(255,255,255,.85),0 3px 7px -2px rgba(146,116,178,.7)}
.rb1{background:linear-gradient(180deg,#7fe3b8,#28ac7d)}
.rb2{background:linear-gradient(180deg,#b9a6ff,#8a6fe0)}
.rb3{background:linear-gradient(180deg,#ffd98a,#eaa22e)}
.pbarcell{position:relative}
.pbarcell .pbar{position:absolute;left:8px;right:auto;top:auto;bottom:5px;height:3px;border-radius:99px;
  background:linear-gradient(90deg,rgba(255,157,186,.5),rgba(169,146,240,.85));
  box-shadow:0 1px 3px rgba(169,146,240,.5)}
.pbarcell .pval{position:relative;z-index:1}

/* ───────── 骨架 / 空态 / 提示 ───────── */
.sk{display:block;height:10px;border-radius:99px;
  background:linear-gradient(90deg,rgba(199,178,224,.22),rgba(255,214,227,.75),rgba(199,178,224,.22));
  background-size:220% 100%;animation:shimmer 1.4s linear infinite;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.8)}
@keyframes shimmer{from{background-position:120% 0}to{background-position:-120% 0}}
.skrow td{padding:12px 11px}
.skrow td:not(:first-child) .sk{margin-left:auto}
.empty{display:flex;flex-direction:column;align-items:center;gap:11px;padding:62px 20px;color:var(--ink-3)}
.empty .ring{width:46px;height:46px;border-radius:50%;display:grid;place-items:center;
  font-size:17px;font-weight:800;color:#b07bd0;
  background:radial-gradient(circle at 50% 18%,#fff,#f4e9ff 70%);
  border:1.5px solid rgba(255,255,255,.95);
  box-shadow:inset 0 2px 1px rgba(255,255,255,.98),inset 0 -6px 12px -8px rgba(169,146,240,.6),
             0 8px 16px -8px rgba(146,116,178,.7)}
.empty b{font-weight:750;font-size:14px;color:var(--ink)}
.empty>div{font-size:11.5px;color:var(--ink-2)}
.empty .lbtn{font-size:11.5px;margin-top:2px}
/* 失败态换一套冷色，和「无结果」的紫色区分开 */
.empty.err .ring{color:#d9566a;
  background:radial-gradient(circle at 50% 18%,#fff,#ffeef0 70%);
  box-shadow:inset 0 2px 1px rgba(255,255,255,.98),inset 0 -6px 12px -8px rgba(217,86,106,.55),
             0 8px 16px -8px rgba(217,86,106,.6)}
.empty.err b{color:#c2455c}
/* 仅供读屏软件：视觉隐藏但可被朗读 */
.sr-only{position:absolute;width:1px;height:1px;margin:-1px;padding:0;overflow:hidden;
  clip:rect(0 0 0 0);white-space:nowrap;border:0}
/* ───────── 多模型对比托盘 ───────── */
.cmptray{position:fixed;left:50%;bottom:22px;transform:translate(-50%,18px);z-index:98;
  display:none;align-items:center;gap:8px;max-width:min(92vw,860px);padding:9px 12px;
  border-radius:var(--r-l);border:1.5px solid rgba(255,255,255,.96);
  background:linear-gradient(180deg,rgba(255,255,255,.97),rgba(255,244,250,.95));
  backdrop-filter:blur(16px);
  box-shadow:inset 0 2px 1px rgba(255,255,255,.98),
             0 10px 20px -10px rgba(146,116,178,.5),
             0 30px 54px -26px rgba(255,141,176,.95);
  opacity:0;transition:opacity var(--t2) var(--ease),transform var(--t3) var(--spring)}
.cmptray.on{display:flex;opacity:1;transform:translate(-50%,0)}
.ct-item{display:inline-flex;align-items:center;gap:5px;padding:3px 8px 3px 10px;border-radius:999px;
  font-size:11.5px;font-weight:600;color:#6a5590;white-space:nowrap;
  background:linear-gradient(180deg,#fff,#f3ecff);border:1px solid rgba(255,255,255,.95);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95)}
.ct-item i{font-style:normal;cursor:pointer;color:#b0557f;font-size:10px;padding:0 2px}
.ct-item i:hover{color:#d9566a}
.ct-act{font-size:11.5px;color:var(--ink-2);font-weight:600;white-space:nowrap}
.ct-act b{color:#b0557f;font-weight:800}
.cmptray button{height:30px;padding:0 14px;font-size:12px}
.ct-clear{font-size:11.5px;color:#b0557f;cursor:pointer;font-weight:650;white-space:nowrap;
  padding:3px 8px;border-radius:999px;transition:background var(--t1) var(--ease)}
.ct-clear:hover{background:#ffe9f2}
.cmpcell{display:flex;gap:5px;align-items:center;justify-content:flex-end}
.addbtn{width:19px;height:19px;margin-left:6px;border-radius:50%;display:inline-grid;
  place-items:center;font-size:11px;font-weight:800;line-height:1;cursor:pointer;flex:none;
  vertical-align:1px;color:#a992f0;
  background:linear-gradient(180deg,#fff,#f0e9ff);border:1.5px solid rgba(255,255,255,.95);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.95),0 2px 4px rgba(146,116,178,.25);
  transition:all var(--t1) var(--spring)}
.addbtn:hover{transform:scale(1.12);color:#e2618f}
.addbtn.on{color:#fff;background:linear-gradient(180deg,#8ef0c8,#3fc59a);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.6),0 2px 6px rgba(63,197,154,.5)}
.toast{position:fixed;left:50%;bottom:28px;transform:translate(-50%,14px) scale(.96);z-index:99;
  padding:11px 20px;border-radius:999px;font-size:12.5px;font-weight:650;color:var(--ink);
  border:1.5px solid rgba(255,255,255,.95);
  background:linear-gradient(180deg,rgba(255,255,255,.98),rgba(255,240,247,.95));
  backdrop-filter:blur(14px);
  box-shadow:inset 0 2px 1px rgba(255,255,255,.98),
             0 8px 16px -8px rgba(146,116,178,.5),
             0 22px 40px -20px rgba(255,141,176,.9);
  opacity:0;pointer-events:none;
  transition:opacity var(--t2) var(--ease),transform var(--t3) var(--spring)}
.toast.on{opacity:1;transform:translate(-50%,0) scale(1)}

/* ───────── 响应式 ───────── */
@media(max-width:1180px){.stats{grid-template-columns:repeat(2,minmax(0,1fr))}}
/* 笔记本宽度（1280 上下）：全列放下会很挤，收紧单元格内距把表格拉回容器内，
   比直接砍列更好——这一档用户仍要看全部分析列 */
@media(min-width:1101px) and (max-width:1500px){
  th,td{padding:7px 8px}
  .addbtn{width:16px;height:16px;margin-left:4px;font-size:10px}
  .spark{gap:2px}
  .spark i{width:5px}
}
@media(hover:none){.stat{transform:none!important}}
@media(max-width:1100px){
  table{border-radius:var(--r-m)}
  #mainTable td:first-child,#mainTable th:first-child{position:sticky;left:0;z-index:12;
    background:#fdf5fa;box-shadow:1px 0 0 rgba(255,255,255,.9)}
  #mainTable thead th:first-child{z-index:22;background:#fffafd}
  #mainTable tr:hover td:first-child{background:#ffe9f1}
  #mainTable tr.grp td:first-child{position:static;box-shadow:none;background:transparent}
}
@media(max-width:720px){
  .wrap{padding:16px 12px 60px}
  h1{font-size:17px}
  .mark{width:38px;height:38px;border-radius:14px;font-size:15px}
  .stat .val{font-size:22px}
  .stats{gap:10px}
  .kinds{display:flex;overflow-x:auto;scrollbar-width:none}
  .kinds::-webkit-scrollbar{display:none}
  .bar{padding:8px}
  .bar-actions{margin-left:0;width:100%;justify-content:flex-end}
  .cmphero-kpi{gap:18px}
  th,td{padding:7px 8px}
  .stat{padding:13px 14px 12px}
  /* 触屏目标：控件加高到 38px，接近 44px 可点区域 */
  input,select,button{height:38px}
  #mainTable td:first-child,#mainTable th:first-child{max-width:148px;overflow:hidden;text-overflow:ellipsis}
  #mainTable td:first-child .muted{display:none}
}
@media(max-width:480px){
  .top{padding:12px 14px;border-radius:var(--r-l)}
  .bar .search{flex:1 1 100%}
  .bar select{flex:1 1 calc(50% - 5px);min-width:0}
  .bar-actions button{flex:1 1 auto;justify-content:center}
  .chk{padding:0 2px}
  #stats{gap:9px}
  /* 手机竖屏再加到 42px，拇指点击不再误触相邻控件 */
  input,select,button{height:42px}
  .search svg{width:28px;height:28px}
  .search input{padding-left:50px}
  th,td{padding:7px 6px}
  #mainTable td:first-child,#mainTable th:first-child{max-width:140px}
}
@media(prefers-reduced-motion:reduce){
  *,*::before,*::after{animation-duration:.01ms!important;animation-iteration-count:1!important;
    transition-duration:.01ms!important;scroll-behavior:auto!important}
}
/* ── RelayLens 全站导航(2026-10-28 与主站整合;晚些时候几何+配色二次对齐)──
   吸顶常驻;配色/几何对齐 assets/theme-relayindex.css 的顶栏锁 v3:导航行 1560px 居中
   (各页容器差异与内联样式被 !important 压平)、高 60px/间距 22px、
   logo 定宽 150px+30px 绿色方块、字 13px、hover 浅绿底绿字、active 浅绿底+下划线内阴影,
   本页不引主站主题文件,故此处自带一份等价样式;scrollbar-gutter:stable 常驻滚动条槽防横移。
   下方看板自己的吸顶元素(表头 thead、对比页 .cmphero)top 同步 +60px,避免滑到导航底下 */
.rl-site-nav{position:sticky;top:0;z-index:50;background:rgba(255,255,255,.96);
  border-bottom:1px solid rgba(23,33,31,.14);backdrop-filter:blur(8px)}
html{scrollbar-gutter:stable}
.rl-site-nav .rl-wrap{max-width:1560px;margin:0 auto;padding:0 20px;height:60px;
  display:flex;align-items:center;gap:22px}
.rl-site-nav .rl-logo{display:inline-flex;align-items:center;gap:10px;flex:none;min-width:150px;
  font-weight:700;font-size:17px;letter-spacing:.02em;color:#17211f;text-decoration:none}
.rl-site-nav .rl-logo .rl-mark{display:inline-grid;place-items:center;width:30px;height:30px;flex:none;
  border-radius:7px;background:#176b58;color:#fff;font-size:15px;font-weight:800}
.rl-site-nav .nav-links{display:flex;gap:2px;flex:1;flex-wrap:nowrap;align-items:center;
  overflow-x:auto;scrollbar-width:none}
.rl-site-nav .nav-links::-webkit-scrollbar{display:none}
.rl-site-nav .nav-links a{flex:0 0 auto;box-sizing:border-box;font-size:13px;font-weight:500;
  color:#66736f;padding:7px 11px;border-radius:6px;text-decoration:none;white-space:nowrap;
  transition:background-color .15s,color .15s}
.rl-site-nav .nav-links a:hover{background:#e3f2eb;color:#176b58}
.rl-site-nav .nav-links a.active{background:#e3f2eb;color:#0e5746;font-weight:500;
  box-shadow:inset 0 -2px 0 #176b58}
thead{top:60px}
.cmphero{top:60px}
</style></head><body>
<header class="rl-site-nav"><div class="rl-wrap">
  <a class="rl-logo" href="__SITE_BASE__index.html" title="返回 RelayLens 首页"><span class="rl-mark">◎</span>RelayLens</a>
  __SITE_NAV__
</div></header>
<div class="wrap">
  <header class="top">
    <div class="brand">
      <div class="mark">¥</div>
      <div>
        <h1>中转站模型价格看板</h1>
        <div class="sub" id="meta">加载中…</div>
      </div>
    </div>
    <div class="topacts">
      <!-- 2026-10-28:原「← RelayLens」按钮移除——顶部已有全站导航条,品牌 logo 与「选站比价」项即回站入口 -->
      <button class="tbtn" id="selBtn" onclick="toggleView('selector')" title="按你的负载与场景，用实测数据推荐该用哪家">选型顾问</button>
      <button class="tbtn" id="chgBtn" onclick="toggleView('changes')">调价监控<span class="tbadge" id="chgN"></span></button>
      <button class="tbtn" id="stnBtn" onclick="toggleView('stations')">站点总览</button>
    </div>
  </header>

  <!-- 口径声明(2026-10-28):看板数据是公开接口自采的站方标价,与 RelayLens 实测口径不同,
       从主站点进来的用户必须能一眼分清两套数据,防止把标价当实测结论 -->
  <div class="caliber"><b>口径说明</b>
    本看板为独立比价工具:价格由各站公开接口自采的<b>站方标价</b>折算(含分组倍率与充值比例),
    非实测计费口径;实测榜单、逐笔对账与避雷黑名单见 <a href="__HOME_URL__">RelayLens 主站</a>。
  </div>

  <div class="fresh" id="fresh"></div>
  <section class="stats" id="stats"></section>

  <div class="bar">
    <div class="search">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round"><circle cx="11" cy="11" r="7"></circle><path d="M20.5 20.5 16.7 16.7"></path></svg>
      <input id="q" placeholder="搜索模型，如 claude / gpt-6 / gemini">
    </div>
    <select id="fStation"><option value="">全部站点</option></select>
    <select id="fVendor"><option value="">全部公司</option></select>
    <select id="fFamily"><option value="">全部系列</option></select>
    <select id="fModel"><option value="">全部模型版本</option></select>
    <label id="availWrap" class="chk" style="display:none"><input type="checkbox" id="fAvail" checked onchange="render()">只看可用分组</label>
    <div class="bar-actions">
      <div class="colwrap">
        <button id="colBtn" onclick="toggleColPop(event)" title="选择要显示的列">列<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round"><path d="M6 9l6 6 6-6"/></svg></button>
        <div class="colpop" id="colPop"></div>
      </div>
      <button onclick="resetFilters()" title="清空筛选，回到默认的「语言模型」视图">恢复默认</button>
      <button onclick="location.reload()" title="重新拉取最新抓取结果">刷新</button>
      <button onclick="exportJson()" title="导出当前筛选结果为 JSON（便于二次分析）">JSON</button>
      <button class="primary" onclick="exportCsv()">导出 CSV</button>
    </div>
  </div>

  <nav class="kinds" id="kinds">
    <span class="kind-ind" id="kindInd"></span>
    <button class="kind" data-kind="" onclick="setKind('')">全部<span class="n"></span></button>
    <button class="kind on" data-kind="text" onclick="setKind('text')">语言<span class="n"></span></button>
    <button class="kind" data-kind="image" onclick="setKind('image')">图片<span class="n"></span></button>
    <button class="kind" data-kind="video" onclick="setKind('video')">视频<span class="n"></span></button>
    <button class="kind" data-kind="audio" onclick="setKind('audio')">音频<span class="n"></span></button>
  </nav>

  <div class="card"><div id="chart"></div></div>
  <div class="tablescroll"><table id="mainTable"><thead id="thead"><tr></tr></thead><tbody id="tb"></tbody></table></div>

  <section class="view" id="viewChanges"></section>
  <section class="view" id="viewStations"></section>
  <section class="view" id="viewSelector"></section>
</div>
<footer class="legend" id="legend">
  <b>口径说明</b>
  <span><i>实付(¥/M)</i>＝模型倍率 × 分组倍率 × $2/M × 充值比例，跨站可比</span>
  <span><i>内部$/M</i>＝该站自己的额度美元，未按充值比例折算，仅用于核对站内价目</span>
  <span><i>对比官方价%</i>＝实付价 ÷ 官方价，低于 100% 即比官方便宜</span>
  <span><i>72h 可用率</i>＝近 3 天模型级真实调用探测的成功率</span>
  <span><i>TTFT</i>＝api-bench 实测首字延迟（请求发出→首个输出 token），p50/p90 分位；<i>缓存命中%</i>＝同前缀连发剔除冷启动后的命中占比——真实扣费实测，未跑的站显示「未测」</span>
  <span><i>快</i>＝服务端预计算的汇总缓存，抓取后自动重建</span>
</footer>
<div class="toast" id="toast"></div>
<div class="sr-only" id="live" role="status" aria-live="polite"></div>
<script>
// 默认按真实人民币排序（站内 $ 是内部额度美元，跨站不可比）。
// h:1 = 前端列表隐藏（数据仍在接口/CSV 里，需要时去掉标记即可恢复）
let rows=[],key='最低实付输入(¥/M)',asc=true,stSel='',famSel='',venSel='',modelSel='',kindSel='text';
// 默认排序：URL 序列化时用它判断"用户是否改过排序"
const DEF_SORT_KEY='最低实付输入(¥/M)', DEF_SORT_ASC=true;
let expanded=new Set();   // 单站页中已展开「其它分组」的模型名
let stDetail=null, summaryMeta='';
// ---- 通用交互反馈：骨架屏 / 轻提示 / 分段指示器 / 吸顶阴影 ----
let toastTimer=null;
function toast(msg){
  const el=document.getElementById('toast'); if(!el) return;
  el.textContent=msg; el.classList.add('on');
  clearTimeout(toastTimer); toastTimer=setTimeout(()=>el.classList.remove('on'),2400);
}
function skeleton(n){
  const tb=document.getElementById('tb'); if(!tb) return;
  let cols=8;
  // 列定义是后面才初始化的 const，首屏调用时可能还在 TDZ 里，取不到就退回 8 列
  try{ cols=Math.max(6, visibleCols().length); }catch(_e){}
  let h='';
  for(let i=0;i<n;i++){
    let cells='';
    for(let j=0;j<cols;j++){
      const w=42+((i*13+j*23)%48);
      cells+='<td><span class="sk" style="width:'+w+'%"></span></td>';
    }
    h+='<tr class="skrow">'+cells+'</tr>';
  }
  tb.innerHTML=h;
}
// 分段控件的高亮块在按钮之间滑动，位置随选中项与文案宽度变化
function syncKindInd(){
  const box=document.getElementById('kinds'), ind=document.getElementById('kindInd');
  if(!box||!ind) return;
  const on=box.querySelector('.kind.on');
  if(!on){ ind.style.opacity='0'; return; }
  ind.style.opacity='1';
  ind.style.width=on.offsetWidth+'px';
  ind.style.transform='translateX('+on.offsetLeft+'px)';
}
addEventListener('resize',()=>{
  clearTimeout(window.__kr);
  window.__kr=setTimeout(()=>{
    syncKindInd();
    // 跨过断点时才重渲染，避免拖拽窗口时反复重建表格
    const key=NARROW()+'|'+TINY();
    if(key!==__narrow){ __narrow=key; render(); }
  },140);
});
addEventListener('scroll',()=>{
  const th=document.getElementById('thead');
  document.body.classList.toggle('stuck', !!th && th.getBoundingClientRect().top<=0 && window.scrollY>8);
},{passive:true});
// 指标卡视差：跟随指针做 ±6° 倾斜，移开即回正。
// 触屏（hover:none）与「减少动态效果」下直接跳过，避免误触发与眩晕。
// ---- 数据新鲜度条：每站一颗状态糖，抓取停摆时立刻可见 ----
// 后端返回的是 UTC 时刻，直接截取会显示成 UTC 时间，用户在东八区会误读
function fmtLocal(iso){
  if(!iso) return '';
  const d=new Date(iso);
  if(isNaN(d.getTime())) return String(iso).replace('T',' ').slice(0,16);
  const p=n=>String(n).padStart(2,'0');
  return (d.getMonth()+1)+'-'+p(d.getDate())+' '+p(d.getHours())+':'+p(d.getMinutes());
}
let dataAgeHours=null;
function renderFreshness(list){
  const box=document.getElementById('fresh'); if(!box) return;
  if(!list.length){ box.innerHTML=''; return; }
  const fmtAge=h=>{
    if(h==null) return '从未成功';
    if(h<1) return Math.max(1,Math.round(h*60))+' 分前';
    if(h<48) return h.toFixed(h<10?1:0)+' 小时前';
    return Math.round(h/24)+' 天前';
  };
  let stale=0, worst=null;
  list.forEach(f=>{
    const h=f.check_hours;      // 用「上次成功检查」判定采集是否还在跑
    // 「从未抓到」的站单独计数，不能混进最大时长里，否则把有数据的站也判成"尚无记录"
    if(h==null){ stale++; return; }
    if(h>=24) stale++;
    worst=(worst==null)?h:Math.max(worst,h);
  });
  dataAgeHours=worst;
  // 明确列出缺哪几站：只说"2 站待更新"用户不知道结论少了谁
  const staleNames=list.filter(f=>f.check_hours==null||f.check_hours>=24).map(f=>f.name);
  const note=staleNames.length
    ? '<span class="fnote">缺 '+staleNames.slice(0,3).join('、')
      +(staleNames.length>3?(' 等 '+staleNames.length+' 站'):'')+' 的数据，结论可能不完整</span>'
    : '';
  const head='<span class="fchip hd"><b>数据新鲜度</b>'
    +(stale?(' · '+stale+' 站待更新'):' · 全部最新')+'</span>'+note;
  const chips=list.map(f=>{
    const h=f.check_hours;
    const cls=(h==null||h>=24)?'bad':(h>=12?'warn':'ok');
    const tip=(f.last_check?('上次检查 '+fmtLocal(f.last_check)):'从未成功抓取')
      +(f.change_hours!=null?('；价格 '+fmtAge(f.change_hours)+'未变动'):'')
      +(f.error?('；最近错误：'+f.error):'');
    return '<span class="fchip '+cls+'" title="'+esc(tip)+'"><i></i>'+esc(f.name)
      +' <b>'+fmtAge(h)+'</b></span>';
  }).join('');
  box.innerHTML=head+chips;
}
function dataHint(){
  const h=dataAgeHours;
  if(h==null) return '尚无成功抓取记录';
  if(h>=24) return '⚠ 已 '+Math.round(h)+' 小时未成功抓取，检查 watch 任务';
  return '距上次抓取 '+h.toFixed(h<10?1:0)+' 小时';
}
// ---- 视图切换：调价监控 / 站点总览（与对比页同一套「聚焦模式」思路）----
let curView='';
function toggleView(v){
  const same=(curView===v);
  curView=same?'':v;
  document.body.classList.remove('v-changes','v-stations','v-selector');
  if(curView) document.body.classList.add('v-'+curView);
  document.getElementById('chgBtn').classList.toggle('on',curView==='changes');
  document.getElementById('stnBtn').classList.toggle('on',curView==='stations');
  document.getElementById('selBtn').classList.toggle('on',curView==='selector');
  if(!curView){ document.getElementById('chart').innerHTML=''; return; }
  if(curView==='changes') loadChanges();
  else if(curView==='stations') loadStations();
  else loadSelector();
}
function closeViews(){ toggleView(curView); }
const CHG_RANGES={'7':168,'30':720,'90':2160};
let chgData=null, chgSort={k:'ts',asc:false};
function loadChanges(){
  const hours=CHG_RANGES[document.getElementById('chgRange')?.value||'7']||168;
  const box=document.getElementById('viewChanges');
  box.innerHTML='<div class="panel"><div class="muted">正在加载调价记录…</div></div>';
  fetch('/api/changes?hours='+hours).then(r=>r.json()).then(d=>{
    if(d.error){ box.innerHTML='<div class="panel"><div class="empty err"><div class="ring">!</div>'
      +'<b>加载失败</b><div>'+esc(d.error)+'</div></div></div>'; return; }
    chgData=d; chgSort={k:'ts',asc:false};
    renderChanges();
  }).catch(e=>{ box.innerHTML='<div class="panel"><div class="empty err"><div class="ring">!</div>'
    +'<b>加载失败</b><div>'+esc(String(e))+'</div></div></div>'; });
}
function chgSortBy(k){
  if(chgSort.k===k) chgSort.asc=!chgSort.asc; else {chgSort.k=k;chgSort.asc=(k==='ts')?false:true;}
  renderChanges();
}
function renderChanges(){
  const d=chgData||{rows:[],summary:{}};
  const box=document.getElementById('viewChanges');
  const rows=d.rows.slice();
  const sgn=chgSort.asc?1:-1;
  rows.sort((a,b)=>{
    const k=chgSort.k;
    const va=(k==='pct')?a.pct:(k==='station'?String(a.station):(k==='model'?String(a.model):String(a.ts)));
    const vb=(k==='pct')?b.pct:(k==='station'?String(b.station):(k==='model'?String(b.model):String(b.ts)));
    if(typeof va==='number'&&typeof vb==='number') return (va-vb)*sgn;
    return String(va).localeCompare(String(vb))*sgn;
  });
  const sm=d.summary||{};
  const up=sm.up||0, down=sm.down||0, tot=sm.total||0;
  const arrow=k=>chgSort.k===k?(chgSort.asc?' ▲':' ▼'):'';
  const trs=rows.slice(0,600).map(x=>{
    const up1=(x.pct||0)>0;
    const cls=up1?'up':'down';
    const sign=up1?'+':'';
    return '<tr><td class="mono">'+esc(fmtLocal(x.ts))+'</td>'
      +'<td>'+esc(x.station)+'</td>'
      +'<td><a class="mlink" href="#cmp:'+encodeURIComponent(x.model)+'" '
      +'onclick="event.stopPropagation()">'+esc(x.model)+'</a></td>'
      +'<td>'+esc(x.group)+'</td>'
      +'<td class="num mono">'+fmt(x.old)+'</td>'
      +'<td class="num mono">'+fmt(x.new)+'</td>'
      +'<td class="num mono '+cls+'">'+(up1?'↑':'↓')+' '+sign+fmt(Math.abs(x.pct),2)+'%</td></tr>';
  }).join('');
  box.innerHTML='<div class="panel">'
    +'<div class="phead"><div><b>调价监控</b><span class="psub">近 '+d.hours/24+' 天 · '
    +'按输入价 $/M 变动（按次/按秒计价模型不在此列）</span></div>'
    +'<div class="pacts"><select id="chgRange" onchange="loadChanges()">'
    +'<option value="7"'+(d.hours===168?' selected':'')+'>近 7 天</option>'
    +'<option value="30"'+(d.hours===720?' selected':'')+'>近 30 天</option>'
    +'<option value="90"'+(d.hours===2160?' selected':'')+'>近 90 天</option>'
    +'</select><span class="backlink" onclick="closeViews()">← 返回列表</span></div></div>'
    +'<div class="kpis">'
    +'<div class="kpi"><span class="kl">涨价</span><span class="kv up">'+up+'</span></div>'
    +'<div class="kpi"><span class="kl">降价</span><span class="kv down">'+down+'</span></div>'
    +'<div class="kpi"><span class="kl">合计变动</span><span class="kv">'+tot+'<small>条</small></span></div>'
    +'<div class="kpi"><span class="kl">涉及站点 / 模型</span><span class="kv">'
    +(sm.stations||0)+'<small>/ '+(sm.models||0)+'</small></span></div>'
    +'</div>'
    +(rows.length
      ?'<table class="dtab"><thead><tr>'
        +'<th onclick="chgSortBy(&quot;ts&quot;)">时间'+arrow('ts')+'</th>'
        +'<th onclick="chgSortBy(&quot;station&quot;)">站点'+arrow('station')+'</th>'
        +'<th onclick="chgSortBy(&quot;model&quot;)">模型'+arrow('model')+'</th>'
        +'<th>分组</th><th class="num">旧 $/M</th><th class="num">新 $/M</th>'
        +'<th class="num" onclick="chgSortBy(&quot;pct&quot;)">涨跌幅'+arrow('pct')+'</th>'
        +'</tr></thead><tbody>'+trs+'</tbody></table>'
        +(rows.length>600?'<div class="muted" style="margin-top:8px">仅显示前 600 条，完整数据可用 '
          +'<code>alerts</code> 命令导出</div>':'')
      :'<div class="empty"><div class="ring">∅</div><b>这段时间内没有价格变动</b>'
        +'<div>价格稳定，或抓取间隔内无变化</div></div>')
    +'</div>';
}
// ══════ 选型顾问（引擎复刻 RelayLens selector，UI 与看板同源）════════
// 原则：①只用实测数据，缺维度重归一化降权而非编造 ②成本按你的真实负载算（含缓存命中收益）
// ③被排除的选项与排除原因并列展示 ④TTFT/可用率未测的站如实标注
const SEL_SCENES={
  general:{label:'通用对话',  in:5000, out:800, hr:.5, w:{cost:40,stab:35,perf:25}},
  code:   {label:'编程 Agent',in:10000,out:1500,hr:.6, w:{cost:30,stab:25,perf:45}},
  office: {label:'办公批处理',in:400, out:150, hr:.1, w:{cost:45,stab:35,perf:20}},
  long:   {label:'长文写作',  in:40000,out:3000,hr:.3, w:{cost:35,stab:30,perf:35}},
  biz:    {label:'企业接入',  in:5000, out:800, hr:.5, w:{cost:20,stab:60,perf:20}},
};
let selInited=false;
const selMoney=v=>v==null?'-':'¥'+(v>=1000?Math.round(v).toLocaleString('en-US'):v.toFixed(v<10?2:1));
function loadSelector(){
  const box=document.getElementById('viewSelector');
  if(selInited){ return; }
  box.innerHTML='<div class="panel"><div class="muted">正在加载模型清单…</div></div>';
  fetch('/api/summary').then(r=>r.json()).then(d=>{
    const models=[...new Map((d.rows||[])
      .filter(r=>r['计费方式']==='per_token')
      .map(r=>[r['模型规范名'],r])).values()]
      .sort((a,b)=>(b['在售站数']||0)-(a['在售站数']||0));
    box.innerHTML=selFormHtml(models);
    selInited=true;
    document.getElementById('selScene').dispatchEvent(new Event('change'));
  }).catch(e=>{ box.innerHTML='<div class="panel"><div class="empty err"><div class="ring">!</div>'
    +'<b>加载失败</b><div>'+esc(String(e))+'</div></div></div>'; });
}
function selFormHtml(models){
  const opts=models.map(m=>'<option value="'+esc(m['模型规范名'])+'">'
    +esc(m['模型规范名'])+'（'+(m['在售站数']||0)+' 站在售）</option>').join('');
  const scenes=Object.entries(SEL_SCENES).map(([k,s])=>
    '<option value="'+k+'">'+esc(s.label)+'</option>').join('');
  return '<div class="panel"><div class="phead"><div><b>选型顾问</b>'
    +'<span class="psub">按你的真实负载算月成本，按场景权重给综合推荐——只用实测数据，缺维度降权而非编造</span></div>'
    +'<div class="pacts"><span class="backlink" onclick="closeViews()">← 返回列表</span></div></div>'
    +'<div class="sel-grid"><div class="sel-form">'
    +'<label>使用场景<select id="selScene" onchange="selApplyScene()">'+scenes+'</select></label>'
    +'<label>目标模型<select id="selModel">'+(opts||'<option value="">（无按量计费模型）</option>')+'</select>'
    +'<span class="hint">仅列出有跨站在售的按量计费模型</span></label>'
    +'<div class="row2">'
    +'<label>日请求数<input id="selDaily" type="number" min="1" value="100"></label>'
    +'<label>缓存命中率<input id="selHr" type="range" min="0" max="95" value="50">'
    +'<span class="hint" id="selHrVal">50%</span></label></div>'
    +'<div class="row2">'
    +'<label>平均输入 tokens<input id="selIn" type="number" min="1" value="5000"></label>'
    +'<label>平均输出 tokens<input id="selOutTok" type="number" min="1" value="800"></label></div>'
    +'<div class="row2">'
    +'<label>月预算上限（¥，选填）<input id="selBudget" type="number" min="0" placeholder="不限"></label>'
    +'<label>首字延迟要求<select id="selTtft"><option value="0">不限</option>'
    +'<option value="3000">≤ 3s</option><option value="1500">≤ 1.5s</option></select>'
    +'<span class="hint">未实测 TTFT 的站将被排除并说明</span></label></div>'
    +'<button class="primary" onclick="runSelector()">生成推荐</button>'
    +'<span class="hint">测算会发真实请求吗？——不会。全部基于已采集的价目与实测指标离线计算</span>'
    +'</div><div class="sel-out" id="selOut">'
    +'<div class="sel-note">选好左侧参数后点「生成推荐」。权重随场景变化：'
    +Object.values(SEL_SCENES).map(s=>esc(s.label)+'（价'+s.w.cost+'/稳'+s.w.stab+'/性'+s.w.perf+'）').join(' · ')
    +'</div></div></div></div>';
}
function selApplyScene(){
  const s=SEL_SCENES[document.getElementById('selScene').value]||SEL_SCENES.general;
  document.getElementById('selIn').value=s.in;
  document.getElementById('selOutTok').value=s.out;
  document.getElementById('selHr').value=Math.round(s.hr*100);
  document.getElementById('selHrVal').textContent=Math.round(s.hr*100)+'%';
}
function runSelector(){
  const model=document.getElementById('selModel').value;
  if(!model){ toast('没有可选模型'); return; }
  const daily=Math.max(1,+document.getElementById('selDaily').value||100);
  const inTok=Math.max(1,+document.getElementById('selIn').value||5000);
  const outTok=Math.max(1,+document.getElementById('selOutTok').value||800);
  const hr=Math.min(.95,Math.max(0,+document.getElementById('selHr').value||0))/100;
  const budget=+document.getElementById('selBudget').value||0;
  const ttftReq=+document.getElementById('selTtft').value||0;
  const scene=SEL_SCENES[document.getElementById('selScene').value]||SEL_SCENES.general;
  const out=document.getElementById('selOut');
  out.innerHTML='<div class="muted">正在计算…</div>';
  Promise.all([
    fetch('/api/compare?model='+encodeURIComponent(model)).then(r=>r.json()),
    fetch('/api/bench?limit=200').then(r=>r.json())
  ]).then(([cmp,bench])=>{
    const ttftBy={};
    (bench.rows||[]).forEach(b=>{
      if(!b.ok||b.ttft_p50_ms==null) return;
      if(!(b.station in ttftBy)||b.ttft_p50_ms<ttftBy[b.station]) ttftBy[b.station]=b.ttft_p50_ms;
    });
    // 每站只留成本最低的分组（同站多分组是渠道差异，选型时按最优渠道算）
    const bySt={};
    (cmp.rows||[]).forEach(r=>{
      if(r['计费方式']!=='per_token') return;
      if(r['实付输入(¥/M)']==null) return;
      const st=r['站点'];
      // 缓存读价未公示的站按输入价保守估算（宁可高估不虚低）
      const cr=(r['实付缓存读(¥/M)']!=null)?r['实付缓存读(¥/M)']:r['实付输入(¥/M)'];
      const perReq=(inTok*(1-hr)*r['实付输入(¥/M)']+inTok*hr*cr+outTok*r['实付输出(¥/M)'])/1e6;
      const monthly=perReq*daily*30;
      if(!(st in bySt)||monthly<bySt[st].monthly) bySt[st]={
        station:st,group:r['分组'],monthly,input:r['实付输入(¥/M)'],
        cacheMissing:r['实付缓存读(¥/M)']==null,
        avail:(r['72小时可用率']&&r['72小时可用率'].p)!=null?r['72小时可用率'].p:null,
        ttft:ttftBy[st]||null};
    });
    const cands=Object.values(bySt), rej=[];
    cands.forEach(c=>{
      if(budget&&c.monthly>budget)
        rej.push({s:c.station,r:'月成本 '+selMoney(c.monthly)+' 超预算 '+selMoney(budget)});
      else if(ttftReq&&c.ttft==null)
        rej.push({s:c.station,r:'TTFT 未实测，无法确认 ≤'+(ttftReq/1000)+'s 的要求'});
      else if(ttftReq&&c.ttft>ttftReq)
        rej.push({s:c.station,r:'TTFT '+fmt(c.ttft)+'ms 超出 ≤'+(ttftReq/1000)+'s 要求'});
    });
    const ok=cands.filter(c=>!rej.some(x=>x.s===c.station));
    if(!ok.length){
      out.innerHTML='<div class="empty"><div class="ring">∅</div><b>没有满足条件的站点</b>'
        +'<div>'+(rej.length?'全部 '+rej.length+' 个候选被排除，原因见下':'该模型暂无按量计费候选')+'</div></div>'
        +(rej.length?'<div class="sel-rej">'+rej.map(x=>'<div><b>'+esc(x.s)+'</b>：'+esc(x.r)+'</div>').join('')+'</div>':'');
      return;
    }
    const minCost=Math.min(...ok.map(c=>c.monthly));
    ok.forEach(c=>{
      c.cs=100*minCost/c.monthly;                       // 成本分：线性，最便宜=100
      c.ss=(c.avail!=null)?c.avail:null;                // 稳定分：72h 可用率直取
      c.ps=(c.ttft!=null)?Math.max(0,Math.min(100,100-(c.ttft-800)/(6000-800)*100)):null;
      const w=scene.w, ws=[];
      if(c.cs!=null)ws.push(w.cost); if(c.ss!=null)ws.push(w.stab); if(c.ps!=null)ws.push(w.perf);
      const sum=ws.reduce((a,b)=>a+b,0)||1;
      c.coverage=sum/(w.cost+w.stab+w.perf);
      c.score=Math.round(((c.cs*w.cost+(c.ss||0)*w.stab+(c.ps||0)*w.perf)/sum)*10)/10;
    });
    ok.sort((a,b)=>b.score-a.score);
    const cheap=[...ok].sort((a,b)=>a.monthly-b.monthly)[0];
    const fast=[...ok].filter(c=>c.ttft!=null).sort((a,b)=>a.ttft-b.ttft)[0];
    const stab=[...ok].filter(c=>c.ss!=null).sort((a,b)=>b.ss-a.ss)[0];
    const card=(t,v,s,win)=>'<div class="sel-card'+(win?' win':'')+'"><div class="t">'+t
      +'</div><div class="v">'+v+'</div><div class="s">'+s+'</div></div>';
    let html='<div class="sel-top">'
      +card('综合推荐',esc(ok[0].station),'综合 '+ok[0].score+' 分 · 月成本 '+selMoney(ok[0].monthly)
        +' · TTFT '+(ok[0].ttft!=null?fmt(ok[0].ttft)+'ms':'未测')+' · 可用率 '
        +(ok[0].ss!=null?fmt(ok[0].ss)+'%':'未测'),true)
      +(cheap&&cheap!==ok[0]?card('最便宜',esc(cheap.station),'月成本 '+selMoney(cheap.monthly)
        +'（'+esc(cheap.group)+' 组）· 综合 '+cheap.score+' 分'):'' )
      +(fast?card('首字最快',esc(fast.station),'TTFT '+fmt(fast.ttft)+'ms · 月成本 '+selMoney(fast.monthly)):'')
      +(stab&&stab!==ok[0]?card('最稳',esc(stab.station),'72h 可用率 '+fmt(stab.ss)+'%'):'')
      +'</div>';
    html+='<table class="sel-table"><thead><tr><th>站点</th><th>分组</th><th>月成本</th>'
      +'<th>综合</th><th>价</th><th>稳</th><th>性</th><th>TTFT</th><th>72h可用</th></tr></thead><tbody>'
      +ok.map(c=>{
        const un='<span class="un">未测</span>';
        const ms=v=>v==null?un:fmt(v)+'ms';
        const pc=v=>v==null?un:fmt(v)+'%';
        return '<tr><td><b>'+esc(c.station)+'</b></td><td class="muted">'+esc(c.group)+'</td>'
          +'<td class="num">'+selMoney(c.monthly)+'</td><td class="num"><b>'+c.score+'</b>'
          +(c.coverage<0.66?' <span class="un">低数据</span>':'')+'</td>'
          +'<td class="num">'+fmt(Math.round(c.cs))+'</td>'
          +'<td class="num">'+(c.ss!=null?fmt(Math.round(c.ss)):un)+'</td>'
          +'<td class="num">'+(c.ps!=null?fmt(Math.round(c.ps)):un)+'</td>'
          +'<td class="num">'+(c.ttft!=null?fmt(c.ttft)+'ms':un)+'</td>'
          +'<td class="num">'+(c.ss!=null?pc(c.ss):un)+'</td></tr>';
      }).join('')+'</tbody></table>';
    const notes=[];
    const cm=[...new Set(ok.filter(c=>c.cacheMissing).map(c=>c.station))];
    if(cm.length) notes.push('「'+esc(cm.join('、'))+'」未公示缓存读价，缓存部分按输入价保守估算（实际可能更省）');
    notes.push('月成本 =（输入×('+(100-Math.round(hr*100))+'%直连 + '+Math.round(hr*100)+'%缓存命中）+ 输出）× '+daily+' 次/日 × 30 天，含缓存收益');
    notes.push('稳定分=72h 可用率直取；性能分=TTFT 线性（≤800ms=100，≥6s=0）；未测维度已重归一化降权，不记零分');
    html+='<div class="sel-note">'+notes.map(n=>'· '+n).join('<br>')+'</div>';
    if(rej.length)
      html+='<div class="sel-rej"><b>被排除的选项（'+rej.length+'）：</b>'
        +rej.map(x=>'<div>· <b>'+esc(x.s)+'</b>：'+esc(x.r)+'</div>').join('')+'</div>';
    out.innerHTML=html;
  }).catch(e=>{ out.innerHTML='<div class="empty err"><div class="ring">!</div>'
    +'<b>计算失败</b><div>'+esc(String(e))+'</div></div>'; });
}
function loadStations(){
  const box=document.getElementById('viewStations');
  box.innerHTML='<div class="panel"><div class="muted">正在加载站点信息…</div></div>';
  fetch('/api/stations').then(r=>r.json()).then(d=>{
    if(d.error){ box.innerHTML='<div class="panel"><div class="empty err"><div class="ring">!</div>'
      +'<b>加载失败</b><div>'+esc(d.error)+'</div></div></div>'; return; }
    renderStations(d.stations||[]);
  }).catch(e=>{ box.innerHTML='<div class="panel"><div class="empty err"><div class="ring">!</div>'
    +'<b>加载失败</b><div>'+esc(String(e))+'</div></div></div>'; });
}
function fmtAge(h){
  if(h==null) return '从未成功';
  if(h<1) return Math.max(1,Math.round(h*60))+' 分钟前';
  if(h<48) return h.toFixed(h<10?1:0)+' 小时前';
  return Math.round(h/24)+' 天前';
}
function renderStations(list){
  const box=document.getElementById('viewStations');
  const okN=list.filter(f=>f.check_hours!=null&&f.check_hours<24).length;
  const prN=list.reduce((a,f)=>a+(f.probes_24h||0),0);
  const prF=list.reduce((a,f)=>a+(f.probe_fail_24h||0),0);
  const trs=list.map(f=>{
    const h=f.check_hours;
    const cls=(h==null||h>=24)?'up':(h>=12?'':'down');
    const rate=f.recharge_rate==null?'未设置':('¥'+fmt(f.recharge_rate)+'/$');
    // 模型探测：每次都是真实 API 调用，失败率高时提示可能是 key 失效
    const p=f.probes_24h||0, pf=f.probe_fail_24h||0;
    const pcls=p===0?'':(pf/p>=0.5?'up':(pf>0?'':'down'));
    const pTxt=p===0?'—':(p+(pf?('（失 '+pf+'）'):''));
    return '<tr><td class="mono">'+esc(f.name)+'</td>'
      +'<td>'+esc(f.mode)+'</td>'
      +'<td class="'+cls+'">'+(f.state||'')+'</td>'
      +'<td class="num">'+f.models+'</td>'
      +'<td class="mono">'+fmtAge(h)+'</td>'
      +'<td class="mono">'+(f.change_hours==null?'-':fmtAge(f.change_hours))+'</td>'
      +'<td class="mono">'+rate+'</td>'
      +'<td class="num mono '+pcls+'" title="近 24 小时模型级真实调用探测次数">'+pTxt+'</td>'
      +'<td class="muted" title="'+esc(f.error||'')+'">'+esc((f.error||'—').slice(0,46))+'</td></tr>';
  }).join('');
  box.innerHTML='<div class="panel">'
    +'<div class="phead"><div><b>站点总览</b><span class="psub">'+list.length+' 个启用站点 · '
    +okN+' 个 24 小时内成功抓取</span></div>'
    +'<div class="pacts"><span class="backlink" onclick="closeViews()">← 返回列表</span></div></div>'
    +'<table class="dtab"><thead><tr><th>站点</th><th>模式</th><th>状态</th>'
    +'<th class="num">模型数</th><th>上次检查</th><th>价格未变动</th><th>充值比例</th>'
    +'<th class="num" title="近 24 小时模型级真实调用探测次数（每次都是真实 API 调用）">模型探测</th>'
    +'<th>最近错误</th></tr></thead><tbody>'+trs+'</tbody></table>'
    +'<div class="muted" style="margin-top:10px">近 24 小时共 '+prN+' 次真实调用探测，失败 '+prF
    +' 次；失败率高的站通常是 key 失效或模型下架，可关掉探测省钱：'
    +'<code>--model-probe-interval 0</code></div>'
    +'<div class="muted">状态说明：active=正常；auth_required=需登录 cookie；'
    +'unsupported=接口非标准；offline=连续失败已标记离线</div></div>';
}
const statsBox=document.getElementById('stats');
function resetTilt(except){
  if(!statsBox) return;
  [...statsBox.children].forEach(c=>{
    if(c===except) return;
    c.style.setProperty('--rx','0deg'); c.style.setProperty('--ry','0deg');
  });
}
if(statsBox){
  statsBox.addEventListener('pointermove',e=>{
    if(e.pointerType!=='mouse') return;
    if(matchMedia('(prefers-reduced-motion: reduce)').matches) return;
    const card=e.target.closest && e.target.closest('.stat'); if(!card) return;
    const r=card.getBoundingClientRect();
    const dx=(e.clientX-r.left)/r.width-.5, dy=(e.clientY-r.top)/r.height-.5;
    card.style.setProperty('--ry',(dx*6).toFixed(2)+'deg');
    card.style.setProperty('--rx',(-dy*6).toFixed(2)+'deg');
    resetTilt(card);
  },{passive:true});
  statsBox.addEventListener('pointerleave',()=>resetTilt(null));
}
const COLS_SUMMARY=[{k:'模型规范名',t:'模型'},{k:'公司',t:'公司',h:1},{k:'计费方式',t:'计费',h:1},{k:'最低价来自站点',t:'最低价站点'},{k:'充值比例(¥/$)',t:'充值比例'},{k:'最低价分组',t:'分组'},{k:'分组倍率',t:'倍率'},
  {k:'模型在该分组可用',t:'可用',h:1},{k:'最低价分辨率',t:'分辨率'},
  {k:'最低按秒价格(¥/秒)',t:'¥/秒'},{k:'最低按次价格(¥/次)',t:'¥/次'},
  {k:'最低按秒价格($/秒)',t:'内部$/秒',h:1},{k:'最低按次价格($/次)',t:'内部$/次',h:1},
  {k:'最低实付输入(¥/M)',t:'输入',g:'Token价格（¥/M）'},{k:'最低实付输出(¥/M)',t:'输出',g:'Token价格（¥/M）'},
  {k:'最低实付缓存读(¥/M)',t:'读取',g:'缓存价格（¥/M）'},{k:'最低实付缓存写(¥/M)',t:'写入',g:'缓存价格（¥/M）'},
  {k:'最低实付输入($/M)',t:'输入内部$/M',h:1},
  {k:'官方输入(¥/M)',t:'输入',g:'官方价格（¥/M）'},{k:'官方输出(¥/M)',t:'输出',g:'官方价格（¥/M）'},
  {k:'对比官方价(%)',t:'对比官方价%'},
  {k:'TTFT p50(ms)',t:'TTFT'},{k:'TTFT p90(ms)',t:'TTFT p90',h:1},
  {k:'缓存命中%',t:'缓存命中'},{k:'实测时间',t:'实测时间',h:1},
  {k:'72小时可用率',t:'72h可用率'},
  {k:'最近12次状态',t:'最近 12 次状态'}];
const COLS_STATION=[{k:'模型规范名',t:'模型'},{k:'公司',t:'公司',h:1},{k:'计费方式',t:'计费',h:1},{k:'分组',t:'分组'},{k:'分组倍率',t:'倍率'},
  {k:'最近12次状态',t:'最近 12 次状态'},
  {k:'模型在该分组可用',t:'可用',h:1},{k:'价格来源',t:'来源',h:1},{k:'分辨率',t:'分辨率'},
  {k:'按秒价格(¥/秒)',t:'¥/秒'},{k:'按次价格(¥/次)',t:'¥/次'},
  {k:'按秒价格($/秒)',t:'内部$/秒',h:1},{k:'按次价格($/次)',t:'内部$/次',h:1},
  {k:'实付输入(¥/M)',t:'输入',g:'Token价格（¥/M）'},{k:'实付输出(¥/M)',t:'输出',g:'Token价格（¥/M）'},
  {k:'实付缓存读(¥/M)',t:'读取',g:'缓存价格（¥/M）'},{k:'实付缓存写(¥/M)',t:'写入',g:'缓存价格（¥/M）'},
  {k:'实付输入($/M)',t:'输入内部$/M',h:1},
  {k:'实付缓存读($/M)',t:'缓存读$/M',h:1},{k:'实付缓存写($/M)',t:'缓存写$/M',h:1},
  {k:'官方输入(¥/M)',t:'输入',g:'官方价格（¥/M）'},{k:'官方输出(¥/M)',t:'输出',g:'官方价格（¥/M）'},
  {k:'对比官方价(%)',t:'对比官方价%'}];
// 按秒（视频）/按次（图片·音乐）行的 token 口径列一律显示「-」，不让 model_ratio=0 的 $0/M 误导
const TOKEN_COLS=['实付输入($/M)','实付输出($/M)','实付输入(¥/M)','实付输出(¥/M)',
  '实付缓存读($/M)','实付缓存写($/M)','实付缓存读(¥/M)','实付缓存写(¥/M)',
  '官方输入(¥/M)','官方输出(¥/M)',
  '最低实付输入($/M)','最低实付输出($/M)','最低实付输入(¥/M)','最低实付输出(¥/M)',
  '最低实付缓存读(¥/M)','最低实付缓存写(¥/M)','官方输出(¥/M)','对比官方价(%)'];
const BILL_LABEL={per_token:'按量',per_second:'按秒',per_call:'按次'};
fetch('/api/summary').then(r=>r.json()).then(d=>{
  // 后端异常时服务返回 {error}，必须显式提示，否则页面只是空表，看不出是接口挂了
  if(d.error){ rows=[]; initFilters(); render();
    document.getElementById('meta').textContent='接口错误：'+d.error;
    failState(d.error); return; }
  // generated_at 现在是「最近一次成功抓取」，不再是请求时刻；按本地时区显示
  const ts=fmtLocal(d.generated_at);
  rows=d.rows||[]; summaryMeta=ts+' · '+rows.length+' 个模型 · '+d.stations+' 个站点';
  document.getElementById('meta').textContent=summaryMeta;
  renderFreshness(d.freshness||[]);
  applyURLState();          // 先恢复链接里的筛选，再初始化下拉与渲染
  initFilters(); render();
  if(stSel) selectStation(stSel);   // 链接指定了站点就拉该站明细
  // 顺手把调价条数挂到页头按钮上（只取计数，不渲染列表）
  fetch('/api/changes?hours=168').then(r=>r.json()).then(c=>{
    const n=(c.summary&&c.summary.total)||0;
    const el=document.getElementById('chgN');
    if(el) el.textContent=n?('近7天 '+n):'';
  }).catch(()=>{});
}).catch(e=>{ failState(String(e&&e.message||e)); });
// 失败态：保留表头骨架，明确告知是接口而非"没数据"
function failState(msg){
  document.getElementById('meta').textContent='数据接口异常';
  document.getElementById('tb').innerHTML='<tr><td colspan="6"><div class="empty err">'
    +'<div class="ring">!</div><b>数据加载失败</b><div>'+esc(msg)+'</div>'
    +'<span class="lbtn" onclick="location.reload()">重试</span></div></td></tr>';
  toast('数据接口异常');
}
const uniq=a=>[...new Set(a)].sort();
// 公司：后端按 vendor_map.json 判定并随行返回（「公司」列），不再靠前端前缀猜
const companyOf=r=>r['公司']||'';
// 首页厂商分组排序：OpenAI / Anthropic 置顶，其余按字母序
const VEN_TOP=['openai','anthropic'];
function venOrder(v){ const i=VEN_TOP.indexOf(String(v||'').toLowerCase()); return i<0?VEN_TOP.length:i; }
// 当前数据集：选中了某站=该站明细，否则=全局汇总。下拉选项据此联动
const currentRows=()=>stDetail?stDetail.rows:rows;
// 供下拉选项使用的数据池：当前数据集再按「除自己外」的其它维度过滤，实现多向联动
function poolRows(exclude){
  let rs=currentRows();
  if(kindSel) rs=rs.filter(kindMatch);
  if(exclude!=='ven'&&venSel) rs=rs.filter(r=>companyOf(r)===venSel);
  if(exclude!=='fam'&&famSel) rs=rs.filter(r=>r['模型家族']===famSel);
  if(exclude!=='mod'&&modelSel) rs=rs.filter(r=>r['模型规范名']===modelSel);
  return rs;
}
// 切换「语言/图片/视频/音频」大类：再点一次同一个＝取消回到全部
// 「语言模型」只看白名单（核心模型），图片/视频/音频仍按模型类型
function kindMatch(r){
  if(!kindSel) return true;
  return kindSel==='text' ? r['核心模型']==='是' : r['模型类型']===kindSel;
}
function setKind(k){
  kindSel=(kindSel===k)?'':k;
  refreshStations(); refreshVendors(); refreshFamilies(); refreshModels(); render();
}
// 按模型大类隐藏与当前计费口径无关的列（同一套规则同时作用于汇总页与单站页）：
//   语言/音频 → 隐藏「分辨率」「¥/秒」「¥/次」（token 计价，无分辨率、不按时长/按次收费）
//   图片/视频 → 隐藏三类 ¥/M 价格组（Token / 缓存 / 官方），只留 ¥/秒、¥/次、分辨率
const HIDE_BY_KIND={
  text:['最低价分辨率','分辨率','最低按秒价格(¥/秒)','最低按次价格(¥/次)',
        '按秒价格(¥/秒)','按次价格(¥/次)'],
  audio:['最低价分辨率','分辨率','最低按秒价格(¥/秒)','最低按次价格(¥/次)',
         '按秒价格(¥/秒)','按次价格(¥/次)'],
  image:['最低实付输入(¥/M)','最低实付输出(¥/M)','最低实付缓存读(¥/M)','最低实付缓存写(¥/M)',
         '最低实付输入($/M)','官方输入(¥/M)','官方输出(¥/M)',
         '实付输入(¥/M)','实付输出(¥/M)','实付缓存读(¥/M)','实付缓存写(¥/M)',
         '实付输入($/M)','实付输出($/M)','实付缓存读($/M)','实付缓存写($/M)'],
  video:['最低实付输入(¥/M)','最低实付输出(¥/M)','最低实付缓存读(¥/M)','最低实付缓存写(¥/M)',
         '最低实付输入($/M)','官方输入(¥/M)','官方输出(¥/M)',
         '实付输入(¥/M)','实付输出(¥/M)','实付缓存读(¥/M)','实付缓存写(¥/M)',
         '实付输入($/M)','实付输出($/M)','实付缓存读($/M)','实付缓存写($/M)'],
};
function visibleCols(){
  const cols=stDetail?COLS_STATION:COLS_SUMMARY;
  const hide=HIDE_BY_KIND[kindSel]||[];
  const userHide=colPrefs()[stDetail?'station':'summary']||[];
  return cols.filter(c=>!c.h&&hide.indexOf(c.k)<0&&userHide.indexOf(c.k)<0);
}
// ---- 列显隐偏好：按「汇总/单站」两套分别记忆，localStorage 持久化 ----
const COL_PREF_KEY='relay_colprefs_v1';
function colPrefs(){
  try{ return JSON.parse(localStorage.getItem(COL_PREF_KEY)||'{}'); }
  catch(_e){ return {}; }
}
function saveColPrefs(p){
  try{ localStorage.setItem(COL_PREF_KEY, JSON.stringify(p)); }catch(_e){}
}
// 弹层挪到 body 下：工具栏的 backdrop-filter 会创建层叠上下文，
// 弹层留在里面无论 z-index 多高都会被吸顶表头盖住
(function portalColPop(){
  const pop=document.getElementById('colPop');
  if(pop && pop.parentElement!==document.body) document.body.appendChild(pop);
})();
// 按「列」按钮的实时位置定位弹层（滚动/缩放时重算，使其跟随页面）。
// 紧贴按钮正下方、左边缘对齐；高度按可用空间自适应（内部滚动），
// 仅当按钮靠近视口底部、下方放不下时才翻到正上方——保证始终锚定在按钮附近
function positionColPop(){
  const pop=document.getElementById('colPop');
  if(!pop||!pop.classList.contains('on')) return;
  const btn=document.getElementById('colBtn');
  if(!btn) return;
  const r=btn.getBoundingClientRect();
  const pw=pop.offsetWidth||212;
  const GAP=4;                                   // 与按钮的最小间距（紧贴感）
  const EDGE=12;                                 // 离视口边的留白
  // 左边缘对齐按钮，再夹在视口内（不超出左右边界）
  const left=Math.max(8, Math.min(r.left, window.innerWidth-pw-8));
  // 优先放按钮下方；下方空间不够（<200）才翻到上方，高度取可用空间、过长内部滚动
  const below=r.bottom+GAP, belowSpace=window.innerHeight-below-EDGE;
  let top, maxH;
  if(belowSpace>=200){ top=below; maxH=Math.min(520, belowSpace); }
  else { const above=r.top-GAP, aboveSpace=r.top-EDGE;
         top=above-Math.min(520, aboveSpace); maxH=Math.min(520, aboveSpace);
         if(top<EDGE) top=EDGE; }
  pop.style.maxHeight=Math.max(160,maxH)+'px';
  // 仅当表头真正吸顶（th.top<=0）且弹层会没入表头时，才压到表头下沿
  const th=document.getElementById('thead');
  if(th){
    const tr=th.getBoundingClientRect();
    if(tr.top<=0 && top<tr.bottom+GAP) top=tr.bottom+GAP;
  }
  pop.style.left=left+'px';
  pop.style.top=top+'px';
}
function toggleColPop(ev){
  ev&&ev.stopPropagation();
  const pop=document.getElementById('colPop');
  const willOpen=!pop.classList.contains('on');
  if(willOpen){
    renderColPop();
    pop.classList.add('on');   // 先显示，positionColPop 才能拿到真实 offsetHeight
    positionColPop();
    // 过渡结束后布局可能再变（字体/换行），再校一次
    requestAnimationFrame(positionColPop);
  } else {
    pop.classList.remove('on');
  }
}
// 弹层开启时跟随滚动/缩放：按钮在视口里移动，弹层也要跟着移动
window.addEventListener('scroll',positionColPop,{passive:true});
window.addEventListener('resize',positionColPop);
function renderColPop(){
  const pop=document.getElementById('colPop');
  const cols=stDetail?COLS_STATION:COLS_SUMMARY;
  const which=stDetail?'station':'summary';
  const hidden=(colPrefs()[which]||[]);
  // 分组列（g 字段）作为小标题，下面的子列归到它名下；同名分组只出现一次
  const groups=[]; let cur=null;
  cols.forEach(c=>{
    if(c.h) return;                       // 默认隐藏的列不列出来
    if(c.g){
      if(!cur||cur.g!==c.g){ cur={g:c.g,items:[]}; groups.push(cur); }
    } else {
      cur=null;                           // 无分组列独立成组（不显示标题）
      groups.push({g:null,items:[]});
    }
    groups[groups.length-1].items.push(c);
  });
  let html='<div class="cp-head"><b>显示列</b>'
    +'<span class="cp-reset" onclick="resetColPrefs()">重置</span></div>';
  groups.forEach(gr=>{
    if(gr.g) html+='<div class="cp-grp">'+esc(gr.g)+'</div>';
    gr.items.forEach(c=>{
      const on=hidden.indexOf(c.k)<0;
      html+='<label><input type="checkbox"'+(on?' checked':'')+' data-k="'+esc(c.k)+'">'
        +esc(c.t||c.k)+'</label>';
    });
  });
  pop.innerHTML=html;
  pop.querySelectorAll('input[data-k]').forEach(cb=>{
    cb.onchange=()=>{
      const p=colPrefs();
      const list=new Set(p[which]||[]);
      if(cb.checked) list.delete(cb.dataset.k); else list.add(cb.dataset.k);
      p[which]=[...list];
      saveColPrefs(p);
      render();
    };
  });
}
function resetColPrefs(){
  const p=colPrefs();
  delete p[stDetail?'station':'summary'];
  saveColPrefs(p);
  renderColPop();
  render();
}
// 点击面板外部即收起（弹层已挂 body，自身与触发按钮都不算"外部"）
document.addEventListener('click',e=>{
  const pop=document.getElementById('colPop');
  if(!pop||!pop.classList.contains('on')) return;
  if(e.target.closest('.colwrap')||e.target.closest('.colpop')) return;
  pop.classList.remove('on');
});
// 当前视图的默认排序列：语言/音频按 token 输入价；图片/视频按时长或按次实付价
function defaultSortKey(){
  if(kindSel==='image'||kindSel==='video'){
    const sec=r=>{const v=r['最低按秒价格(¥/秒)'];return v!==''&&v!=null;};
    return baseRows().some(sec)?'最低按秒价格(¥/秒)':'最低按次价格(¥/次)';
  }
  return '最低实付输入(¥/M)';
}
// 排序列若在当前视图被隐藏（如切到图片/视频后 token 价列消失），换回该视图默认列
function ensureSortKey(){
  if(visibleCols().some(c=>c.k===key)) return;
  key=defaultSortKey();
}
function initFilters(){
  refreshStations(); refreshVendors(); refreshFamilies(); refreshModels();
}
// 反向联动：站点下拉按当前公司/系列/版本收窄（只列出有匹配模型的站）；基于全局汇总的在售站点
function refreshStations(){
  let base=rows;
  if(kindSel) base=base.filter(kindMatch);
  if(venSel) base=base.filter(r=>companyOf(r)===venSel);
  if(famSel) base=base.filter(r=>r['模型家族']===famSel);
  if(modelSel) base=base.filter(r=>r['模型规范名']===modelSel);
  const sts=uniq(base.flatMap(r=>(r['在售站点']||'').split('、')).filter(Boolean));
  document.getElementById('fStation').innerHTML='<option value="">全部站点</option>'+sts.map(s=>'<option>'+esc(s)+'</option>').join('');
  document.getElementById('fStation').value=sts.includes(stSel)?stSel:'';
  if(stSel && !sts.includes(stSel)){ stSel=''; stDetail=null; }   // 当前站不再匹配则退回全局
}
function refreshVendors(){
  const vens=uniq(poolRows('ven').map(companyOf).filter(Boolean));
  document.getElementById('fVendor').innerHTML='<option value="">全部公司</option>'+vens.map(s=>'<option>'+esc(s)+'</option>').join('');
  document.getElementById('fVendor').value=vens.includes(venSel)?venSel:'';
  if(!vens.includes(venSel)) venSel='';
}
function refreshFamilies(){
  const fams=uniq(poolRows('fam').map(r=>r['模型家族']).filter(Boolean));
  document.getElementById('fFamily').innerHTML='<option value="">全部系列</option>'+fams.map(s=>'<option>'+esc(s)+'</option>').join('');
  document.getElementById('fFamily').value=fams.includes(famSel)?famSel:'';
  if(!fams.includes(famSel)) famSel='';
}
function refreshModels(){
  const mods=uniq(poolRows('mod').map(r=>r['模型规范名']).filter(Boolean));
  document.getElementById('fModel').innerHTML='<option value="">全部模型版本</option>'+mods.map(s=>'<option>'+esc(s)+'</option>').join('');
  document.getElementById('fModel').value=mods.includes(modelSel)?modelSel:'';
  if(!mods.includes(modelSel)) modelSel='';
}
document.getElementById('q').addEventListener('input',()=>render());
document.getElementById('fStation').onchange=e=>{ selectStation(e.target.value); };
// 选中某站＝拉该站全量明细；URL 恢复与下拉切换共用这一条路径
function selectStation(name){
  stSel=name;
  const afterLoad=()=>{ refreshVendors(); refreshFamilies(); refreshModels(); render(); };
  if(stSel){
    document.getElementById('meta').textContent='正在加载 '+stSel+' 的全部模型…';
    skeleton(8);
    // 切站时重新播放指标卡的级联上浮，提示"换了数据集"
    const sb=document.getElementById('stats'); if(sb) sb.classList.remove('land');
    fetch('/api/station?name='+encodeURIComponent(stSel)).then(r=>r.json()).then(d=>{
      if(d.error){ stDetail=null; toast('站点数据加载失败：'+d.error); }
      else stDetail=d;
      afterLoad();
    }).catch(()=>{stDetail=null;toast('站点数据加载失败，请稍后重试');afterLoad();});
  } else { stDetail=null; afterLoad(); }
}
document.getElementById('fVendor').onchange=e=>{
  venSel=e.target.value; famSel=''; modelSel='';   // 公司变化→重置系列与版本
  refreshStations(); refreshFamilies(); refreshModels(); render();
};
document.getElementById('fFamily').onchange=e=>{
  famSel=e.target.value; modelSel='';              // 系列变化→重置版本
  refreshStations(); refreshModels(); render();
};
document.getElementById('fModel').onchange=e=>{
  modelSel=e.target.value;                         // 版本是叶子：反联动收窄站点/公司/系列选项
  refreshStations(); refreshVendors(); refreshFamilies(); render();
};
function resetFilters(){
  stSel=''; venSel=''; famSel=''; modelSel=''; kindSel='text'; stDetail=null;  // 清空筛选并回到默认「语言模型」
  document.getElementById('q').value='';           // 清空搜索
  document.getElementById('fAvail').checked=true;  // 恢复「只看可用分组」默认勾选
  refreshStations(); refreshVendors(); refreshFamilies(); refreshModels(); render();
}
// ---- 筛选状态写进 URL：刷新不丢、可直接分享 ----
// 用 replaceState 而非 pushState：每次输入都改 URL 但不堆历史，后退键仍然好用
function syncURL(){
  const p=new URLSearchParams();
  const q=(document.getElementById('q').value||'').trim();
  if(q) p.set('q',q);
  if(stSel) p.set('st',stSel);
  if(venSel) p.set('ven',venSel);
  if(famSel) p.set('fam',famSel);
  if(modelSel) p.set('mod',modelSel);
  if(kindSel) p.set('kind',kindSel);
  if(!document.getElementById('fAvail').checked) p.set('avail','0');
  if(key!==DEF_SORT_KEY || asc!==DEF_SORT_ASC){ p.set('sort',key); p.set('asc',asc?'1':'0'); }
  const qs=p.toString();
  history.replaceState(null,'',location.pathname+(qs?('?'+qs):'')+location.hash);
}
function applyURLState(){
  const p=new URLSearchParams(location.search);
  const q=p.get('q'); if(q) document.getElementById('q').value=q;
  if(p.has('kind')) kindSel=p.get('kind')||'';
  stSel=p.get('st')||''; venSel=p.get('ven')||''; famSel=p.get('fam')||''; modelSel=p.get('mod')||'';
  if(p.get('avail')==='0') document.getElementById('fAvail').checked=false;
  if(p.get('sort')){ key=p.get('sort'); asc=p.get('asc')!=='0'; }
  // 下拉的 value 要等 initFilters 重建选项后才有效，这里只恢复状态
}
document.getElementById('thead').onclick=e=>{
  const th=e.target.closest('th[data-k]'); if(!th)return;
  const k=th.dataset.k; if(k===key)asc=!asc; else {key=k;asc=true;} render();
};
// 表头键盘可达：Enter / 空格 触发排序（Tab 本身就能到达表头）
document.getElementById('thead').addEventListener('keydown',e=>{
  if(e.key!=='Enter'&&e.key!==' ') return;
  const th=e.target.closest('th[data-k]'); if(!th) return;
  e.preventDefault();
  const k=th.dataset.k; if(k===key)asc=!asc; else {key=k;asc=true;} render();
});
// 「/」直达搜索框：不在输入控件里时才拦截，避免打断打字
document.addEventListener('keydown',e=>{
  if(e.key!=='/'||e.metaKey||e.ctrlKey||e.altKey) return;
  const t=e.target;
  if(t&&(t.tagName==='INPUT'||t.tagName==='SELECT'||t.tagName==='TEXTAREA'||t.isContentEditable)) return;
  e.preventDefault();
  const q=document.getElementById('q'); q.focus(); q.select();
});
// 统一过滤：ignoreKind=true 时忽略大类（用于给四个按钮算各自的条数）
function filtered(ignoreKind){
  const kw=(document.getElementById('q').value||'').toLowerCase();
  let rs=stDetail?stDetail.rows:rows;
  if(kw) rs=rs.filter(r=>(r['模型规范名']||'').toLowerCase().includes(kw));
  if(!ignoreKind&&kindSel) rs=rs.filter(kindMatch);
  if(venSel) rs=rs.filter(r=>companyOf(r)===venSel);
  if(famSel) rs=rs.filter(r=>r['模型家族']===famSel);
  if(modelSel) rs=rs.filter(r=>r['模型规范名']===modelSel);
  if(stDetail){ if(document.getElementById('fAvail').checked) rs=rs.filter(r=>r['模型在该分组可用']==='是'); }
  else if(stSel) rs=rs.filter(r=>('、'+(r['在售站点']||'')+'、').includes('、'+stSel+'、'));
  return rs;
}
// 四个大类按钮上的条目数 + 高亮状态（「语言模型」按白名单计数）
function updateKindCounts(){
  const rs=filtered(true), c={text:0,image:0,video:0,audio:0};
  rs.forEach(r=>{
    if(r['核心模型']==='是') c.text++;
    if(r['模型类型']&&r['模型类型']!=='text') c[r['模型类型']]=(c[r['模型类型']]||0)+1;
  });
  document.querySelectorAll('.kind').forEach(b=>{
    const k=b.dataset.kind;
    b.classList.toggle('on', k===(kindSel||''));
    const n=b.querySelector('.n'); if(n) n.textContent=k?(' '+(c[k]||0)):(' '+rs.length);
  });
  syncKindInd();
}
function baseRows(){
  return filtered(false).slice().sort((a,b)=>{
    // 可用性小格子列：按「最近 12 次探测里的失败次数」排序，无数据的沉底
    if(key==='最近12次状态'){
      const f=r=>{const a=r['最近12次状态'];return Array.isArray(a)?a.filter(x=>!x.ok).length:null;};
      const x=f(a),y=f(b);
      if(x===null&&y===null) return 0;
      if(x===null) return 1;
      if(y===null) return -1;
      return (x-y)*(asc?1:-1);
    }
    // 72 小时可用率：按百分比排序，无探测数据的沉底
    if(key==='72小时可用率'){
      const f=r=>{const x=r['72小时可用率'];return (x&&typeof x==='object')?x.p:null;};
      const x=f(a),y=f(b);
      if(x===null&&y===null) return 0;
      if(x===null) return 1;
      if(y===null) return -1;
      return (x-y)*(asc?1:-1);
    }
    // 不同计费口径的列互相为空（token 行无 $/次、按次行无 $/M），
    // 空值统一沉底，避免 '' 被当字符串把按次行排到最前
    const r1=a[key],r2=b[key];
    const x=(r1===''||r1==null)?null:Number(r1), y=(r2===''||r2==null)?null:Number(r2);
    if(x!==null&&y!==null&&!isNaN(x)&&!isNaN(y)){const d=x-y; if(d) return d*(asc?1:-1);}
    else if(x===null&&y!==null) return 1;
    else if(y===null&&x!==null) return -1;
    return String(r1??'').localeCompare(String(r2??''))*(asc?1:-1);
  });
}
function cell(r,c){
  // 模型名可点：跳「该模型各站对比」（用 hash 路由，可前进/后退、可分享链接）
  if(c.k==='模型规范名'){
    const m=String(r['模型规范名']||'');
    const inSet=cmpSet.has(m);
    let html=`<a class="mlink" href="#cmp:${encodeURIComponent(m)}"`
      +` onclick="event.stopPropagation()" title="查看 ${esc(m)} 的各站对比">${esc(m)}</a>`
      // 多模型对比入口：点名字是单模型对比，点圆钮是加入多模型集合
      +`<span class="addbtn${inSet?' on':''}" data-m="${esc(m)}" title="${inSet?'从多模型对比中移除':'加入多模型对比'}" `
      +`onclick="event.stopPropagation();toggleCmpSet(this.dataset.m)">${inSet?'✓':'+'}</span>`;
    // 单站页：多分组模型在「最低价」行的模型名后加「展开/折叠其它分组」开关
    if(stDetail && r._ui && r._ui.n>1 && r._ui.best){
      const open=expanded.has(m), caret=open?'▾':'▸';
      const label=open?('收起 '+(r._ui.n-1)+' 组'):('+'+ (r._ui.n-1)+' 组');
      html+=` <span class="exp" onclick="event.stopPropagation();toggleModel('${encodeURIComponent(m)}')"`
        +` title="展开/折叠该模型其它分组">${caret}${label}</span>`;
    }
    // 汇总页：模型名下补官方锚价（¥/M），呼应「官方锚」分组展示
    if(!stDetail){
      const off=r['官方输入(¥/M)'];
      if(off!==''&&off!=null) html+=`<div class="muted" style="font-size:11px;line-height:1.35">官方 ¥${fmt(Number(off))}/M</div>`;
    }
    return `<td>${html}</td>`;
  }
  const v=r[c.k], b=r['计费方式']||'per_token', isTok=b==='per_token';
  if(c.k==='模型在该分组可用') return `<td>${v==='是'?'✔':'✘'}</td>`;
  if(c.k==='计费方式') return `<td><span class="${isTok?'tok':'call'}">${BILL_LABEL[b]||b}</span></td>`;
  if(!isTok&&TOKEN_COLS.includes(c.k)) return '<td class="muted">-</td>';
  // Token价格组的「输入」列加粗：默认排序按输入价，数字加粗提示用户排序依据
  if(c.k==='最低实付输入(¥/M)'||c.k==='实付输入(¥/M)'){
    if(v===''||v==null) return '<td class="muted">-</td>';
    return `<td class="price" style="font-weight:700">${fmt(Number(v))}</td>`;
  }
  if(c.k==='对比官方价(%)'){
    if(v===''||v==null) return '<td class="disc">-</td>';
    const n=Number(v);
    const s=n>=10?n.toFixed(0):(n>=1?n.toFixed(1):n.toPrecision(2));
    return `<td class="disc${n>100?' hi':''}">${s}%</td>`;
  }
  if(c.k==='官方输入(¥/M)') return `<td class="muted">${v===''||v==null?'-':fmt(v)}<span style="opacity:.55;font-size:11px"> ${esc(r['官方来源']||'')}</span></td>`;
  if(c.k==='官方输出(¥/M)'){
    // 无官方输出价但有输入价时也显示「-」，避免空单元格看不出是缺失
    if(v===''||v==null) return '<td class="muted">-</td>';
    return `<td class="price">${fmt(v)}</td>`;
  }
  if(c.k==='72小时可用率'){
    if(v==null||typeof v!=='object') return '<td class="muted">-</td>';
    const p=v.p, ok=v.ok, tot=v.tot, lst=v.lst||'';
    // 高可用（≥99%）绿色，95%~99% 琥珀，低于 95% 红色告警
    const color=p>=99?'var(--good)':(p>=95?'var(--warn)':'var(--bad)');
    const src=v.src==='probe'?'模型调用探测':(v.src==='station'?'站点可达性':'探测');
    const tip='最近 72 小时'+src+'：可达 '+ok+' / 共 '+tot+' 次'+(lst?('；'+esc(lst)):'');
    return `<td class="nowrap" style="color:${color};font-weight:600" title="${tip}">${p}%</td>`;
  }
  // 性能实测列（api-bench）：带单位展示；没测过显示琥珀「未测」而不是「-」，
  // 与「不适用」区分——这是没跑过，不是跑不了（三态空值语义）
  if(c.k==='TTFT p50(ms)'||c.k==='TTFT p90(ms)'){
    if(v===''||v==null) return '<td class="un">未测</td>';
    return `<td class="nowrap" title="${c.k}：请求发出到首个输出 token 的毫秒数（api-bench 实测）">${fmt(v)}ms</td>`;
  }
  if(c.k==='缓存命中%'){
    if(v===''||v==null) return '<td class="un">未测</td>';
    return `<td class="nowrap" title="同前缀连发剔除冷启动后 cache_read>0 的请求占比（api-bench 实测）">${fmt(v)}%</td>`;
  }
  if(c.k==='最近12次状态'){
    const arr=Array.isArray(v)?v:[];
    if(!arr.length) return '<td class="muted stick-r">-</td>';
    // 连续失败 2 次及以上用红色，最近一次失败用黄色（与告警色一致）
    let streak=0;
    for(let i=arr.length-1;i>=0;i--){ if(arr[i].ok) break; streak++; }
    const cells=arr.map(x=>{
      const cls=x.ok?'ok':(streak>=2?'bad2':'bad');
      const t=x.ok?'可达':'不可达';
      return `<i class="${cls}" title="${t} ${String(x.ts||'').slice(5,16).replace('T',' ')}"></i>`;
    }).join('');
    const bad=arr.filter(x=>!x.ok).length;
    const tip=`最近 ${arr.length} 次探测：可达 ${arr.length-bad}，不可达 ${bad}`;
    // stick-r：横向滚动时这列钉在最右，始终完整可见（不再被裁切）
    return `<td class="stick-r"><span class="spark" title="${tip}">${cells}</span></td>`;
  }
  // 充值比例：与单站页 meta 行同一形式 —— ¥N/$（1¥=M$），N=实际充值比例
  if(c.k==='充值比例(¥/$)'){
    if(v===''||v==null) return '<td class="muted">-</td>';
    const n=Number(v);
    if(!isFinite(n)||n<=0) return `<td class="muted">${esc(v)}</td>`;
    const inv=(1/n).toFixed(4).replace(/0+$/,'').replace(/\\.$/,'');
    return `<td class="muted" title="${fmt(n)}¥ 购买 $1 站点额度（来源：${esc(r['充值比例来源']||'未注明')}）">`
      +`¥${fmt(n)}/$（1¥=${inv}$）</td>`;
  }
  if(v===''||v==null) return '<td class="muted">-</td>';
  if(typeof v==='number'||(/\\(\\$\\/[次秒]\\)$/.test(c.k)&&!isNaN(Number(v)))) return `<td class="price">${fmt(Number(v))}</td>`;
  return `<td>${esc(v)}</td>`;
}
// 单站页：同一模型的「真实人民币」单价（按计费方式取对应列），用于挑出最低价分组
function realPriceOf(r){
  const b=r['计费方式']||'per_token';
  const k=b==='per_token'?'实付输入(¥/M)':(b==='per_call'?'按次价格(¥/次)':'按秒价格(¥/秒)');
  const v=r[k];
  if(v===''||v==null) return Infinity;
  const n=Number(v);
  return isNaN(n)?Infinity:n;
}
// 单站页折叠：同一模型只显示价格最低的「分组」行，其余打上 _ui.folded 隐藏，可展开
function applyFold(rs){
  rs.forEach(r=>{ r._ui=null; });
  if(!stDetail) return;
  const byModel={};
  rs.forEach(r=>{ (byModel[r['模型规范名']] ||= []).push(r); });
  for(const m in byModel){
    const arr=byModel[m];
    if(arr.length<=1) continue;       // 只有一个分组的模型无需折叠
    let bi=0, bp=Infinity;
    arr.forEach((r,i)=>{ const p=realPriceOf(r); if(p<bp){ bp=p; bi=i; } });
    arr.forEach((r,i)=>{ r._ui={best:i===bi, n:arr.length, folded:(i!==bi && !expanded.has(m))}; });
  }
}
function toggleModel(m){
  m=decodeURIComponent(m);
  if(expanded.has(m)) expanded.delete(m); else expanded.add(m);
  render();
}
// ---- 顶部指标卡：随当前筛选视图实时刷新 ----
function statTile(lab,val,hint,unit){
  const v=(typeof val==='number')?Number(val).toLocaleString('en-US'):esc(val);
  return '<div class="stat"><div class="lab">'+esc(lab)+'</div><div class="val">'+v
    +(unit?'<small>'+esc(unit)+'</small>':'')+'</div>'
    +(hint?'<div class="hint" title="'+esc(hint)+'">'+esc(hint)+'</div>':'')+'</div>';
}
function updateStats(data){
  const box=document.getElementById('stats'); if(!box) return;
  // 首屏（或切站后）才播放级联上浮；日常筛选不重复，避免动效干扰
  if(!box.classList.contains('land')) box.classList.add('land');
  const N=v=>{if(v===''||v==null)return null;const n=Number(v);return isFinite(n)&&n>0?n:null;};
  const byCall=kindSel==='image'||kindSel==='video';
  const pk=stDetail?(byCall?'按次价格(¥/次)':'实付输入(¥/M)')
                   :(byCall?'最低按次价格(¥/次)':'最低实付输入(¥/M)');
  const pkSec=stDetail?'按秒价格(¥/秒)':'最低按秒价格(¥/秒)';
  const useSec=byCall && data.some(r=>N(r[pkSec])!==null);
  const k=useSec?pkSec:pk;
  const models=new Set(), stations=new Set(), offs=[];
  let low=null;
  data.forEach(r=>{
    if(r['模型规范名']) models.add(r['模型规范名']);
    if(stDetail){ if(r['站点']) stations.add(r['站点']); }
    else String(r['在售站点']||'').split('、').filter(Boolean).forEach(s=>stations.add(s));
    const v=N(r[k]);
    if(v!==null && (!low||v<low.v)) low={v, m:r['模型规范名'], s:stDetail?r['站点']:(r['最低价来自站点']||'')};
    const op=r['对比官方价(%)'];
    if(op!==''&&op!=null&&isFinite(Number(op))) offs.push(Number(op));
  });
  const med=a=>{if(!a.length)return null;const s=a.slice().sort((x,y)=>x-y),n=s.length;
    return n%2?s[(n-1)/2]:(s[n/2-1]+s[n/2])/2;};
  const mp=med(offs);
  const unit=useSec?'/秒':(byCall?'/次':'/M');
  const lowTxt=low?('¥'+fmt(low.v)):'-';
  const lowHint=low?[low.m,low.s].filter(Boolean).join(' · '):'当前视图无价格数据';
  const ts=(summaryMeta.split(' · ')[0]||'').trim();
  // 该指标是「站点价 ÷ 官方价」，3.8% 表示只有官方价的 3.8%，不要写成"接近官方价"
  const mpTxt=mp==null?'-':((mp>=10?mp.toFixed(0):(mp>=1?mp.toFixed(1):mp.toPrecision(2)))+'%');
  const mpHint=mp==null?'缺少官方基准':(mp<100?('比官方价低 '+(100-mp).toFixed(0)+'%')
                                                 :('比官方价高 '+(mp-100).toFixed(0)+'%'));
  box.innerHTML=[
    statTile('模型', models.size, stDetail?('该站 '+data.length+' 个分组行'):('覆盖 '+stations.size+' 个站点')),
    statTile(useSec?'最低按秒价':(byCall?'最低按次价':'最低输入价'), lowTxt, lowHint, unit),
    statTile('对比官方价（中位）', mpTxt, mpHint),
    stDetail?statTile('该站充值比例', stDetail.exchange_rate==null?'未设置':('¥'+fmt(stDetail.exchange_rate)),
              stDetail.exchange_rate==null?'在 stations.json 补 recharge_rate':'¥ 购买 $1 站点额度')
            :statTile('数据更新时间', ts||'-', dataHint()),
  ].join('');
  // 无障碍播报：筛选变化后让读屏软件知道当前视图有多少内容（视觉上看指标卡即可）
  const live=document.getElementById('live');
  if(live) live.textContent=(stDetail?(stDetail.name+'：'):'全部站点：')+models.size+' 个模型，'
    +data.length+' 行，最低'+lowTxt+' 每 '+unit.replace('/','');
}
// 窄屏只保留决策必需的列，其余交给横向滚动/导出 CSV，避免整页被宽表撑开。
// 三档：宽屏全列 / 平板核心列 / 手机最小列（只留模型·站点·单价·对比）
const NARROW=()=>window.innerWidth<=1100;
const TINY=()=>window.innerWidth<=480;
const CORE_MOBILE=['模型规范名','最低价来自站点','分组','分组倍率','模型在该分组可用',
  '最低实付输入(¥/M)','最低实付输出(¥/M)','实付输入(¥/M)','实付输出(¥/M)',
  '最低按次价格(¥/次)','最低按秒价格(¥/秒)','按次价格(¥/次)','按秒价格(¥/秒)',
  '最低价分辨率','分辨率','对比官方价(%)','72小时可用率'];
const CORE_TINY=['模型规范名','最低价来自站点',
  '最低实付输入(¥/M)','实付输入(¥/M)','最低按次价格(¥/次)','按次价格(¥/次)',
  '最低按秒价格(¥/秒)','按秒价格(¥/秒)','对比官方价(%)'];
let __narrow=NARROW()+'|'+TINY();
function render(){
  ensureSortKey();
  const allowed=NARROW()?(TINY()?CORE_TINY:CORE_MOBILE):null;
  const view=visibleCols().filter(c=>!allowed||allowed.indexOf(c.k)>=0);
  const data=baseRows();
  applyFold(data);   // 单站页：按模型折叠，仅显示最低价分组
  // 汇总页按「公司（厂商）」分组排列：OpenAI / Anthropic 置顶，其余字母序；
  // 组内保持当前排序（稳定排序）
  if(!stDetail) data.sort((a,b)=>{
    const ra=venOrder(companyOf(a)), rb=venOrder(companyOf(b));
    return ra!==rb ? ra-rb : String(companyOf(a)).localeCompare(String(companyOf(b)));
  });
  document.getElementById('availWrap').style.display=stDetail?'':'none';
  if(stDetail){
    const er=stDetail.exchange_rate, inv=er?(1/er).toFixed(4):'?';
    document.getElementById('meta').textContent=stDetail.name+' · '+stDetail.models+
      ' 个模型 · '+data.length+' 行（模型×分组，默认每模型仅显示最低价分组，可展开）· 充值比例 ¥'+(er==null?'未获取':er)+
      '/$（1¥='+inv+'$）'+(stDetail.truncated?(' · 该站分组过多（原始 '+stDetail.total+
      ' 行），已按模型只保留最低价分组'):'');
  } else { document.getElementById('meta').textContent=summaryMeta; }
  updateStats(data);
  // 两级表头：有分组列时才需要第二行（子列表头），无分组的列跨两行 rowspan=2；
  // 若当前视图没有分组列（如图片/视频模型隐藏了全部 ¥/M 组），表头只剩一行，
  // 此时若仍写 rowspan="2" 会把第一行数据挤到右侧，必须退化为单行表头。
  const hasG=view.some(c=>c.g), rsp=hasG?' rowspan="2"':'';
  // 表头可聚焦 + aria-sort：键盘用户与读屏软件能感知"当前按哪列排"
  const gth=c=>c.cmp?'<th'+rsp+'>对比</th>':
    (c.g?'':'<th'+rsp+' tabindex="0" aria-sort="'+(key===c.k?(asc?'ascending':'descending'):'none')
      +'" data-k="'+c.k+'" class="'+(key===c.k?'on ':'')+(view[view.length-1]===c?'stick-r':'')+'">'+c.t
      +(key===c.k?('<span class="sa">'+(asc?'▲':'▼')+'</span>'):'')+'</th>');
  let head1='',head2='',i=0;
  while(i<view.length){
    const c=view[i];
    if(!c.g){ head1+=gth(c); i++; continue; }
    let j=i; while(j<view.length&&view[j].g===c.g) j++;
    const sub=view.slice(i,j);
    head1+='<th colspan="'+sub.length+'">'+c.g+'</th>';
    head2+=sub.map(x=>x.cmp?'<th>对比</th>':
      '<th tabindex="0" aria-sort="'+(key===x.k?(asc?'ascending':'descending'):'none')+'" data-k="'+x.k+'"'
      +(key===x.k?' class="on"':'')+'>'+x.t
      +(key===x.k?('<span class="sa">'+(asc?'▲':'▼')+'</span>'):'')+'</th>').join('');
    i=j;
  }
  document.getElementById('thead').innerHTML='<tr>'+head1+'</tr>'+(head2?'<tr>'+head2+'</tr>':'');
  let lastVen=null;
  const body=data.length?data.slice(0,600).map(r=>{
    const hidden=(r._ui&&r._ui.folded)?' style="display:none"':'';
    const onclick=stDetail?'':` onclick="hist(decodeURIComponent('${encodeURIComponent(r['模型规范名'])}'))"`;
    // 汇总页：每个厂商前插入一条分组表头（跨全部列），组内为该公司模型
    let grp='';
    if(!stDetail){
      const ven=companyOf(r)||'未知厂商';
      if(ven!==lastVen){ grp='<tr class="grp"><td colspan="'+view.length+'">'+esc(ven)+'</td></tr>'; lastVen=ven; }
    }
    return grp+'<tr'+onclick+hidden+'>'+view.map(c=>cell(r,c)).join('')+'</tr>';
  }).join(''):'<tr><td colspan="'+view.length+'"><div class="empty">'
    +'<div class="ring">∅</div><b>当前筛选没有匹配的模型</b>'
    +'<div>放宽筛选项，或回到默认视图重新查看</div>'
    +'<span class="lbtn" onclick="resetFilters()">恢复默认</span></div></td></tr>';
  const tbEl=document.getElementById('tb');
  tbEl.innerHTML=body;
  // 数据切换时整表轻微淡入：动效只提示"内容已更新"，不打断阅读
  tbEl.classList.remove('swap'); void tbEl.offsetWidth; tbEl.classList.add('swap');
  updateKindCounts();
  syncURL();   // 把当前筛选/排序写回地址栏，刷新与分享都不丢
}
function exportCsv(){
  // 导出列与页面当前可见列一致（含按模型大类隐藏的规则），避免导出多余列
  const keys=visibleCols().map(c=>c.cmp?null:c.k).filter(Boolean);
  const rs=baseRows(); if(!rs.length){alert('当前筛选无数据');return;}
  const esc=v=>{if(v==null)return '';v=String(v);return /[",\\n]/.test(v)?'"'+v.replace(/"/g,'""')+'"':v;};
  const csv='\\uFEFF'+[keys.join(',')].concat(rs.map(r=>keys.map(k=>esc(r[k])).join(','))).join('\\n');
  const a=document.createElement('a');
  a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'}));
  a.download=stDetail?('relay_'+stDetail.name+'.csv'):'relay_summary.csv'; a.click(); URL.revokeObjectURL(a.href);
  toast('已导出 '+rs.length+' 行 · '+keys.length+' 列');
}
// JSON 导出：保留字段名与层级，便于二次分析（CSV 会把中文表头拍平）
function exportJson(){
  const keys=visibleCols().map(c=>c.cmp?null:c.k).filter(Boolean);
  const rs=baseRows(); if(!rs.length){alert('当前筛选无数据');return;}
  const out=rs.map(r=>{
    const o={};
    keys.forEach(k=>{ const v=r[k]; o[k]=(v===''||v===null)?null:v; });
    return o;
  });
  const a=document.createElement('a');
  a.href=URL.createObjectURL(new Blob([JSON.stringify(out,null,1)],{type:'application/json'}));
  a.download=stDetail?('relay_'+stDetail.name+'.json'):'relay_summary.json'; a.click(); URL.revokeObjectURL(a.href);
  toast('已导出 JSON '+out.length+' 条');
}
// HTML 转义：站点/分组/模型/系列等抓取字符串一律先转义再拼进 innerHTML，
// 避免名字里的 < & " ' 破坏表格结构（也防脚本注入）
function esc(v){return String(v==null?'':v).replace(/&/g,'&amp;').replace(/</g,'&lt;')
  .replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}
function fmt(v){if(v==null||v==='')return '-';return Number(v).toPrecision(4).replace(/0+$/,'').replace(/\\.$/,'');}
function hist(m){
  document.getElementById('chart').innerHTML='<div class="muted">加载 '+esc(m)+' 历史…</div>';
  fetch('/api/history?model='+encodeURIComponent(m)).then(r=>r.json()).then(d=>{
    const s=(d.series||[]); if(!s.length){document.getElementById('chart').innerHTML=
      '<div class="muted">无历史数据</div>';return;}
    const by={}; s.forEach(p=>{(by[p.station+' / '+p.group]=by[p.station+' / '+p.group]||[]).push(p)});
    const names=Object.keys(by).slice(0,8);
    const W=1000,H=260,PL=54,PR=16,PT=14,PB=30;
    let mx=0; names.forEach(n=>by[n].forEach(p=>mx=Math.max(mx,p.input_usd_per_m||0)));
    if(!mx){document.getElementById('chart').innerHTML='<div class="muted">'+esc(m)+
      ' 为按秒/按次计价，没有 $/M 走势可画；请在「对比」里看各站的 $/秒 或 $/次</div>';return;}
    // 纵轴取整刻度（1/2/5×10^n），避免出现 3.7 这类难看的上限
    const nice=v=>{const e=Math.pow(10,Math.floor(Math.log10(v)));const f=v/e;
      return (f<=1?1:f<=2?2:f<=5?5:10)*e;};
    const top=nice(mx||1);
    const maxLen=Math.max.apply(null,names.map(n=>by[n].length));
    const X=j=>PL+j*(W-PL-PR)/Math.max(1,maxLen-1);
    const Y=v=>H-PB-(v/top)*(H-PT-PB);
    // 固定序列色：马卡龙色系在浅底上要压深一档，否则线条糊在白面上
    const PALETTE=['#ff8fb1','#3fb894','#d99a2b','#8a6fe0','#3aa8c9','#e8617a','#6bbf59','#6a8ff0'];
    const color=i=>PALETTE[i%PALETTE.length];
    let grid='';
    for(let t=0;t<=4;t++){
      const v=top*t/4, y=Y(v);
      grid+='<line x1="'+PL+'" x2="'+(W-PR)+'" y1="'+y.toFixed(1)+'" y2="'+y.toFixed(1)+'" '
        +'stroke="rgba(140,115,170,.20)" stroke-width="1"/>'
        +'<text x="'+(PL-9)+'" y="'+(y+3.5).toFixed(1)+'" text-anchor="end" font-size="10" '
        +'fill="#9185a6">'+fmt(v)+'</text>';
    }
    const line=(n,i)=>{const a=by[n],c=color(i),last=a[a.length-1];
      return '<path fill="none" stroke="'+c+'" stroke-width="2" stroke-linecap="round" '
        +'stroke-linejoin="round" d="'+a.map((p,j)=>(j?'L':'M')+X(j).toFixed(1)+' '
        +Y(p.input_usd_per_m||0).toFixed(1)).join(' ')+'"/>'
        +'<circle cx="'+X(a.length-1).toFixed(1)+'" cy="'+Y(last.input_usd_per_m||0).toFixed(1)
        +'" r="2.6" fill="'+c+'"/>';};
    const all=names.reduce((acc,n)=>acc.concat(by[n]),[]);
    const times=all.map(p=>p.ts).filter(Boolean).sort();
    const lab=v=>String(v||'').slice(5,16).replace('T',' ');
    let axis='';
    if(times.length>1){
      axis='<text x="'+PL+'" y="'+(H-9)+'" font-size="10" fill="#9185a6">'+esc(lab(times[0]))+'</text>'
        +'<text x="'+(W-PR)+'" y="'+(H-9)+'" text-anchor="end" font-size="10" fill="#9185a6">'
        +esc(lab(times[times.length-1]))+'</text>';
    }
    const legend=names.map((n,i)=>'<span class="lg"><i style="background:'+color(i)+'"></i>'
      +esc(n)+'</span>').join('');
    document.getElementById('chart').innerHTML='<div class="ch-head"><b>'+esc(m)+'</b>'
      +'<span class="muted">输入价走势 · $/M · 最多 8 条线</span></div>'
      +'<svg id="histSvg" viewBox="0 0 '+W+' '+H+'" style="width:100%;height:auto;margin-top:6px">'
      +grid+axis+names.map(line).join('')
      // 十字准线：默认隐藏，鼠标进入图表才显示。线 + 各系列端点 + 数值浮层
      +'<g id="chCross" style="display:none">'
      +'<line id="chVLine" y1="'+PT+'" y2="'+(H-PB)+'" stroke="rgba(140,115,170,.45)" '
      +'stroke-width="1" stroke-dasharray="3 3"/>'
      +'<g id="chDots"></g>'
      +'<rect id="chTipBg" rx="6" fill="rgba(255,255,255,.97)" stroke="rgba(169,146,240,.5)"/>'
      +'<text id="chTip" font-size="10.5" fill="#4a3d5c"></text>'
      +'</g></svg>'
      +'<div class="ch-legend">'+legend+'</div>';
    wireHistCrosshair(names, by, X, Y, PL, PR, W, PT, PB, color);
  });
// 走势图十字准线：鼠标移动时按 x 反查最近的数据点，竖向虚线 + 各线端点高亮 + 数值框
function wireHistCrosshair(names, by, X, Y, PL, PR, W, PT, PB, color){
  const svg=document.getElementById('histSvg'); if(!svg) return;
  const g=document.getElementById('chCross'), vl=document.getElementById('chVLine'),
        dots=document.getElementById('chDots'), tip=document.getElementById('chTip'),
        tipBg=document.getElementById('chTipBg');
  const maxLen=Math.max.apply(null,names.map(n=>by[n].length));
  function idxFromEvent(e){
    const r=svg.getBoundingClientRect();
    const px=(e.clientX-r.left)/r.width*W;          // viewBox 坐标
    return Math.max(0, Math.min(maxLen-1,
      Math.round((px-PL)/((W-PL-PR)/Math.max(1,maxLen-1)))));
  }
  function draw(i){
    const x=X(i);
    vl.setAttribute('x1',x); vl.setAttribute('x2',x);
    let html='', lines=[], minY=1e9;
    names.forEach((n,k)=>{
      const a=by[n]; if(i>=a.length) return;
      const p=a[i], y=Y(p.input_usd_per_m||0);
      minY=Math.min(minY,y);
      html+='<circle cx="'+x.toFixed(1)+'" cy="'+y.toFixed(1)+'" r="3.2" fill="'+color(k)+'"/>';
      lines.push(color(k)+' '+esc(n)+' '+fmt(p.input_usd_per_m));
    });
    dots.innerHTML=html;
    const ts=lines.length?String(by[names[0]][i].ts||'').slice(5,16).replace('T',' '):'';
    const txt=ts+'\\n'+lines.join('\\n');
    tip.textContent=txt;
    // 数值框放在准线右侧；靠近右缘时翻到左侧，避免超出图表
    const tw=Math.max(60, ts.length*6+30), flip=(x+tw+14>W-PR);
    const bx=flip?x-tw-10:x+10;
    tipBg.setAttribute('x',bx-4); tipBg.setAttribute('y',Math.max(PT,minY-14));
    tipBg.setAttribute('width',tw+8); tipBg.setAttribute('height',14*lines.length+18);
    tip.setAttribute('x',bx); tip.setAttribute('y',Math.max(PT+12,minY-4));
    lines.forEach((ln,k)=>{
      const t=document.createElementNS('http://www.w3.org/2000/svg','tspan');
      t.setAttribute('x',bx); t.setAttribute('dy',k?13:13);
      t.textContent=ln; tip.appendChild(t);
    });
  }
  svg.addEventListener('pointermove',e=>{
    if(e.pointerType==='touch') return;
    g.style.display='block';
    // 清掉上一帧的 tspan，只留首行时间文本
    [...tip.querySelectorAll('tspan')].forEach(t=>t.remove());
    draw(idxFromEvent(e));
  });
  svg.addEventListener('pointerleave',()=>{ g.style.display='none'; });
}
}
// ---- 跨站对比「页」：hash 路由 #cmp:<模型规范名>，可前进/后退、可分享 ----
function goCmp(m){
  const want='cmp:'+m;
  if(decodeURIComponent((location.hash||'').replace(/^#/,''))===want) routeHash();
  else location.hash='#cmp:'+encodeURIComponent(m);
}
// ---- 多模型对比：选择集 + 底部托盘 ----
const CMP_SET_KEY='relay_cmpset_v1';
let cmpSet=new Set();
try{ cmpSet=new Set(JSON.parse(localStorage.getItem(CMP_SET_KEY)||'[]')); }catch(_e){}
function saveCmpSet(){
  try{ localStorage.setItem(CMP_SET_KEY, JSON.stringify([...cmpSet])); }catch(_e){}
  renderCmpTray();
}
function toggleCmpSet(m){
  if(!m) return;
  if(cmpSet.has(m)) cmpSet.delete(m); else cmpSet.add(m);
  saveCmpSet();
  render();          // 刷新按钮上的 ✓ 状态
}
function openCmpSet(){
  if(!cmpSet.size) return;
  location.hash='#cmp:'+encodeURIComponent([...cmpSet].join(','));
}
function clearCmpSet(){ cmpSet.clear(); saveCmpSet(); render(); }
function renderCmpTray(){
  let tray=document.getElementById('cmpTray');
  if(!tray){
    tray=document.createElement('div');
    tray.id='cmpTray'; tray.className='cmptray';
    document.body.appendChild(tray);
  }
  if(!cmpSet.size){ tray.classList.remove('on'); tray.innerHTML=''; return; }
  const items=[...cmpSet].map(m=>
    '<span class="ct-item">'+esc(m)
    +'<i onclick="event.stopPropagation();toggleCmpSet(&quot;'+esc(m)+'&quot;)" title="移除">✕</i></span>'
  ).join('');
  tray.innerHTML=items
    +'<span class="ct-act"><b>'+cmpSet.size+'</b> 个模型</span>'
    +'<button class="primary" onclick="openCmpSet()">开始对比</button>'
    +'<span class="ct-clear" onclick="clearCmpSet()">清空</span>';
  tray.classList.add('on');
}
renderCmpTray();
function closeCmp(){ location.hash=''; }
// 复制对比结论：模型名 + 最低价 + 站点，粘进工单/群聊即可
function copyCmpBest(){
  const rows=document.querySelectorAll('.cmp tbody tr');
  let best=null;
  for(const tr of rows){
    const rb=tr.querySelector('.rb');
    if(rb && rb.textContent.indexOf('最低')>=0){ best=tr; break; }
  }
  if(!best){ toast('没有可复制的最低价'); return; }
  const cells=[...best.querySelectorAll('td')].map(td=>td.textContent.trim());
  // 站点名、分组、单价三列：第一列可能是「最低」徽标+站点，取最后一个片段
  const model=(cmpList.length>1)?cmpList.join(' vs '):cmpName;
  const txt=model+' 最低 '+cells.filter(Boolean).slice(1,3).join(' ')+' '+cells[cells.length-1];
  const done=()=>toast('已复制：'+txt.slice(0,60));
  if(navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(txt).then(done).catch(()=>fallbackCopy(txt,done));
  } else fallbackCopy(txt,done);
}
// 剪贴板 API 在非 HTTPS/旧浏览器不可用，退回临时 textarea + execCommand
function fallbackCopy(txt,done){
  const ta=document.createElement('textarea');
  ta.value=txt; ta.style.position='fixed'; ta.style.opacity='0';
  document.body.appendChild(ta); ta.select();
  try{ document.execCommand('copy'); done(); }
  catch(e){ toast('复制失败，请手动选择'); }
  document.body.removeChild(ta);
}
function routeHash(){
  const h=decodeURIComponent((location.hash||'').replace(/^#/,''));
  if(h.indexOf('cmp:')===0){ cmp(h.slice(4)); }
  else if(h===''){ const c=document.getElementById('chart'); if(c) c.innerHTML=''; document.body.classList.remove('cmpmode'); }
}
window.addEventListener('hashchange',routeHash);
// 对比页排序状态：cmpKey 为空 = 后端默认序（按量→按秒→按次，段内升序，视频按分辨率分块）
let cmpName='', cmpRows=[], cmpKey='', cmpAsc=true;
let cmpList=[];      // 多模型对比时的模型列表（长度 1 时行为与单模型完全一致）
// 对比页：默认「每站只留最优分组」，避免同一站在十几个分组里重复出现、挤占前三名
let cmpBestOnly=true;
// 每站只保留可比单价最低的一行（保持首次出现顺序，排序交给外层）
function bestPerStation(rowsIn){
  const best={}, order=[];
  rowsIn.forEach(r=>{
    const k=r['站点']||'';
    const p=cmpPriceOf(r);
    if(!(k in best)){ best[k]={r,p}; order.push(k); return; }
    const cur=best[k];
    if(p!==null && (cur.p===null || p<cur.p)) best[k]={r,p};
  });
  return order.map(k=>best[k].r);
}
function toggleBestOnly(v){ cmpBestOnly=!!v; renderCmp(); }
// 前四列是人民币口径（跨站可比），后面「内部$」是该站自己的额度币种
// 列与单站列表页保持一致（首列「站点」取代单站页的「模型」，其余列定义/顺序/隐藏标准完全相同）
const CMP_COLS=[
  {k:'站点',t:'站点'},
  {k:'公司',t:'公司',h:1},{k:'计费方式',t:'计费',h:1},
  {k:'分组',t:'分组'},{k:'分组倍率',t:'倍率',num:true,any:true},
  {k:'模型在该分组可用',t:'可用',h:1},{k:'价格来源',t:'来源',h:1},
  {k:'分辨率',t:'分辨率'},
  {k:'按秒价格(¥/秒)',t:'¥/秒',num:true,sec:true},
  {k:'按次价格(¥/次)',t:'¥/次',num:true,cal:true},
  {k:'按秒价格($/秒)',t:'内部$/秒',num:true,sec:true,h:1},
  {k:'按次价格($/次)',t:'内部$/次',num:true,cal:true,h:1},
  {k:'实付输入(¥/M)',t:'输入',num:true,tok:true,g:'Token价格（¥/M）'},
  {k:'实付输出(¥/M)',t:'输出',num:true,tok:true,g:'Token价格（¥/M）'},
  {k:'实付缓存读(¥/M)',t:'读取',num:true,tok:true,g:'缓存价格（¥/M）'},
  {k:'实付缓存写(¥/M)',t:'写入',num:true,tok:true,g:'缓存价格（¥/M）'},
  {k:'实付输出($/M)',t:'输出$/M',num:true,tok:true,h:1},
  {k:'实付输入($/M)',t:'输入内部$/M',num:true,tok:true,h:1},
  {k:'实付缓存读($/M)',t:'缓存读$/M',num:true,tok:true,h:1},
  {k:'实付缓存写($/M)',t:'缓存写$/M',num:true,tok:true,h:1},
  {k:'官方输入(¥/M)',t:'输入',num:true,tok:true,g:'官方价格（¥/M）'},
  {k:'官方输出(¥/M)',t:'输出',num:true,tok:true,g:'官方价格（¥/M）'},
  {k:'对比官方价(%)',t:'对比官方价%',num:true,tok:true},
  {k:'72小时可用率',t:'72h可用率'},
  {k:'最近12次状态',t:'最近 12 次状态'}];
const numOf=v=>{if(v===''||v==null)return null;const n=Number(v);return isNaN(n)?null:n;};
// 该行在某一列的值；量纲不符（如按秒行看输入$/M）返回 null，排序时恒排末尾
function cmpValOf(r,c){
  const b=r['计费方式']||'per_token';
  if(c.tok&&b!=='per_token') return null;
  if(c.sec&&b!=='per_second') return null;
  if(c.cal&&b!=='per_call') return null;
  return c.num?numOf(r[c.k]):String(r[c.k]==null?'':r[c.k]);
}
// 该模型「可比单价」：按计费口径取对应人民币列，用于跨站挑出最便宜的前三条
function cmpPriceOf(r){
  const b=r['计费方式']||'per_token';
  const k=b==='per_token'?'实付输入(¥/M)':(b==='per_second'?'按秒价格(¥/秒)':'按次价格(¥/次)');
  return numOf(r[k]);
}
// 该模型的主价格列（价格条/差价徽标/KPI 都围绕它），按计费口径取键名
function cmpPriceKey(rs){
  const b=(rs[0]||{})['计费方式']||'per_token';
  return b==='per_token'?'实付输入(¥/M)':(b==='per_second'?'按秒价格(¥/秒)':'按次价格(¥/次)');
}
// 跨站对比页按「模型种类」决定展示列，规则与首页一致：
//   文本/音频（token 计费）→ 隐藏 分辨率、¥/秒、¥/次；保留 Token/缓存/官方 三组 ¥/M
//   图片（按次）→ 隐藏三类 ¥/M 价格组 + ¥/秒；保留 ¥/次、分辨率
//   视频（按时长）→ 隐藏三类 ¥/M 价格组 + ¥/次；保留 ¥/秒、分辨率
const CMP_HIDE_BY_KIND={
  text:['分辨率','按秒价格(¥/秒)','按次价格(¥/次)'],
  audio:['分辨率','按秒价格(¥/秒)','按次价格(¥/次)'],
  image:['实付输入(¥/M)','实付输出(¥/M)','实付缓存读(¥/M)','实付缓存写(¥/M)',
         '官方输入(¥/M)','官方输出(¥/M)','按秒价格(¥/秒)'],
  video:['实付输入(¥/M)','实付输出(¥/M)','实付缓存读(¥/M)','实付缓存写(¥/M)',
         '官方输入(¥/M)','官方输出(¥/M)','按次价格(¥/次)'],
};
// 与首页一致地判定模型种类：图片/视频/音频按站点标注的模型类型，其余按语言处理。
// 首页的 kindMatch 对 image/video/audio 正是用「模型类型」；语言模型虽用白名单，
// 但白名单里也有按次计费的图片模型，若以白名单优先会让它们误用文本布局。
// 也不能用「计费方式」推断：真实数据里有图片模型按 token 计费、文本模型按次/按秒计费。
function cmpKindOf(rs){
  const r=rs[0]||{};
  const mk=r['模型类型'];
  return (mk==='image'||mk==='video'||mk==='audio')?mk:'text';
}
function cmpSort(k){
  if(k===cmpKey){ cmpAsc=!cmpAsc; } else { cmpKey=k; cmpAsc=true; }
  renderCmp();
}
// 对比页表头键盘可达：#chart 容器不会重建，事件委托长期有效
document.getElementById('chart').addEventListener('keydown',e=>{
  if(e.key!=='Enter'&&e.key!==' ') return;
  const th=e.target.closest('.cmp th[data-k]'); if(!th) return;
  e.preventDefault(); cmpSort(th.dataset.k);
});
// Esc 退出对比页，回到列表
document.addEventListener('keydown',e=>{
  if(e.key!=='Escape') return;
  if((location.hash||'').indexOf('#cmp:')===0) closeCmp();
});
// 按列定义逐格渲染，h:1 的列直接跳过（表头与单元格数量永远一致）
const CMP_TEXT_COLS={站点:1,公司:1,分组:1,分辨率:1,计费方式:1,价格来源:1};
// 百分比列的位数规则与主列表 cell() 保持一致：>=10 取整，>=1 保留 1 位，其余 2 位有效数字
const pctTxt=n=>n>=10?n.toFixed(0):(n>=1?n.toFixed(1):n.toPrecision(2));
function cmpRowHtml(r, rank, cols, ctx){
  const b=r['计费方式']||'per_token', isTok=b==='per_token', dash='<span class="muted">-</span>';
  const cells=cols.map(c=>{
    const k=c.k;
    if(k==='站点'){
      const badge=rank?('<span class="rb rb'+rank+'">'+(rank==='1'?'最低':(rank==='2'?'第2低':'第3低'))+'</span>'):'';
      return '<td>'+badge+(r[k]==null?'':esc(r[k]))+'</td>';
    }
    if(k==='计费方式'){
      return '<td><span class="'+(isTok?'tok':'call')+'">'+(BILL_LABEL[b]||b)+'</span></td>';
    }
    if(k==='模型规范名'){
      // 多模型视图里点模型名可跳到该模型的单独对比
      return '<td><a class="mlink" href="#cmp:'+encodeURIComponent(r[k]||'')+'" '
        +'onclick="event.stopPropagation()">'+esc(r[k]||'')+'</a></td>';
    }
    if(CMP_TEXT_COLS[k]) return '<td>'+(r[k]==null?'':esc(r[k]))+'</td>';
    if(k==='对比官方价(%)'){
      // 按百分比展示（如 95% / 99.3% / 0.42%）；非 token 行无官方基准，显示「-」
      const v=numOf(r[k]);
      return '<td class="disc'+(v!==null&&v>100?' hi':'')+'">'
        +(c.tok&&isTok&&v!==null?pctTxt(v)+'%':dash)+'</td>';
    }
    if(k==='72小时可用率'){
      const x=r[k];
      if(x==null||typeof x!=='object') return '<td class="muted">-</td>';
      // ≥99% 绿（稳）、95%~99% 琥珀（波动）、<95% 红（告警），与主列表一致
      const color=x.p>=99?'var(--good)':(x.p>=95?'var(--warn)':'var(--bad)');
      const src=x.src==='probe'?'模型调用探测':(x.src==='station'?'站点可达性':'探测');
      return `<td class="nowrap" style="color:${color};font-weight:600" `
        +`title="最近 72 小时${src}：可达 ${x.ok} / 共 ${x.tot} 次">${x.p}%</td>`;
    }
    if(k==='最近12次状态'){
      const arr=Array.isArray(r[k])?r[k]:[];
      if(!arr.length) return '<td class="muted">-</td>';
      // 连续失败 2 次及以上红色，最近一次失败黄色（与主列表一致）
      let streak=0;
      for(let i=arr.length-1;i>=0;i--){ if(arr[i].ok) break; streak++; }
      const cells=arr.map(x=>{
        const cls=x.ok?'ok':(streak>=2?'bad2':'bad');
        const t=x.ok?'可达':'不可达';
        return `<i class="${cls}" title="${t} ${String(x.ts||'').slice(5,16).replace('T',' ')}"></i>`;
      }).join('');
      const bad=arr.filter(x=>!x.ok).length;
      const tip=`最近 ${arr.length} 次探测：可达 ${arr.length-bad}，不可达 ${bad}`;
      return `<td><span class="spark" title="${tip}">${cells}</span></td>`;
    }
    const v=c.num?numOf(r[k]):null;
    const num=v!==null;
    // 量纲不符（按秒行看 ¥/M、按量行看 ¥/秒）一律显示「-」；
    // any=1 的列（分组倍率）与计费方式无关，所有行都显示
    const ok=c.any||(c.tok&&isTok)||(c.sec&&b==='per_second')||(c.cal&&b==='per_call');
    // 主价格列：价格条用「对数归一」，避免 13× 这类极端值把其它条压成一条线
    if(ctx && k===ctx.pk && ok && num){
      const lo=ctx.minP, hi=ctx.maxP;
      let w=0;
      if(hi!=null && hi>0){
        if(lo!=null && lo>0 && hi>lo) w=Math.log(v/lo)/Math.log(hi/lo)*100;
        else w=v/hi*100;
      }
      w=Math.max(6,Math.min(100,Math.round(w)));
      return '<td class="price pbarcell"><span class="pbar" style="width:'+w+'%"></span>'
        +'<span class="pval" style="font-weight:700">'+fmt(v)+'</span></td>';
    }
    // Token价格组的「输入」列加粗：默认按输入价排序，数字加粗提示排序依据
    const bw=(k==='实付输入(¥/M)'&&ok&&num)?' style="font-weight:700"':'';
    return '<td class="'+(ok&&num?'price':'muted')+'"'+bw+'>'+(ok&&num?fmt(v):dash)+'</td>';
  }).join('');
  return '<tr'+(rank?' class="cheap"':'')+'>'+cells+'</tr>';
}
function renderCmp(){
  const box=document.getElementById('chart');
  let rs=cmpRowsForRender();
  const total=cmpRows.length;
  const kind=cmpKindOf(rs);
  // 仅 token 计价（语言/音频）做「每站只留最优分组」：token 模型一个站常有十几个分组会刷屏；
  // 图片已按站去重，视频的分辨率是有效维度，按站合并会丢掉其它档位报价
  const canBest=(kind==='text'||kind==='audio');
  if(cmpBestOnly && canBest && rs.length>1) rs=bestPerStation(rs);
  let hide=(CMP_HIDE_BY_KIND[kind]||[]).slice();
  // 安全网：按种类隐藏后若没有任何「有数据」的价格列可见，就把有数据的价格组放出来，
  // 避免「图片模型却按 token 计费」等混合计费模型出现整片空价格。
  const PRICE_COLS=['实付输入(¥/M)','实付输出(¥/M)','实付缓存读(¥/M)','实付缓存写(¥/M)',
    '官方输入(¥/M)','官方输出(¥/M)','按次价格(¥/次)','按秒价格(¥/秒)'];
  const hasPrice=k=>rs.some(r=>{const v=r[k];return v!==''&&v!=null&&Number(v)>0;});
  if(!PRICE_COLS.some(k=>hide.indexOf(k)<0&&hasPrice(k))){
    hide=hide.filter(k=>PRICE_COLS.indexOf(k)<0||!hasPrice(k));
  }
  // 排序键若落到被隐藏列，则退回默认序（避免表头消失却仍在按隐藏列排序）
  if(hide.indexOf(cmpKey)>=0) cmpKey='';
  const col=CMP_COLS.filter(c=>c.k===cmpKey)[0];
  if(col){
    const sign=cmpAsc?1:-1;
    rs.sort((a,b)=>{
      const va=cmpValOf(a,col), vb=cmpValOf(b,col);
      const na=(va===null||va===''), nb=(vb===null||vb==='');
      if(na&&nb) return 0;
      if(na) return 1;          // 空值/「-」永远排末尾，不随升降序翻转
      if(nb) return -1;
      if(col.num) return (va-vb)*sign;
      return String(va).localeCompare(String(vb))*sign;
    });
  }
  // 主价格列与 KPI（最低/中位/价差）：强调色面板、价格条都围绕它
  const pk=cmpPriceKey(rs);
  let pvals=rs.map(r=>numOf(r[pk])).filter(v=>v!==null&&v>0);
  if(!pvals.length) pvals=rs.map(r=>cmpPriceOf(r)).filter(v=>v!==null);
  const minP=pvals.length?Math.min.apply(null,pvals):null;
  const maxP=pvals.length?Math.max.apply(null,pvals):null;
  const medP=pvals.length?pvals.slice().sort((a,b)=>a-b)[Math.floor(pvals.length/2)]:null;
  let lowSt='';
  if(minP!==null){ const lr=rs.find(r=>numOf(r[pk])===minP)||rs.find(r=>cmpPriceOf(r)===minP); lowSt=lr?lr['站点']:''; }
  const money=v=>(v==null?'-':'¥'+fmt(v));
  const spread=(minP&&maxP&&minP>0)?(fmt(maxP/minP)+'×'):'-';
  const kindLabel={text:'语言/文本',audio:'音频',image:'图片',video:'视频'}[kind]||'文本';
  const cnt=(cmpBestOnly && canBest && total>rs.length)?(rs.length+' 站（共 '+total+' 个分组）'):(rs.length+' 条');
  // 只有 token 计价模型才显示「每站只留最优分组」开关（对图片/视频无意义）
  const toggleHtml=canBest?('<label class="cmptoggle"><input type="checkbox"'+(cmpBestOnly?' checked':'')
    +' onchange="toggleBestOnly(this.checked)"> 每站只留最优分组</label>'):'';
  const bar='<div class="cmphero">'
    +'<div class="cmphero-row"><div class="cmphero-title">'
    +esc(cmpList.length>1?cmpList.join(' · '):cmpName)
    +'<span class="cmpbadge">跨站对比 '+cnt+'</span></div>'
    +'<div class="cmphero-actions">'+toggleHtml
    +'<span class="backlink" onclick="copyCmpBest()" title="复制模型名与最低价，可直接粘进工单/群聊">复制最低价</span>'
    +'<span class="backlink" onclick="closeCmp()">← 返回列表</span></div></div>'
    +'<div class="cmphero-kpi">'
    +'<span>最低 <b>'+money(minP)+'</b> <span class="muted">'+esc(lowSt)+'</span></span>'
    +'<span>价差 <b>'+spread+'</b></span>'
    +'<span>中位 <b>'+money(medP)+'</b></span>'
    +'<span class="cmphero-note">人民币口径（已按各站充值比例折算）；名次徽标标出最低价三条；'+kindLabel+'视图；点表头排序</span>'
    +'</div></div>';
  // 头部与单元格共用同一套可见列（已按模型种类隐藏无关列，与首页一致）
  const CORE_CMP=['站点','分组','倍率','实付输入(¥/M)','实付输出(¥/M)','对比官方价(%)','72小时可用率'];
  let cols=CMP_COLS.filter(c=>!c.h && hide.indexOf(c.k)<0
    && (!NARROW() || CORE_CMP.some(k=>c.k===k || c.t===k)));
  // 多模型对比：最前面插入「模型」列，否则不同模型的价格混在一张表里读不出来
  const multi=cmpList.length>1;
  if(multi) cols=[{k:'模型规范名',t:'模型'}].concat(cols);
  // 两级表头（与主列表一致）：分组列合并 colspan，子列单独一行；
  // 无分组列时（图片/视频模型隐藏了全部 ¥/M 组）退化为单行表头，不能再写 rowspan=2
  const hasG2=cols.some(c=>c.g), rsp2=hasG2?' rowspan="2"':'';
  let h1='',h2='',ci=0;
  while(ci<cols.length){
    const c0=cols[ci];
    if(!c0.g){ h1+='<th'+rsp2+'>'+c0.t+'</th>'; ci++; continue; }
    let cj=ci; while(cj<cols.length&&cols[cj].g===c0.g) cj++;
    const sub=cols.slice(ci,cj);
    h1+='<th colspan="'+sub.length+'">'+c0.g+'</th>';
    h2+=sub.map(x=>'<th tabindex="0" aria-sort="'+(x.k===cmpKey?(cmpAsc?'ascending':'descending'):'none')
      +'" data-k="'+x.k+'" onclick="cmpSort(&quot;'+x.k+'&quot;)"'
      +(x.k===cmpKey?' class="on"':'')+'>'+x.t
      +(x.k===cmpKey?('<span class="sa">'+(cmpAsc?'▲':'▼')+'</span>'):'')+'</th>').join('');
    ci=cj;
  }
  const head='<thead><tr>'+h1+'</tr>'+(h2?'<tr>'+h2+'</tr>':'')+'</thead>';
  // 价格最低的三条：按模型计费口径取可比单价，排名前 3（含并列）。
  // 多模型对比时**按模型分别排名**，否则便宜模型会占满前三名。
  const priced=rs.map((r,i)=>({i, p:cmpPriceOf(r), m:r['模型规范名']})).filter(x=>x.p!==null);
  const rankOf={};
  if(priced.length){
    if(multi){
      const byM={};
      priced.forEach(x=>{ (byM[x.m]=byM[x.m]||[]).push(x); });
      Object.keys(byM).forEach(m=>{
        byM[m].sort((a,b)=>a.p-b.p);
        let rank=0, prev=null;
        byM[m].forEach(x=>{ if(x.p!==prev){ rank++; prev=x.p; } if(rank<=3) rankOf[x.i]=String(rank); });
      });
    } else {
      priced.sort((a,b)=>a.p-b.p);
      let rank=0, prev=null;
      priced.forEach(x=>{ if(x.p!==prev){ rank++; prev=x.p; } if(rank<=3) rankOf[x.i]=String(rank); });
    }
  }
  const ctx={pk, minP, maxP};
  const body='<tbody>'+rs.map((r,i)=>cmpRowHtml(r, rankOf[i]||'', cols, ctx)).join('')+'</tbody>';
  box.innerHTML='<div class="cmpwrap">'+bar+'<table class="cmp">'+head+body+'</table></div>';
}
function cmp(m){
  const box=document.getElementById('chart');
  cmpName=m; cmpRows=[]; cmpKey=''; cmpAsc=true;
  document.body.classList.add('cmpmode');   // 聚焦模式：隐藏主列表/筛选，突出对比页
  const list=(m||'').split(',').filter(Boolean);
  box.innerHTML='<div class="muted">加载 '+esc(list.join('、'))+' 跨站对比…</div>';
  // 多模型并行取数；单个模型失败不影响其余
  Promise.all(list.map(x=>
    fetch('/api/compare?model='+encodeURIComponent(x)).then(r=>r.json())
      .catch(()=>({error:String(x)+' 加载失败'}))
  )).then(ds=>{
    const bad=ds.filter(d=>d.error);
    const rows=[];
    ds.forEach(d=>{ if(!d.error) (d.rows||[]).forEach(r=>rows.push(r)); });
    if(!rows.length){
      box.innerHTML='<div class="muted">'+esc(list.join('、'))+' 无对比数据</div>';
      document.body.classList.remove('cmpmode'); return;
    }
    cmpRows=rows; cmpList=list.slice();
    if(bad.length) toast('部分模型加载失败：'+bad.map(b=>b.error).join('；'));
    renderCmp();
    box.classList.remove('swap'); void box.offsetWidth; box.classList.add('swap');
    box.scrollIntoView({behavior:'smooth',block:'start'});
  }).catch(e=>{ box.innerHTML='<div class="muted">加载失败：'+esc(e)+'</div>'; document.body.classList.remove('cmpmode'); });
}
// 多模型对比时把「模型」列插到最前，并按模型分组排序，避免不同模型的价格混排
function cmpRowsForRender(){
  let rs=cmpRows.slice();
  if(cmpList.length>1){
    const order={};
    cmpList.forEach((m,i)=>order[m]=i);
    rs.sort((a,b)=>{
      const ma=order[a['模型规范名']]!==undefined?order[a['模型规范名']]:99;
      const mb=order[b['模型规范名']]!==undefined?order[b['模型规范名']]:99;
      return ma-mb || String(a['模型规范名']).localeCompare(String(b['模型规范名']));
    });
  }
  return rs;
}
// 支持带 #cmp:<模型> 的直达链接（刷新/分享/后退）。
// 必须放在脚本末尾：routeHash → cmp 会写 cmpName 等 let 变量，
// 若在声明前调用会触发 TDZ 异常并中断整个脚本（整页空白）
routeHash();
// 所有声明就绪后再铺骨架屏，首屏不会白屏等接口
skeleton(9);
</script></body></html>
"""


def cmd_serve(args):
    """本地 Web 看板：/api/summary、/api/history、/api/compare、/api/stations。"""
    import http.server
    import socketserver
    from urllib.parse import urlparse, parse_qs

    db = args.db or DB_PATH

    # 返回主站地址(2026-10-28 与 RelayLens 整合):口径条的跳转目标,
    # 注入 DASHBOARD_HTML 的 __HOME_URL__ 占位符(与 RelayLens 构建期的占位符方案同思路)。
    # 默认生产主站;本地预览时设 RELAY_HOME_URL=http://127.0.0.1:5173/index.html 即指回本地预览。
    home_url = os.environ.get("RELAY_HOME_URL") or "https://aipaihangbang.cn/relay"

    # 全站导航(2026-10-28):页顶挂与其它页面同款的站点导航条(品牌 + 8 项菜单 + 当前页高亮)。
    # 站内页链接需站点根地址:优先 RELAY_SITE_BASE,否则从 RELAY_HOME_URL 推导(剥掉尾部 index.html)。
    site_base = os.environ.get("RELAY_SITE_BASE")
    if not site_base:
        if home_url.endswith("/index.html"):
            site_base = home_url[: -len("index.html")]
        elif home_url.endswith("/"):
            site_base = home_url
        else:
            site_base = home_url + "/"
    # 清单与 build-relay.js 的 NAV 常量同源(8 项);当前页=看板自身(「模型比价」),
    # 链接默认 "/"(看板独立部署在根路径;反代到子路径时设 RELAY_SELF_URL)。
    self_url = os.environ.get("RELAY_SELF_URL", "/")
    _NAV_ITEMS = [
        ("index.html", "选站比价"), ("ranking.html", "全站榜单"), (None, "模型比价"),
        ("selector.html", "选型顾问"), ("blacklist.html", "避雷黑名单"),
        ("pitfalls.html", "避坑指南"), ("method.html", "方法论"), ("tools.html", "自测工具箱"),
    ]

    def _nav_a(page, label):
        # 选型顾问在看板内已有同名视图：顶部导航直接打开它，
        # 不再跳主站 selector.html（该页尚未部署，点了 404）
        if page == "selector.html":
            return '<a href="#" onclick="toggleView(\'selector\');return false">{}</a>'.format(label)
        return '<a href="{}"{}>{}</a>'.format(
            (site_base + page) if page else self_url,
            ' class="active" aria-current="page"' if page is None else "",
            label)

    site_nav = '<nav class="nav-links" aria-label="主导航">' + "".join(
        _nav_a(page, label) for page, label in _NAV_ITEMS) + "</nav>"

    # —— 接口级缓存：汇总/单站数据只在「有新快照或新探测」时才重算 ——
    # 看板的搜索/排序/筛选都是前端本地完成，只有切站点会请求 /api/station；
    # 不缓存时每次切站都要重算全部行（无限星河 236 模型 × 282 分组 = 6.6 万行），
    # 体感就是「点一下筛选卡好几秒」。
    _cache = {"ver": None, "summary": None, "station": {}}
    # 单站页返回行数上限：超限时先按模型保留最低价分组（保证每个模型都有最优行）再截断
    _STATION_ROW_CAP = 20000

    def _data_version(conn):
        """数据版本：任一表有新写入（抓取/探测）即失效，缓存自动重建。
        官方价基准与站点配置的文件修改时间也算——重建基准或改了充值比例后
        快照数没变，不带上它们看板会一直用旧值（曾因此 aicost 实付价
        按 7.3 算了半小时）。"""
        out = []
        for t in ("snapshots", "probes", "model_availability"):
            try:
                out.append(conn.execute(f"SELECT COALESCE(MAX(id),0) FROM {t}").fetchone()[0])
            except Exception:
                out.append(0)
        for path in (OFFICIAL_PATH, STATIONS_PATH):
            try:
                out.append(int(os.path.getmtime(path)))
            except Exception:
                out.append(0)
        return tuple(out)

    def _cached_summary(conn, ver, build):
        """汇总行缓存：先查内存，再查库，都没有才现算并落库。

        库级缓存的意义：watch 抓完新快照后，下一个请求（哪怕 serve 刚重启）
        也不用再等那 2 秒；换进程、重启浏览器都不影响。
        """
        if _cache.get("ver") == ver and _cache.get("summary") is not None:
            return _cache["summary"], False
        key = ",".join(str(x) for x in ver)
        try:
            row = conn.execute("SELECT payload FROM summary_cache WHERE ver=?",
                               (key,)).fetchone()
            if row:
                payload = json.loads(row["payload"])
                _cache["ver"], _cache["summary"] = ver, payload
                _cache["station"].clear()
                return payload, True
        except Exception:
            pass
        payload = build()
        try:
            conn.execute("INSERT OR REPLACE INTO summary_cache(ver,payload,built_at)"
                         " VALUES(?,?,?)", (key, json.dumps(payload, ensure_ascii=False), utc_now()))
            conn.execute("DELETE FROM summary_cache WHERE ver<>?", (key,))
            conn.commit()
        except Exception:
            pass
        _cache["ver"], _cache["summary"] = ver, payload
        _cache["station"].clear()
        return payload, False

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, obj, status=200):
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            if status != 200 or self.path.startswith("/api/ingest"):
                # 插件上报/查询需要跨域访问；其余本地接口不受影响
                self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urlparse(self.path)
            q = parse_qs(u.query)
            if u.path == "/":
                # __SITE_NAV__/__SITE_BASE__:全站导航条与站内链接;__HOME_URL__:口径条跳转目标
                body = (DASHBOARD_HTML
                        .replace("__SITE_NAV__", site_nav)
                        .replace("__SITE_BASE__", site_base)
                        .replace("__HOME_URL__", home_url)
                        .encode("utf-8"))
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                # 页面是单文件随代码变的：禁缓存，防止浏览器拿旧 HTML 点新按钮没反应
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            conn = connect(db)
            try:
                if u.path == "/api/summary":
                    ver = _data_version(conn)

                    def _build_summary():
                        rows = build_summary_rows(conn)
                        fresh = station_freshness(conn)
                        # generated_at 用「最新一次成功抓取」而不是请求时刻：
                        # 抓取一旦停摆，页面必须立刻看出数据是旧的
                        data_ts = max([f["last_ok"] for f in fresh if f["last_ok"]] or [""])
                        return {"generated_at": data_ts or utc_now(),
                                "data_time": data_ts,
                                "server_time": utc_now(),
                                "stations": len(fresh),
                                "freshness": fresh,
                                "rows": rows}

                    payload, _hit = _cached_summary(conn, ver, _build_summary)
                    self._json(payload)
                elif u.path == "/api/history":
                    canon = agg_key(canonical_model((q.get("model") or [""])[0]))
                    series = []
                    # 先用模型聚合键命中所有快照（走 (snapshot_id, model_agg) 复合索引），
                    # 再只为这些快照查分组倍率；原来是遍历全部快照、每快照查两次，
                    # 历史一长就线性变慢
                    hits = conn.execute(
                        "SELECT k.id AS sid, k.ts, st.name AS station,"
                        " mp.model_ratio, mp.enable_groups"
                        " FROM model_prices mp JOIN snapshots k ON k.id=mp.snapshot_id"
                        " JOIN stations st ON st.id=k.station_id"
                        " WHERE k.ok=1 AND mp.model_agg=? ORDER BY k.id",
                        (canon,)).fetchall()
                    grp_cache = {}
                    for hit in hits:
                        sid = hit["sid"]
                        if sid not in grp_cache:
                            grp_cache[sid] = {r["group_name"]: r["ratio"] for r in conn.execute(
                                "SELECT group_name, ratio FROM group_ratios WHERE snapshot_id=?",
                                (sid,))}
                        grp = grp_cache[sid]
                        base = (hit["model_ratio"] or 0) * RATIO_UNIT_USD_PER_M
                        enabled = set(json.loads(hit["enable_groups"] or "[]"))
                        names = sorted(enabled | set(grp.keys())) or [""]
                        for g in names:
                            series.append({"ts": hit["ts"], "station": hit["station"],
                                           "group": g,
                                           "input_usd_per_m": base * grp.get(g, 1.0)})
                    self._json({"model": canon, "series": series})
                elif u.path == "/api/compare":
                    # 支持一次对比多个模型：?model=a,b,c（上限 8 个）
                    raw = (q.get("model") or [""])[0]
                    names = [agg_key(canonical_model(x)) for x in raw.split(",") if x.strip()][:8]
                    rows = []
                    for canon in names:
                        rows.extend(compare_rows(conn, canon))
                    self._json({"model": ",".join(names), "models": names,
                                "rows": rows[:400]})
                elif u.path == "/api/station":
                    name = (q.get("name") or [""])[0]
                    st = conn.execute(
                        "SELECT id, mode FROM stations WHERE name=?", (name,)).fetchone()
                    if not st:
                        self._json({"error": f"站点不存在: {name}"})
                    else:
                        ver = _data_version(conn)
                        ent = _cache["station"].get(name)
                        if ent is None or ent[0] != ver:
                            # 只算该站的快照：原来生成全站 7 万行再按站名过滤，纯浪费
                            srows = list(iter_detail_rows(conn, station_id=st["id"]))
                            total = len(srows)
                            truncated = False
                            if total > _STATION_ROW_CAP:
                                # 分组过多的站（无限星河 236 模型 × 282 分组 = 6.6 万行）：
                                # 先按模型保留最低价分组，保证每个模型都有最优行，仍超上限再截断
                                best = {}
                                for r in srows:
                                    k = r.get("模型规范名") or ""
                                    p = _detail_real_price(r) or float("inf")
                                    if k not in best or p < best[k][0]:
                                        best[k] = (p, r)
                                srows = [v[1] for v in best.values()]
                                if len(srows) > _STATION_ROW_CAP:
                                    srows = srows[:_STATION_ROW_CAP]
                                truncated = True
                            snap = conn.execute(
                                "SELECT exchange_rate, usd_exchange_rate FROM snapshots "
                                "WHERE station_id=? AND ok=1 ORDER BY id DESC LIMIT 1",
                                (st["id"],)).fetchone()
                            # 与 iter_detail_rows 同口径：手动覆盖（recharge_rate）优先，
                            # 其次 /api/status 汇率（ex_rate==1 且有真实汇率时用后者）。
                            # 原来直接用快照值，手动覆盖的站（如 heyroute 0.25）meta 显示错比例
                            mrow = conn.execute(
                                "SELECT recharge_rate FROM stations WHERE id=?",
                                (st["id"],)).fetchone()
                            if mrow and mrow["recharge_rate"] is not None:
                                er = mrow["recharge_rate"]
                            elif snap:
                                er = snap["exchange_rate"]
                                if er == 1.0 and snap["usd_exchange_rate"] not in (None, 1.0):
                                    er = snap["usd_exchange_rate"]
                            else:
                                er = None
                            models = len({r["模型规范名"] for r in srows})
                            groups = len({r["分组"] for r in srows})
                            ent = (ver, {"name": name, "mode": st["mode"],
                                         "exchange_rate": er,
                                         "models": models, "groups": groups, "rows": srows,
                                         "total": total, "truncated": truncated})
                            _cache["station"][name] = ent
                        self._json(ent[1])
                elif u.path == "/api/stations":
                    # 站点总览：名称/模式/状态 + 模型数 + 数据时间 + 距今小时数 + 最近错误
                    self._json({"generated_at": utc_now(),
                                "stations": station_freshness(conn)})
                elif u.path == "/api/changes":
                    # 调价监控：price_changes 的涨降价流水
                    hours = float((q.get("hours") or ["168"])[0] or 168)
                    thr = float((q.get("threshold") or ["0"])[0] or 0)
                    data = collect_price_changes(conn, hours, thr)
                    data["generated_at"] = utc_now()
                    self._json(data)
                elif u.path == "/api/user_reports":
                    # 插件上报的用户侧探针/价格数据（供查看与后续聚合展示）
                    kind = (q.get("kind") or [""])[0]
                    try:
                        limit = min(500, max(1, int((q.get("limit") or ["100"])[0])))
                    except ValueError:
                        limit = 100
                    rows = []
                    sql = ("SELECT ur.*, s.name AS station FROM user_reports ur "
                           "LEFT JOIN stations s ON s.id=ur.station_id")
                    args_q = []
                    conds = []
                    if kind in ("probe", "prices"):
                        conds.append("ur.kind=?")
                        args_q.append(kind)
                    if conds:
                        sql += " WHERE " + " AND ".join(conds)
                    sql += " ORDER BY ur.id DESC LIMIT ?"
                    args_q.append(limit)
                    for r in conn.execute(sql, args_q):
                        try:
                            payload = json.loads(r["payload"])
                        except Exception:
                            payload = None
                        rows.append({"id": r["id"], "kind": r["kind"],
                                     "station": r["station"], "origin": r["origin"],
                                     "origin_host": r["origin_host"],
                                     "ext_version": r["ext_version"],
                                     "received_at": r["received_at"],
                                     "payload": payload})
                    self._json({"generated_at": utc_now(), "count": len(rows), "rows": rows})
                elif u.path == "/api/bench":
                    # api-bench 性能实测历史（全站或单站）
                    name = (q.get("station") or [""])[0]
                    try:
                        limit = min(200, max(1, int((q.get("limit") or ["50"])[0])))
                    except ValueError:
                        limit = 50
                    sql = ("SELECT b.*, s.name AS station FROM bench_results b "
                           "JOIN stations s ON s.id=b.station_id")
                    args_q = []
                    if name:
                        sql += " WHERE s.name=?"
                        args_q.append(name)
                    sql += " ORDER BY b.id DESC LIMIT ?"
                    args_q.append(limit)
                    rows = []
                    for r in conn.execute(sql, args_q):
                        rows.append({"station": r["station"], "model": r["model_raw"],
                                     "protocol": r["protocol"], "ts": r["ts"], "ok": bool(r["ok"]),
                                     "requests": r["requests"], "fails": r["fails"],
                                     "ttft_p50_ms": r["ttft_p50_ms"], "ttft_p90_ms": r["ttft_p90_ms"],
                                     "ttft_p99_ms": r["ttft_p99_ms"], "tpot_ms": r["tpot_ms"],
                                     "decode_tps": r["decode_tps"], "req_per_s": r["req_per_s"],
                                     "cache_hit_pct": r["cache_hit_pct"],
                                     "cache_token_pct": r["cache_token_pct"],
                                     "roll_capture_pct": r["roll_capture_pct"],
                                     "roll_saved_pct": r["roll_saved_pct"]})
                    self._json({"generated_at": utc_now(), "count": len(rows), "rows": rows})
                else:
                    self.send_error(404)
            except Exception as e:
                self._json({"error": f"{type(e).__name__}: {e}"})
            finally:
                conn.close()

        # ── My API Box 插件上报接入（POST /api/ingest/probe | /api/ingest/prices）──
        # 复用插件自带的社区上报管线（src/services/communitySubmission）：
        # 用户在插件里填端点即可，payload 已由插件脱敏（key/accessToken/余额绝不离开本地）。
        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def _match_station(self, conn, host):
            """按 origin host 匹配在册站点（domains 存的是域名数组，支持子域）。"""
            if not host:
                return None
            host = host.lower().strip(".")
            for r in conn.execute("SELECT id, name, domains FROM stations WHERE enabled=1"):
                try:
                    doms = json.loads(r["domains"] or "[]")
                except Exception:
                    doms = []
                for d in doms:
                    d = str(d).lower()
                    d = d.split("://")[-1].strip("/").strip(".")
                    if d and (host == d or host.endswith("." + d)):
                        return r
            return None

        def do_POST(self):
            u = urlparse(self.path)
            if u.path not in ("/api/ingest/probe", "/api/ingest/prices"):
                self.send_error(404)
                return
            ip = self.client_address[0]
            now = time.time()
            window = _INGEST_RATES.setdefault(ip, [])
            while window and window[0] < now - 60:
                window.pop(0)
            if len(window) >= 30:
                self._json({"ok": False, "error": "rate limited"}, status=429)
                return
            window.append(now)
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = 0
            if length <= 0 or length > 262144:      # 插件 payload 远小于此，防滥用
                self._json({"ok": False, "error": "bad content length"}, status=400)
                return
            raw = self.rfile.read(length)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                self._json({"ok": False, "error": "invalid json"}, status=400)
                return
            if not isinstance(payload, dict):
                self._json({"ok": False, "error": "payload must be an object"}, status=400)
                return
            kind = "probe" if u.path.endswith("/probe") else "prices"
            if kind == "probe" and not isinstance(payload.get("probes"), dict):
                self._json({"ok": False, "error": "missing probes object"}, status=400)
                return
            if kind == "prices" and not isinstance(payload.get("models"), list):
                self._json({"ok": False, "error": "missing models array"}, status=400)
                return
            origin = str(payload.get("station_url") or "")
            host = (urlparse(origin).hostname or "").lower()
            body_hash = hashlib.sha256(raw).hexdigest()
            conn = connect(db)
            try:
                st = self._match_station(conn, host)
                cur = conn.execute(
                    "INSERT OR IGNORE INTO user_reports "
                    "(kind, station_id, origin, origin_host, ext_version, body_hash, "
                    " payload, client_ip, received_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (kind, st["id"] if st else None, origin, host,
                     str(payload.get("extension_version") or ""), body_hash,
                     json.dumps(payload, ensure_ascii=False), ip, utc_now()))
                conn.commit()
                self._json({"ok": True, "dedup": cur.rowcount == 0,
                            "station": st["name"] if st else None,
                            "known": bool(st)})
            except Exception as e:
                self._json({"ok": False, "error": f"{type(e).__name__}: {e}"}, status=500)
            finally:
                conn.close()

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    with Server((args.host, args.port), Handler) as httpd:
        print(f"看板已启动：http://{args.host}:{args.port}/  （Ctrl+C 停止）")
        print("接口：/api/summary  /api/station?name=X  /api/history?model=X  /api/compare?model=X  /api/stations")
        print("插件上报：POST /api/ingest/probe  POST /api/ingest/prices（My API Box 社区上报端点）")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n已停止")
    return 0


def _ts_to_epoch(ts: str) -> float:
    try:
        return datetime.fromisoformat(ts.replace("Z", "")).timestamp()
    except Exception:
        return 0.0


def station_freshness(conn):
    """各启用站的数据新鲜度：最近一次成功快照、距今小时数、模型数、最近错误。

    之前 /api/summary 的 generated_at 用的是"请求时刻"，页面会显示当前时间，
    而数据可能是两天前抓的——等于谎报新鲜度。这里给出真实的数据时间。
    """
    out = []
    now = _ts_to_epoch(utc_now())
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(timespec="seconds")
    for r in conn.execute("""
        SELECT s.id, s.name, s.mode, s.state, s.last_check, s.recharge_rate,
          (SELECT MAX(ts) FROM snapshots k WHERE k.station_id=s.id AND k.ok=1) last_ok,
          (SELECT COUNT(*) FROM model_prices mp JOIN snapshots k ON mp.snapshot_id=k.id
            WHERE k.station_id=s.id AND k.ok=1 AND
                  k.id=(SELECT MAX(id) FROM snapshots WHERE station_id=s.id AND ok=1)) n,
          (SELECT error FROM snapshots k WHERE k.station_id=s.id ORDER BY k.id DESC LIMIT 1) err,
          -- 模型级真实调用探测：近 24 小时次数与失败次数（每次探测都是一次真实 API 调用）
          (SELECT COUNT(*) FROM model_availability ma WHERE ma.station_id=s.id
             AND ma.status IN ('probe_ok','probe_fail') AND ma.ts>=?) probes_24h,
          (SELECT COUNT(*) FROM model_availability ma WHERE ma.station_id=s.id
             AND ma.status='probe_fail' AND ma.ts>=?) probe_fail_24h
        FROM stations s WHERE s.enabled=1 ORDER BY s.id""", (cutoff_24h, cutoff_24h)):
        def _age(ts):
            return round((now - _ts_to_epoch(ts)) / 3600.0, 1) if ts else None
        out.append({"name": r["name"], "mode": r["mode"] or "newapi", "state": r["state"],
                    # last_check = 上次成功抓到数据（采集是否还在跑）；last_ok = 价格上次变动
                    "last_check": r["last_check"] or "", "check_hours": _age(r["last_check"]),
                    "last_ok": r["last_ok"] or "", "change_hours": _age(r["last_ok"]),
                    "models": int(r["n"] or 0), "error": (r["err"] or "")[:160],
                    "recharge_rate": r["recharge_rate"],
                    "probes_24h": int(r["probes_24h"] or 0),
                    "probe_fail_24h": int(r["probe_fail_24h"] or 0)})
    return out


def collect_price_changes(conn, hours: float = 168.0, threshold: float = 0.0) -> dict:
    """调价监控数据：price_changes 表 → {summary, rows}。

    price_changes 记录的是「输入价 $/M」的变动（token 模型口径），
    按次/按秒计价的模型没有输入价，故不出现在这里。
    """
    cutoff = time.time() - max(1.0, hours) * 3600
    rows = []
    for r in conn.execute("""
            SELECT pc.ts, pc.model_canon, pc.grp, pc.old_input_usd_m,
                   pc.new_input_usd_m, pc.pct, s.name AS station
            FROM price_changes pc JOIN stations s ON s.id = pc.station_id
            ORDER BY pc.id DESC"""):
        if _ts_to_epoch(r["ts"]) < cutoff:
            continue
        pct = r["pct"] or 0.0
        if threshold and abs(pct) < threshold:
            continue
        rows.append({"ts": r["ts"], "station": r["station"], "model": r["model_canon"] or "",
                     "group": r["grp"] or "",
                     "old": r["old_input_usd_m"], "new": r["new_input_usd_m"],
                     "pct": round(pct, 2)})
    stations = {x["station"] for x in rows}
    models = {x["model"] for x in rows}
    return {"hours": hours,
            "summary": {"total": len(rows),
                        "up": sum(1 for x in rows if x["pct"] > 0),
                        "down": sum(1 for x in rows if x["pct"] < 0),
                        "stations": len(stations), "models": len(models)},
            "rows": rows[:3000]}


def _push_notify(title: str, body: str, url: str) -> bool:
    """推送到通知渠道。支持 ntfy（POST 正文）、Server酱（form desp）、
    其余按通用 webhook（JSON {title, content}）。成功返回 True。"""
    if not url:
        return False
    try:
        if "ntfy.sh" in url:
            req = urllib.request.Request(
                url, data=body.encode("utf-8"), method="POST",
                headers={"Title": title, "Tags": "chartIncreasing"})
            urllib.request.urlopen(req, timeout=10).read()
        elif "sctapi" in url or "ftqq" in url:            # Server酱
            data = urllib.parse.urlencode({"title": title, "desp": body}).encode()
            req = urllib.request.Request(url, data=data, method="POST")
            urllib.request.urlopen(req, timeout=10).read()
        else:                                              # 通用 webhook
            payload = json.dumps({"title": title, "content": body}).encode("utf-8")
            req = urllib.request.Request(url, data=payload, method="POST",
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10).read()
        return True
    except Exception as e:
        print(f"  [推送失败] {type(e).__name__}: {str(e)[:100]}")
        return False


def _watch_digest(summary: dict, threshold: float, top: int) -> str:
    """把一轮的异动压成推送摘要文本。按站点×模型去重：
    同模型多分组只留变动最大的一档并标注组数，避免摘要被分组刷屏。"""
    chg = [c for c in summary["changes"] if abs(c["pct"] or 0) >= threshold]
    if not chg:
        return ""
    best = {}
    for c in chg:
        key = (c["station"], c["model"])
        cur = best.get(key)
        if cur is None:
            cur = dict(c)
            cur["_n"] = 1
            best[key] = cur
        else:
            cur["_n"] += 1
            if abs(c["pct"]) > abs(cur["pct"]):
                cur.update(c)
                cur["_n"] = best[key]["_n"]
    rows = sorted(best.values(), key=lambda c: -abs(c["pct"]))
    lines = [f"{len(chg)} 处调价 ≥{threshold}%（{len(rows)} 个模型，"
             f"快照 {summary['new']} 站 / 失败 {summary['failed']} 站）"]
    for c in rows[:top]:
        arrow = "↑" if c["pct"] > 0 else "↓"
        grp = c["grp"][:14] + (f" 等{c['_n']}组" if c["_n"] > 1 else "")
        lines.append(f"{c['station']} {c['model'][:24]} {grp} "
                     f"{fmt(c['old'])}→{fmt(c['new'])} {arrow}{abs(c['pct']):.1f}%")
    if len(rows) > top:
        lines.append(f"… 等 {len(rows)} 个模型（完整明细用 alerts 命令）")
    return "\n".join(lines)


def _dur_text(sec) -> str:
    """时长友好显示：秒 / 分钟 / 小时。"""
    sec = float(sec)
    if sec >= 3600:
        return f"{sec / 3600:.1f} 小时"
    if sec >= 60:
        return f"{sec / 60:.0f} 分钟"
    return f"{sec:.0f} 秒"


def _watch_probe_models(conn, args) -> list:
    """watch 的模型级探测清单：优先 --probe-model；否则取在售站数最多的前 N 个。
    只挑白名单内的语言模型 —— chat 探针只适用于对话类，图片/视频会被误判为不可用。"""
    explicit = getattr(args, "probe_model", None) or []
    if explicit:
        return list(dict.fromkeys(explicit))
    top = max(1, int(getattr(args, "probe_model_top", 8) or 8))
    try:
        rows = conn.execute("""
            SELECT mp.model_canon AS canon, COUNT(DISTINCT k.station_id) AS n
            FROM model_prices mp JOIN snapshots k ON mp.snapshot_id=k.id
            WHERE k.ok=1 AND k.id IN (SELECT MAX(id) FROM snapshots WHERE ok=1 GROUP BY station_id)
              AND mp.model_canon IS NOT NULL
            GROUP BY mp.model_canon ORDER BY n DESC, mp.model_canon LIMIT ?""",
            (top * 4,)).fetchall()
    except Exception:
        return []
    return [r["canon"] for r in rows if in_model_whitelist(r["canon"])][:top]


class _Tee:
    """把输出同时写到控制台与日志文件；pythonw 下 sys.stdout 可能是 None。"""

    def __init__(self, *streams):
        self.streams = [s for s in streams if s is not None]

    def write(self, s):
        for st in self.streams:
            try:
                st.write(s)
            except Exception:
                pass
        return len(s)

    def flush(self):
        for st in self.streams:
            try:
                st.flush()
            except Exception:
                pass


def _watch_logger():
    """watch 常驻进程的日志：pythonw 没有控制台，出错只能靠文件排查。"""
    path = os.environ.get("RELAY_WATCH_LOG", "watch.log")
    try:
        if os.path.exists(path) and os.path.getsize(path) > 5 * 1024 * 1024:
            os.replace(path, path + ".1")
        f = open(path, "a", encoding="utf-8", buffering=1)
        sys.stdout = _Tee(sys.stdout, f)
        sys.stderr = _Tee(sys.stderr, f)
        print(f"\n===== watch 启动 {utc_now()} =====")
        return f
    except Exception:
        return None


def cmd_watch(args):
    """定时任务：站点可达性高频探测（--probe-interval，默认 600s/10 分钟），
    价格抓取低频（--interval，默认 43200s/12 小时，抓取同时产出模型可用性检测）。
    抓取轮与上次快照 diff，超过 --threshold 的异动打印摘要并（可选）推送。"""
    _watch_logger()
    conn = connect()
    push_url = args.push or os.environ.get("RELAY_PUSH_URL", "")
    mode = f"，推送 → {push_url}" if push_url else "（无推送，仅打印）"
    print(f"站点可达性探测：每 {_dur_text(args.probe_interval)} 一次"
          "（全网连续可用时自动降频至 1/3，出现异常时翻倍加密）")
    print(f"价格抓取 + 模型可用性检测：每 {_dur_text(args.interval)} 一次，"
          f"调价告警阈值 {args.threshold}%{mode}"
          + (f"，最多 {args.max_runs} 轮抓取" if args.max_runs else "（Ctrl+C 停止）"))
    mp_int = max(0, int(getattr(args, "model_probe_interval", 0) or 0))
    if mp_int:
        plist = (("指定：" + "、".join(args.probe_model)) if getattr(args, "probe_model", None)
                 else f"自动取站数最多的前 {getattr(args, 'probe_model_top', 8)} 个语言模型")
        print(f"模型级真实调用探测：每 {_dur_text(mp_int)} 一次（{plist}；需站点配 api_key）")
    runs = 0
    probes = 0
    mprobes = 0
    next_scrape = 0.0
    next_probe = 0.0
    next_model_probe = 0.0 if mp_int else float("inf")
    # 探测降频：全网稳定时拉长间隔（省请求、降低被封风险），
    # 出现异常时加密以便尽快发现恢复。streak = 连续「全网可用」轮数。
    ok_streak = 0
    probe_cur = args.probe_interval
    try:
        while True:
            now = time.monotonic()
            did = False
            # ---- 站点可达性探测（高频，轻量）----
            if now >= next_probe:
                probes += 1
                all_ok = False
                try:
                    pr = run_probe_round(conn, args, verbose=False)
                    bad = [n for n, (_, r) in pr["results"].items() if not r["ok"]]
                    all_ok = bool(pr["total"]) and pr["ok"] == pr["total"]
                    print(f"[可达性 #{probes}] {utc_now()}  {pr['ok']}/{pr['total']} 可用"
                          + (f"，异常：{'、'.join(bad[:5])}" if bad else ""))
                except Exception as e:
                    print(f"[可达性 #{probes}] 异常: {type(e).__name__}: {e}")
                ok_streak = ok_streak + 1 if all_ok else 0
                # 连续 6 轮全绿 → 1/3 频率；3 轮 → 1/2；有异常 → 翻倍加密
                factor = 3.0 if ok_streak >= 6 else (2.0 if ok_streak >= 3 else (0.5 if not all_ok else 1.0))
                probe_cur = min(max(args.probe_interval * factor, 60), 3600)
                if probes == 1 or factor != 1.0:
                    print(f"  ↳ 探测间隔调整为 {_dur_text(probe_cur)}"
                          f"（连续 {ok_streak} 轮全网可用）")
                j = random.uniform(-args.jitter, args.jitter) if args.jitter else 0
                next_probe = time.monotonic() + max(5, probe_cur + j)
                did = True
            # ---- 价格抓取 + 可用性检测（低频）----
            if now >= next_scrape:
                runs += 1
                print(f"\n----- 第 {runs} 轮抓取 {utc_now()} -----")
                try:
                    summary = run_scrape_round(conn, args)
                except Exception as e:
                    print(f"本轮异常: {type(e).__name__}: {e}")
                    summary = {"new": 0, "unchanged": 0, "failed": 0, "total": 0,
                               "changes": [], "empty": False}
                digest = _watch_digest(summary, args.threshold, args.top)
                if digest:
                    title = f"[中转站监控] 第{runs}轮 {len(summary['changes'])} 处调价"
                    print("\n" + digest)
                    if push_url:
                        if _push_notify(title, digest, push_url):
                            print(f"  [已推送] {push_url[:60]}")
                elif not summary.get("empty"):
                    print(f"  本轮无异动 ≥{args.threshold}%")
                j = random.uniform(-args.jitter, args.jitter) if args.jitter else 0
                next_scrape = time.monotonic() + max(60, args.interval + j)
                did = True
            # ---- 模型级真实调用探测（低频，需 api_key）----
            if mp_int and now >= next_model_probe:
                mprobes += 1
                try:
                    models = _watch_probe_models(conn, args)
                    if not models:
                        print(f"[模型探测 #{mprobes}] 没有可探测的模型（白名单为空或无快照）")
                    else:
                        st_list = load_stations(conn)
                        ok = fail = unknown = skipped = 0
                        rr = {}
                        for m in models:
                            sub = argparse.Namespace(model=m, concurrency=args.concurrency,
                                                     timeout=args.timeout)
                            rr = _avail_probe(conn, st_list, sub, verbose=False,
                                              name_by_sid=_station_model_names(conn, m))
                            if rr.get("skip"):
                                skipped += 1
                            else:
                                ok += rr.get("ok", 0)
                                fail += rr.get("fail", 0)
                                unknown += rr.get("unknown", 0)
                        if skipped and not (ok or fail or unknown):
                            print(f"[模型探测 #{mprobes}] 跳过：{rr.get('reason', '没有可探测站点')}")
                        else:
                            print(f"[模型探测 #{mprobes}] {utc_now()}  {len(models)} 个模型 · "
                                  f"可调用 {ok}，不可调用 {fail}"
                                  + (f"，不确定 {unknown}" if unknown else ""))
                except Exception as e:
                    print(f"[模型探测 #{mprobes}] 异常: {type(e).__name__}: {e}")
                j = random.uniform(-args.jitter, args.jitter) if args.jitter else 0
                next_model_probe = time.monotonic() + max(60, mp_int + j)
                did = True
            if args.max_runs and runs >= args.max_runs:
                print(f"\n已完成 {runs} 轮抓取，退出")
                break
            if not did:
                # 睡到最近的下一次动作
                wait = min(next_scrape, next_probe, next_model_probe) - time.monotonic()
                time.sleep(max(1, wait))
    except KeyboardInterrupt:
        print("\n已停止")
    return 0


def _probe_http(station: dict, timeout: float):
    """HTTP 可达性探针（无需 key）：记录状态码与延迟。"""
    st, _, lat, err, _ = http_get(normalize_domain(station["domains"][0]) + "/",
                                  timeout, {}, 0, 1.0)
    return {"ok": 1 if st == 200 else 0, "status": st, "latency_ms": lat,
            "error": err, "ttft": None, "snippet": ""}


def _probe_api(station: dict, model: str, timeout: float):
    """真实调用探针（需 key）：发最小 chat 请求，测可用性与首字节延迟。"""
    if not _requests:
        return {"ok": 0, "status": None, "latency_ms": None, "error": "需要 requests 库",
                "ttft": None, "snippet": ""}
    base = normalize_domain(station.get("api_base") or station["domains"][0])
    url = base + "/v1/chat/completions"
    payload = {"model": model, "messages": [{"role": "user", "content": "hi"}],
               "max_tokens": 1, "stream": True}
    t0 = time.monotonic()
    ttft = None
    snippet = ""
    try:
        hdr = {"Authorization": f"Bearer {station['api_key']}",
               "Content-Type": "application/json"}
        for k, v in (station.get("headers") or {}).items():
            if v is not None and v != "":
                hdr[str(k)] = str(v)
        r = _session().post(url, json=payload, timeout=timeout, headers=hdr,
                            stream=True)
        for chunk in r.iter_content(chunk_size=512):
            if chunk:
                ttft = int((time.monotonic() - t0) * 1000)
                snippet = chunk.decode("utf-8", "replace")[:200]
                break
        ok = (r.status_code == 200)
        # 失败时把响应体带出来：403/503 常见是「分组无权访问该模型」或 Cloudflare，
        # 只记 HTTP 状态码无法定位，写进 detail 便于排查
        err = None if ok else (f"HTTP {r.status_code}: {snippet}" if snippet
                               else f"HTTP {r.status_code}")
        return {"ok": 1 if ok else 0, "status": r.status_code,
                "latency_ms": int((time.monotonic() - t0) * 1000), "error": err,
                "ttft": ttft, "snippet": snippet}
    except Exception as e:
        return {"ok": 0, "status": None, "latency_ms": int((time.monotonic() - t0) * 1000),
                "error": f"{type(e).__name__}: {str(e)[:120]}", "ttft": None, "snippet": ""}


def run_probe_round(conn, args, verbose: bool = True) -> dict:
    """一轮站点可达性探测（可复用于 watch 定时任务）。
    无 key → HTTP 可达性；配了 key 且指定 --model → 真实 API 调用。
    返回 {ok, total, results: {站名: (kind, 结果)}}。"""
    stations = load_stations(conn)
    if getattr(args, "station", None):
        stations = [s for s in stations if s["name"] in args.station]
    todo = []
    for s in stations:
        kind = "api" if (s.get("api_key") and getattr(args, "model", None)) else "http"
        todo.append((s, kind))
    n_api = sum(1 for _, k in todo if k == "api")
    if verbose:
        print(f"探测 {len(todo)} 站（真实调用 {n_api}，HTTP 可达性 {len(todo) - n_api}）…")
    results = {}
    with ThreadPoolExecutor(max_workers=max(1, getattr(args, "concurrency", 6))) as ex:
        futs = {}
        for s, kind in todo:
            fn = (lambda st: _probe_api(st, args.model, args.timeout)) if kind == "api" \
                else (lambda st: _probe_http(st, args.timeout))
            futs[ex.submit(fn, s)] = (s, kind)
        for fut in as_completed(futs):
            s, kind = futs[fut]
            try:
                results[s["name"]] = (kind, fut.result())
            except Exception as e:
                results[s["name"]] = (kind, {"ok": 0, "status": None, "latency_ms": None,
                                             "error": str(e)[:120], "ttft": None, "snippet": ""})
    n_ok = 0
    n_ok = 0
    for s, kind in todo:
        kind, r = results[s["name"]]
        n_ok += r["ok"]
        conn.execute(
            "INSERT INTO probes(station_id, ts, kind, model, ok, status_code, latency_ms,"
            " ttft_ms, error, response_snippet) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (s["id"], utc_now(), kind, getattr(args, "model", None) if kind == "api" else None,
             r["ok"], r["status"], r["latency_ms"], r["ttft"], r["error"], r["snippet"]))
        if verbose:
            tag = "可用" if r["ok"] else "不可用"
            extra = f"  状态码 {r['status']}" if r["status"] else f"  {r['error'] or ''}"
            ttft = f"  首字节 {r['ttft']}ms" if r["ttft"] else ""
            print(f"  [{tag}] {s['name']:<14} {kind:<5} 延迟 {r['latency_ms']}ms{ttft}{extra}")
    conn.commit()
    if verbose:
        print(f"\n汇总：{n_ok}/{len(todo)} 可用（结果已存 probes 表）")
    return {"ok": n_ok, "total": len(todo), "results": results}


def backup_database(conn, backup_dir: str = "backups", keep: int = 14) -> str:
    """用 SQLite 在线备份 API 拷一份数据库 → 备份文件路径。

    不锁库、不影响 watch 抓取；比文件拷贝安全（拷贝中途正在写会损坏）。
    watch 每轮抓取后调用一次，按日期命名并只保留最近 keep 份。
    """
    os.makedirs(backup_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(backup_dir, f"relay_data_{stamp}.db")
    dst = sqlite3.connect(path)
    try:
        with dst:
            conn.backup(dst)
    finally:
        dst.close()
    old = sorted(f for f in os.listdir(backup_dir) if f.startswith("relay_data_"))
    for f in old[:-keep] if keep > 0 else []:
        try:
            os.remove(os.path.join(backup_dir, f))
        except OSError:
            pass
    return path


def cmd_backup(args):
    if not os.path.exists(DB_PATH):
        print(f"数据库不存在：{DB_PATH}")
        return 1
    conn = connect()
    if args.vacuum:
        conn.execute("VACUUM")
        conn.commit()
        print("已 VACUUM 压缩")
    path = backup_database(conn, args.dir, args.keep)
    size = os.path.getsize(path) / 1048576
    print(f"已备份 → {path}（{size:.1f} MB）")
    return 0


def cmd_bench(args):
    """性能实测（集成 api-bench，MIT，纯标准库）：TTFT / TPOT / 吞吐 / 缓存命中 / 滚动缓存。

    真实请求、真实扣费——只手动触发，绝不进 watch 自动轮。
    结果落 bench_results 表，看板汇总新增「TTFT p50」「缓存命中」两列（未测显示琥珀「未测」）。
    """
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "api_bench", f"{args.protocol}_bench.py")
    if not os.path.exists(script):
        print(f"找不到基准脚本：{script}")
        return 1
    conn = connect()
    stations = load_stations(conn)
    st = next((s for s in stations if s["name"] == args.station), None)
    if not st:
        print(f"站点不存在或未启用：{args.station}")
        return 1
    if not st.get("api_key"):
        print(f"该站未配置 api_key（stations.json），无法发真实请求")
        return 1
    base = (st.get("api_base") or f"https://{(st.get('domains') or [''])[0]}").rstrip("/")
    path = "/v1/messages" if args.protocol == "messages" else "/v1/responses"
    url = base + path

    # 默认模型：从该站最新快照里挑一个便宜且适配协议的（可 --model 覆盖）
    model = args.model
    if not model:
        model = _bench_pick_model(conn, st["id"], args.protocol)
        if not model:
            print("快照里没找到可测模型，请用 --model 指定")
            return 1
    print(f"目标：{st['name']}  {url}\n模型：{model}  协议：{args.protocol}  "
          f"画像：{'quick(省额度)' if args.quick else '标准'}")
    print("⚠ 将发送真实请求并消耗额度；缓存/滚动阶段带长上下文，费用约为普通请求的几倍\n")

    out_json = os.path.join("bench_out", f"{args.station}_{args.protocol}_"
                            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    os.makedirs(os.path.dirname(out_json), exist_ok=True)
    # --html 有默认输出名，不给路径会写脏 cwd；用户没指定就丢进 bench_out 临时文件
    # 注意：--max-tokens/--prefix-tokens 是全局参数，必须放在 report 子命令之前
    # （README 明确提醒，冒烟测试实测放后面会 unrecognized arguments）
    cmd = [sys.executable, script,
           "--url", url, "--model", model, "--profile", args.profile,
           "--timeout", str(args.timeout),
           "--max-tokens", "200" if args.quick else "2048",
           "--prefix-tokens", "2000" if args.quick else "8000",
           "report", "--json", out_json,
           "--html", args.html or os.path.join(os.path.dirname(out_json), "_tmp_report.html")]
    if args.quick:
        cmd += ["--concurrency", "2", "--requests", "6",
                "--cache-samples", "8", "--cache-interval", "2",
                "--roll-turns", "6", "--roll-turn-tokens", "300"]
    else:
        cmd += ["--concurrency", str(args.concurrency), "--requests", str(args.requests),
                "--cache-samples", str(args.cache_samples), "--cache-interval", str(args.cache_interval),
                "--roll-turns", str(args.roll_turns)]

    env = dict(os.environ)
    env["BENCH_API_KEY"] = st["api_key"]       # 密钥走环境变量，不进进程列表
    # Windows 控制台默认 GBK，脚本输出的 ⚠️/✅ 会直接 UnicodeEncodeError 崩掉
    # （JSON 都来不及写），强制子进程 UTF-8
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    t0 = time.time()
    try:
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True,
                              encoding="utf-8", errors="replace", timeout=args.timeout + 600)
    except subprocess.TimeoutExpired:
        print("基准脚本超时")
        return 1
    tail = (proc.stdout or "").strip().splitlines()[-6:]
    print("\n".join(tail))
    if proc.returncode != 0:
        print(f"\n基准脚本退出码 {proc.returncode}；stderr 尾部：\n"
              + "\n".join((proc.stderr or "").strip().splitlines()[-4:]))
        return 1
    try:
        with open(out_json, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"解析结果失败：{e}")
        return 1
    _bench_store(conn, st["id"], model, args.protocol, data, out_json)
    lat = (data.get("latency") or {}).get("first_text") or {}
    cache = data.get("cache") or {}
    roll = data.get("roll") or {}
    print(f"\n已落库 → {out_json}（耗时 {time.time()-t0:.0f}s）")
    print(f"TTFT p50={lat.get('p50_ms','-')}ms p90={lat.get('p90_ms','-')}ms | "
          f"TPOT={(data.get('latency') or {}).get('tpot_visible', {}).get('p50_ms','-')}ms | "
          f"缓存命中 {cache.get('hit_rate', '-')}%（token 口径 {cache.get('token_hit_rate','-')}%） | "
          f"滚动捕获 {round((roll.get('capture') or 0)*100)}% 省下 {roll.get('saved','-')}%")
    return 0


def _bench_pick_model(conn, station_id: int, protocol: str) -> str:
    """从该站最新快照挑默认被测模型：便宜优先（flash/haiku 系），且与协议匹配。"""
    import re as _re
    prefs = ([_re.compile(r"claude.*(haiku|sonnet)", _re.I)]
             if protocol == "messages" else
             [_re.compile(r"gpt-.*(mini|nano|flash)", _re.I)])
    prefs += [_re.compile(p, _re.I) for p in
              (r"glm-.*flash", r"deepseek.*flash", r"claude", r"gpt")]
    row = conn.execute(
        "SELECT mp.model_name FROM model_prices mp JOIN snapshots k ON k.id=mp.snapshot_id "
        "WHERE k.station_id=? AND k.ok=1 AND mp.billing_unit IS NULL "
        "ORDER BY k.id DESC", (station_id,)).fetchall()
    names = [r["model_name"] for r in row]
    for p in prefs:
        for n in names:
            if p.search(n):
                return n
    return names[0] if names else ""


def _bench_store(conn, station_id: int, model: str, protocol: str, data: dict, path: str):
    """api-bench --json 结果 → bench_results 行（证据原文随行存档）。"""
    lat = (data.get("latency") or {})
    ft = lat.get("first_text") or {}
    tp = lat.get("tpot_visible") or {}
    thr = data.get("throughput") or {}
    cache = data.get("cache") or {}
    roll = data.get("roll") or {}
    counts = data.get("counts") or {}
    canon = agg_key(canonical_model(model))
    conn.execute(
        "INSERT INTO bench_results (station_id, model_agg, model_raw, protocol, ts, ok, "
        " requests, fails, ttft_p50_ms, ttft_p90_ms, ttft_p99_ms, tpot_ms, decode_tps, "
        " req_per_s, cache_hit_pct, cache_token_pct, roll_capture_pct, roll_saved_pct, "
        " errors, payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (station_id, canon, model, protocol, utc_now(),
         1 if (counts.get("ok") or 0) > 0 else 0,
         counts.get("ok"), counts.get("fail"),
         ft.get("p50_ms"), ft.get("p90_ms"), ft.get("p99_ms"), tp.get("p50_ms"),
         thr.get("decode_tps"), thr.get("req_per_s"),
         cache.get("hit_rate"), cache.get("token_hit_rate"),
         (round(roll["capture"] * 100, 1) if isinstance(roll.get("capture"), (int, float)) else None),
         roll.get("saved"),
         json.dumps(data.get("error_classes") or {}, ensure_ascii=False),
         json.dumps(data, ensure_ascii=False)))
    conn.commit()


def bench_metrics_by_station(conn) -> dict:
    """每站最近一次成功实测的 {站名: TTFT/缓存指标}——汇总列与 /api/bench 共用。"""
    out = {}
    for r in conn.execute("""
            SELECT b.*, s.name AS station FROM bench_results b
            JOIN stations s ON s.id=b.station_id
            WHERE b.id IN (SELECT MAX(id) FROM bench_results
                           WHERE station_id=b.station_id AND ok=1)"""):
        out[r["station"]] = {
            "ttft_p50_ms": r["ttft_p50_ms"], "ttft_p90_ms": r["ttft_p90_ms"],
            "cache_hit_pct": r["cache_hit_pct"], "cache_token_pct": r["cache_token_pct"],
            "model": r["model_raw"], "protocol": r["protocol"], "ts": r["ts"]}
    return out


def cmd_probe(args):
    """可用性探针：无 key 时跑 HTTP 可达性，有 key 时跑真实 API 调用。"""
    conn = connect()
    stations = load_stations(conn)
    if args.station:
        stations = [s for s in stations if s["name"] in args.station]
    if not stations:
        print("没有匹配的站点")
        return 1
    run_probe_round(conn, args, verbose=True)
    return 0


# ---------------------------------------------------------------------------
# 模型可用性检测：抓各站「当前在售模型清单」与库内快照比对（上架/下架），
# 配了 API key 的站还可做真实调用探针（probe_ok / probe_fail）。
# ---------------------------------------------------------------------------

def _avail_models_of(res: dict) -> dict:
    """抓价结果 → {聚合键: 站方模型名}（该站当前在售模型）。"""
    if not res.get("ok"):
        return {}
    try:
        models, _groups, _ver = parse_pricing(res["pricing"])
    except Exception:
        return {}
    out = {}
    for m in models:
        canon = m.get("model_canon") or canonical_model(m["model_name"])
        key = agg_key(canon)
        if key:
            out[key] = m["model_name"]
    return out


def _avail_snapshot_models(conn, station_id: int) -> dict:
    """该站最近一次成功快照里的模型集合 {聚合键: 模型名}（作为上轮在售基线）。"""
    row = conn.execute("SELECT id FROM snapshots WHERE station_id=? AND ok=1 "
                       "ORDER BY id DESC LIMIT 1", (station_id,)).fetchone()
    if not row:
        return {}
    out = {}
    for r in conn.execute("SELECT model_agg, model_name FROM model_prices WHERE snapshot_id=?",
                          (row["id"],)):
        if r["model_agg"]:
            out[r["model_agg"]] = r["model_name"]
    return out


def _avail_state(conn) -> dict:
    """当前可用性状态 {(station_id, model_agg): 行} —— 每对取最新一条。"""
    state = {}
    for r in conn.execute("SELECT station_id, model_agg, model_name, ts, status, detail, "
                          "latency_ms FROM model_availability ORDER BY id"):
        state[(r["station_id"], r["model_agg"])] = dict(r)
    return state


def _avail_by_model(conn) -> dict:
    """按模型聚合可用性：{聚合键: {listed, gone, probe_ok, probe_fail, checked, ts, stations}}。
    「上下架」与「真实探测」分别取各自最近一条 —— 否则一次探测失败/unknown 会覆盖
    掉 listed 状态，把该站从「在售站」集合里剔除，站点级可用率就统计不到了。"""
    listing = {}   # (sid, key) -> (status, ts)，status ∈ listed/gone
    probing = {}   # (sid, key) -> (status, ts)，status ∈ probe_ok/probe_fail
    for r in conn.execute(
            "SELECT station_id, model_agg, status, ts FROM model_availability ORDER BY id"):
        k = (r["station_id"], r["model_agg"])
        if r["status"] in ("listed", "gone"):
            listing[k] = (r["status"], r["ts"])
        elif r["status"] in ("probe_ok", "probe_fail"):
            probing[k] = (r["status"], r["ts"])
    out = {}
    for (sid, key) in set(listing) | set(probing):
        if not key:
            continue
        d = out.setdefault(key, {"listed": 0, "gone": 0, "probe_ok": 0, "probe_fail": 0,
                                 "checked": 0, "ts": "", "stations": set()})
        ls = listing.get((sid, key))
        if ls:
            d[ls[0]] += 1
            d["checked"] += 1
            if ls[0] == "listed":
                d["stations"].add(sid)
            if ls[1] > d["ts"]:
                d["ts"] = ls[1]
        ps = probing.get((sid, key))
        if ps:
            d[ps[0]] += 1
            if not ls:
                d["checked"] += 1
            if ps[1] > d["ts"]:
                d["ts"] = ps[1]
    return out


def _avail_refresh(conn, stations, args) -> dict:
    """抓各站当前模型清单，与库内最近快照比对，写入上架/下架变更。"""
    state = _avail_state(conn)
    pending = []
    stat = {"ok": 0, "fail": 0, "up": 0, "down": 0, "listed": 0, "models": 0}
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as ex:
        futs = {ex.submit(fetch_station, st, args.timeout, 1, 1.0): st for st in stations}
        for fut in as_completed(futs):
            st = futs[fut]
            try:
                res = fut.result()
            except Exception as e:
                stat["fail"] += 1
                err = f"{type(e).__name__}: {str(e)[:150]}"
                old = state.get((st["id"], ""))
                if not old or old["status"] != "unknown" or old["detail"] != err:
                    pending.append((st["id"], "", None, "unknown", err, None))
                continue
            if not res.get("ok"):
                stat["fail"] += 1
                err = " | ".join(res.get("errors", []))[:200] or "抓取失败"
                old = state.get((st["id"], ""))
                if not old or old["status"] != "unknown" or old["detail"] != err:
                    pending.append((st["id"], "", None, "unknown", err, None))
                continue
            stat["ok"] += 1
            cur = _avail_models_of(res)
            stat["models"] += len(cur)
            # 基线 = 最近成功快照的模型 ∪ 上轮检测标记为「在售」的模型
            # （快照不一定每次都更新，只用快照会漏掉「检测后又被下架」的模型）
            prev = _avail_snapshot_models(conn, st["id"])
            for (psid, pkey), pv in state.items():
                if psid == st["id"] and pkey and pv["status"] == "listed" and pkey not in prev:
                    prev[pkey] = pv.get("model_name") or pkey
            for key, name in cur.items():
                if key in prev:
                    prev.pop(key, None)             # 仍在售 → 从「疑似下架」集合中移除
                    old = state.get((st["id"], key))
                    if old and old["status"] == "listed":
                        continue                   # 无变化不重复写（表只记变更）
                    stat["listed"] += 1
                    pending.append((st["id"], key, name, "listed", "在售", None))
                else:
                    stat["up"] += 1
                    pending.append((st["id"], key, name, "listed", "新上架", None))
            for key, name in prev.items():
                old = state.get((st["id"], key))
                if old and old["status"] == "gone":
                    continue
                stat["down"] += 1
                pending.append((st["id"], key, name, "gone", "已下架", None))
    if pending:
        ts = utc_now()
        conn.executemany(
            "INSERT INTO model_availability(station_id, model_agg, model_name, ts, status,"
            " detail, latency_ms) VALUES(?,?,?,?,?,?,?)",
            [(sid, key, name, ts, status, detail, lat)
             for sid, key, name, status, detail, lat in pending])
        conn.commit()
    return stat


def _station_model_names(conn, model: str) -> dict:
    """该模型在各站最新快照里的「原始模型名」：{station_id: model_name}。
    真实调用必须用站点自己认的名字（同一模型各站可能叫 claude-opus-4-8 / claude-opus-4-8-2025…），
    否则会得到 model_not_found，被误判为不可用。"""
    key = agg_key(canonical_model(model))
    out = {}
    try:
        rows = conn.execute("""
            SELECT k.station_id AS sid, mp.model_name, mp.model_canon, mp.model_agg
            FROM model_prices mp JOIN snapshots k ON mp.snapshot_id = k.id
            WHERE k.ok = 1
              AND k.id IN (SELECT MAX(id) FROM snapshots WHERE ok=1 GROUP BY station_id)""").fetchall()
    except Exception:
        return out
    for r in rows:
        k = r["model_agg"] or agg_key(r["model_canon"] or canonical_model(r["model_name"]))
        if k == key and r["model_name"]:
            out[r["sid"]] = r["model_name"]
    return out


def _avail_probe(conn, stations, args, verbose: bool = True, name_by_sid: dict = None) -> dict:
    """真实调用探针：仅对配置了 API key 的站生效。
    name_by_sid 可给每站指定原始模型名（缺省用 args.model）。"""
    todo = [s for s in stations if s.get("api_key")]
    if not todo:
        return {"skip": True, "reason": "没有配置 API key 的站点（真实调用需在 stations.json 填 api_key）"}
    if not args.model:
        return {"skip": True, "reason": "请用 --model 指定要真实调用的模型名"}
    stat = {"ok": 0, "fail": 0, "unknown": 0, "skip": False, "rows": 0}
    nm = lambda st: (name_by_sid or {}).get(st["id"]) or args.model
    pending = []
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as ex:
        futs = {ex.submit(_probe_api, st, nm(st), args.timeout): st for st in todo}
        for fut in as_completed(futs):
            st = futs[fut]
            try:
                r = fut.result()
            except Exception as e:
                r = {"ok": 0, "status": None, "latency_ms": None,
                     "error": f"{type(e).__name__}: {str(e)[:120]}", "ttft": None, "snippet": ""}
            key = agg_key(canonical_model(args.model))
            body = (r.get("snippet") or r.get("error") or "").lower()
            # 额度不足 / 模型名对不上 / 限流 = 「不确定」，不能记成 probe_fail，
            # 否则会把「余额为 0」「名字不一致」误判成「该模型不可用」
            transient = any(k in body for k in (
                "quota", "insufficient", "model_not_found", "no available channel",
                "not supported", "invalid model", "rate limit", "too many requests"))
            if r["ok"]:
                status = "probe_ok"; stat["ok"] += 1
            elif r.get("status") in (404, 405, 429) or transient:
                status = "unknown"; stat["unknown"] += 1
            else:
                status = "probe_fail"; stat["fail"] += 1
            pending.append((st["id"], key, nm(st), status,
                            (r.get("error") or f"HTTP {r.get('status')}"), r.get("latency_ms")))
            if verbose:
                tag = "可调用" if r["ok"] else ("不确定" if status == "unknown" else "不可调用")
                print(f"  [{tag}] {st['name']:<14} {nm(st):<26} "
                      f"{r.get('latency_ms')}ms  {r.get('error') or ''}")
    if pending:
        ts = utc_now()
        conn.executemany(
            "INSERT INTO model_availability(station_id, model_agg, model_name, ts, status,"
            " detail, latency_ms) VALUES(?,?,?,?,?,?,?)",
            [(sid, key, name, ts, status, detail, lat)
             for sid, key, name, status, detail, lat in pending])
        conn.commit()
    stat["rows"] = len(pending)
    return stat


def cmd_availability(args):
    """模型可用性检测：上架/下架比对（--refresh）、真实调用探针（--probe）、报告（--report）。"""
    conn = connect()
    stations = load_stations(conn)
    if args.station:
        stations = [s for s in stations if s["name"] in args.station]
    if not stations:
        print("没有匹配的站点")
        return 1
    do_refresh = bool(args.refresh)
    do_probe = bool(args.probe)
    do_report = bool(args.report) or not (do_refresh or do_probe)
    result = {"stations": [s["name"] for s in stations]}
    if do_refresh:
        st = _avail_refresh(conn, stations, args)
        result["refresh"] = st
        print(f"可用性刷新：{st['ok']} 站成功 / {st['fail']} 站失败，检出在售模型 {st['models']} 个；"
              f"新上架 {st['up']}，下架 {st['down']}，新增在售 {st['listed']}")
    if do_probe:
        pr = _avail_probe(conn, stations, args,
                          name_by_sid=_station_model_names(conn, args.model) if args.model else {})
        result["probe"] = pr
        if pr.get("skip"):
            print(f"跳过真实调用探针：{pr['reason']}")
        else:
            print(f"真实调用探针：可调用 {pr['ok']}，不可调用 {pr['fail']}"
                  + (f"，不确定 {pr.get('unknown', 0)}" if pr.get('unknown') else "")
                  + "（结果已存 model_availability）")
    if do_report:
        report = _avail_report(conn, args)
        result["report"] = report
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            _print_avail_report(report, args)
    return 0


def _avail_report(conn, args) -> dict:
    """汇总可用性报告：站点维度 + 模型维度 + 近 N 小时变更。"""
    state = _avail_state(conn)
    by_model = _avail_by_model(conn)
    station_names = {r["id"]: r["name"] for r in conn.execute("SELECT id, name FROM stations")}
    st_rows = []
    for sid, name in station_names.items():
        items = {k: v for (s, k), v in state.items() if s == sid and k}
        if not items and (sid, "") not in state:
            continue
        last = state.get((sid, ""), {})
        listed = sum(1 for v in items.values() if v["status"] == "listed")
        gone = sum(1 for v in items.values() if v["status"] == "gone")
        ts = max([v["ts"] for v in items.values()] + ([last["ts"]] if last else []))
        st_rows.append({"station": name, "listed": listed, "gone": gone,
                        "status": last.get("status") or "listed",
                        "error": last.get("detail") if last.get("status") == "unknown" else None,
                        "ts": ts})
    st_rows.sort(key=lambda x: (x["status"] != "unknown", -x["listed"]))
    models = []
    for key, d in by_model.items():
        models.append({"model": key, "listed": d["listed"], "gone": d["gone"],
                       "probe_ok": d["probe_ok"], "probe_fail": d["probe_fail"],
                       "checked": d["checked"], "ts": d["ts"],
                       "stations": sorted(station_names.get(s, str(s)) for s in d["stations"])})
    models.sort(key=lambda x: (-x["listed"], x["model"]))
    since = (datetime.now(timezone.utc) - timedelta(hours=max(0, args.hours))).isoformat()
    changes = []
    for (sid, key), v in state.items():
        if not key or v["ts"] < since:
            continue
        if v["status"] in ("listed", "gone") and v.get("detail") in ("新上架", "已下架"):
            changes.append({"ts": v["ts"], "station": station_names.get(sid, str(sid)),
                            "model": v.get("model_name") or key, "model_agg": key,
                            "status": v["status"], "detail": v["detail"]})
    changes.sort(key=lambda x: x["ts"], reverse=True)
    return {"generated_at": utc_now(), "window_hours": args.hours,
            "stations": st_rows, "models": models, "changes": changes}


def _print_avail_report(rep, args):
    print(f"\n可用性报告（生成于 {rep['generated_at']}）")
    print("\n【站点】")
    print(f"  {'站点':<16}{'在售':>5}{'下架':>5}  {'状态':<10}最近检测")
    for r in rep["stations"]:
        tag = {"listed": "正常", "gone": "正常", "unknown": "检测失败",
               "probe_ok": "正常", "probe_fail": "部分不可用"}.get(r["status"], r["status"])
        print(f"  {r['station']:<16}{r['listed']:>5}{r['gone']:>5}  {tag:<10}"
              f"{(r['ts'] or '-')[:19]}{('  ' + r['error'][:60]) if r.get('error') else ''}")
    print(f"\n【模型】（共 {len(rep['models'])} 个有检测记录）")
    print(f"  {'模型':<40}{'在售':>4}{'下架':>4}{'检测站':>6}  站点")
    for r in rep["models"][:args.top]:
        print(f"  {r['model']:<40}{r['listed']:>4}{r['gone']:>4}{r['checked']:>6}  "
              f"{'、'.join(r['stations'][:4])}{'…' if len(r['stations']) > 4 else ''}")
    if rep["changes"]:
        print(f"\n【近 {rep['window_hours']} 小时上下架变更】（{len(rep['changes'])} 条）")
        for c in rep["changes"][:args.top]:
            mark = "上架" if c["status"] == "listed" else "下架"
            print(f"  [{mark}] {c['station']:<14}{c['model']:<36}{c['ts'][:19]}")
    else:
        print(f"\n【近 {rep['window_hours']} 小时上下架变更】无")


def cmd_official(args):
    """官方价基准表：--fetch 以 LiteLLM 权威库重建；--verify 交叉核验现有值；
    --list 查看覆盖；--fill 用 getcheapai 补全 LiteLLM 未覆盖的长尾。"""
    conn = connect()
    prices, aliases = load_official_prices()

    if args.fetch:
        if args.refresh or not os.path.exists(LITELLM_CACHE):
            print("下载 LiteLLM 官方定价库…")
            st, body, _, err, _ = http_get(LITELLM_URL, args.timeout, {}, 1, 1.0)
            if st != 200:
                print(f"下载失败：HTTP {st} {err}"); return 1
            with open(LITELLM_CACHE, "wb") as f:
                f.write(body)
            print(f"  已缓存 → {LITELLM_CACHE}")
        prices, _ = rebuild_official_from_litellm(
            conn, official_only=not args.include_resellers)
        meta = {
            "source": "litellm",
            "url": LITELLM_URL,
            "fetched_at": utc_now()[:10],
            "note": ("官方价基准（USD / 1M tokens）。source 分级："
                     "litellm=社区权威定价库(被数千生产应用用于成本核算)；"
                     "manual=已与厂商官方页人工核验；"
                     "getcheapai=第三方聚合佐证(仅供参考，可能非官方直采)。"),
        }
        save_official(meta, prices, aliases)
        by_src = {}
        for v in prices.values():
            by_src[v.get("source", "?")] = by_src.get(v.get("source", "?"), 0) + 1
        n_fp = sum(1 for v in prices.values() if v.get("first_party", True))
        n_rs = sum(1 for v in prices.values()
                   if v.get("source") == "litellm" and not v.get("first_party", True))
        print(f"已以 LiteLLM 重建官方价：共 {len(prices)} 条，来源分布 {by_src}")
        print(f"其中 litellm 条目第一方 {len(prices) - n_rs - by_src.get('manual', 0)} 个，"
              f"转售商兜底 {n_rs} 个"
              + ("（仅官方模式，转售商报价已剔除）" if not args.include_resellers else ""))
        if n_rs and args.include_resellers:
            resellers = sorted({(v.get("litellm_key") or "").split("/")[0]
                                for v in prices.values()
                                if v.get("source") == "litellm"
                                and not v.get("first_party", True)
                                and v.get("litellm_key")})
            print(f"  转售商 provider: {', '.join(resellers[:12])}（加 --official-only 可剔除）")
        return 0

    if args.verify:
        # 首选：厂商 ground-truth 交叉核验（LiteLLM vs 人工核验样本）
        if os.path.exists(GROUND_TRUTH_PATH):
            with open(GROUND_TRUTH_PATH, encoding="utf-8") as f:
                gt = json.load(f)
            gprices = gt.get("prices", {})
            print(f"=== 用厂商 ground-truth 核验官方价基准"
                  f"（{len(gprices)} 个旗舰，差异 >1% 标红）===")
            bad = 0
            for k, gv in sorted(gprices.items()):
                cur = lookup_official((prices, aliases), k)
                if not cur:
                    print(f"  ✗ {k:<22} 基准表缺失该旗舰（GT {gv['input']}/{gv['output']}）")
                    bad += 1
                    continue
                # GT 与基准的 USD 侧比对：$ 侧优先 OpenRouter，cny 档按汇率折算
                g_cny = gv.get("currency") == "cny"
                g_in = gv["input"] / MARKET_FX if g_cny else gv["input"]
                g_out = gv["output"] / MARKET_FX if g_cny else gv["output"]
                b_cny = cur.get("currency") == "cny"
                if cur.get("usd_input"):
                    b_in, b_out = cur["usd_input"], cur.get("usd_output") or 0
                else:
                    b_in = (cur.get("input") or 0) / MARKET_FX if b_cny else (cur.get("input") or 0)
                    b_out = (cur.get("output") or 0) / MARKET_FX if b_cny else (cur.get("output") or 0)
                di = abs(b_in - g_in) / max(1e-9, g_in)
                do = abs(b_out - g_out) / max(1e-9, g_out)
                if di > 0.01 or do > 0.01:
                    print(f"  ⚠ {k:<22} 基准 {cur.get('input')}/{cur.get('output')}"
                          f"  GT {gv['input']}/{gv['output']}  [{gv.get('source', '')}]")
                    bad += 1
                # 缓存价：GT 有而基准缺失/不一致也提示
                for field, label in (("cache_read", "缓存命中"), ("cache_write", "缓存写入")):
                    if gv.get(field) is None:
                        continue
                    cv = cur.get(field)
                    if cv is None:
                        print(f"  · {k:<22} {label}价基准缺失（GT {gv[field]}）")
                    elif abs(cv - gv[field]) / max(1e-9, gv[field]) > 0.01:
                        print(f"  ⚠ {k:<22} {label} 基准 {cv}  GT {gv[field]}")
                        bad += 1
            # 缓存价结构核验：命中应在输入的 2%~60%，写入应在 100%~160%
            struct = 0
            for k, v in sorted(prices.items()):
                cr, cw, inp = v.get("cache_read"), v.get("cache_write"), v.get("input") or 0
                if cr is not None and inp > 0 and not (0.02 <= cr / inp <= 0.6):
                    print(f"  ⚠ {k:<22} 缓存命中/输入 = {cr / inp:.1%}（超出 2%~60% 常规区间）")
                    struct += 1
                if cw is not None and inp > 0 and cw > 0 and not (1.0 <= cw / inp <= 1.6):
                    print(f"  ⚠ {k:<22} 缓存写入/输入 = {cw / inp:.1%}（超出 100%~160% 常规区间）")
                    struct += 1
            print(f"\n价差异常 {bad} 个，缓存结构异常 {struct} 个"
                  f"（GT: {GROUND_TRUTH_PATH}；可修正 GT 或复核后 --fetch 重建）")
            return 0

        # 兜底：无 GT 时退回 LiteLLM 自检（仅对非 litellm 来源条目）
        if not os.path.exists(LITELLM_CACHE):
            print("需先 --fetch 下载 LiteLLM 库"); return 1
        idx = _litellm_index(load_litellm())
        print(f"=== 用 LiteLLM 核验现有官方价（差异 >1% 视为异常）===")
        bad = 0
        for k, v in sorted(prices.items(), key=lambda kv: (kv[1].get("source", ""), kv[0])):
            if v.get("source") == "litellm":
                continue
            m = match_litellm(k, idx)
            if not m:
                continue
            di = abs((v.get("input", 0) or 0) - m["input"]) / max(1e-9, m["input"])
            do = abs((v.get("output", 0) or 0) - m["output"]) / max(1e-9, m["output"])
            if di > 0.01 or do > 0.01:
                print(f"  ⚠ {k:<28} 现表 {v.get('input')}/{v.get('output')}"
                      f"  LiteLLM {m['input']}/{m['output']}  (来源 {v.get('source')})")
                bad += 1
        print(f"异常项 {bad} 个（以上需人工复核；可 --fetch 用 litellm 覆盖）")
        return 0

    if args.fill:
        if not os.path.exists(LITELLM_CACHE):
            print("需先 --fetch 下载 LiteLLM 库"); return 1
        idx = _litellm_index(load_litellm())
        targets = sorted(db_model_aggs(conn))
        targets = [t for t in targets
                   if not lookup_official((prices, aliases), t)][: args.top]
        print(f"为 {len(targets)} 个 LiteLLM 未覆盖模型尝试 getcheapai 二级补全…")
        added = {}
        for canon in targets:
            url = f"https://www.getcheapai.com/zh-cn/{canon}"
            st, body, _, err, _ = http_get(url, args.timeout, {}, 1, 1.0)
            if st != 200:
                continue
            html = body.decode("utf-8", "replace")
            if "officialInputUsd" not in html:
                continue
            m_in = re.search(r'\\"officialInputUsd\\":([0-9.]+)', html)
            m_out = re.search(r'\\"officialOutputUsd\\":([0-9.]+)', html)
            if not m_in:
                continue
            prices[canon] = {"input": float(m_in.group(1)),
                             "output": float(m_out.group(1)) if m_out else None,
                             "source": "getcheapai"}
            added[canon] = prices[canon]
            time.sleep(args.delay)
        if added:
            save_official({"note": "官方价基准（USD/1M tokens）", "updated": utc_now()[:10]},
                          prices, aliases)
            print(f"已用 getcheapai 补全 {len(added)} 个 → {OFFICIAL_PATH}")
        else:
            print("未获取到新的官方价（页面结构变化或网络受限）")
        return 0

    if args.fetch_cn:
        # 清掉库中已不存在的过期键（如快照后缀模型归一化合并前的旧条目）
        aggs = db_model_aggs(conn)
        stale = [k for k in prices if k not in aggs]
        for k in stale:
            prices.pop(k, None)
        if stale:
            print(f"已清理过期基准键 {len(stale)} 个（模型已归一化合并或下架）")
        n, changed = merge_cn_official(prices, args.timeout)
        print(f"国内渠道抓取完成，命中库内模型并按来源优先级替换 {n} 个：")
        for k, i, o, src in changed[: args.top]:
            print(f"  {k:<26} ¥{i}/¥{o}  (来源 {src})")
        if changed:
            try:
                with open(OFFICIAL_PATH, encoding="utf-8") as f:
                    meta = json.load(f).get("_meta", {})
            except Exception:
                meta = {}
            meta["cn_updated"] = utc_now()[:10]
            meta["cn_note"] = ("¥ 列（国内基准）优先级: 人工核验 > 千问/DeepSeek/GLM 官方网 > 硅基流动/百炼 > 厂商官网 > litellm(×汇率)；"
                               "$ 列（国际参考）= OpenRouter")
            save_official(meta, prices, aliases)
            print(f"\n已写入 {OFFICIAL_PATH}（国内 ¥ 档 currency=cny，折扣按真实人民币换算）")
        else:
            print("无可更新（页面结构变化或网络受限）")
        return 0

    # --list（默认）：覆盖情况 + 来源分布
    rows = conn.execute("""
        SELECT DISTINCT mp.model_agg AS agg FROM model_prices mp JOIN snapshots k
        ON mp.snapshot_id=k.id WHERE k.ok=1 AND k.id IN
        (SELECT MAX(id) FROM snapshots WHERE ok=1 GROUP BY station_id)""").fetchall()
    aggs = [r["agg"] for r in rows]
    covered = [a for a in aggs if lookup_official((prices, aliases), a)]
    by_src = {}
    for a in covered:
        v = lookup_official((prices, aliases), a)
        by_src[v.get("source", "?")] = by_src.get(v.get("source", "?"), 0) + 1
    print(f"聚合模型 {len(aggs)} 个，已有官方价基准 {len(covered)} 个"
          f"（覆盖 {len(covered) * 100 // max(1, len(aggs))}%）")
    print(f"来源分布：{by_src}")
    # litellm 条目的渠道成色：第一方 vs 转售商兜底
    n_rs = sum(1 for a in covered
               if (prices.get(a) or {}).get("source") == "litellm"
               and not (prices.get(a) or {}).get("first_party", True))
    n_lit = by_src.get("litellm", 0)
    if n_lit:
        print(f"其中 litellm {n_lit} 条：第一方 {n_lit - n_rs}，"
              f"转售商兜底 {n_rs}（默认仅取官方，--include-resellers 可放开转售商兜底）")
    missing = sorted(set(aggs) - set(covered))
    print(f"缺失 {len(missing)} 个（按收录站数排序，前 {args.top}）：")
    n_st = {r[0]: r[1] for r in conn.execute("""
        SELECT mp.model_agg, COUNT(DISTINCT k.station_id) n FROM model_prices mp
        JOIN snapshots k ON mp.snapshot_id=k.id WHERE k.ok=1 AND k.id IN
        (SELECT MAX(id) FROM snapshots WHERE ok=1 GROUP BY station_id)
        GROUP BY mp.model_agg""")}
    for a in sorted(missing, key=lambda x: -n_st.get(x, 0))[: args.top]:
        print(f"  {a:<40} {n_st.get(a, 0)} 站")
    return 0


def cmd_add(args):
    with open(STATIONS_PATH, encoding="utf-8") as f:
        stations = json.load(f)
    stations = [s for s in stations if s["name"] != args.name]
    entry = {"name": args.name,
             "domains": [normalize_domain(d) for d in args.domain],
             "notes": args.notes or ""}
    if args.api_base:
        entry["api_base"] = normalize_domain(args.api_base)
    if args.cookie:
        entry["cookie"] = args.cookie
    if args.api_key:
        entry["api_key"] = args.api_key
    if args.mode:
        entry["mode"] = args.mode
    if getattr(args, "models_path", None):
        entry["models_path"] = args.models_path
    if args.recharge_rate is not None:
        entry["recharge_rate"] = args.recharge_rate
    stations.append(entry)
    with open(STATIONS_PATH, "w", encoding="utf-8") as f:
        json.dump(stations, f, ensure_ascii=False, indent=2)
    print(f"已添加站点 {args.name} → {entry['domains']}"
          + (f"（API: {entry['api_base']}）" if args.api_base else "")
          + (f"（模式: {args.mode}）" if args.mode else "")
          + ("（带 Cookie）" if args.cookie else "")
          + ("（带 API key）" if args.api_key else "")
          + (f"（充值比例 ¥{args.recharge_rate}/$，手动覆盖）" if args.recharge_rate is not None else ""))
    return 0


def cmd_remove(args):
    with open(STATIONS_PATH, encoding="utf-8") as f:
        stations = json.load(f)
    stations = [s for s in stations if s["name"] != args.name]
    with open(STATIONS_PATH, "w", encoding="utf-8") as f:
        json.dump(stations, f, ensure_ascii=False, indent=2)
    conn = connect()
    conn.execute("UPDATE stations SET enabled=0 WHERE name=?", (args.name,))
    conn.commit()
    print(f"已移除站点 {args.name}（历史数据保留）")
    return 0


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    p = argparse.ArgumentParser(description="中转站模型分组与价格抓取工具")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("scrape", help="抓取全部（或指定）站点")
    sp.add_argument("--station", action="append", help="只抓指定站点名，可多次")
    sp.add_argument("--concurrency", type=int, default=8)
    sp.add_argument("--timeout", type=float, default=15)
    sp.add_argument("--force", action="store_true", help="忽略 pricing_version 强制落库")
    sp.add_argument("--include-offline", action="store_true",
                    help="重试被跳过的站（离线/需Cookie/接口不支持）")
    sp.add_argument("--retries", type=int, default=2,
                    help="网络类错误的重试次数（401/404 不重试），默认 2")
    sp.add_argument("--backoff", type=float, default=1.0,
                    help="重试退避基数秒，指数增长，默认 1.0")
    sp.set_defaults(func=cmd_scrape)

    sp = sub.add_parser("watch", help="定时任务：可达性探测 + 价格抓取 + 调价监控（Ctrl+C 停止，超阈值可推送）")
    sp.add_argument("--interval", type=int, default=43200,
                    help="价格抓取间隔秒数，默认 43200（12 小时）")
    sp.add_argument("--probe-interval", type=int, default=600,
                    help="站点可达性探测间隔秒数，默认 600（10 分钟）")
    sp.add_argument("--jitter", type=float, default=5.0, help="随机抖动秒数，默认 5")
    sp.add_argument("--model-probe-interval", type=int, default=1800,
                    help="模型级真实调用探测间隔秒数，默认 1800（30 分钟），0=关闭")
    sp.add_argument("--probe-model", action="append",
                    help="模型级探测的模型名，可多次；不填则自动取站数最多的前 N 个语言模型")
    sp.add_argument("--probe-model-top", type=int, default=8,
                    help="自动选模型时取前 N 个（按在售站数），默认 8")
    sp.add_argument("--max-runs", type=int, default=0, help="最多跑几轮，0=不限")
    sp.add_argument("--threshold", type=float, default=5.0,
                    help="推送/摘要的调价告警阈值 %%，默认 5（落库阈值仍为 1%%）")
    sp.add_argument("--push", default=os.environ.get("RELAY_PUSH_URL", ""),
                    help="通知渠道 URL：ntfy.sh 主题 / Server酱 sendkey / 通用 webhook；"
                         "也可用环境变量 RELAY_PUSH_URL")
    sp.add_argument("--top", type=int, default=10, help="每轮摘要最多列几条，默认 10")
    sp.add_argument("--no-backup", action="store_true", dest="no_backup",
                    help="本轮结束后不自动备份数据库（默认每轮备份一次）")
    for a in [("--station", dict(action="append", help="只抓指定站点名")),
              ("--concurrency", dict(type=int, default=8)),
              ("--timeout", dict(type=float, default=15)),
              ("--force", dict(action="store_true")),
              ("--include-offline", dict(action="store_true")),
              ("--retries", dict(type=int, default=2)),
              ("--backoff", dict(type=float, default=1.0))]:
        sp.add_argument(a[0], **a[1])
    sp.set_defaults(func=cmd_watch)

    sp = sub.add_parser("backup", help="备份数据库（SQLite 在线备份，不锁写）")
    sp.add_argument("--dir", default="backups", help="备份目录，默认 backups/")
    sp.add_argument("--keep", type=int, default=14, help="保留最近 N 份，默认 14")
    sp.add_argument("--vacuum", action="store_true", help="备份前先 VACUUM 压缩（会临时锁库）")
    sp.set_defaults(func=cmd_backup)

    sp = sub.add_parser("probe", help="可用性探针（HTTP 可达性 / 真实 API 调用）")
    sp.add_argument("--station", action="append", help="只探测指定站点")
    sp.add_argument("--model", help="真实调用探针使用的模型名（需站点配了 api_key）")
    sp.add_argument("--concurrency", type=int, default=6)
    sp.add_argument("--timeout", type=float, default=20)
    sp.set_defaults(func=cmd_probe)

    sp = sub.add_parser("bench", help="性能实测（api-bench：TTFT/吞吐/缓存命中/滚动缓存；真实扣费）")
    sp.add_argument("--station", required=True, help="站点名（stations.json 中需已配 api_key）")
    sp.add_argument("--model", help="被测模型（默认从该站最新快照挑便宜档）")
    sp.add_argument("--protocol", choices=("messages", "responses"), default="messages",
                    help="messages=Claude /v1/messages；responses=OpenAI /v1/responses")
    sp.add_argument("--profile", choices=("generic", "claude-code"), default="generic",
                    help="claude-code=按 Claude Code 请求形态发包（含专用头/system 块）")
    sp.add_argument("--quick", action="store_true",
                    help="省额度画像：6 请求/并发2/缓存8发/滚动6轮，约 1~2 分钟")
    sp.add_argument("--concurrency", type=int, default=10)
    sp.add_argument("--requests", type=int, default=50)
    sp.add_argument("--cache-samples", type=int, default=40)
    sp.add_argument("--cache-interval", type=int, default=8)
    sp.add_argument("--roll-turns", type=int, default=12)
    sp.add_argument("--html", help="HTML 报告输出路径（默认写 bench_out/ 临时文件）")
    sp.add_argument("--timeout", type=int, default=180, help="单请求超时秒数")
    sp.set_defaults(func=cmd_bench)

    sp = sub.add_parser("official", help="官方价基准表（LiteLLM 权威源重建 / 核验 / 补全）")
    sp.add_argument("--fetch", action="store_true", help="以 LiteLLM 社区定价库重建官方价")
    sp.add_argument("--include-resellers", action="store_true", dest="include_resellers",
                    help="允许 LiteLLM 转售商报价兜底（默认仅取第一方官方报价）")
    sp.add_argument("--verify", action="store_true",
                    help="用厂商 ground-truth(official_ground_truth.json) 核验：价差标红 + 缓存价结构检查")
    sp.add_argument("--refresh", action="store_true", help="强制重新下载 LiteLLM 库")
    sp.add_argument("--list", action="store_true", help="查看覆盖率与来源分布（默认）")
    sp.add_argument("--fill", action="store_true", help="用 getcheapai 补全 LiteLLM 未覆盖的长尾")
    sp.add_argument("--fetch-cn", action="store_true",
                    help="抓国内渠道 ¥ 价作基准（DeepSeek/GLM 官方网 > 硅基流动 platform_cn > 厂商官网 vendor_cn > litellm×汇率）")
    sp.add_argument("--top", type=int, default=30, help="最多处理/显示多少个模型")
    sp.add_argument("--delay", type=float, default=0.8, help="抓取间隔，礼貌访问")
    sp.add_argument("--timeout", type=float, default=20)
    sp.set_defaults(func=cmd_official)

    sub.add_parser("list", help="查看站点与最近抓取状态").set_defaults(func=cmd_list)

    sp = sub.add_parser("export", help="导出详细数据")
    sp.add_argument("--format", choices=["csv", "json", "summary", "both", "all"], default="csv",
                    help="csv=逐行明细 / json=嵌套结构 / summary=每模型最低价汇总 / both=csv+json / all=全部")
    sp.add_argument("--out", default=f"relay_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    sp.add_argument("--model", help="只导出指定模型")
    sp.set_defaults(func=cmd_export)

    sp = sub.add_parser("cheapest", help="每模型最低价排行")
    sp.add_argument("--model", help="只看指定模型")
    sp.add_argument("--top", type=int, default=30)
    sp.set_defaults(func=cmd_cheapest)

    sp = sub.add_parser("history", help="模型价格历史")
    sp.add_argument("--model", required=True)
    sp.add_argument("--station", help="只看指定站点")
    sp.add_argument("--group", help="只看指定分组")
    sp.add_argument("--format", choices=["csv", "json"], default="csv")
    sp.add_argument("--out")
    sp.set_defaults(func=cmd_history)

    sub.add_parser("models", help="模型清单").set_defaults(func=cmd_models)

    sp = sub.add_parser("compare", help="同模型跨站比价视图")
    sp.add_argument("--model", help="指定模型（规范名）")
    sp.add_argument("--top", type=int, default=5, help="未指定模型时，按收录站数取前 N 个")
    sp.add_argument("--rows", type=int, default=8, help="每个模型展示多少行")
    sp.add_argument("--out", help="导出 CSV 路径")
    sp.set_defaults(func=cmd_compare)

    sp = sub.add_parser("alerts", help="价格异动告警")
    sp.add_argument("--threshold", type=float, default=5.0, help="变动百分比阈值，默认 5")
    sp.add_argument("--hours", type=float, default=0, help="只看最近 N 小时，0=不限")
    sp.add_argument("--min-price", type=float, default=0, help="只看单价高于该值的")
    sp.add_argument("--top", type=int, default=30, help="最多显示条数")
    sp.add_argument("--out", help="导出 CSV 路径")
    sp.set_defaults(func=cmd_alerts)

    sp = sub.add_parser("availability", help="模型可用性检测（上架/下架比对 + 真实调用探针）")
    sp.add_argument("--refresh", action="store_true",
                    help="抓各站当前在售模型清单，与库内快照比对（上架/下架）")
    sp.add_argument("--probe", action="store_true",
                    help="对配置了 API key 的站发真实请求，判定模型是否可调用")
    sp.add_argument("--report", action="store_true", help="输出可用性报告（默认）")
    sp.add_argument("--station", action="append", help="只处理指定站点名，可多次")
    sp.add_argument("--model", help="--probe 用的模型名")
    sp.add_argument("--hours", type=float, default=24, help="报告中最近多少小时内的上下架变更")
    sp.add_argument("--top", type=int, default=30, help="报告最多显示条数")
    sp.add_argument("--concurrency", type=int, default=8)
    sp.add_argument("--timeout", type=float, default=15)
    sp.add_argument("--json", action="store_true", help="报告以 JSON 输出")
    sp.set_defaults(func=cmd_availability)

    sp = sub.add_parser("serve", help="启动本地 Web 看板")
    sp.add_argument("--host", default="127.0.0.1")
    sp.add_argument("--port", type=int, default=8080)
    sp.add_argument("--db", help="数据库路径，默认 relay_data.db")
    sp.set_defaults(func=cmd_serve)

    sp = sub.add_parser("add", help="添加站点")
    sp.add_argument("--name", required=True)
    sp.add_argument("--domain", action="append", required=True)
    sp.add_argument("--api-base", help="自定义 API 基地址（SPA 站的 api 子域）")
    sp.add_argument("--cookie", help="登录态 Cookie（应对 requireAuth 的站）")
    sp.add_argument("--api-key", help="该站 API key（可用性探针真实调用用）")
    sp.add_argument("--mode", choices=["newapi", "direct", "html_rsc", "model_plaza",
                                       "public_models", "models_feed"],
                    help="抓取模式：newapi=倍率接口 / direct=直定价接口 / html_rsc=SSR内嵌数据 / "
                         "model_plaza=模型广场接口（/api/v1/model-plaza，按每 token 单价）/ "
                         "public_models=公开直定价接口（/api/public/models，$/M，无分组）/ "
                         "models_feed=自建站公开价目 JSON（配合 --models-path，价格已含分组折扣）")
    sp.add_argument("--models-path", dest="models_path",
                    help="models_feed 模式的接口路径，如 /top-api-tools-api/models")
    sp.add_argument("--recharge-rate", type=float, dest="recharge_rate",
                    help="手动指定充值比例（¥ per $ 额度）：该站 /api/status 不返回 price 时用，"
                         "如 1.0=1元1美元额度；留空则用抓取值")
    sp.add_argument("--notes", default="")
    sp.set_defaults(func=cmd_add)

    sp = sub.add_parser("remove", help="移除站点（保留历史）")
    sp.add_argument("--name", required=True)
    sp.set_defaults(func=cmd_remove)

    args = p.parse_args()
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
