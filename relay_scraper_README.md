# relay_scraper — 中转站模型分组与价格抓取工具

按《中转站数据采集方法论》实现的 P1~P2 级工具：定时抓取 one-api/new-api 系中转站公开的
`/api/pricing` + `/api/status`，按倍率单位折算绝对价格，SQLite 时序存储，支持导出详细数据。

- 零第三方依赖（Python 3.8+ 标准库）
- 并发抓取、多域名故障转移、`pricing_version` 变更检测、失败隔离
- 导出 **38,507 行**级别的详细数据（站点 × 模型 × 分组，含全倍率与 $/¥ 折算价）

## 快速开始

```bash
python relay_scraper.py scrape                  # 抓取全部站点（内置 15 站）
python relay_scraper.py list                    # 查看各站状态（正常/需登录Cookie/接口不支持/离线）
python relay_scraper.py export --format all     # 一次导出 明细CSV + 嵌套JSON + 汇总表
python relay_scraper.py cheapest --top 20       # 每模型最低价（含较官方价折扣）
python relay_scraper.py history --model claude-opus-4-8
python relay_scraper.py models                  # 规范名模型清单（* = 有官方价基准）
```

## 核心能力

- **官方价统一人民币口径**：所有官方价列只保留 ¥（输入/输出），国外模型按市场汇率
  `RELAY_MARKET_FX`（默认 7.2）折算。国内模型 = 国内渠道实价（硅基流动/百炼 > 厂商官网）；
  国外模型 = 条目自身官方 $ 价（litellm 第一方 / GT 人工核验）× 汇率。
  ⚠ 不能用 OpenRouter 价当官方基准：OR `/models` 返回的是**集市快照价**（多 provider 取一，
  实测 kimi-k3 官方 $3/15 vs OR $0.304/11.936，差 10 倍）——OR 价只作 $ 参考存库
  （`usd_input`/`usd_source`），供 `--verify` 比对，不参与折扣。
  明细/汇总「官方来源」列标注成色：`siliconflow`（国内直采）、`litellm×7.2`/`manual×7.2`
  （官方$折算）、`openrouter×7.2`（仅集市参考，无更好来源时）。
  官方价文件为**原子写**（临时文件+os.replace），防止中途异常留下 0 字节损坏文件。
- **价格页权威组价（自定义组倍率补全）**：`/api/pricing` 的 `group_ratio` 只含部分组，但
  模型 `enable_groups` 常含大量**自定义组**（`svip`/`赠送`/`openai-first-topup`/`国产套餐`/
  `AWS稳`…）——这些组的倍率 API **根本不返回**，按默认 1.0 算会漏掉真正最便宜的组、
  最低价系统性偏高。已在 `stations.json` 对 **Modelflare** 开 `page_prices: true`：
  `save_snapshot` 时额外抓 `/pricing/{model}` 取权威按组价（输入/输出 $/M）覆盖 ratio 推算，
  直接对齐站点自己的价格工具（实测 claude-haiku-5-5 的 claude-first-topup $0.003/$0.015 等）。
  其余 one-api 类站（Kapibala/LinksApi/RunApi/Xcode/RelayApiApp）是纯 SPA 壳、API 又不完整，
  **自定义组价格在公开途径取不到**（见下方「已知局限」）。
  - **页面价与倍率的关系**：实测页面价 = 基准价 × 分组倍率（424 组次零例外），所以
    `model_group_rows` 校验通过时返回「基准价 + 真实倍率」，面板才显示 `openai-award 0.03`
    而不是 1，实付仍等于页面价；对不上（倍率未公布/站点另有调价）才退回「页面绝对价 + 倍率 1」。
  - **同组多档取基准档**（`page_tier_rank`）：阶梯模型的页面把**长上下文档**标成 `Standard`、
    把基础档标成 `Context length <= 272000`（HTML 里是 `&lt;=`）。早期按字面「Standard 优先」
    会把 2~5 倍的长上下文价当成实付价（gpt-6-luna openai-award 0.015 → 正确 0.0075），
    现改为：上下文阈值最小档 > Standard > 其它。
  - **分组倍率补全**（`group_definitions`）：`group_ratio` 只列公开组，首充活动组
    （`claude-first-topup 0.03` / `openai-first-topup 0.015`）的倍率在 `group_definitions`，
    仅补「确实出现在某模型 enable_groups 里的 id」（避免 `media`/`grok`/`claude-tmp` 这类别名）。
- **分组可用性过滤（最低价正确性）**：`/api/pricing` 的 `group_ratio` 列出全站所有分组，
  但每个模型只在 `enable_groups` 列出的分组可买。早期版本把**不可用分组**也纳入最低价，
  导致 97.8% 的汇总行最低价系统性偏低（实测 o1-pro 在 RunApi 显示 $27.65 的 codex 组价，
  真实可买价是 $225 的 Plus 组，差 8.1 倍）。现规则：① `enable_groups` 为空 = 全分组可用
  （new-api 语义，修复了 Cubence/SssAiApi 共 32 个模型被误排除）；② 汇总最低价、compare、
  看板、cheapest 只统计「模型在该分组可用=是」的行。明细 CSV 仍保留全部分组行并带
  「模型在该分组可用」列供审计。站点自己的价格工具同样只显示可用分组——两者现已对齐。
- **充值比例（¥/$）抓取**：倍率制站的真实成本 = 标价$ × 站点充值比例。比例从各站 `/api/status`
  的 `price` 字段抓取并随快照落库（`snapshots.exchange_rate`），当前 11 站中 9 站自动获取
  （实测 ¥0.30/0.80/1.00/6.75 各种形态）。抓不到的站（`/api/status` 不返回 price，如
  SssAiApi）可**手动覆盖**：`stations.json` 对应条目加 `"recharge_rate": 1.0`（或
  `add --name X --domain ... --recharge-rate 1.0` 重新添加），明细列「充值比例来源」标注
  `status`/`手动`；无值时才按市场汇率 7.2 兜底。
  **Modelflare 例外**：其 `/api/status` 的 `price`/`usd_exchange_rate` 均返回 1（one-api 内部
  "美元汇率"设为虚拟值），但**真实充值按人民币对美元汇率**计价——已手动覆盖
  `"recharge_rate": 6.72`（2026-10-19 PBOC 中间价 6.7208）。汇率每日浮动，此固定值需定期更新。
- **折扣口径（真实人民币对齐）**：国内大模型官方以 ¥ 计价、多数中转站的"美元额度"实为
  虚拟单位（实测充值比例 0.3~6.75 ¥/$），因此「较官方价%」按**真实人民币**计算：
  `实付¥ = 实付$ × 站点充值比例`（站点未公布比例时按市场汇率）、
  `官方¥` 优先用国内渠道 ¥ 档，否则 `官方USD × 市场汇率 RELAY_MARKET_FX（默认 7.2，可环境变量覆盖）`。
  修正示例：Xcode 站 glm-5-3 从 -186%（误判比官方贵）修正为 **+50%**（¥4 vs 硅基流动官方¥8）。
  展示上 `$` 与 `¥` 双列并列：实付输入/输出(¥/M)、官方输入(¥/M)、官方计价货币。
- **官方价基准（权威渠道，可溯源 + 双源核验）**：以 **LiteLLM 社区定价库**为权威源，
  **默认只取第一方报价**：同一聚合键有多个 provider 条目时（`zai/glm-5.3` vs
  `azure/glm-5.3` vs `coralbricks/glm-5.3`），按「裸名 > 模型厂商自营（家族→厂商映射）>
  github_copilot > 转售商」仲裁，`first_party=false` 的转售商条目**默认剔除**
  （实测 199→122 条，38% 曾被 azure/aihubmix/dashscope 等转售价冒充）；兜底匹配同样先试厂商
  provider。确需转售商兜底时 `--fetch --include-resellers`。无第一方价的模型若在
  OpenRouter 有档则按 `source=openrouter` 建档（明示非官方价，¥列按汇率折算），保证折扣列不空。
  （`official --fetch` 下载 `model_prices_and_context_window.json` 并重建 `official_prices.json`，
  覆盖 434 个聚合模型中的 203 个、约 46%）。每条带 `source` 分级：
  `litellm`（社区权威）/ `manual`（已与厂商官方页人工核验）/
  `getcheapai`（第三方聚合佐证，仅 `--fill` 兜底长尾，仅供参考）。
  **裸名冲突优先厂商自营**：同一模型有多个 provider 条目时（`zai/glm-5.3` vs
  `coralbricks/glm-5.3`），按「模型家族→厂商」映射选第一方，避免把 azure/dashscope
  转售价当官方价（曾致 glm-5.3 误取 1.12 而非厂商 1.40）。缓存命中/写入价一并入库。
  **双源核验**：`official_ground_truth.json` 收录 17 个头部旗舰的人工核验价
  （逐条来源页/日期/置信度），`official --verify` 据此标红漂移项，并对全部条目做
  缓存价结构检查（命中应≈输入 2%~60%、写入≈100%~160%）——已借此发现 LiteLLM 的
  claude-opus-5-5/sonnet-5-5/gpt-6.1-sol 缓存命中价(5%)与厂商惯例(10%)不符、
  deepseek-v4-1-flash 缓存写入 900% 等录入错误。
  `official --list` 查看覆盖率与来源分布。汇总的 `较官方价(%)`（正数=更便宜）
  按聚合键匹配官方价；可用 `aliases` 把站方非标准命名映射到已有基准。
- **双层归一化**（508 个原始名 → 422 个显示规范名 → 419 个聚合键）：
  - `model_canon` **显示名**：保留厂商分隔符写法（`gpt-5.6-sol`、`gemini-3.8-flash`）
  - `model_agg` **聚合键**：抹平 `.` 与 `-` 差异，用于跨站聚合/查表
    （因此 `claude-haiku-4.5` ≡ `claude-haiku-4-5` 会被合并计数）
  剥离供应商前缀（`openai/`、`Pro/BAAI/`）、日期快照后缀（完整年份 `20251001`/`2025-10-01`、
  短写 `YYMMDD 250615`、`YYMM 2509`、`MMDD 0731/0813/0806`，月份日历校验防误伤版本号，
  剥序长先短后防 `260215→2602`）、噪声后缀（`-latest`/`-preview`/`-exp`）、
  冒号标记（`:free`/`:thinking`）。
  **推理档位不丢弃**——`thinking`/`high`/`low`/`medium`/`max` 单独存入 `模型变体` 列，
  既能在规范名上聚合（站数、最低价），又保留档位间的价格差异。
  `-max` 有歧义（`qwen-max` 是型号、`claude-opus-4-7-max` 才是档位），仅在紧跟版本号时才当档位。
  跨命名习惯的等价（如 `deepseek-chat` ≡ `deepseek-v3`）在 `canon_aliases.json` 里配置。
- **模型家族**：厂商 + 系列（`claude-sonnet` / `gpt-6` / `gemini-3.8`），共 168 个家族，用于同系列聚合。
- **失败原因分类**：401/403 → `需登录Cookie`；404 → `接口不支持`；DNS 失败 → `离线`；
  超时 → `网络抖动`。非活跃站自动跳过，`scrape --include-offline` 强制重试。
- **时序快照 + 变更检测**：`pricing_version` 未变不落库；价格历史随时可查。
- **流式导出**：CSV 逐行写出，大结果集不占内存。
- **阶梯计价与样例成本**：`tiers`（上下文阈值倍率）纳入计算，新增 `阶梯计价明细` 列；
  并给出 `样例请求成本($)`（默认 100K 输入 + 10K 输出一次请求的实际花费，会命中阶梯档）。
- **重试退避**：网络类错误（超时/连接失败/429/5xx）自动指数退避重试（默认 2 次）；
  401/403/404 是确定性结果，不重试。
- **连接复用**：requests 按线程持有 Session 连接池（实测延迟明显下降，如 Portix 8.3s → 2.9s）；
  无 requests 时自动退回 urllib。
- **定时调度**：`watch --interval 300` 前台循环，或交给系统计划任务（见下）。
- **可用性探针**：`probe` 无 key 时测 HTTP 可达性，配了 `api_key` 后发真实最小请求测可用性与首字节延迟，
  结果存 `probes` 表。
- **官方价补全**：`official --fill` 从公开比价页抓取缺失模型的官方定价写入基准表（本次覆盖率 7% → 21%）。
- **自动迁移**：新增字段自动补列并回填历史数据，升级无需删库（按 `SCHEMA_VERSION` 只迁移一次）。
- **跨站比价**：`compare` 列出同一模型在所有站点/分组的价格并给出价差倍数
  （实测 claude-sonnet-5-5：8 站 200 个分组，最低 $0.02/M vs 最高 $36/M，**价差 1800×**）。
- **价格异动告警**：每次落库自动与上一快照比对，超 1% 的变动写入 `price_changes`，
  `alerts --threshold 5` 查看涨降价，抓取时同步提示"⚠ 价格异动 N 处"。
- **持续监控**：`watch` 每轮抓取后输出调价摘要（按站点×模型去重，多分组只留最大变动档），
  超过 `--threshold`（默认 5%）即推送：`--push` 支持 ntfy.sh 主题、Server酱、
  通用 webhook（JSON `{title, content}`），也可用环境变量 `RELAY_PUSH_URL`；
  抓取轮与 `scrape` 完全同源（`run_scrape_round`）。
- **Web 看板**：`serve --port 8080` 起本地服务，模型搜索 + **站点 / 模型族 / 系列三级筛选**
  （族→系列级联：选 `claude` 后系列框只剩 claude-*）+   搜索框下方 **语言 / 图片 / 视频 / 音频
  四个点击型大类筛选**（再点一次取消，按钮上带当前筛选下的条数，与其它下拉联动；其中
  「语言模型」按 `model_whitelist.json` 白名单过滤，其余三个按模型类型）+ 排序 + SVG 走势图 +
  **单模型跨站对比**（每行「对比」进 比价表）+ **导出当前筛选 CSV**（含 BOM，Excel 直开）；
  接口 `/api/summary`（含 在售站点）、`/api/history?model=X`、`/api/compare?model=X`、`/api/stations`。
- **点模型名进对比页**：表格里的模型名是链接（`#cmp:<模型规范名>`，可刷新/分享/浏览器后退），
  点开即在该模型各站报价表里定位（含 站点 / 公司 / 分组 / 分辨率 / 计费 / 倍率 / 输入 / 输出 /
  $秒 / $次 / ¥ / 价格来源，带「返回列表」）；行内其它位置点击仍是走势图。
  **表头可点击排序**：价格列（输入/输出/$秒/$次/¥/倍率）按数值排，文本列按字典序排；
  再次点击反向；量纲不符的「-」恒排末尾（不随升降序翻到前面）；当前排序列的最低价行标绿。
  视频对比按分辨率分块（便宜档在前、块内按站价升序），每站每档只取最低价分组。
  **非 token 模型**在汇总与单站视图里单独占行，由「计费」列区分
  `按量 / 按秒 / 按次`：视频走 `$/秒`（按生成秒数累加）、图片·音乐走 `$/次`，
  token 口径列显示 `-`；汇总表按「按量 → 按秒 → 按次」分段、段内各自升序；
  这类模型无 $/M 走势，历史图给出提示。

## 命令一览

| 命令 | 说明 | 关键参数 |
|---|---|---|
| `scrape` | 抓取全部/指定站点 | `--station 名`（可多次）、`--concurrency 8`、`--timeout 15`、`--retries 2`、`--backoff 1.0`、`--force`（忽略版本号强制落库）、`--include-offline`（重试被跳过的站） |
| `list` | 站点状态、模型数、最近抓取时间与失败原因 | |
| `export` | 导出详细数据 | `--format csv\|json\|summary\|both\|all`、`--out 路径`、`--model 模型名` |
| `cheapest` | 每模型最低实付输入价排行（含折扣/站数） | `--top N`、`--model` |
| `history` | 单模型价格历史（时序） | `--model`、`--station`、`--group`、`--format csv\|json` |
| `models` | 规范名模型清单（`*` = 有官方价基准） | |
| `compare` | 同模型跨站比价视图 | `--model X`、`--top 5`（按站数取前N个模型）、`--rows 8`、`--out file.csv` |
| `alerts` | 价格异动告警 | `--threshold 5`、`--hours N`、`--min-price`、`--out file.csv` |
| `serve` | 本地 Web 看板 | `--port 8080`、`--host 127.0.0.1`、`--db` |
| `watch` | 定时抓取 + 调价监控（超阈值推送，Ctrl+C 停止） | `--interval 300`（秒）、`--threshold 5`（告警%）、`--push URL`（ntfy/Server酱/webhook）、`--top 10`、`--max-runs N`（测试用） |
| `probe` | 可用性探针：HTTP 可达性 / 真实 API 调用 | `--model`（配了 api_key 才走真实调用）、`--station`、`--concurrency` |
| `official` | 官方价基准表（LiteLLM 仅官方重建/国内渠道/双源核验/补全） | `--fetch`（重建，默认仅第一方）、`--include-resellers`（放开转售商兜底）、`--fetch-cn`（国内官网+硅基流动 ¥ 价 + OpenRouter $ 档）、`--verify`（GT 核验+缓存结构检查）、`--list`（覆盖率）、`--fill`（getcheapai 兜底）、`--refresh` |
| `add` / `remove` | 增删站点（改 `stations.json`，remove 保留历史） | `--name`、`--domain`（可多次）、`--api-base`、`--cookie`、`--mode newapi\|direct\|html_rsc` |

环境变量：`RELAY_DB`（数据库路径，默认 `relay_data.db`）、`RELAY_STATIONS`（站点清单路径）。
`RELAY_HOME_URL`（serve 用，默认 `https://aipaihangbang.cn/relay`）：口径声明条的跳转目标——
2026-10-28 起看板作为 RelayLens 的模型比价页接入，本地预览时可设为
`http://127.0.0.1:5173/index.html` 指回本地站点。
`RELAY_SITE_BASE`（serve 用，默认从 RELAY_HOME_URL 推导）：页顶全站导航条里各页链接的站点根地址。
`RELAY_SELF_URL`（serve 用，默认 `/`）：导航条里「模型比价」当前项的链接（看板被反代到子路径时设置）。

## 导出的详细数据

**CSV（50 列，UTF-8 BOM，Excel 直开）**——每行 = 站点 × 模型 × 分组 ×（视频再 × 分辨率/档位）：

| 列组 | 内容 |
|---|---|
| 标识 | 站点、域名、数据时间、模型规范名、原始模型名、模型变体(thinking/high/low…)、模型家族、**公司**（出品厂商，见 `vendor_map.json`）、计费方式(per_token/per_second/per_call)、模型类型(text/image/video/audio)、核心模型(是否在语言模型白名单)、厂商ID |
| 分组 | 分组名、分组倍率（站点公布的真实倍率）、模型在该分组是否可用、价格来源（价格页/接口/倍率推算） |
| 原始倍率 | model_ratio、completion_ratio、cache_ratio、create_cache_ratio、model_price、tiers |
| 按次 | `按次价格($/次)` 内部额度币种 + `按次价格(¥/次)` 真实人民币（仅 per_call 行有值：图片、音乐等真正每次计价） |
| 按秒 | `分辨率` + `按秒价格($/秒)` 内部 + `按秒价格(¥/秒)` 真实人民币（仅 per_second 行有值：**每个分辨率/档位一行**，按价格升序；档位可带修饰前缀如 `1080p · pro · Audio`） |
| 计价倍率 | `计价倍率`（命中 `station_pricing_rules.json` 时非空，已乘进上面两个单价列） |
| 标价($/M) | 输入/输出/缓存读/缓存写（= 倍率 × $2） |
| 实付($/M) | 输入/输出/**缓存读/缓存写**（= 标价 × 分组倍率；官网价格页展示的就是这一口径） |
| 实付(¥/M) | 输入/输出（= 实付$ × 充值比例） |
| 其他 | 充值比例、支持接口、抓取延迟、system_name |

**JSON**——按 站点 → 模型 → 分组 三层嵌套，含 `ratios`、`base_prices_usd_per_m`、
`tiers`、`supported_endpoints`、每分组的 `effective_usd_per_m` / `effective_rmb_per_m`，
适合程序消费。

## 媒体模型的「按尺寸 / 按分辨率」价（新增 2026-10-20）

`billing_mode=image_size` / `video_resolution` 的站点（Aixoras 11 张图 + 6 个视频）以前抓不到价：
它们的 `model_ratio` 是**占位值 37.5**（= $75/M），而真实价在另外两个字段里——
`image_size_price`（每张图 × 尺寸，如 `[{size:"1024x1024", price:0.03}]`）与
`video_resolution_price`（每秒 × 分辨率，如 `{"480p":0.4,"4k":4.5}`）。

处理（`parse_pricing` + `_size_levels` + `model_group_rows`）：

- 两个字段分别存入 `model_prices.image_size_price` / `video_resolution_price`（SCHEMA_VERSION 9）；
- **每档一行**：视频按分辨率、图片按尺寸展开，`分辨率` 列放档位标签，实付 = 档位价 × 分组倍率
  （站点前端源码 `p = d => i(d * u)`，`u` = `min(group_ratio)`，实测 IMAGE-VIP ×1.5 → 0.045）；
- 档位排序：视频按价格升序、图片按像素面积升序；
- 占位倍率清零，避免 `$75/M` 的假 token 价；`价格来源=接口`。

## 出品公司判定（`vendor_map.json`）

明细行与汇总行都带 `公司` 列，看板「全部公司」下拉直接用它（不再前端猜前缀）。
判定顺序：`exact`（精确规范名） > `prefix`（前缀，表内靠前者优先） > `contains` > `default`
（默认 `其他/未识别`，不会被静默错归类）。规范名由 `canonical_model()` 生成（小写、抹平
日期与分隔符差异）。`normalize` 用于把站点给的英文厂商名统一成中文口径
（`Alibaba Cloud→阿里云`、`ByteDance→字节跳动`、`Zhipu AI→智谱AI`、`Moonshot AI→月之暗面`）。

当前 437 个模型覆盖 27 家：OpenAI 76、阿里云 73、Google 42、字节跳动 39、快手 25、
Meta 21、MiniMax 20、Anthropic 19、xAI 18、Midjourney 14、智谱AI 13、爱诗科技 10、
站点自有工具 10、DeepSeek 9、Black Forest Labs 8、生数科技 6、Jina AI 5、小米 5、
月之暗面 5、TypeSafe 4、讯飞 4、Suno 4、Cohere 3、网易有道 1、BAAI 1、腾讯 1、
其他/未识别 1（`music-3.0`，站点未公布厂商）。
新增站点/模型后若出现未识别，只需往 `vendor_map.json` 的 `exact`/`prefix` 补一行。

## 计价口径（与源码对齐）

```
倍率 1 = $0.002/1K tokens = $2/1M tokens        （new-api QuotaPerUnit = 500,000，$1 = 50万额度）
输入价($/M)  = model_ratio × 2
输出价($/M)  = model_ratio × completion_ratio × 2
缓存读($/M)  = 输入价 × cache_ratio ；缓存写 = 输入价 × create_cache_ratio
实付价($/M)  = 上述 × group_ratio（分组折扣；缓存读写同样要乘）
人民币(¥)    = 美元 × 充值比例（/api/status 的 price 字段，"1元=$1" = ×1）
按秒价($/秒) = model_price（**视频模型**：站点把单价定义到"每秒"，实付 = 生成秒数 ×
               单价 × 分辨率/步数倍率；原值直读，不乘分组倍率、无 ¥/M 与折扣）
按次价($/次) = model_price（**图片 / 音乐**等真正每次计价；同样原值直读）
               三者量纲不同，不同轴，不可混排比价
```

**币种口径（易错点）**：`$/M`、`$/秒`、`$/次` 是「站点内部额度美元」，不是真实美元。
真实成本只在 `¥` 列（= 内部$ × 充值比例）。`price=1` 的站（如 WanmoAPI 万模API）是
**「¥1 = $1 额度」**盘，所以它的 `$` 与 `¥` 数值相同，官网价格页也用
`quota_display_type=CNY` + `custom_currency_exchange_rate=1` 直接把这些数字前面标成 `¥`。

**因此**：汇总表/`cheapest`/`compare`/看板的**排序与最低价一律按真实人民币**（`_detail_real_price`），
否则 ¥1=$1（充值比例 1）与 ¥7.3=$1 的站混在一起按内部$ 排，会差出 7 倍以上（实测同一模型
`gpt-6-luna`：按内部$ 冠军是 Modelflare $0.0075，按真实人民币冠军其实是 Xcode ¥0.015）。
跨站横比只认 `¥/M`、`¥/秒`、`¥/次`；`内部$` 列仅用于核对站点自己的标价。

### 站点私有倍率（`station_pricing_rules.json`）

少数站的 `/api/pricing` 里 `model_price` 只是**内部额度单价**，挂牌价实际是
`model_price × 分组倍率 × multiplier`，而 `multiplier` 硬编码在该站前端 bundle 里、
公开 API 根本不返回。已核实的倍率记在 `station_pricing_rules.json`，抓取/展示时自动套用，
明细 CSV 的 `计价倍率` 列可审计；**未列入的模型保持原值**（宁可少算，不瞎猜）。

| 站点 | 匹配规则 | 倍率 | 单位 | 核对依据 |
|---|---|---|---|---|
| Portix | `happyhorse-1.0*` / `happyhorse-1.1*` | 90 | `$/秒` | 前端 `quota_type=4` 分支 `r=90` + `timeUnit=s`；实测 `0.0014706 × 0.715 × 90 = 0.09463` ≈ 站点显示 `$0.0946/秒` |

新增规则只需在 JSON 里加一条 `{prefix:[...], multiplier:N, billing:"per_second"|"per_call"}`
（或 `exact` 精确名），无需改代码；核对方法见文件内 `source` 字段（前端 bundle 地址）。

## 站点管理

`stations.json` 是站点清单的唯一事实源（每次运行同步进 DB），`stations.enabled` 决定该站是否参与
抓取与展示：

- `enabled=0` 的站**不抓取、也不出现在汇总/对比/导出/看板**（`latest_snapshots` 已按 enabled 过滤），
  但历史快照仍保留在库里，改回 `enabled=1` 即可恢复。
- 抓取模式（`mode`）：`newapi`（/api/pricing 倍率）/ `direct`（直定价子域）/
  `html_rsc`（Next.js RSC）/ **`model_plaza`**（`/api/v1/model-plaza`，UUAPI 用）。

`model_plaza` 与 new-api 的三个差异（见 `parse_model_plaza`）：价格是**每单个 token 单价**
（2e-06 = $2/1M，故 `model_ratio = 单价 × 500000`）；分组倍率在 `group.rate_multiplier`（模型价格本身不含）；
`billing_mode=video` 时 `intervals[].tier_label` 就是 480p/720p/…、`per_request_price` 是每秒价，
`billing_mode=per_request/image` 且有 `per_request_price` 时按次计价。

**`billing_expr`（tiered_expr）定价**：部分站的 `model_ratio` 是**占位值**（MemeCode 20 个全是
`37.5`，等价 $75/M；ApiNebula 5 个、WanmoAPI 2 个同样），真实价格只在
`billing_expr` 里，形如
`tier("standard", p * 5 + cr * 0.5 + cc * 6.25 + cc1h * 10 + c * 25)`。
`parse_billing_expr()` 解析出各变量的系数，**系数本身就是 $/1M 基准价**（系数 × 分组倍率 = 官网展示价，
实测 MemeCode 20/20、WanmoAPI `gpt-5.6-luna` 0.2×1.5=0.3 全对）。变量：`p`=输入、`c`=输出、
`cr`=缓存读、`cc`=缓存写(5m)、`cc1h`=缓存写(1h)。多档时取**基准档**：标签含 `standard` 优先，
否则取输入系数最小的一档（WanmoAPI 把 `fast`/`priority` 档排在前面，直接取第一档会错 2 倍）。

**倍率未公布的分组**（如 MemeCode `Claude-Curosr`/`vip`、Modelflare `codex-pro-root`/`企业国模专用`）：
倍率按 1.0 兜底会被当成「最便宜」，因此这类行的 `实付` 列**留空**（`价格来源=倍率未知`），
且不参与汇总最低价——宁可空着也不展示算不出来的价。

当前保留（2026-10-20 用户指定 6 站）：

| 站点 | 域名 | 模式 | 充值比例 | 说明 |
|---|---|---|---|---|
| Xcode | xcode.best | newapi | status=1 | ¥1=$1 |
| ApiNebula | apinebula.ai | newapi | status=1 | ¥1=$1 |
| UUAPI | uuapi.io | model_plaza | 手动 1 | 站点明示「充值¥1=$1」 |
| WanmoAPI | www.wanmoapi.com | newapi | status=1 | ¥1=$1，官网价格页标 ¥ |
| Aixoras | aixoras.com | newapi | 手动 1 | status price=1/usd=1 |
| MemeCode | memecode.de | newapi | 手动 1 | 价格在 `billing_expr` 里；标准视图(¥)/充值视图($) 差 7.3 → 内部$即人民币，`usd_exchange_rate` 只是显示汇率 |

## 模型白名单（`model_whitelist.json`）

看板上「语言模型」按钮不看关键词分类，而是按 `model_whitelist.json` 里的清单过滤（用户 2026-10-20
指定）。匹配规则是**规范名前缀**：`gpt-5.6` 同时覆盖 `gpt-5.6-cyber`，
`qwen3-coder` 覆盖 `qwen3-coder-plus` / `qwen3-coder-next`，所以不必把每个变体都列一遍。

- 增删模型只改这个 JSON，无需动代码；`/api/summary` 与单站接口都会带上 `核心模型` 列（`是`/空），
  CSV 里同名列可用来做数据核对。
- 白名单为空（或文件缺失）时，「语言模型」按钮退化为「全部语言类模型」，不会把列表清空。
- 列表里的 `gpt-image-2` / `gpt-image-2.5-*` / `gpt-audio` 同样会出现在「图片模型」「音频模型」里；
  若只想让纯语言模型进「语言模型」，把这几条删掉即可。
- 目前库里尚未收录、只做了预留的条目：`gpt-5.6-cyber`、`qwen-coder`、`glm-5-code`。

## 站点管理

`stations.json` 是唯一事实源（每次运行自动同步进 DB）。三种抓取模式：

| mode | 适用场景 | 抓取方式 |
|---|---|---|
| `newapi`（默认） | one-api/new-api 系 | `GET {域名}/api/pricing`（倍率格式） |
| `direct` | 自建 SPA 后端（API 在 `api.<域名>` 子域） | `GET {api_base}/api/config/models`（`input_price` 直定价 $/M，分组来自 `channels`） |
| `html_rsc` | Next.js SSR 站 | `GET {域名}/pricing`，解析 HTML 内嵌 RSC 的 `model_name`/`input_price`/`output_price` |

```bash
# 普通站
python relay_scraper.py add --name MyRelay --domain relay.example.com
# SPA 站：API 在 api 子域，直定价格式
python relay_scraper.py add --name SssAiApi --domain sssaiapi.com --api-base https://api.sssaiapi.com --mode direct
# 定价接口需登录的站：注册后从浏览器 DevTools 复制 Cookie
python relay_scraper.py add --name ByteCat --domain bytecatcode.org --cookie "session=xxxxx"
# Next.js SSR 站
python relay_scraper.py add --name Cubence --domain cubence.ai --mode html_rsc
```

**连续失败 ≥3 次的站自动标记离线**（`list` 显示"离线"，`scrape` 默认跳过，`--include-offline` 强制重试；成功一次即恢复）。

## 生产部署（定时 + 探针）

```bash
# 前台循环（适合临时跑）
python relay_scraper.py watch --interval 1800 --retries 2
```

Windows 计划任务（每 30 分钟）：

```powershell
schtasks /create /tn "relay-price-scrape" /sc minute /mo 30 `
  /tr "python C:\path\to\relay_scraper.py scrape --retries 2" /f
```

Linux / macOS cron：

```cron
*/30 * * * * cd /path/to && python3 relay_scraper.py scrape --retries 2 >> scrape.log 2>&1
```

配合探针（每 15 分钟测一次可达性；配了 `api_key` 的站会做真实调用）：

```bash
python relay_scraper.py add --name MyRelay --domain relay.example.com --api-key "sk-xxxxx"
python relay_scraper.py probe --model gpt-5-nano
```

## 失败站处置手册（实测诊断，2026-10-17）

| 失败现象 | 根因 | 解法 |
|---|---|---|
| `/api/pricing` → 404 | 非 new-api 路由（自建 SPA / 其他程序） | ① DevTools 看 XHR 找真实接口；② 常见模式：API 在 `api.<域名>` 子域（扒 JS 找 baseURL 拼接函数可确认）→ 配 `--api-base --mode direct`；③ Next.js 站直接解析 `/pricing` HTML 内嵌 RSC → `--mode html_rsc` |
| `/api/pricing` → 401 | 站方关闭了公开定价（`/api/status` 里 `pricing.requireAuth=true`） | 注册账号 → 浏览器登录 → DevTools → Application/Cookies 复制 `session` → `add --cookie "session=..."` |
| 返回 HTML（JSON 解析失败） | SPA fallback 或 Cloudflare 拦截 | 带浏览器 UA + `Accept: application/json`；仍不行则按 SPA 站处理 |
| DNS 解析失败 / 连接超时 | 域名过期、跑路、被墙 | 标记离线；找备用域名（`add --domain` 可配多个，按顺序故障转移） |

> 已实测解决：SssAiApi（api 子域 + 直定价）、Cubence（RSC 内嵌）；ByteCat 需用户提供 Cookie；UUAPI/HapiOpen/Codex2Api 待 DevTools 抓包确认接口。

## 实现要点（对应方法论）

1. **发现**：内置清单 + `add` 命令扩展（友情链接遍历、aff 反查可后续自动化）
2. **采集**：仅用零鉴权公开接口 `/api/pricing` + `/api/status`；浏览器 UA（Cloudflare 会拦
   `Python-urllib` 默认 UA）；仅读环境变量代理，忽略 Windows 注册表代理（本机代理软件
   未运行时会 10061 连接被拒）
3. **调度**：并发默认 8；`pricing_version` 不变则跳过落库；无版本号的站每次全量快照
4. **存储**：stations / snapshots / model_prices / group_ratios 四表，全量 timeseries
5. **归一化**：模型名小写去空白为 `model_key`；分组做并集展开（`enable_groups` ∪
   `group_ratio`），并标记"模型在该分组可用"；分组仅站内可比
6. **边界**：非 token 计费（`model_price`/`quota_type=1`）再分两档——
   **视频 → `per_second`（$/秒）**、**图片·音乐 → `per_call`（$/次）**，
   判定优先用站点自带的 `model_type`/`tags`（如 Portix 的 `视频`/`绘画`），
   缺失时按模型名关键词兜底（`video/i2v/t2v` 压过 `image`）；两者都不混入 $/M，
   汇总/对比/看板中单独成段、各按自己的单价排序；
   阶梯计价参与计算（阈值倍率对整次请求生效）；无汇率站点仍导出美元价

## 合规提醒

仅抓取站点自行公开的接口，请保持低频（建议 ≥5 分钟间隔），遵守各站 robots 与使用条款；
数据仅供比价参考，收录不代表推荐。官方价基准抓取自公开比价页，请自行核对后再用于决策。

## 本次实测产出（2026-10-17）

- 15 站尝试 → **11 站成功**（508 个原始名 → 437 显示名 → 434 聚合键、168 个家族、
  38,755 行 模型×分组 明细、286 行汇总、35 列）
- 已解决 2 个原失败站：SssAiApi（api 子域直定价，23 模型）、Cubence（RSC 解析，18 模型）
- 剩余 4 站：ByteCat（需 Cookie）、UUAPI/HapiOpen/Codex2Api（待抓包），已自动分类并跳过；
  HTTP 探针显示这 4 站站点本身均可达（15/15），问题在接口路由而非站点离线
- 官方价基准：`official --fetch` 以 LiteLLM 权威库重建，覆盖 22% → 44%（96 → 203 个模型可算折扣，且剔除 getcheapai 派生噪声值）
- 连接池生效：Portix 抓取延迟 8.3s → 2.9s
- `final_v4.csv` / `.json` / `.summary`：明细 + 嵌套 + 每模型最低价汇总（含阶梯计价与样例成本）
