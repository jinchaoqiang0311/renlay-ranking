# 看板挂载契约(主站 ↔ renlay-ranking)

> 两仓库共用的唯一契约页。任何一方改动路径/参数/端口,必须先改这里,两边同步。
> 主站仓库:relaylens · 看板仓库:renlay-ranking(`relay_scraper.py serve`)

## 挂载

| 项 | 值 |
|---|---|
| 看板进程 | `python relay_scraper.py serve --port 8899`(本机 127.0.0.1,pythonw 隐藏窗口) |
| 推荐环境变量 | `RELAY_HOME_URL=http://127.0.0.1:5173/index.html` · `RELAY_SELF_URL=/price/` · 可选 `RL_DASH_TOKEN=<令牌>` |
| 主站挂载点 | 本地:preview-server.js 反代 `/price/*` 与 `/api/*` → 127.0.0.1:8899;生产:nginx 同规则 |
| 主站入口链接 | 全站 `href="/price/"`(导航/页脚/详情卡) |

## 路由契约(看板必须提供)

| 路由 | 语义 | 备注 |
|---|---|---|
| `/` | 看板页(带 `?st=<站名>&kind=text` 筛选参数) | 主站链接不带尾斜线也可达 |
| `/healthz` | 存活探针,200 + JSON `{ok,pid,service}` | 主站探活用;放所有鉴权之前;**任何版本都必须实现** |
| `/api/summary` `/api/station?name=` `/api/history?model=` `/api/compare?model=` `/api/stations` | 数据接口 | 主站不直接消费,看板页自用 |
| `/api/ingest/probe` `/api/ingest/prices`(POST) | 社区上报 | 受令牌门与 IP 限流保护 |

## 鉴权(可选,演进#4 接缝加固)

- 看板环境变量 `RL_DASH_TOKEN` 设置后:除 `/healthz` 外,GET/POST 均要求 `X-RL-Token` 头或 `?token=` 参数
- 主站侧注入:preview-server 设同名环境变量即自动给反代请求加 `X-RL-Token` 头;nginx 用 `proxy_set_header X-RL-Token <令牌>;`
- 不设置 = 行为与历史完全一致(本机使用阶段保持关闭)

## 探活与降级(主站侧)

- rl-common.js 的 `RL.dashProbe()`:`fetch("/price/healthz")` 2.5s 超时,结果写 sessionStorage 缓存 60s
- 探活失败 → 全站 `a[href="/price/"]` 加 `rl-dash-down` 标记(悬停提示"看板维护中"),页面其余功能不受影响

## 单实例锁(看板侧)

- serve 启动前先试连目标端口:能连上 = 已有实例,打印提示后退出(根治 allow_reuse_address 双绑事故)
- 守护式重启:`serve-guarded.ps1`(停旧 PID → 带推荐环境变量启动 → 轮询 /healthz)
