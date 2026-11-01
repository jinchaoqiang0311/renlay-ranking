# serve-guarded.ps1 —— 看板守护式重启(演进:根治双进程抢绑 8899 与悄悄死掉)
# 用法:powershell -ExecutionPolicy Bypass -File serve-guarded.ps1 [-Port 8899] [-Token <令牌>]
# 行为:① 按命令行匹配停掉旧 serve 进程(CIM;Get-NetTCPConnection 在本机漏报,HANDOFF 坑#3)
#       ② 以推荐环境变量启动新 serve(pythonw 隐藏窗口)③ 轮询 /healthz 最多 10 秒
param([int]$Port = 8899, [string]$Token = "")
if (-not $Port) { $Port = 8899 }
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$procs = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "relay_scraper.py serve" }
foreach ($p in $procs) { Write-Host "停止旧看板进程 PID=$($p.ProcessId)"; Stop-Process -Id $p.ProcessId -Force }
Start-Sleep -Milliseconds 800
$env:RELAY_HOME_URL = "http://127.0.0.1:5173/index.html"
$env:RELAY_SELF_URL = "/price/"
if ($Token) { $env:RL_DASH_TOKEN = $Token } else { Remove-Item Env:RL_DASH_TOKEN -ErrorAction SilentlyContinue }
Start-Process -FilePath "pythonw" -ArgumentList "relay_scraper.py", "serve", "--port", "$Port" -WorkingDirectory $root -WindowStyle Hidden
$alive = $false
for ($i = 0; $i -lt 10; $i++) {
  Start-Sleep -Seconds 1
  try { $r = Invoke-WebRequest -Uri "http://127.0.0.1:$Port/healthz" -UseBasicParsing -TimeoutSec 2; if ($r.StatusCode -eq 200) { $alive = $true; break } } catch { }
}
if ($alive) {
  Write-Host "OK 看板已启动并健康:http://127.0.0.1:$Port/healthz"
} else {
  Write-Host "FAIL 启动后 10 秒内 /healthz 未就绪;手动前台运行:python relay_scraper.py serve --port $Port"
  exit 1
}