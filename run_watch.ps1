# 看板常驻守护（登录时执行）：确保「价格抓取循环」和「看板服务」各有一个实例在跑。
# 用 pythonw 启动：无控制台窗口，父进程退出后不会被一起带走。
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $dir

function Ensure-Proc($match, $argsList) {
    $hit = Get-CimInstance Win32_Process -Filter "Name='pythonw.exe'" |
           Where-Object { $_.CommandLine -like "*$match*" }
    if (-not $hit) {
        Start-Process pythonw -ArgumentList $argsList -WorkingDirectory $dir | Out-Null
        Write-Output "started: $match"
    } else {
        Write-Output "already running: $match"
    }
}

Ensure-Proc 'relay_scraper.py watch' @('relay_scraper.py', 'watch', '--interval', '43200')
Ensure-Proc 'relay_scraper.py serve' @('relay_scraper.py', 'serve', '--port', '8899')
