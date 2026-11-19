param([string]$Url,[string]$TokenFile,[int]$Port=9333)
# Membuka dashboard di jendela Chrome sungguhan, mengisi token ke sessionStorage lewat protokol DevTools (sama dengan
# mengetik token lalu menekan Simpan), lalu memuat ulang halaman. Penangkapan jendela dilakukan terpisah oleh cap.ps1.
$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
$profile = Join-Path $env:TEMP "dashshot-profile"
Start-Process $chrome -ArgumentList "--remote-debugging-port=$Port","--user-data-dir=$profile","--no-first-run","--no-default-browser-check","--window-size=1400,1500","--new-window",$Url
$page = $null
for ($i = 0; $i -lt 30 -and -not $page; $i++) {
  Start-Sleep -Milliseconds 500
  try { $page = (Invoke-RestMethod "http://127.0.0.1:$Port/json") | Where-Object { $_.type -eq "page" -and $_.url -like "$Url*" } | Select-Object -First 1 } catch {}
}
if (-not $page) { Write-Error "halaman dashboard tidak ditemukan di DevTools"; exit 1 }
$token = (Get-Content -Raw $TokenFile).Trim()
$ws = New-Object System.Net.WebSockets.ClientWebSocket
$ws.ConnectAsync([Uri]$page.webSocketDebuggerUrl, [Threading.CancellationToken]::None).Wait()
$expr = "sessionStorage.setItem('dash-token', " + ($token | ConvertTo-Json) + "); location.reload(); 'ok'"
$msg = @{ id = 1; method = "Runtime.evaluate"; params = @{ expression = $expr } } | ConvertTo-Json -Compress
$bytes = [Text.Encoding]::UTF8.GetBytes($msg)
$ws.SendAsync([ArraySegment[byte]]$bytes, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, [Threading.CancellationToken]::None).Wait()
$buf = New-Object byte[] 8192
$res = $ws.ReceiveAsync([ArraySegment[byte]]$buf, [Threading.CancellationToken]::None)
$res.Wait()
Start-Sleep -Seconds 2
$front = [Text.Encoding]::UTF8.GetBytes('{"id":2,"method":"Page.bringToFront"}')
$ws.SendAsync([ArraySegment[byte]]$front, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, [Threading.CancellationToken]::None).Wait()
$ws.ReceiveAsync([ArraySegment[byte]]$buf, [Threading.CancellationToken]::None).Wait()
$ws.Dispose()
Write-Output "token dipasang"
