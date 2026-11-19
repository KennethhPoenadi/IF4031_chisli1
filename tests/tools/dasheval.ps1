param([string]$Expr,[int]$Port=9333)
# Menjalankan satu ekspresi JavaScript pada tab dashboard yang sudah terbuka (mis. menggulir ke kartu tertentu sebelum ditangkap).
$page = (Invoke-RestMethod "http://127.0.0.1:$Port/json") | Where-Object { $_.type -eq "page" -and $_.url -like "*dashboard*" } | Select-Object -First 1
if (-not $page) { Write-Error "tab dashboard tidak ditemukan"; exit 1 }
$ws = New-Object System.Net.WebSockets.ClientWebSocket
$ws.ConnectAsync([Uri]$page.webSocketDebuggerUrl, [Threading.CancellationToken]::None).Wait()
$buf = New-Object byte[] 65536
foreach ($m in @(@{ id = 1; method = "Page.bringToFront" }, @{ id = 2; method = "Runtime.evaluate"; params = @{ expression = $Expr; returnByValue = $true } })) {
  $b = [Text.Encoding]::UTF8.GetBytes(($m | ConvertTo-Json -Compress))
  $ws.SendAsync([ArraySegment[byte]]$b, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, [Threading.CancellationToken]::None).Wait()
  $r = $ws.ReceiveAsync([ArraySegment[byte]]$buf, [Threading.CancellationToken]::None); $r.Wait()
}
Write-Output ([Text.Encoding]::UTF8.GetString($buf, 0, $r.Result.Count))
$ws.Dispose()
