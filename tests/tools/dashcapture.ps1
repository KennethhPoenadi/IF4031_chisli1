param([string]$Out,[int]$Port=9333)
# Menangkap tampilan tab dashboard yang sedang terbuka di Chrome (render asli peramban lewat DevTools Page.captureScreenshot).
$page = (Invoke-RestMethod "http://127.0.0.1:$Port/json") | Where-Object { $_.type -eq "page" -and $_.url -like "*dashboard*" } | Select-Object -First 1
if (-not $page) { Write-Error "tab dashboard tidak ditemukan"; exit 1 }
$ws = New-Object System.Net.WebSockets.ClientWebSocket
$ws.ConnectAsync([Uri]$page.webSocketDebuggerUrl, [Threading.CancellationToken]::None).Wait()
$b = [Text.Encoding]::UTF8.GetBytes('{"id":1,"method":"Page.captureScreenshot","params":{"format":"png"}}')
$ws.SendAsync([ArraySegment[byte]]$b, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, [Threading.CancellationToken]::None).Wait()
$ms = New-Object IO.MemoryStream
$buf = New-Object byte[] 65536
do {
  $r = $ws.ReceiveAsync([ArraySegment[byte]]$buf, [Threading.CancellationToken]::None); $r.Wait()
  $ms.Write($buf, 0, $r.Result.Count)
} while (-not $r.Result.EndOfMessage)
$ws.Dispose()
$json = [Text.Encoding]::UTF8.GetString($ms.ToArray()) | ConvertFrom-Json
[IO.File]::WriteAllBytes($Out, [Convert]::FromBase64String($json.result.data))
Write-Output "ok"
