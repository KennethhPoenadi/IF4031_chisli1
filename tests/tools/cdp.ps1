param([string]$Action,[string]$Url,[string]$Match,[string]$Expr,[string]$Out,[string]$SecretFile,[int]$Port=9333)
# Alat kecil DevTools Chrome untuk bukti layar:
#   -Action open    -Url U                  membuka tab baru pada Chrome ber-DevTools (port $Port)
#   -Action eval    -Match M -Expr JS       menjalankan JavaScript pada tab yang URL-nya memuat M (SECRET diganti isi -SecretFile)
#   -Action shot    -Match M -Out FILE.png  menangkap tampilan tab itu (Page.captureScreenshot, render asli peramban)
function Tab($m) { (Invoke-RestMethod "http://127.0.0.1:$Port/json") | Where-Object { $_.type -eq "page" -and $_.url -like "*$m*" } | Select-Object -First 1 }
function Call($tab, $method, $params) {
  $ws = New-Object System.Net.WebSockets.ClientWebSocket
  $ws.ConnectAsync([Uri]$tab.webSocketDebuggerUrl, [Threading.CancellationToken]::None).Wait()
  $msg = @{ id = 1; method = $method; params = $params } | ConvertTo-Json -Compress -Depth 5
  $b = [Text.Encoding]::UTF8.GetBytes($msg)
  $ws.SendAsync([ArraySegment[byte]]$b, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, [Threading.CancellationToken]::None).Wait()
  $ms = New-Object IO.MemoryStream; $buf = New-Object byte[] 65536
  do { $r = $ws.ReceiveAsync([ArraySegment[byte]]$buf, [Threading.CancellationToken]::None); $r.Wait(); $ms.Write($buf, 0, $r.Result.Count) } while (-not $r.Result.EndOfMessage)
  $ws.Dispose()
  return ([Text.Encoding]::UTF8.GetString($ms.ToArray()) | ConvertFrom-Json)
}
switch ($Action) {
  "open" { Invoke-RestMethod -Method Put "http://127.0.0.1:$Port/json/new?$Url" | Out-Null; Write-Output "dibuka" }
  "eval" {
    $t = Tab $Match; if (-not $t) { Write-Error "tab '$Match' tidak ditemukan"; exit 1 }
    if ($SecretFile) { $Expr = $Expr.Replace("SECRET", ((Get-Content -Raw $SecretFile).Trim() | ConvertTo-Json)) }
    $res = Call $t "Runtime.evaluate" @{ expression = $Expr; awaitPromise = $true; returnByValue = $true }
    Write-Output ($res.result.result.value)
  }
  "shot" {
    $t = Tab $Match; if (-not $t) { Write-Error "tab '$Match' tidak ditemukan"; exit 1 }
    # Tab di latar belakang tidak digambar (Grafana hanya merender panel yang terlihat): bawa ke depan dan beri waktu render.
    Call $t "Page.bringToFront" @{} | Out-Null
    Start-Sleep -Seconds 8
    $res = Call $t "Page.captureScreenshot" @{ format = "png" }
    [IO.File]::WriteAllBytes($Out, [Convert]::FromBase64String($res.result.data)); Write-Output "ok"
  }
}
