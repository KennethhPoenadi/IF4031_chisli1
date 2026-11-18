#!/usr/bin/env bash
# Menjalankan satu perintah LIVE di jendela konsol Windows sungguhan, lalu menangkap jendela itu (bukan seluruh desktop)
# menjadi PNG. Keluaran perintah juga disalin ke berkas log. Kode keluar perintah dikembalikan.
#
#   tests/tools/realshot.sh <out.png> <judul-jendela> <log> -- <perintah...>
#
# Syarat: dijalankan dari WSL; jendela konsol akan muncul sebentar di layar Windows dan jangan ditutup sebelum selesai.
# Variabel lingkungan tambahan dibaca dari /tmp/evidence.env bila ada.
set -uo pipefail
OUT="$1"; TITLE="$2"; LOG="$3"; shift 3
[ "${1:-}" = "--" ] && shift
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COLS="${SHOT_COLS:-175}" LINES_N="${SHOT_LINES:-48}"
RUN=/tmp/realshot-run.sh DONE=/tmp/realshot.done RCF=/tmp/realshot.rc
rm -f "$DONE" "$RCF"; mkdir -p "$(dirname "$OUT")" "$(dirname "$LOG")"
printf '%s\n' "${SHOT_CMD:-$*}" > /tmp/realshot.cmd

{
  echo '#!/usr/bin/env bash'
  echo 'set -a; . "$HOME/.xops/prod.env"; [ -f /tmp/evidence.env ] && . /tmp/evidence.env; set +a'
  echo "cd '$ROOT'"
  echo 'export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s-config}"'
  echo "printf '%s\n' '# '\"\$(date '+%Y-%m-%d %H:%M:%S %Z')\"'  host: '\"\$(hostname)\"'  cluster:'"
  echo "kubectl get nodes -o custom-columns=NODE:.metadata.name,STATUS:.status.conditions[-1].type,ARCH:.status.nodeInfo.architecture,IP:.status.addresses[0].address --no-headers | sed 's/^/#   /'"
  # Baris perintah yang tampil di layar: SHOT_CMD bila diberikan (mudah dibaca), kalau tidak argumen apa adanya.
  echo "echo \"\$ \$(cat /tmp/realshot.cmd)\""
  printf '%q ' "$@"; echo "2>&1 | tee '$LOG'"
  echo "echo \"\${PIPESTATUS[0]}\" > $RCF"
  echo "echo; echo \"# selesai: \$(date '+%H:%M:%S')  kode keluar: \$(cat $RCF)\""
  echo "touch $DONE; sleep 45"
} > "$RUN"
chmod +x "$RUN"

powershell.exe -NoProfile -Command "Start-Process conhost.exe -ArgumentList 'cmd.exe /c title $TITLE & mode con cols=$COLS lines=$LINES_N & cd /d C:\\ & wsl.exe -e bash $RUN'" >/dev/null 2>&1

# Tunggu perintah selesai. Gagal cepat bila jendela tidak pernah mulai atau ditutup di tengah jalan.
started=0
for i in $(seq 1 3600); do
  [ -f "$DONE" ] && break
  if pgrep -f "realshot-run.s[h]" >/dev/null; then started=1
  elif [ "$started" = 1 ]; then echo "realshot: jendela ditutup sebelum perintah selesai (jangan tutup jendelanya)" >&2; exit 4
  elif [ "$i" -gt 40 ]; then echo "realshot: jendela konsol tidak pernah mulai menjalankan perintah" >&2; exit 5
  fi
  sleep 1
done
[ -f "$DONE" ] || { echo "realshot: perintah tidak selesai dalam 1 jam" >&2; exit 3; }
WIN_OUT="C:\\Users\\kenne\\AppData\\Local\\Temp\\realshot-$$.png"
cp "$ROOT/tests/tools/cap.ps1" /mnt/c/Users/kenne/AppData/Local/Temp/cap.ps1
cap_out="$(powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\kenne\AppData\Local\Temp\cap.ps1' -Title "$TITLE" -Out "$WIN_OUT" -Wait 1 2>&1 | tr -d '\r' | tail -3)"
case "$cap_out" in ok*) ;; *) echo "realshot: penangkapan jendela gagal: $cap_out" >&2; rm -f "$OUT"; exit 6 ;; esac
cp "/mnt/c/Users/kenne/AppData/Local/Temp/realshot-$$.png" "$OUT" && rm -f "/mnt/c/Users/kenne/AppData/Local/Temp/realshot-$$.png"
echo "screenshot: $OUT ($(( $(stat -c %s "$OUT") / 1024 )) KB)"
exit "$(cat "$RCF" 2>/dev/null || echo 1)"
