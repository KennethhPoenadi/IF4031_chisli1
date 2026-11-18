#!/usr/bin/env bash
# Menjalankan satu putaran loadgen sambil memantau memori node; menghentikan beban bila memori node mana pun >= batas.
#
#   tests/tools/loadgen_probe.sh <VUS> <DURASI> [RAMP] [MAX_NODE_MEM_PERSEN] [OUT_JSON]
#   mis. tests/tools/loadgen_probe.sh 2000 60s 15s 90
#
# Lingkungan: KUBECONFIG, TARGET_IP (bawaan 127.0.0.1, alamat ingress), ~/.xops/prod.env untuk kredensial client.
set -uo pipefail
VUS="${1:?VUS}"; DUR="${2:?durasi}"; RAMP="${3:-15s}"; MAXMEM="${4:-90}"; OUTJ="${5:-/tmp/loadgen-$VUS.json}"
cd "$(dirname "$0")/../.."
set -a; . "$HOME/.xops/prod.env"; set +a
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s-config}"
LG="${LOADGEN:-$HOME/bin/loadgen}"
rm -f "$OUTJ"

restarts() { kubectl -n sistem-informasi-kebencanaan-bnpb get pods --no-headers | awk '{s+=$4} END{print s+0}'; }
r0="$(restarts)"
# Penanda mulai serempak: mesin lain menunggu berkas ini muncul di server berkas (GO_FILE), lalu mulai bersamaan.
[ -n "${GO_FILE:-}" ] && touch "$GO_FILE"
CLIENT_ID="$AUTH_CLIENT_MEDIA_ID" CLIENT_SECRET="$AUTH_CLIENT_MEDIA_SECRET" \
  "$LG" -api "${API_BASE_URL:-http://api.prod.localtest.me}" -auth "${AUTH_BASE_URL:-http://auth.prod.localtest.me}" \
  -ip "${TARGET_IP:-127.0.0.1}" ${LG_PATHS:+-paths "$LG_PATHS"} -vus "$VUS" -duration "$DUR" -ramp "$RAMP" -out "$OUTJ" >"/tmp/loadgen-$VUS.log" 2>&1 &
pid=$!
peak_ctl=0; peak_wrk=0; aborted=""; t0=$(date +%s)
while kill -0 "$pid" 2>/dev/null; do
  sleep 5
  top=$(kubectl top nodes --no-headers 2>/dev/null)
  ctl=$(echo "$top" | awk '$1=="chisli-control"{gsub("%","",$5); print $5}')
  wrk=$(echo "$top" | awk '$1=="chisli-worker-1"{gsub("%","",$5); print $5}')
  [ "${ctl:-0}" -gt "$peak_ctl" ] && peak_ctl=${ctl:-0}
  [ "${wrk:-0}" -gt "$peak_wrk" ] && peak_wrk=${wrk:-0}
  avail=$(awk '/MemAvailable/{print int($2/1024)}' /proc/meminfo)   # memori tersedia sebenarnya di WSL (termasuk cache yang dapat dilepas)
  [ "${peak_min_avail:-99999}" -gt "$avail" ] && peak_min_avail=$avail
  printf '   t+%3ds mem_control=%s%% mem_mac=%s%% wsl_tersedia=%sMB | %s\n' "$(( $(date +%s) - t0 ))" "${ctl:-?}" "${wrk:-?}" "$avail" "$(grep -E '^t\+' /tmp/loadgen-$VUS.log | tail -1 | cut -c1-90)"
  if [ "${ctl:-0}" -ge "$MAXMEM" ] || [ "${wrk:-0}" -ge "$MAXMEM" ]; then aborted="memori node >= ${MAXMEM}%"; kill "$pid" 2>/dev/null; break; fi
  if [ "$avail" -lt "${MIN_AVAIL_MB:-600}" ]; then aborted="memori tersedia WSL ${avail}MB < ${MIN_AVAIL_MB:-600}MB"; kill "$pid" 2>/dev/null; break; fi
done
wait "$pid" 2>/dev/null
r1="$(restarts)"
# Hapus penanda mulai agar uji berikutnya tidak dimulai otomatis oleh mesin lain yang menunggu GO.
[ -n "${GO_FILE:-}" ] && rm -f "$GO_FILE"
grep -E '^==|^   (permintaan|gagal|token)|^HASIL' "/tmp/loadgen-$VUS.log"
echo "   puncak memori: control=${peak_ctl}% mac=${peak_wrk}% wsl_tersedia_min=${peak_min_avail:-?}MB  restart_pod=$(( r1 - r0 ))${aborted:+  DIHENTIKAN: $aborted}"
