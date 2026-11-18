#!/usr/bin/env bash
# Menjalankan satu putaran k6 (${K6_SCRIPT:-p2-sustained.js}) sambil memantau memori k6 (RSS) dan beban node.
# Berhenti sendiri (kill k6) bila memori node mana pun melewati batas, supaya cluster tidak ikut mati.
#
#   tests/tools/k6_probe.sh <VUS> <DURASI> [MAX_NODE_MEM_PERSEN]
#   mis. tests/tools/k6_probe.sh 1000 30s 90
#
# Lingkungan: KUBECONFIG, TARGET_IP (bawaan 127.0.0.1), ~/.xops/prod.env untuk kredensial client.
set -uo pipefail
VUS="${1:?VUS}"; DUR="${2:?durasi, mis. 30s}"; MAXMEM="${3:-90}"
cd "$(dirname "$0")/../.."
set -a; . "$HOME/.xops/prod.env"; set +a
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s-config}"
K6="${K6:-$HOME/bin/k6}"
OUT="${OUT:-/tmp/k6probe-$VUS}"; mkdir -p "$OUT"

export API_BASE_URL="${API_BASE_URL:-http://api.prod.localtest.me}" AUTH_BASE_URL="${AUTH_BASE_URL:-http://auth.prod.localtest.me}"
export TARGET_IP="${TARGET_IP:-127.0.0.1}" CLIENT_ID="$AUTH_CLIENT_MEDIA_ID" CLIENT_SECRET="$AUTH_CLIENT_MEDIA_SECRET"
export VUS DURATION="$DUR"

restarts() { kubectl -n sistem-informasi-kebencanaan-bnpb get pods --no-headers | awk '{s+=$4} END{print s+0}'; }
r0="$(restarts)"
( cd tests/k6 && exec "$K6" run --quiet --summary-export "$OUT/summary.json" ${K6_SCRIPT:-p2-sustained.js} >"$OUT/k6.log" 2>&1 ) &
kpid=$!
peak_rss=0; peak_ctl=0; peak_wrk=0; aborted=""
t0=$(date +%s)
while kill -0 "$kpid" 2>/dev/null; do
  sleep 5
  rss=$(ps -o rss= -C k6 2>/dev/null | awk '{s+=$1} END{print int(s/1024)}')
  top=$(kubectl top nodes --no-headers 2>/dev/null)
  ctl=$(echo "$top" | awk '$1=="chisli-control"{gsub("%","",$5); print $5}')
  wrk=$(echo "$top" | awk '$1=="chisli-worker-1"{gsub("%","",$5); print $5}')
  [ "${rss:-0}" -gt "$peak_rss" ] && peak_rss=$rss
  [ "${ctl:-0}" -gt "$peak_ctl" ] && peak_ctl=${ctl:-0}
  [ "${wrk:-0}" -gt "$peak_wrk" ] && peak_wrk=${wrk:-0}
  printf '   t+%3ds k6_rss=%sMB mem_control=%s%% mem_mac=%s%%\n' "$(( $(date +%s) - t0 ))" "${rss:-0}" "${ctl:-?}" "${wrk:-?}"
  if [ "${ctl:-0}" -ge "$MAXMEM" ] || [ "${wrk:-0}" -ge "$MAXMEM" ]; then aborted="memori node >= ${MAXMEM}%"; kill "$kpid" 2>/dev/null; break; fi
done
wait "$kpid" 2>/dev/null
r1="$(restarts)"

python3 - "$OUT/summary.json" "$VUS" "$peak_rss" "$peak_ctl" "$peak_wrk" "$aborted" "$r0" "$r1" <<'EOF'
import json, sys
f, vus, rss, ctl, wrk, aborted, r0, r1 = sys.argv[1:9]
try:
    m = json.load(open(f))["metrics"]
except Exception:
    print(f"VU={vus}: ringkasan k6 tidak ada ({aborted or 'k6 berhenti'}); puncak RSS k6={rss}MB mem_control={ctl}% mem_mac={wrk}%"); sys.exit(2)
g = lambda k, f="count": (m.get(k) or {}).get(f, 0)
req = g("data_requests"); c429 = g("controlled_429s"); ok = sum(g(f"{t}_status_2xx") for t in ("seismic", "volcanic"))
s401 = sum(g(f"{t}_status_401") for t in ("seismic", "volcanic")); s5 = sum(g(f"{t}_status_50{c}") for t in ("seismic", "volcanic") for c in "03")
net = sum(g(f"{t}_network_errors") for t in ("seismic", "volcanic"))
fail = (m.get("non_429_failures") or {}).get("value", 0)
dur = (m.get("http_req_duration") or {})
print(f"VU={vus}: permintaan={req} 2xx={ok} 429={c429} 401={s401} 5xx={s5} galat_jaringan={net} gagal_non429={fail*100:.2f}% "
      f"p95={dur.get('p(95)', 0):.0f}ms rps={g('http_reqs', 'rate'):.0f} token_gagal={g('auth_token_failures')}")
print(f"       puncak: RSS k6={rss}MB (={float(rss)/max(int(vus),1):.2f}MB/VU) mem_control={ctl}% mem_mac={wrk}% restart_pod={int(r1)-int(r0)}"
      + (f"  DIHENTIKAN: {aborted}" if aborted else ""))
EOF
