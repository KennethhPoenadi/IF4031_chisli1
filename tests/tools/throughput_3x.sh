#!/usr/bin/env bash
# P2: throughput dengan 3 instance Aggregator harus >= 2,0x throughput dengan 1 instance.
# Beban dan durasi sama pada kedua konfigurasi; throughput = permintaan sukses (2xx) per detik pada beban penuh.
#
#   tests/tools/throughput_3x.sh [VU] [DURASI] [RAMP]      bawaan: 600 40s 10s
set -uo pipefail
cd "$(dirname "$0")/../.."
VUS="${1:-600}"; DUR="${2:-40s}"; RAMP="${3:-10s}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s-config}"
N=sistem-informasi-kebencanaan-bnpb
OUT="${OUT_DIR:-docs/bukti/p2/2node/throughput}"; mkdir -p "$OUT"
orig_min=$(kubectl -n $N get hpa aggregator -o jsonpath='{.spec.minReplicas}'); orig_max=$(kubectl -n $N get hpa aggregator -o jsonpath='{.spec.maxReplicas}')
restore() { kubectl -n $N patch hpa aggregator --type merge -p "{\"spec\":{\"minReplicas\":$orig_min,\"maxReplicas\":$orig_max}}" >/dev/null 2>&1 && echo "# HPA aggregator dikembalikan ke $orig_min..$orig_max"; }
trap restore EXIT

declare -A TP
for n in 1 3; do
  echo "== aggregator dikunci $n replika"
  kubectl -n $N patch hpa aggregator --type merge -p "{\"spec\":{\"minReplicas\":$n,\"maxReplicas\":$n}}" >/dev/null
  for _ in $(seq 1 60); do
    r=$(kubectl -n $N get deploy aggregator -o jsonpath='{.status.readyReplicas}/{.spec.replicas}')
    [ "$r" = "$n/$n" ] && break; sleep 5
  done
  echo "   replika siap: $r; menunggu 20 dtk agar stabil"; sleep 20
  tests/tools/loadgen_probe.sh "$VUS" "$DUR" "$RAMP" 93 "$OUT/loadgen-$n.json" 2>&1 | grep -E '^   (permintaan|gagal)|^HASIL'
  TP[$n]=$(python3 - "$OUT/loadgen-$n.json" "$DUR" "$RAMP" <<'EOF'
import json,sys,re
d=json.load(open(sys.argv[1]))
dur=int(re.sub(r'\D','',sys.argv[2])); print(round(d['status_2xx']/dur,1))
EOF
)
  echo "   throughput sukses (2xx): ${TP[$n]} permintaan/detik"
  sleep 20
done

python3 - "${TP[1]}" "${TP[3]}" "$OUT" "$VUS" <<'EOF'
import json,sys
t1,t3=float(sys.argv[1]),float(sys.argv[2]); out=sys.argv[3]
ratio=t3/t1 if t1 else 0
res={"vus":int(sys.argv[4]),"throughput_1_instance_rps":t1,"throughput_3_instance_rps":t3,"rasio":round(ratio,2),"batas":2.0,"lolos":ratio>=2.0}
json.dump(res,open(out+"/throughput.json","w"),indent=2)
print(f"\n== throughput 1 instance: {t1} rps | 3 instance: {t3} rps | rasio: {ratio:.2f}x (batas 2,0x)")
print("HASIL:", "LOLOS" if res["lolos"] else "GAGAL")
sys.exit(0 if res["lolos"] else 1)
EOF
