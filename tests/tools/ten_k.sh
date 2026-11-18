#!/usr/bin/env bash
# Sisi laptop dari uji 10.000 VU dua tempat (laptop + VM Colima di Mac), dengan pergantian replika Aggregator 3 -> 5 -> 2.
#
# Mesin lain menunggu berkas GO di server berkas, lalu mulai bersamaan dengan skrip ini.
# Beban penuh berlangsung DURASI detik sesudah ramp-up; replika diganti pada detik 0, 60, dan 110 sejak mulai.
#
#   tests/tools/ten_k.sh [VU_LAPTOP] [DURASI] [RAMP]          bawaan: 5000 150s 30s
# Lingkungan: KUBECONFIG, OUT_DIR (folder bukti).
set -uo pipefail
cd "$(dirname "$0")/../.."
VUS="${1:-5000}"; DUR="${2:-150s}"; RAMP="${3:-30s}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s-config}"
N=sistem-informasi-kebencanaan-bnpb
OUT="${OUT_DIR:-docs/bukti/p2/2node/load10k}"; mkdir -p "$OUT"
rm -f "$OUT/scale-timeline.txt" "$OUT/replicas.txt"

orig_min=$(kubectl -n $N get hpa aggregator -o jsonpath='{.spec.minReplicas}'); orig_max=$(kubectl -n $N get hpa aggregator -o jsonpath='{.spec.maxReplicas}')
restore() { kubectl -n $N patch hpa aggregator --type merge -p "{\"spec\":{\"minReplicas\":$orig_min,\"maxReplicas\":$orig_max}}" >/dev/null 2>&1 && echo "   HPA aggregator dikembalikan ke $orig_min..$orig_max"; }
trap restore EXIT

echo "# 10.000 VU dua tempat: sisi laptop $VUS VU, beban penuh $DUR, ramp $RAMP; replika aggregator 3 -> 5 -> 2"
echo "# menunggu hingga loadgen_probe memberi sinyal GO..."

t0=$(date +%s)
(
  for step in 0:3 60:5 110:2; do
    at=${step%%:*}; n=${step##*:}
    while [ $(( $(date +%s) - t0 )) -lt "$at" ]; do sleep 1; done
    kubectl -n $N patch hpa aggregator --type merge -p "{\"spec\":{\"minReplicas\":$n,\"maxReplicas\":$n}}" >/dev/null
    echo "$(date '+%H:%M:%S') t+$(( $(date +%s) - t0 ))s aggregator -> $n replika" | tee -a "$OUT/scale-timeline.txt"
  done
) &
scaler=$!
( while sleep 5; do printf '%s ' "$(date '+%H:%M:%S')"; kubectl -n $N get deploy aggregator --no-headers 2>/dev/null | awk '{print "siap="$2}'; done ) > "$OUT/replicas.txt" 2>&1 &
watcher=$!

GO_FILE=/tmp/lgserve/GO OUT_JSON="$OUT/loadgen-laptop.json" \
  tests/tools/loadgen_probe.sh "$VUS" "$DUR" "$RAMP" 93 "$OUT/loadgen-laptop.json" 2>&1 | awk '/^   t\+/{c++; if (c%3!=1) next} {print}'
rc=${PIPESTATUS[0]}
wait "$scaler" 2>/dev/null; kill "$watcher" 2>/dev/null
echo "# jumlah replika aggregator selama uji (dicatat tiap 5 dtk): $(awk '{print $2}' "$OUT/replicas.txt" | sort | uniq -c | tr '\n' ' ')"
exit "$rc"
