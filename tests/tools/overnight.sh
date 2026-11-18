#!/usr/bin/env bash
# Menjalankan sisa bukti P4 dan P5 tanpa pengawasan: menunggu satellite-ingestor Ready, lalu P4 (live, crash), P5 (priority, gameday).
# Setiap langkah merekam keluaran asli ke log dan screenshot jendela konsol sungguhan. Hasil: docs/bukti/<p4|p5>/2node/.
#   nohup tests/tools/overnight.sh > /tmp/overnight.log 2>&1 &
set -uo pipefail
cd "$(dirname "$0")/../.."
ROOT="$PWD"
set -a; . "$HOME/.xops/prod.env"; set +a
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s-config}" AGG_URL="${AGG_URL:-http://localhost:8090}" AGG_INTERNAL_KEY="$AGGREGATOR_API_KEY"
export POLL_INTERVAL_SECONDS="$AGGREGATOR_POLL_INTERVAL_SECONDS" PVMBG_URL="${PVMBG_URL:-http://localhost:8091}" SKIP_CLEAN=1 LIVE=1
N=sistem-informasi-kebencanaan-bnpb
eval "$(sed -n '/^wait_clean()/,/^}/p;/^run()/,/^}/p' tests/tools/run_evidence.sh)"
echo "== $(date '+%F %T') menunggu satellite-ingestor Ready (maks 150 menit)"
for _ in $(seq 1 450); do
  r=$(kubectl -n $N get pod -l app.kubernetes.io/name=satellite-ingestor -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null)
  [ "$r" = true ] && break; sleep 20
done
if [ "$r" = true ]; then
  echo "== $(date '+%T') ingestor Ready"
  run p4 live "P4: aliran feed satelit live" python3 -u tests/p4/p4.py live --menit 5
  run p4 crash "P4: pemroses dibunuh, pulih tanpa kehilangan dan ganda" python3 -u tests/p4/p4.py crash
else
  echo "== ingestor tidak Ready dalam 150 menit; P4 dilewati"
fi
echo "== $(date '+%T') P5"
run p5 priority "P5 kriteria 5: Tim Lapangan tetap dilayani saat Media ditolak" python3 -u tests/p5/p5.py priority
sleep 60
run p5 gameday "P5 kriteria 6: game day gabungan tanpa intervensi" python3 -u tests/p5/gameday.py --recovery 240
echo "== $(date '+%F %T') selesai"
