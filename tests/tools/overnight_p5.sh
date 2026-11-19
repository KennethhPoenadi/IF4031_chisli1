#!/usr/bin/env bash
# P5 tanpa pengawasan: priority lalu game day gabungan. Tidak bergantung pada satellite-ingestor.
set -uo pipefail
cd "$(dirname "$0")/../.."
ROOT="$PWD"
set -a; . "$HOME/.xops/prod.env"; set +a
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s-config}" AGG_URL="${AGG_URL:-http://localhost:8090}" AGG_INTERNAL_KEY="$AGGREGATOR_API_KEY"
export POLL_INTERVAL_SECONDS="$AGGREGATOR_POLL_INTERVAL_SECONDS" PVMBG_URL="${PVMBG_URL:-http://localhost:8091}" SKIP_CLEAN=1 LIVE=1
eval "$(sed -n '/^wait_clean()/,/^}/p;/^run()/,/^}/p' tests/tools/run_evidence.sh)"
echo "== $(date '+%F %T') P5 priority"
run p5 priority "P5 kriteria 5: Tim Lapangan tetap dilayani saat Media ditolak" python3 -u tests/p5/p5.py priority
sleep 90
echo "== $(date '+%T') P5 gameday"
run p5 gameday "P5 kriteria 6: game day gabungan tanpa intervensi" python3 -u tests/p5/gameday.py --recovery 240
echo "== $(date '+%F %T') P5 selesai"
