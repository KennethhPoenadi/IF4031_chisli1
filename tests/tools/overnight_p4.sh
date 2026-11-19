#!/usr/bin/env bash
# P4 tanpa pengawasan: menunggu satellite-ingestor Ready (maks 6 jam), lalu live dan crash.
set -uo pipefail
cd "$(dirname "$0")/../.."
ROOT="$PWD"
set -a; . "$HOME/.xops/prod.env"; set +a
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s-config}" AGG_URL="${AGG_URL:-http://localhost:8090}" AGG_INTERNAL_KEY="$AGGREGATOR_API_KEY"
export POLL_INTERVAL_SECONDS="$AGGREGATOR_POLL_INTERVAL_SECONDS" PVMBG_URL="${PVMBG_URL:-http://localhost:8091}" SKIP_CLEAN=1 LIVE=1
N=sistem-informasi-kebencanaan-bnpb
eval "$(sed -n '/^wait_clean()/,/^}/p;/^run()/,/^}/p' tests/tools/run_evidence.sh)"
echo "== $(date '+%F %T') menunggu satellite-ingestor Ready (maks 6 jam)"
for _ in $(seq 1 1080); do
  r=$(kubectl -n $N get pod -l app.kubernetes.io/name=satellite-ingestor -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null)
  [ "$r" = true ] && break; sleep 20
done
[ "$r" = true ] || { echo "== ingestor tidak Ready; P4 dilewati"; exit 1; }
echo "== $(date '+%T') ingestor Ready"
run p4 live "P4: aliran feed satelit live" python3 -u tests/p4/p4.py live --menit 5
run p4 crash "P4: pemroses dibunuh, pulih tanpa kehilangan dan ganda" python3 -u tests/p4/p4.py crash
echo "== $(date '+%F %T') P4 selesai"
