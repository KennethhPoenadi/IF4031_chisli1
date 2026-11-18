#!/usr/bin/env bash
# P2 kriteria 4: jam satu node bergeser. Tester Agent menggeser jam (skenario latihan clock-skew) SAMBIL tes `once`
# mengukur laju permintaan ke mock dan kesinambungan ID. Bila dua instance sama-sama berhasil mengambil dan menyimpan
# pada periode yang sama, laju melonjak melebihi satu instance dan muncul ID ganda: tes gagal.
#
# Lingkungan sama dengan p2.py (KUBECONFIG, AGG_URL, AGG_INTERNAL_KEY, AUDIT_TOKEN, POLL_INTERVAL_SECONDS, OUT_DIR).
set -uo pipefail
cd "$(dirname "$0")/../.."
before="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "== memicu skenario Tester Agent clock-skew (jam digeser pada node yang dipilih agen)"
kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice clock-skew 2>&1 | sed 's/^/   /'

echo "== mengukur selama jam bergeser: laju permintaan ke mock dan kesinambungan ID (90 dtk)"
python3 -u tests/p2/p2.py once --seconds 90
rc=$?

echo
echo "== log Tester Agent (rantai MAC, tidak dapat dipalsukan) sejak $before"
kubectl logs -n tester-agent -l app=tester-agent --tail=40 --prefix 2>&1 | grep '"action"' |
  python3 -c "
import sys,json,re
for l in sys.stdin:
    m=re.search(r'\[pod/([^/]+)/agent\] (\{.*\})',l)
    if not m: continue
    try: d=json.loads(m.group(2))
    except ValueError: continue
    print('   ',m.group(1),'seq',d['seq'],d['action'])" | tail -8
exit $rc
