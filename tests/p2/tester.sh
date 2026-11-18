#!/usr/bin/env bash
# Menjalankan satu skenario latihan Tester Agent lalu memeriksa dampaknya pada sistem:
#   - laju permintaan ke mock tetap setara satu instance dan ID mock berurutan tanpa duplikat/celah (p2.py once),
#   - keadaan replikasi di Audit API (partial, under_replicated, unit yang dibangun ulang),
#   - log Tester Agent (rantai MAC) sebagai bukti skenario benar-benar berjalan.
#
#   tests/p2/tester.sh <clock-skew|kill-leader|kill-store|partition|wipe-node>
# Lingkungan: KUBECONFIG, AGG_URL, AGG_INTERNAL_KEY, AUDIT_TOKEN, POLL_INTERVAL_SECONDS, OUT_DIR.
set -uo pipefail
cd "$(dirname "$0")/../.."
sc="${1:?usage: tester.sh <skenario>}"

echo "== memicu skenario Tester Agent: $sc"
kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice "$sc" 2>&1 | sed 's/^/   /'

echo "== mengukur selama skenario berjalan: laju permintaan ke mock dan kesinambungan ID (90 dtk)"
python3 -u tests/p2/p2.py once --seconds 90
rc=$?

echo
echo "== keadaan replikasi di Audit API sesudah skenario (menunggu pulih sampai 5 menit)"
for _ in $(seq 1 30); do
  st="$(curl -s -m20 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGG_URL/internal/audit/distribution" |
    python3 -c "import json,sys;d=json.load(sys.stdin);print('partial=%s under_replicated=%s rebuilding=%s tak_terjangkau=%s' % (d['partial'],d['under_replicated'],d['rebuilding_units'],d['unavailable_units']))" 2>/dev/null)"
  case "$st" in "partial=False under_replicated=0 rebuilding=[] tak_terjangkau=[]") break ;; esac
  sleep 10
done
echo "   $st"
case "$st" in "partial=False under_replicated=0 rebuilding=[] tak_terjangkau=[]") ;; *) rc=1 ;; esac

echo
echo "== log Tester Agent (rantai MAC, tidak dapat dipalsukan)"
kubectl logs -n tester-agent -l app=tester-agent --tail=60 --prefix 2>&1 | grep '"action"' |
  python3 -c "
import sys,json,re
for l in sys.stdin:
    m=re.search(r'\[pod/([^/]+)/agent\] (\{.*\})',l)
    if not m: continue
    try: d=json.loads(m.group(2))
    except ValueError: continue
    print('   ',m.group(1),'seq',d['seq'],d['action'])" | tail -8
echo "   pod aplikasi sesudah skenario:"
kubectl -n sistem-informasi-kebencanaan-bnpb get pods --no-headers | awk '{split($2,a,"/"); if (a[1]!=a[2] || $3!="Running") print "   TIDAK SIAP:",$1,$2,$3}' | head -5
exit $rc
