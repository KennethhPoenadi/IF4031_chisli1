#!/usr/bin/env bash
# Menampilkan keadaan cluster saat ini untuk bukti: node, pod per node, dan sebaran data per node (Audit API).
# Lingkungan: KUBECONFIG, AGG_URL, AUDIT_TOKEN.
set -uo pipefail
N="${APP_NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
AGG_URL="${AGG_URL:-http://localhost:8090}"

echo "== node"
kubectl get nodes -o custom-columns=NODE:.metadata.name,STATUS:.status.conditions[-1].reason,ARCH:.status.nodeInfo.architecture,IP:.status.addresses[0].address,UMUR:.metadata.creationTimestamp
echo
echo "== pod aplikasi per node"
kubectl -n "$N" get pods -o wide --no-headers | awk '
  { svc=$1; sub(/-[0-9a-f]{8,10}-[a-z0-9]{5}$/, "", svc); sub(/-[0-9]+$/, "", svc)
    node=$7; split($2, rd, "/"); ready=(rd[1]==rd[2] && $3=="Running"); tot[node]++; if (ready) ok[node]++
    cnt[node" "svc]++; svcs[svc]=1; nodes[node]=1 }
  END { for (n in nodes) { line=""
          for (s in svcs) if (cnt[n" "s]) line = line sprintf("%s x%d, ", s, cnt[n" "s])
          printf "%-16s %d pod (%d siap): %s\n", n, tot[n], ok[n], line } }' | sort | sed 's/, $//'
echo
echo "== sebaran data per node (Audit API)"
curl -s -m30 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGG_URL/internal/audit/distribution" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('   partial=%s under_replicated=%s rebuilding=%s tak_terjangkau=%s' % (d['partial'],d['under_replicated'],d['rebuilding_units'],d['unavailable_units']))
for n in d['nodes']: print('   %-16s unit=%-22s salinan=%-7s primary=%-7s replica=%s' % (n['node'],','.join(n['units']),n['copies'],n['primary'],n['replica']))
for u in d['units']: print('   %-8s di %-16s salinan=%s' % (u['unit'],u.get('node',''),u['copies']))
"
