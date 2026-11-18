#!/usr/bin/env bash
# Menjalankan rangkaian bukti P1 dan P2 pada cluster 2 node, merekam keluaran asli ke log, lalu merendernya jadi PNG.
#
# Prasyarat: KUBECONFIG ke cluster k3s, port-forward svc/aggregator di localhost:8090, ingress di 127.0.0.1:80,
# ~/.xops/prod.env berisi token. Hasil: docs/bukti/<p1|p2>/2node/{*.json,logs/*.log,img/*.png}
#
#   tests/tools/run_evidence.sh p1       # hanya P1
#   tests/tools/run_evidence.sh p2       # hanya P2
#   tests/tools/run_evidence.sh          # keduanya
set -uo pipefail
cd "$(dirname "$0")/../.."
ROOT="$PWD"
set -a; . "$HOME/.xops/prod.env"; set +a
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s-config}"
export AGG_URL="${AGG_URL:-http://localhost:8090}" AGG_INTERNAL_KEY="$AGGREGATOR_API_KEY"
export POLL_INTERVAL_SECONDS="$AGGREGATOR_POLL_INTERVAL_SECONDS"

wait_clean() {
  for _ in $(seq 1 60); do
    d=$(curl -s -m20 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGG_URL/internal/audit/distribution" |
      python3 -c "import json,sys;d=json.load(sys.stdin);print('ok' if not d['partial'] and not d['rebuilding_units'] and d['under_replicated']==0 else 'tunggu')" 2>/dev/null)
    [ "$d" = ok ] && return 0
    sleep 10
  done
  echo "PERINGATAN: cluster belum bersih setelah 10 menit" >&2
}

# run <problem> <nama> <judul> <perintah...>
run() {
  local prob="$1" name="$2" title="$3"; shift 3
  local dir="docs/bukti/$prob/2node"; mkdir -p "$dir/logs" "$dir/img"
  export OUT_DIR="$ROOT/$dir"
  [ "${SKIP_CLEAN:-0}" = 1 ] || wait_clean
  echo "=== $prob/$name"
  local start rc; start="$(date '+%Y-%m-%d %H:%M:%S')"
  if [ "${LIVE:-0}" = 1 ]; then
    # Tes berjalan LIVE di jendela konsol Windows sungguhan; hanya jendela itu yang ditangkap jadi PNG.
    {
      echo "export OUT_DIR='$OUT_DIR' AGG_URL='$AGG_URL' AGG_INTERNAL_KEY='$AGG_INTERNAL_KEY'"
      echo "export POLL_INTERVAL_SECONDS='$POLL_INTERVAL_SECONDS' KUBECONFIG='$KUBECONFIG'"
      [ -n "${PVMBG_URL:-}" ] && echo "export PVMBG_URL='$PVMBG_URL'"
    } > /tmp/evidence.env; chmod 600 /tmp/evidence.env
    SHOT_CMD="$*" tests/tools/realshot.sh "$dir/img/$name.png" "BUKTI-$prob-$name" "$dir/logs/$name.log" -- "$@"
    rc=$?
  else
    "$@" 2>&1 | tee "$dir/logs/$name.log"
    rc=${PIPESTATUS[0]}
    python3 tests/tools/shot.py --log "$dir/logs/$name.log" --title "$title" --cmd "${*}" --time "$start" --out "$dir/img/$name.png"
  fi
  echo "--- $prob/$name rc=$rc"
  [ "$rc" = 0 ] || echo "GAGAL: $prob/$name (bukti JSON tidak disimpan oleh tes ini)" >&2
}

p1() {
  run p1 load "P1 kriteria 1-2: pemuatan 20.000 event, sebaran per node" python3 -u tests/p1/p1.py load --synthetic 20000 --seed "$(date +%s)"
  run p1 surge "P1: lonjakan satu wilayah (3.000 event), sebaran per node" python3 -u tests/p1/p1.py surge --region "Kepulauan Mentawai" --count 3000
  run p1 paginate "P1 kriteria 3: baca lintas unit per halaman (50), urutan dan duplikat" python3 -u tests/p1/p1.py paginate
  run p1 report "P1: sebaran data per unit dan per node, konsisten dengan checksum" python3 -u tests/p1/p1.py report
  run p1 dashboard_check "P1 kriteria 7: angka dashboard sama dengan hitungan Audit API" python3 -u tests/p1/dashboard_check.py
  run p1 unit_loss "P1 kriteria 4-6: hapus unit (disk kosong) lalu matikan unit kedua" python3 -u tests/p1/unit_loss.py --victim store-0 --second store-1
}

p2() {
  run p2 once "P2 kriteria 2: tiap sumber diambil tepat sekali (5 replika)" python3 -u tests/p2/p2.py once
  run p2 unhealthy "P2 kriteria 1: instance tidak sehat dikeluarkan dari titik masuk" python3 -u tests/p2/p2.py unhealthy
  run p2 failover "P2 kriteria 3: leader dibunuh, instance lain melanjutkan <= 60 dtk" python3 -u tests/p2/p2.py failover
  run p2 freeze "P2 kriteria 5: leader dibekukan melewati lease lalu dilepas (fencing)" python3 -u tests/p2/p2.py freeze
  run p2 rolling "P2 kriteria 8: rolling update aggregator saat beban berjalan" python3 -u tests/p2/p2.py rolling
}

p3() {
  export PVMBG_URL="${PVMBG_URL:-http://localhost:8091}"
  run p3 access "P3 kriteria 5 (akses): pesan gagal hanya dapat diperiksa BNPB Internal Ops" python3 -u tests/p3/p3.py access
  run p3 schema "P3 kriteria 1,2,4,5: perubahan skema PVMBG, pesan parked, sumber lain tetap jalan" python3 -u tests/p3/p3.py schema
  run p3 corrupt "P3 kriteria 3: respons korup (terpotong, bersarang dalam, UTF-8 rusak, berlebih)" python3 -u tests/p3/p3.py corrupt
  run p3 reports "P3 kriteria 6: laporan lapangan 202 tidak hilang dan tidak diproses ganda saat komponen dibunuh" python3 -u tests/p3/p3.py reports
  run p3 recover "P3 kriteria 4: setelah sistem disesuaikan, pesan yang gagal diproses ulang dan pulih tanpa duplikat" python3 -u tests/p3/p3.py recover
}

p4() {
  run p4 live "P4: aliran feed satelit live diproses beberapa menit" python3 -u tests/p4/p4.py live --menit 5
  run p4 crash "P4 kriteria 5,8: pemroses dibunuh saat aliran berjalan, pulih tanpa kehilangan dan ganda" python3 -u tests/p4/p4.py crash
}

p5() {
  run p5 critical "P5 kriteria 1: event critical sampai ke notifier, p95 <= 5 dtk" python3 -u tests/p5/p5.py critical
  run p5 quarantine "P5 kriteria 2: data melanggar kontrak disisihkan dengan alasan" python3 -u tests/p5/p5.py quarantine
  run p5 unhealthy "P5 kriteria 3,4: sensor tidak sehat, sumber lain tetap cepat, breaker menahan permintaan, pulih sendiri" python3 -u tests/p5/p5.py unhealthy
  run p5 priority "P5 kriteria 5: Tim Lapangan tetap dilayani saat Media ditolak terkontrol" python3 -u tests/p5/p5.py priority
}

case "${1:-all}" in
  p4) p4 ;;
  p5) p5 ;;
  p1) p1 ;;
  p2) p2 ;;
  p3) p3 ;;
  p2-unhealthy) run p2 unhealthy "P2 kriteria 1: instance tidak sehat dikeluarkan dari titik masuk" python3 -u tests/p2/p2.py unhealthy ;;
  all) p1; p2 ;;
  *) echo "usage: $0 [p1|p2]" >&2; exit 2 ;;
esac
