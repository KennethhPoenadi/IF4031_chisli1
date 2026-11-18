#!/usr/bin/env python3
"""Bukti Problem 1 kriteria 7: angka dashboard analitik harus sama dengan hitungan langsung dari Audit API.

Dashboard (/dashboard/api/summary) menghitung agregasi di penyimpanan. Skrip ini menghitung ulang dari jalur
yang berbeda: membaca SEMUA HazardEvent lewat Audit API (/internal/audit/events), lalu menghitung per jenis,
per severity, dan per wilayah sendiri. Kedua angka dibandingkan.

Karena data terus masuk (polling BMKG/PVMBG, sensor pantai), pembacaan Audit API difilter dengan `until`
(ingested_at sebelum waktu cetak) pada kedua sisi: Audit API memakai `since`, jadi event yang masuk sesudah
potret diabaikan dengan memotong hasil di sisi klien dan dashboard dibatasi rentang `to` yang sama.

Lingkungan: AGG_URL, AUDIT_TOKEN, DASHBOARD_TOKEN.

  python3 tests/p1/dashboard_check.py
"""
import argparse
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
import p1  # noqa: E402

CATEGORIES = ("SEISMIC", "TSUNAMI", "VOLCANIC", "WILDFIRE", "COASTAL")


def category(ev):
    if ev["hazard_type"] == "SEISMIC" and "tsunami_warning_threat_level" in (ev.get("attributes") or {}):
        return "TSUNAMI"
    return ev["hazard_type"]


def parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def read_all(c, upto):
    events, cursor, partial = {}, "", False
    while True:
        q = "/internal/audit/events?limit=1000" + (f"&cursor={cursor}" if cursor else "")
        page = c.audit_get(q)
        partial = partial or page["partial"]
        for ev in page["items"]:
            if parse(ev["ingested_at"]) <= upto:
                events[ev["hazard_id"]] = ev
        cursor = page.get("next_cursor")
        if not cursor:
            return events, partial


def dashboard(c, token, rng_from, rng_to):
    url = f"{c.agg}/dashboard/api/summary?from={rng_from}&to={rng_to}"
    code, body = p1.http_json("GET", url, {"Authorization": "Bearer " + token})
    if code != 200:
        raise SystemExit(f"dashboard HTTP {code}: {body}")
    return body


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out")
    args = ap.parse_args()
    c, d = p1.Client(), p1.out_dir(args)
    token = os.environ.get("DASHBOARD_TOKEN", "")
    if not token:
        raise SystemExit("DASHBOARD_TOKEN wajib")

    # Potret yang konsisten: pakai kejadian yang sudah tersimpan sebelum 'cut', dan hitung agregasi dashboard
    # berdasarkan occurred_at dalam rentang lebar. Dashboard mengelompokkan menurut waktu kejadian
    # (occurred_at), maka hitungan Audit API juga memakai occurred_at yang sama.
    cut = datetime.now(timezone.utc)
    time.sleep(1)
    start = (cut - timedelta(days=300)).replace(minute=0, second=0, microsecond=0)
    # Batas atas rentang dibuat 3 menit di belakang sekarang: event BMKG/PVMBG terus masuk (occurred_at sekitar
    # sekarang) selama pembacaan Audit API yang memakan puluhan detik, dan akan terhitung di dashboard tetapi tidak
    # di potret Audit API. Kedua sisi dibandingkan pada rentang yang sudah stabil.
    end = (cut - timedelta(minutes=3)).replace(second=0, microsecond=0)
    events, audit_partial = read_all(c, cut)
    # Dashboard hanya menghitung event dengan occurred_at di dalam rentang; sisi Audit API memakai rentang sama.
    events = {k: e for k, e in events.items() if start <= parse(e["occurred_at"]) < end}
    mine_types = Counter(category(e) for e in events.values())
    mine_sev = Counter(e["severity"] for e in events.values())
    mine_area = Counter(e["area_name"] for e in events.values())

    dash = dashboard(c, token, start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ"))
    # Perubahan yang sah sesudah potret: event baru dan event yang diperbarui di tempat (laporan PVMBG berganti
    # status dan ingested_at-nya maju). Audit API menyaring menurut ingested_at (>= since), jadi jumlahnya dapat
    # dihitung. Selisih dashboard vs potret tidak boleh melebihi jumlah ini.
    changed = 0
    cursor = ""
    while True:
        q = f"/internal/audit/events?since={cut.strftime('%Y-%m-%dT%H:%M:%SZ')}&limit=1000" + (f"&cursor={cursor}" if cursor else "")
        pg = c.audit_get(q)
        changed += sum(1 for e in pg["items"] if start <= parse(e["occurred_at"]) < end)
        cursor = pg.get("next_cursor")
        if not cursor:
            break
    late = changed
    dash_types = {k: v for k, v in dash["totals"].items() if v}
    dash_sev = {r["key"]: r["count"] for r in dash["severity"] if r["count"]}
    dash_area = {r["area"]: r["count"] for r in dash["areas"]}

    diffs = {"jenis": {}, "severity": {}, "wilayah": {}}
    for k in set(mine_types) | set(dash_types):
        if mine_types.get(k, 0) != dash_types.get(k, 0):
            diffs["jenis"][k] = {"audit": mine_types.get(k, 0), "dashboard": dash_types.get(k, 0)}
    for k in set(mine_sev) | set(dash_sev):
        if mine_sev.get(k, 0) != dash_sev.get(k, 0):
            diffs["severity"][k] = {"audit": mine_sev.get(k, 0), "dashboard": dash_sev.get(k, 0)}
    for k in set(mine_area) | set(dash_area):
        if mine_area.get(k, 0) != dash_area.get(k, 0):
            diffs["wilayah"][k] = {"audit": mine_area.get(k, 0), "dashboard": dash_area.get(k, 0)}

    # Toleransi hanya untuk event yang benar-benar masuk selama pengukuran (late).
    total_diff = sum(abs(v["audit"] - v["dashboard"]) for g in diffs.values() for v in g.values())
    # Satu event yang berubah dapat menggeser satu hitungan pada tiap dari tiga pengelompokan (jenis, severity, wilayah).
    ok = total_diff <= late * 3 and not audit_partial and not dash["partial"]
    result = {
        "total_audit": len(events), "total_dashboard": dash["total"], "event_berubah_sesudah_potret": late, "selisih_total": total_diff,
        "partial_audit": audit_partial, "partial_dashboard": dash["partial"],
        "unavailable_units": dash["unavailable_units"], "rebalancing": dash["rebalancing"],
        "per_jenis": {"audit": dict(mine_types), "dashboard": dash_types},
        "per_severity": {"audit": dict(mine_sev), "dashboard": dash_sev},
        "jumlah_wilayah": {"audit": len(mine_area), "dashboard": len(dash_area)},
        "selisih": diffs, "lolos": ok,
    }
    if ok:
        p1.save(d, "dashboard_check.json", result)
    else:
        os.makedirs(os.path.join(d, "gagal"), exist_ok=True)
        p1.save(os.path.join(d, "gagal"), "dashboard_check.json", result)
    print(f"total  audit={len(events)}  dashboard={dash['total']}  (event baru atau diperbarui sesudah potret: {late}, selisih total {total_diff})")
    print("jenis   :", dict(mine_types))
    print("severity:", dict(mine_sev))
    print(f"wilayah : {len(mine_area)} wilayah (audit) vs {len(dash_area)} (dashboard)")
    print("partial : audit", audit_partial, "| dashboard", dash["partial"], "| tak terjangkau", dash["unavailable_units"])
    for g, v in diffs.items():
        if v:
            print(f"selisih {g}: {v}")
    print(f"\nHASIL: {'LOLOS' if ok else 'GAGAL'}")
    print("bukti disimpan di", d)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
