#!/usr/bin/env python3
"""Bukti Problem 1 kriteria 4, 5, dan 6: kehilangan satu unit penyimpanan beserta datanya, pemulihan ke disk
kosong, lalu kehilangan unit berikutnya, sementara baca dan tulis terus berjalan.

Langkah:
  1. Baseline: seluruh source_ref_id sumber yang diperiksa (bawaan BMKG, termasuk katalog) dari Audit API.
  2. Lalu lintas latar: baca halaman terbaru, checksum, dan tulis batch kecil terus-menerus. Setiap respons
     dicatat (kode HTTP, latensi, tanda partial). Kriteria: tidak ada 5xx selain 503 terkontrol, tidak ada
     permintaan yang menggantung.
  3. Hapus pod dan volume unit korban (disk kosong). StatefulSet membuatnya ulang dengan volume baru.
  4. Tunggu unit korban terisi kembali oleh perbaikan, lalu pastikan seluruh ID baseline masih ada.
  5. Matikan unit kedua (tanpa menghapus data) dan pastikan data tetap lengkap dan tidak partial.

Lingkungan sama dengan p1.py (AGG_URL, AGG_INTERNAL_KEY, AUDIT_URL, AUDIT_TOKEN), ditambah KUBECONFIG.
Menghapus volume adalah tindakan merusak: jalankan hanya pada cluster uji.

  python3 tests/p1/unit_loss.py --victim store-1 --second store-2
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(__file__))
import p1  # noqa: E402

NS = os.environ.get("APP_NAMESPACE", "sistem-informasi-kebencanaan-bnpb")


def kubectl(*args, check=True):
    r = subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True, timeout=300)
    if check and r.returncode != 0:
        raise SystemExit(f"kubectl {' '.join(args)} gagal: {r.stderr.strip()}")
    return r.stdout.strip()


def aget(c, path, tries=40):
    """Pembacaan verifikasi (bukan lalu lintas yang diukur): mengulang saat 503/putus sesaat selama pemulihan."""
    last = None
    for _ in range(tries):
        try:
            code, res = p1.http_json("GET", c.audit + path, {"Authorization": "Bearer " + c.token}, timeout=60)
        except Exception as e:  # noqa: BLE001
            code, res = 0, {"error": str(e)}
        if code == 200:
            return res
        last = (code, res)
        time.sleep(3)
    raise SystemExit(f"audit {path} gagal setelah {tries} percobaan: {last}")


def all_refs(c, source):
    """Seluruh source_ref_id satu sumber lewat Audit API (berhalaman)."""
    refs, cursor, partial = set(), "", False
    while True:
        q = f"/internal/audit/events?source={source}&limit=1000" + (f"&cursor={cursor}" if cursor else "")
        page = aget(c, q)
        refs.update(ev["source_ref_id"] for ev in page["items"])
        partial = partial or page["partial"]
        cursor = page.get("next_cursor")
        if not cursor:
            return refs, partial


class Traffic:
    """Baca dan tulis latar. Mencatat setiap respons untuk membuktikan tidak ada 500 atau permintaan
    menggantung selama unit hilang dan pulih."""

    def __init__(self, c, timeout=10.0):
        self.c, self.timeout = c, timeout
        self.records, self.lock, self.stop = [], threading.Lock(), threading.Event()
        self.seq = 0

    def _call(self, kind, method, url, headers, body=None):
        t = time.monotonic()
        try:
            code, res = p1.http_json(method, url, headers, body, timeout=self.timeout)
        except Exception as e:  # timeout atau koneksi putus: dihitung sebagai menggantung/gagal
            code, res = 0, {"error": str(e)}
        dur = time.monotonic() - t
        partial = bool(res.get("partial") or res.get("meta", {}).get("partial")) if isinstance(res, dict) else False
        with self.lock:
            self.records.append({"t": time.time(), "kind": kind, "code": code, "dur": round(dur, 3), "partial": partial})

    def _reader(self):
        while not self.stop.is_set():
            self._call("read", "GET", f"{self.c.agg}/hazard-events?limit=50", {"X-Internal-Key": self.c.key})
            self._call("checksum", "GET", f"{self.c.audit}/internal/audit/checksum?source=BMKG",
                       {"Authorization": "Bearer " + self.c.token})
            self.stop.wait(0.5)

    def _writer(self):
        while not self.stop.is_set():
            batch = []
            for _ in range(20):
                self.seq += 1
                batch.append({"event_id": f"outage-{START}-{self.seq:06d}", "magnitude": 4.2, "depth_km": 12.0,
                              "epicenter_lat": -1.5, "epicenter_lon": 100.1, "region_name": "Sumatera Barat",
                              "occurred_at": p1.iso(p1.datetime.now(p1.timezone.utc)), "potential_tsunami": False})
            self._call("write", "POST", f"{self.c.agg}/internal/ingest/events", {"X-Internal-Key": self.c.key},
                       {"events": batch})
            self.stop.wait(1.0)

    def __enter__(self):
        self.threads = [threading.Thread(target=f, daemon=True) for f in (self._reader, self._writer)]
        for t in self.threads:
            t.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        for t in self.threads:
            t.join(timeout=self.timeout + 5)

    def summary(self, since=None):
        with self.lock:
            recs = [r for r in self.records if since is None or r["t"] >= since]
        out = {}
        for kind in ("read", "checksum", "write"):
            rs = [r for r in recs if r["kind"] == kind]
            if not rs:
                continue
            codes = {}
            for r in rs:
                codes[str(r["code"])] = codes.get(str(r["code"]), 0) + 1
            durs = sorted(r["dur"] for r in rs)
            out[kind] = {"jumlah": len(rs), "kode": codes, "partial": sum(r["partial"] for r in rs),
                         "p95_detik": durs[int(0.95 * (len(durs) - 1))], "maks_detik": durs[-1]}
        return out


def bad(summary):
    """Respons yang melanggar kriteria: 5xx selain 503 terkontrol, atau gagal/menggantung (kode 0)."""
    n = 0
    for s in summary.values():
        for code, cnt in s["kode"].items():
            if code == "0" or (code.startswith("5") and code != "503"):
                n += cnt
    return n


def unit_copies(c, unit):
    d = aget(c, "/internal/audit/distribution")
    return next((u["copies"] for u in d["units"] if u["unit"] == unit), 0), d


def wait_ready(pod, timeout=300):
    end = time.time() + timeout
    while time.time() < end:
        ready = kubectl("get", "pod", pod, "-o", "jsonpath={.status.containerStatuses[0].ready}", check=False)
        if ready == "true":
            return True
        time.sleep(3)
    return False


START = int(time.time())


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--victim", default="store-1", help="unit yang dihapus beserta volumenya")
    ap.add_argument("--second", default="store-2", help="unit yang dimatikan sesudah pemulihan (kriteria 6)")
    ap.add_argument("--source", default="BMKG")
    ap.add_argument("--repair-timeout", type=int, default=1200)
    ap.add_argument("--out")
    args = ap.parse_args()
    c, d = p1.Client(), p1.out_dir(args)
    report = {"victim": args.victim, "second": args.second}

    print(f"== 1. baseline {args.source}")
    base, base_partial = all_refs(c, args.source)
    before_copies, dist0 = unit_copies(c, args.victim)
    report["baseline"] = {"event": len(base), "partial": base_partial, f"salinan_{args.victim}": before_copies}
    print(f"   {len(base)} event, {args.victim} memegang {before_copies} salinan")
    if base_partial:
        raise SystemExit("baseline partial: pastikan semua unit hidup sebelum uji")

    with Traffic(c) as tr:
        time.sleep(5)
        print(f"== 2. hapus pod dan volume {args.victim} (disk kosong)")
        t_loss = time.time()
        kubectl("delete", "pvc", f"data-{args.victim}", "--wait=false")
        kubectl("delete", "pod", args.victim, "--wait=false")
        time.sleep(5)
        up = wait_ready(args.victim)
        t_up = time.time()
        fresh, _ = unit_copies(c, args.victim)
        print(f"   {args.victim} hidup lagi setelah {t_up - t_loss:.0f} dtk dengan {fresh} salinan (disk kosong)")
        report["pemulihan"] = {"siap": up, "detik_sampai_hidup": round(t_up - t_loss, 1), "salinan_awal": fresh}

        print("== 3. tunggu sistem melaporkan dirinya kembali tahan kehilangan satu unit")
        # Sinyal resmi dari Audit API: tidak ada event yang salinannya kurang dan tidak ada unit yang masih
        # dibangun ulang. Bukan ambang jumlah salinan: data baru terus masuk selama uji.
        end, dist = time.time() + args.repair_timeout, {}
        while time.time() < end:
            dist = aget(c, "/internal/audit/distribution")
            if dist["under_replicated"] == 0 and not dist["rebuilding_units"] and not dist["partial"]:
                break
            time.sleep(10)
        t_repaired = time.time()
        copies = next((u["copies"] for u in dist["units"] if u["unit"] == args.victim), 0)
        complete = dist.get("under_replicated", 1) == 0 and not dist.get("rebuilding_units") and not dist.get("partial")
        print(f"   {args.victim}: {copies} salinan; setelah {t_repaired - t_loss:.0f} dtk "
              f"under_replicated={dist.get('under_replicated')} rebuilding={dist.get('rebuilding_units')} lengkap={complete}")
        report["pemulihan"].update({"salinan_akhir": copies, "detik_sampai_lengkap": round(t_repaired - t_loss, 1),
                                    "under_replicated": dist.get("under_replicated"),
                                    "rebuilding_units": dist.get("rebuilding_units"), "lengkap": complete})
        report["lalu_lintas_kehilangan_unit"] = tr.summary(since=t_loss)

        print(f"== 4. periksa tidak ada event {args.source} yang hilang")
        after, after_partial = all_refs(c, args.source)
        missing = base - after
        report["kriteria4"] = {"baseline": len(base), "sesudah": len(after), "hilang": len(missing),
                               "contoh_hilang": sorted(missing)[:10], "partial": after_partial}
        print(f"   hilang: {len(missing)} dari {len(base)} (partial={after_partial})")

        print(f"== 5. matikan {args.second} (kriteria 6)")
        t2 = time.time()
        kubectl("delete", "pod", args.second, "--wait=false")
        time.sleep(3)
        during, during_partial = all_refs(c, args.source)
        missing2 = base - during
        wait_ready(args.second)
        report["kriteria6"] = {"hilang_saat_unit_kedua_mati": len(missing2), "partial": during_partial,
                               "lalu_lintas": tr.summary(since=t2)}
        print(f"   saat {args.second} mati: hilang {len(missing2)}, partial={during_partial}")
        report["lalu_lintas_total"] = tr.summary()

    violations = bad(report["lalu_lintas_total"])
    ok = (up and not missing and not missing2 and violations == 0
          and report["pemulihan"]["lengkap"])
    report["lolos"] = ok
    report["pelanggaran_lalu_lintas"] = violations
    if ok:
        p1.save(d, "unit_loss.json", report)
    else:
        os.makedirs(os.path.join(d, "gagal"), exist_ok=True)
        p1.save(os.path.join(d, "gagal"), "unit_loss.json", report)
    print("\n== lalu lintas selama uji:")
    for k, v in report["lalu_lintas_total"].items():
        print(f"   {k:<9} {v['jumlah']:>4} permintaan, kode {v['kode']}, partial {v['partial']}, "
              f"p95 {v['p95_detik']} dtk, maks {v['maks_detik']} dtk")
    print(f"\nHASIL: {'LOLOS' if ok else 'GAGAL'} (respons melanggar: {violations})")
    print("bukti disimpan di", d)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
