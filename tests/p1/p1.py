#!/usr/bin/env python3
"""Bukti Problem 1 (penyimpanan terdistribusi): pemuatan katalog, lonjakan wilayah, dan laporan sebaran.

Hanya memakai pustaka standar Python 3.9+.

Subperintah:
  load    memuat katalog gempa (CSV/JSON/JSONL) atau data sintetis berpola miring ke Aggregator
  surge   mensimulasikan lonjakan gempa di satu wilayah dan mengukur dampaknya per node
  report  mencetak sebaran data per unit dan per node dari Audit API, membandingkannya dengan checksum
  paginate membaca event terbaru lintas seluruh unit per halaman (mis. 50) dan memeriksa urutan,
          duplikat, serta tanda partial (kriteria 3)

Lingkungan:
  AGG_URL           alamat Aggregator untuk endpoint internal (bawaan http://localhost:8090)
  AGG_INTERNAL_KEY  nilai X-Internal-Key (AGGREGATOR_API_KEY)
  AUDIT_URL         alamat Audit API (bawaan = AGG_URL)
  AUDIT_TOKEN       token Audit API
  OUT_DIR           folder bukti (bawaan evidence/p1/<waktu>)

Contoh (Aggregator dijangkau lewat: kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/aggregator 8090):
  python3 tests/p1/p1.py load --synthetic 50000
  python3 tests/p1/p1.py load --file katalog.csv
  python3 tests/p1/p1.py surge --region "Kepulauan Mentawai" --count 5000
  python3 tests/p1/p1.py report
"""
import argparse
import csv
import hashlib
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

BATCH = 2000

# Jalur gempa yang aktif (pusat, sebaran derajat, wilayah). Data nyata menumpuk di sini, sehingga
# katalog sintetis dengan bobot miring meniru ketidakmerataan alami yang disebut di soal.
HOTSPOTS = [
    ("Kepulauan Mentawai", -2.0, 99.6, 0.8),
    ("Sumatera Barat", -0.9, 100.2, 0.6),
    ("Bengkulu", -4.0, 102.2, 0.6),
    ("Banten", -6.8, 105.9, 0.5),
    ("Nusa Tenggara Timur", -9.5, 122.5, 1.2),
]
UNIFORM_REGIONS = ["Aceh", "Sulawesi Tengah", "Maluku", "Papua", "Jawa Timur", "Kalimantan Timur", "Bali"]

ALIASES = {
    "event_id": ["event_id", "id", "eventid", "event"],
    "magnitude": ["magnitude", "mag", "m"],
    "depth_km": ["depth_km", "depth", "kedalaman"],
    "epicenter_lat": ["epicenter_lat", "latitude", "lat"],
    "epicenter_lon": ["epicenter_lon", "longitude", "lon", "lng"],
    "region_name": ["region_name", "place", "region", "area", "wilayah", "lokasi"],
    "occurred_at": ["occurred_at", "time", "datetime", "origin_time", "waktu"],
    "potential_tsunami": ["potential_tsunami", "tsunami"],
}


# ---------------------------------------------------------------- HTTP

def http_json(method, url, headers, body=None, timeout=120):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json", **headers})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read()
        e.close()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"raw": raw.decode(errors="replace")}


class Client:
    def __init__(self):
        self.agg = os.environ.get("AGG_URL", "http://localhost:8090").rstrip("/")
        self.audit = os.environ.get("AUDIT_URL", self.agg).rstrip("/")
        self.key = os.environ.get("AGG_INTERNAL_KEY", "")
        self.token = os.environ.get("AUDIT_TOKEN", "")

    def ingest(self, events):
        """Kirim satu batch. Idempoten, jadi aman diulang bila penyimpanan sedang tidak terjangkau."""
        for attempt in range(8):
            code, res = http_json("POST", self.agg + "/internal/ingest/events", {"X-Internal-Key": self.key}, {"events": events})
            if code == 200:
                return res
            if code == 503:
                time.sleep(min(2 ** attempt * 0.25, 5))
                continue
            raise SystemExit(f"ingest gagal: HTTP {code} {res}")
        raise SystemExit("ingest gagal: penyimpanan tidak terjangkau setelah 8 percobaan")

    def audit_get(self, path):
        code, res = http_json("GET", self.audit + path, {"Authorization": "Bearer " + self.token})
        if code != 200:
            raise SystemExit(f"audit {path} gagal: HTTP {code} {res}")
        return res


# ---------------------------------------------------------------- data

def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def synthetic(n, skew, seed):
    rnd = random.Random(seed)
    weights = [5, 4, 3, 2, 2]
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(n):
        if rnd.random() < skew:
            name, lat, lon, spread = rnd.choices(HOTSPOTS, weights)[0]
            lat, lon = rnd.gauss(lat, spread), rnd.gauss(lon, spread)
        else:
            name = rnd.choice(UNIFORM_REGIONS)
            lat, lon = rnd.uniform(-10, 5), rnd.uniform(95, 140)
        mag = round(min(9.0, 2.5 + rnd.expovariate(0.9)), 1)
        yield {
            "event_id": f"cat-{seed}-{i:07d}",
            "magnitude": mag,
            "depth_km": round(rnd.uniform(3, 150), 1),
            "epicenter_lat": round(max(-11, min(6, lat)), 4),
            "epicenter_lon": round(max(94, min(141, lon)), 4),
            "region_name": name,
            "occurred_at": iso(start + timedelta(seconds=i * 37)),
            "potential_tsunami": mag >= 7.0,
        }


def pick(row, field):
    low = {k.strip().lower(): v for k, v in row.items()}
    for alias in ALIASES[field]:
        if alias in low and low[alias] not in (None, ""):
            return low[alias]
    return None


def normalize_time(v):
    s = str(v).strip()
    if s.replace(".", "", 1).isdigit():
        x = float(s)
        return iso(datetime.fromtimestamp(x / 1000 if x > 1e11 else x, timezone.utc))
    return s.replace(" ", "T") if "T" not in s and " " in s else s


def from_row(row, i):
    ev = {}
    for field in ALIASES:
        v = pick(row, field)
        if v is not None:
            ev[field] = v
    for f in ("magnitude", "depth_km", "epicenter_lat", "epicenter_lon"):
        if f in ev:
            try:
                ev[f] = float(ev[f])
            except (TypeError, ValueError):
                pass  # dibiarkan apa adanya: Aggregator menolaknya beserta alasannya
    if "occurred_at" in ev:
        ev["occurred_at"] = normalize_time(ev["occurred_at"])
    if "potential_tsunami" in ev:
        ev["potential_tsunami"] = str(ev["potential_tsunami"]).strip().lower() in ("1", "true", "ya", "yes")
    else:
        ev["potential_tsunami"] = isinstance(ev.get("magnitude"), float) and ev["magnitude"] >= 7.0
    if "event_id" not in ev:
        # ID stabil dari isi, supaya memuat ulang berkas yang sama tidak menggandakan data.
        basis = "|".join(str(ev.get(k)) for k in ("occurred_at", "epicenter_lat", "epicenter_lon", "magnitude"))
        ev["event_id"] = "cat-" + hashlib.sha1(basis.encode()).hexdigest()[:16]
    return ev


def read_file(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        if path.endswith(".csv"):
            for i, row in enumerate(csv.DictReader(f)):
                yield from_row(row, i)
            return
        text = f.read()
    if path.endswith(".jsonl"):
        rows = (json.loads(line) for line in text.splitlines() if line.strip())
    else:
        data = json.loads(text)
        rows = data["events"] if isinstance(data, dict) and "events" in data else data
    for i, row in enumerate(rows):
        yield from_row(row, i)


def chunks(it, size):
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf


# ---------------------------------------------------------------- beban per node

class NodeLoad:
    """Cuplikan `kubectl top nodes` berkala selama sebuah fase, bila kubectl tersedia."""

    def __init__(self, interval=3.0):
        self.interval = interval
        self.samples = {}
        self._stop = threading.Event()
        self._t = None
        self.enabled = shutil.which("kubectl") is not None

    def _run(self):
        while not self._stop.is_set():
            try:
                out = subprocess.run(["kubectl", "top", "nodes", "--no-headers"], capture_output=True, text=True, timeout=10).stdout
                for line in out.splitlines():
                    p = line.split()
                    if len(p) >= 5:
                        s = self.samples.setdefault(p[0], {"cpu_m": [], "mem_mi": []})
                        s["cpu_m"].append(int(p[1].rstrip("m")) if p[1].endswith("m") else int(float(p[1]) * 1000))
                        s["mem_mi"].append(int(p[3].rstrip("Mi")))
            except Exception:
                pass
            self._stop.wait(self.interval)

    def __enter__(self):
        if self.enabled:
            self._t = threading.Thread(target=self._run, daemon=True)
            self._t.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        if self._t:
            self._t.join(timeout=5)

    def summary(self):
        return {n: {"cpu_m_avg": round(statistics.mean(s["cpu_m"])), "cpu_m_max": max(s["cpu_m"]),
                    "mem_mi_max": max(s["mem_mi"]), "samples": len(s["cpu_m"])}
                for n, s in self.samples.items() if s["cpu_m"]}


def node_copies(dist):
    return {n["node"]: n["copies"] for n in dist["nodes"]}


def table(dist):
    lines = [f"{'node':<22}{'unit':<24}{'salinan':>9}{'primary':>9}{'replica':>9}  persen"]
    total = sum(n["copies"] for n in dist["nodes"]) or 1
    for n in dist["nodes"]:
        lines.append(f"{n['node']:<22}{','.join(n['units']):<24}{n['copies']:>9}{n['primary']:>9}{n['replica']:>9}  {100 * n['copies'] / total:5.1f}%")
    lines.append(f"event unik: {dist['total_events']}   RF: {dist['replication_factor']}   "
                 f"max/min: {dist['max_min_ratio']:.2f}   partial: {dist['partial']}   tak terjangkau: {dist['unavailable_units']}")
    return "\n".join(lines)


def out_dir(args):
    d = args.out or os.environ.get("OUT_DIR") or os.path.join("evidence", "p1", datetime.now().strftime("%Y%m%d-%H%M%S"))
    os.makedirs(d, exist_ok=True)
    return d


def save(d, name, obj):
    with open(os.path.join(d, name), "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------- perintah

def run_phase(c, events_iter, label, d, workers=1):
    before = c.audit_get("/internal/audit/distribution")
    stats = {"received": 0, "stored": 0, "rejected": 0, "failed": 0}
    rejections, lat = [], []
    lock = threading.Lock()

    def send(batch):
        t = time.monotonic()
        res = c.ingest(batch)
        with lock:
            lat.append(time.monotonic() - t)
            for k in stats:
                stats[k] += res.get(k, 0)
            rejections.extend(res.get("rejections", [])[: max(0, 50 - len(rejections))])

    t0 = time.monotonic()
    with NodeLoad() as nl:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(send, chunks(events_iter, BATCH)))
    secs = time.monotonic() - t0
    after = c.audit_get("/internal/audit/distribution")

    b, a = node_copies(before), node_copies(after)
    delta = {n: a.get(n, 0) - b.get(n, 0) for n in sorted(set(a) | set(b))}
    result = {
        "fase": label, "durasi_detik": round(secs, 2),
        "throughput_event_per_detik": round(stats["received"] / secs, 1) if secs else None,
        "latensi_batch_detik": {"p50": round(statistics.median(lat), 3) if lat else None,
                                "p95": round(sorted(lat)[int(0.95 * (len(lat) - 1))], 3) if lat else None},
        **stats, "contoh_penolakan": rejections,
        "tambahan_salinan_per_node": delta,
        "beban_node_kubectl_top": nl.summary(),
        "distribusi_sesudah": after,
    }
    save(d, f"{label}.json", result)
    print(f"\n== {label}: {stats['stored']} tersimpan, {stats['rejected']} ditolak, {stats['failed']} gagal, "
          f"{result['throughput_event_per_detik']} event/detik")
    print("tambahan salinan per node:", delta)
    if nl.enabled:
        print("beban node (kubectl top):", json.dumps(result["beban_node_kubectl_top"]))
    print(table(after))
    if stats["rejected"]:
        print(f"{stats['rejected']} event ditolak, contoh alasan: {rejections[:3]}")
    return result


def cmd_load(args):
    c, d = Client(), out_dir(args)
    if args.file:
        events = read_file(args.file)
    else:
        events = synthetic(args.synthetic, args.skew, args.seed)
    run_phase(c, events, "load", d, workers=args.workers)
    print("\nbukti disimpan di", d)


def cmd_surge(args):
    c, d = Client(), out_dir(args)
    base = datetime.now(timezone.utc)
    seed = int(time.time())
    rnd = random.Random(seed)
    spot = next((h for h in HOTSPOTS if h[0] == args.region), (args.region, -2.0, 99.6, 0.5))

    def gen():
        for i in range(args.count):
            yield {
                "event_id": f"surge-{seed}-{i:06d}", "magnitude": round(rnd.uniform(3.0, 5.5), 1),
                "depth_km": round(rnd.uniform(5, 40), 1),
                "epicenter_lat": round(rnd.gauss(spot[1], 0.3), 4), "epicenter_lon": round(rnd.gauss(spot[2], 0.3), 4),
                "region_name": args.region, "occurred_at": iso(base - timedelta(seconds=i)), "potential_tsunami": False,
            }

    res = run_phase(c, gen(), "surge", d, workers=args.workers)
    delta = res["tambahan_salinan_per_node"]
    if len(delta) >= 2 and min(delta.values()) > 0:
        print(f"\nrasio beban tulis terbesar/terkecil per node: {max(delta.values()) / min(delta.values()):.2f} "
              f"(sharding memakai hazard_id, bukan wilayah, sehingga lonjakan satu wilayah menyebar merata)")
    print("bukti disimpan di", d)


def cmd_report(args):
    c, d = Client(), out_dir(args)
    # Data terus masuk selama pengukuran, jadi distribusi diambil sebelum dan sesudah checksum. Konsisten
    # berarti jumlah checksum berada di antara keduanya (tidak ada event yang hilang atau ganda).
    before = c.audit_get("/internal/audit/distribution")
    check = c.audit_get("/internal/audit/checksum")
    dist = c.audit_get("/internal/audit/distribution")
    print(table(dist))
    print(f"\nchecksum  : {check['checksum']}\njumlah    : {check['count']}   partial: {check['partial']}")
    lo, hi = before["total_events"], dist["total_events"]
    consistent = lo <= check["count"] <= hi and not check["partial"] and not dist["partial"]
    print(f"konsisten dengan checksum: {'YA' if consistent else 'TIDAK'} "
          f"(distribusi {lo} -> {hi}, checksum {check['count']})")
    save(d, "report.json", {"distribusi_sebelum": before, "distribusi": dist, "checksum": check, "konsisten": consistent})
    print("bukti disimpan di", d)
    return 0 if consistent else 1


def cmd_paginate(args):
    c, d = Client(), out_dir(args)
    cursor, seen, prev, pages, lat, partial_pages, violations = "", set(), None, 0, [], 0, []
    while pages < args.pages:
        url = f"{c.agg}/hazard-events?limit={args.limit}" + (f"&cursor={cursor}" if cursor else "")
        t = time.monotonic()
        code, body = http_json("GET", url, {"X-Internal-Key": c.key})
        lat.append(time.monotonic() - t)
        if code != 200:
            violations.append(f"halaman {pages + 1}: HTTP {code}")
            break
        pages += 1
        meta = body.get("meta", {})
        if meta.get("partial"):
            partial_pages += 1
        for ev in body.get("data", []):
            # Bandingkan sebagai waktu, bukan string: JSON memangkas nol pecahan detik (".94Z" vs ".948Z").
            key = (datetime.fromisoformat(ev["ingested_at"].replace("Z", "+00:00")), ev["hazard_id"])
            if ev["hazard_id"] in seen:
                violations.append(f"duplikat {ev['hazard_id']}")
            seen.add(ev["hazard_id"])
            # Urutan terbaru lebih dulu: (ingested_at, hazard_id) harus menurun tegas.
            if prev is not None and not (key[0] < prev[0] or (key[0] == prev[0] and key[1] < prev[1])):
                violations.append(f"urutan salah: {prev} lalu {key}")
            prev = key
        cursor = meta.get("next_cursor") or ""
        if not cursor:
            break
    result = {
        "halaman": pages, "ukuran_halaman": args.limit, "event": len(seen),
        "halaman_partial": partial_pages,
        "latensi_halaman_detik": {"p50": round(statistics.median(lat), 3) if lat else None,
                                  "p95": round(sorted(lat)[int(0.95 * (len(lat) - 1))], 3) if lat else None},
        "pelanggaran": violations[:20], "jumlah_pelanggaran": len(violations),
    }
    save(d, "paginate.json", result)
    print(f"{pages} halaman x {args.limit}, {len(seen)} event unik, {partial_pages} halaman partial, "
          f"latensi p50 {result['latensi_halaman_detik']['p50']} dtk, p95 {result['latensi_halaman_detik']['p95']} dtk")
    print("urutan benar dan tanpa duplikat:", "YA" if not violations else f"TIDAK ({len(violations)} pelanggaran)")
    for v in violations[:5]:
        print("  -", v)
    print("bukti disimpan di", d)
    return 0 if not violations else 1


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("load", "surge", "report", "paginate"):
        p = sub.add_parser(name)
        p.add_argument("--out", help="folder bukti")
        if name == "load":
            g = p.add_mutually_exclusive_group(required=True)
            g.add_argument("--file", help="katalog .csv/.json/.jsonl")
            g.add_argument("--synthetic", type=int, metavar="N", help="jumlah event sintetis")
            p.add_argument("--skew", type=float, default=0.7, help="porsi event pada jalur gempa (0-1)")
            p.add_argument("--seed", type=int, default=7)
            p.add_argument("--workers", type=int, default=2)
        if name == "surge":
            p.add_argument("--region", default="Kepulauan Mentawai")
            p.add_argument("--count", type=int, default=5000)
            p.add_argument("--workers", type=int, default=4)
        if name == "paginate":
            p.add_argument("--limit", type=int, default=50)
            p.add_argument("--pages", type=int, default=40)
    args = ap.parse_args()
    sys.exit({"load": cmd_load, "surge": cmd_surge, "report": cmd_report, "paginate": cmd_paginate}[args.cmd](args) or 0)


if __name__ == "__main__":
    main()
