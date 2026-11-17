"""Uji skrip p1.py terhadap Aggregator tiruan. Jalankan: python3 -m unittest discover -s tests/p1"""
import contextlib
import hashlib
import io
import json
import os
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.dirname(__file__))
import p1  # noqa: E402

UNITS = {"store-0": "node-a", "store-1": "node-b", "store-2": "node-b"}


class Stub(BaseHTTPRequestHandler):
    events = {}
    fail_next = 0

    def log_message(self, *a):
        pass

    def _send(self, code, body):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self):
        if self.headers.get("X-Internal-Key") != "k":
            return self._send(403, {})
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if Stub.fail_next > 0:
            Stub.fail_next -= 1
            return self._send(503, {"failed": 1})
        stored = rejected = 0
        for ev in body["events"]:
            if not isinstance(ev.get("magnitude"), float) or "event_id" not in ev:
                rejected += 1
                continue
            Stub.events["hz-" + ev["event_id"]] = ev
            stored += 1
        self._send(200, {"received": len(body["events"]), "stored": stored, "rejected": rejected, "failed": 0,
                         "rejections": [{"index": 0, "reason": "x"}] if rejected else []})

    def do_GET(self):
        if self.headers.get("Authorization") != "Bearer t":
            return self._send(401, {})
        names = sorted(UNITS)
        per = {n: {"unit": n, "node": UNITS[n], "copies": 0, "primary": 0, "replica": 0} for n in names}
        for hid in Stub.events:
            h = int(hashlib.sha256(hid.encode()).hexdigest(), 16)
            a, b = names[h % 3], names[(h + 1) % 3]
            per[a]["copies"] += 1; per[a]["primary"] += 1
            per[b]["copies"] += 1; per[b]["replica"] += 1
        nodes = {}
        for u in per.values():
            n = nodes.setdefault(u["node"], {"node": u["node"], "units": [], "copies": 0, "primary": 0, "replica": 0})
            n["units"].append(u["unit"])
            for k in ("copies", "primary", "replica"):
                n[k] += u[k]
        ns = list(nodes.values())
        cp = [n["copies"] for n in ns]
        if self.path.startswith("/internal/audit/distribution"):
            return self._send(200, {"total_events": len(Stub.events), "replication_factor": 2, "units": list(per.values()),
                                    "nodes": ns, "max_min_ratio": max(cp) / min(cp) if min(cp) else 0,
                                    "partial": False, "unavailable_units": []})
        if self.path.startswith("/internal/audit/checksum"):
            return self._send(200, {"count": len(Stub.events), "checksum": "abc", "partial": False})
        self._send(404, {})


class P1Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = HTTPServer(("127.0.0.1", 0), Stub)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        os.environ.update(AGG_URL=f"http://127.0.0.1:{cls.srv.server_port}", AGG_INTERNAL_KEY="k", AUDIT_TOKEN="t")

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        Stub.events.clear()
        Stub.fail_next = 0
        self.out = tempfile.mkdtemp()

    def run_cli(self, *argv):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            sys.argv = ["p1.py", *argv]
            try:
                p1.main()
            except SystemExit as e:
                self.code = e.code
        return buf.getvalue()

    def test_synthetic_load_spreads_across_nodes_and_report_is_consistent(self):
        out = self.run_cli("load", "--synthetic", "4500", "--out", self.out)
        self.assertEqual(len(Stub.events), 4500)
        res = json.load(open(os.path.join(self.out, "load.json")))
        self.assertEqual(res["stored"], 4500)
        self.assertEqual(sum(res["tambahan_salinan_per_node"].values()), 9000)  # RF 2
        self.assertTrue(all(v > 0 for v in res["tambahan_salinan_per_node"].values()), "semua node harus kebagian")
        self.assertIn("event unik: 4500", out)
        self.run_cli("report", "--out", self.out)
        self.assertEqual(self.code, 0)

    def test_load_is_idempotent_when_repeated(self):
        self.run_cli("load", "--synthetic", "300", "--out", self.out)
        self.run_cli("load", "--synthetic", "300", "--out", self.out)
        self.assertEqual(len(Stub.events), 300)

    def test_retries_when_storage_unavailable(self):
        Stub.fail_next = 2
        self.run_cli("load", "--synthetic", "100", "--out", self.out)
        self.assertEqual(len(Stub.events), 100)

    def test_csv_catalog_with_usgs_style_columns_and_bad_rows(self):
        path = os.path.join(self.out, "k.csv")
        with open(path, "w") as f:
            f.write("time,latitude,longitude,depth,mag,place\n")
            f.write("1730000000000,-0.9,100.3,12,5.4,Padang\n")
            f.write("2026-11-01 03:04:05,-2.1,99.8,30,7.2,Mentawai\n")
            f.write("2026-11-01 03:04:06,-2.1,99.8,30,tidak-angka,Rusak\n")
        out = self.run_cli("load", "--file", path, "--out", self.out)
        res = json.load(open(os.path.join(self.out, "load.json")))
        self.assertEqual((res["stored"], res["rejected"]), (2, 1))
        self.assertTrue(any(e["potential_tsunami"] for e in Stub.events.values()), "M7.2 berpotensi tsunami")
        self.assertIn("1 event ditolak", out)

    def test_surge_goes_to_one_region_and_load_stays_balanced(self):
        self.run_cli("load", "--synthetic", "3000", "--out", self.out)
        out = self.run_cli("surge", "--region", "Kepulauan Mentawai", "--count", "4000", "--out", self.out)
        res = json.load(open(os.path.join(self.out, "surge.json")))
        regions = {e["region_name"] for k, e in Stub.events.items() if "surge-" in k}
        self.assertEqual(regions, {"Kepulauan Mentawai"})
        d = res["tambahan_salinan_per_node"]
        self.assertLess(max(d.values()) / min(d.values()), 2.5)
        self.assertIn("rasio beban tulis", out)

    def test_report_fails_when_total_disagrees_with_checksum(self):
        self.run_cli("load", "--synthetic", "50", "--out", self.out)
        real = Stub.do_GET

        def lying(self_):
            if self_.path.startswith("/internal/audit/checksum"):
                return self_._send(200, {"count": 49, "checksum": "x", "partial": False})
            return real(self_)
        Stub.do_GET = lying
        try:
            self.run_cli("report", "--out", self.out)
        finally:
            Stub.do_GET = real
        self.assertEqual(self.code, 1)


if __name__ == "__main__":
    unittest.main()
