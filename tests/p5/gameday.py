#!/usr/bin/env python3
"""P5 kriteria 6: game day. Beberapa kegagalan yang tidak diumumkan disuntikkan bergantian saat jalur peringatan Tim Lapangan
dipantau; setelah suntikan terakhir tidak ada intervensi apa pun dan sistem harus pulih sendiri.

Urutan (detik sejak mulai): 30 sensor dibekukan 60 dtk; 60 PVMBG outage selama 60 dtk; 150 Tester Agent membunuh pod penyimpanan
(kill-store, skenario latihan); sesudahnya jendela pemulihan tanpa intervensi. Selama itu lonjakan beban sumber lain berjalan terus.

Target keandalan jalur peringatan Tim Lapangan (SLO), ditetapkan sebelum uji:
  - ketersediaan >= 99,5% permintaan Tim Lapangan berstatus 2xx (429 yang terkontrol tidak dihitung gagal)
  - p95 latensi <= 5 dtk
Sisa anggaran kesalahan (error budget) dilaporkan: berapa persen dari 0,5% yang terpakai.

  python3 tests/p5/gameday.py [--recovery 240]
Lingkungan: KUBECONFIG, AGG_URL, AUDIT_TOKEN, DASHBOARD_TOKEN, PVMBG_URL, PVMBG_TOKEN, AUTH_CLIENT_FIELD_ID, AUTH_CLIENT_FIELD_SECRET.
"""
import argparse
import http.client
import json
import os
import subprocess
import sys
import atexit
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "p1"))
sys.path.insert(0, os.path.join(HERE, "..", "p2"))
sys.path.insert(0, os.path.join(HERE, "..", "p3"))
import p1  # noqa: E402
import p2  # noqa: E402
import p3  # noqa: E402

NS = os.environ.get("APP_NAMESPACE", "sistem-informasi-kebencanaan-bnpb")
SLO_AVAILABILITY = 0.995
SLO_P95_S = 5.0


def kubectl(*args, check=False, timeout=120):
    r = subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True, timeout=timeout)
    return r.stdout


_pf = None
atexit.register(lambda: _pf and _pf.terminate())


def pvmbg_forward():
    """Port-forward milik skrip sendiri ke pvmbg-mock supaya tidak bergantung pada forward luar yang bisa putus di tengah uji."""
    global _pf
    if _pf and _pf.poll() is None:
        return
    _pf = subprocess.Popen(["kubectl", "-n", NS, "port-forward", "svc/pvmbg-mock", "18183:8082"], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
    p3.PVMBG = "http://127.0.0.1:18183"
    time.sleep(2)


def pvmbg_outage(enabled):
    last = None
    for _ in range(5):
        try:
            pvmbg_forward()
            code, res = p1.http_json("POST", p3.PVMBG + "/admin/outage", {"Authorization": "Bearer " + os.environ["PVMBG_TOKEN"]},
                                     {"enabled": enabled}, timeout=10)
            return code
        except Exception as e:  # forward putus: pasang ulang dan coba lagi
            last = e
            if _pf:
                _pf.terminate()
            time.sleep(2)
    raise last


class FieldProbe(threading.Thread):
    """Memantau jalur peringatan Tim Lapangan: baca kejadian (GET) tiap detik dan kirim laporan lapangan (POST) tiap 5 detik."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop = threading.Event()
        self.rows = []  # (t, jenis, kode, detik)
        self.cid, self.secret = os.environ["AUTH_CLIENT_FIELD_ID"], os.environ["AUTH_CLIENT_FIELD_SECRET"]

    def run(self):
        tok, tok_at, n = "", 0.0, 0
        while not self.stop.is_set():
            t0 = time.time()
            try:
                if not tok or time.time() - tok_at > 40:
                    tok, tok_at = p3.token(self.cid, self.secret), time.time()
                conn = http.client.HTTPConnection("127.0.0.1", 80, timeout=10)
                conn.request("GET", "/hazard-events?limit=5", headers={"Host": "api.prod.localtest.me", "Authorization": "Bearer " + tok})
                resp = conn.getresponse()
                resp.read()
                code = resp.status
                self.rows.append((t0, "baca", code, time.time() - t0))
                if n % 5 == 0:
                    t1 = time.time()
                    conn = http.client.HTTPConnection("127.0.0.1", 80, timeout=10)
                    conn.request("POST", "/field-reports", body=json.dumps({"kind": "gameday", "n": n}),
                                 headers={"Host": "api.prod.localtest.me", "Authorization": "Bearer " + tok,
                                          "Content-Type": "application/json"})
                    resp = conn.getresponse()
                    resp.read()
                    self.rows.append((t1, "tulis", resp.status, time.time() - t1))
            except Exception:
                self.rows.append((t0, "baca", 0, time.time() - t0))
                tok = ""
            n += 1
            self.stop.wait(max(0.0, 1.0 - (time.time() - t0)))


def pods_not_ready():
    out = kubectl("get", "pods", "--no-headers")
    bad = []
    for ln in out.splitlines():
        f = ln.split()
        if len(f) < 3:
            continue
        a, b = f[1].split("/")
        if f[2] == "Completed":
            continue
        if a != b or f[2] != "Running":
            bad.append(f[0])
    return bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--recovery", type=int, default=240)
    args = ap.parse_args()
    token = os.environ.get("DASHBOARD_TOKEN", "")
    c = p1.Client()
    sensor_pod = kubectl("get", "pod", "-l", "app.kubernetes.io/name=sensor-pantai", "-o", "jsonpath={.items[0].metadata.name}").strip()
    sensor_cid = p2.container_id(sensor_pod, "sensor-pantai")
    coastal0 = len(p5_events(c))
    probe = FieldProbe()
    timeline = []

    def mark(what):
        timeline.append((round(time.time() - T0, 1), what))
        print(f"[{timeline[-1][0]:>6}s] {what}", flush=True)

    with p2.Load("ops.prod.localtest.me", token, 8):
        T0 = time.time()
        probe.start()
        mark("mulai: beban sumber lain dan pemantauan jalur Tim Lapangan berjalan")
        try:
            time.sleep(30)
            mark("GANGGUAN 1: Sensor Pantai dibekukan 60 dtk")
            p2.ctr("pause", sensor_cid)
            time.sleep(30)
            mark("GANGGUAN 2: PVMBG outage 60 dtk (bersamaan dengan sensor beku)")
            pvmbg_outage(True)
            time.sleep(30)
            p2.ctr("resume", sensor_cid)
            mark("sensor dilepas (akhir gangguan 1)")
            time.sleep(30)
            pvmbg_outage(False)
            mark("PVMBG pulih (akhir gangguan 2)")
            time.sleep(30)
            mark("GANGGUAN 3: Tester Agent membunuh pod penyimpanan (kill-store)")
            r = subprocess.run(["kubectl", "exec", "-n", "tester-agent", "daemonset/tester-agent", "--", "/app", "run", "practice", "kill-store"],
                               capture_output=True, text=True, timeout=600)
            mark(f"skenario kill-store selesai (kode {r.returncode})")
            t_last_fault = time.time()
            mark(f"JENDELA PEMULIHAN {args.recovery} dtk: tidak ada intervensi")
            time.sleep(args.recovery)
        except BaseException:
            # hanya bila skrip gagal di tengah: jangan tinggalkan sensor beku atau PVMBG mati
            for fn in (lambda: p2.ctr("resume", sensor_cid), lambda: pvmbg_outage(False)):
                try:
                    fn()
                except Exception:
                    pass
            raise
        mark("jendela pemulihan selesai")
        probe.stop.set()
        probe.join(timeout=15)

    rows = list(probe.rows)
    ok_codes = lambda code: 200 <= code < 300 or code == 429
    total = len(rows)
    fail = [r for r in rows if not ok_codes(r[2])]
    avail = 1 - len(fail) / total if total else 0
    lat = sorted(r[3] for r in rows)
    p95 = lat[int(0.95 * (len(lat) - 1))] if lat else None
    after = [r for r in rows if r[0] >= t_last_fault + 30]
    after_fail = [r for r in after if not ok_codes(r[2])]
    budget = (1 - SLO_AVAILABILITY)
    used = (len(fail) / total) / budget if total else None
    bad_pods = pods_not_ready()
    dist = {}
    try:
        dist = c.audit_get("/internal/audit/distribution")
    except Exception:
        pass
    coastal1 = len(p5_events(c))
    print(f"\npermintaan Tim Lapangan: {total}; gagal: {len(fail)}; ketersediaan {avail * 100:.2f}% (target {SLO_AVAILABILITY * 100:.1f}%)")
    print(f"p95 latensi: {p95:.3f} dtk (target {SLO_P95_S} dtk); anggaran kesalahan terpakai: {used * 100:.0f}% dari {budget * 100:.1f}%")
    print(f"sesudah pemulihan 30 dtk pertama: {len(after)} permintaan, gagal {len(after_fail)}")
    print(f"pod belum siap sesudah jendela: {bad_pods or 'tidak ada'}; under_replicated={dist.get('under_replicated')} partial={dist.get('partial')}")
    print(f"event COASTAL tersimpan {coastal0} -> {coastal1} (sensor dipakai lagi: {coastal1 > coastal0})")
    ok = bool(total and avail >= SLO_AVAILABILITY and p95 is not None and p95 <= SLO_P95_S and not after_fail
              and not bad_pods and coastal1 > coastal0)
    result = {"garis_waktu": timeline, "permintaan": total, "gagal": len(fail), "ketersediaan": round(avail, 5),
              "slo_ketersediaan": SLO_AVAILABILITY, "p95_dtk": p95, "slo_p95_dtk": SLO_P95_S, "anggaran_terpakai": used,
              "gagal_sesudah_pulih": len(after_fail), "pod_belum_siap": bad_pods, "under_replicated": dist.get("under_replicated"),
              "coastal": [coastal0, coastal1],
              "kode_gagal": sorted({r[2] for r in fail})}
    d = p1.out_dir(argparse.Namespace(out=os.environ.get("OUT_DIR")))
    result["lolos"] = ok
    p1.save(d, "p5_gameday.json", result)
    print(f"\nHASIL gameday: {'LOLOS' if ok else 'GAGAL'}")
    sys.exit(0 if ok else 1)


def p5_events(c):
    out, cursor = {}, ""
    while True:
        page = c.audit_get("/internal/audit/events?source=SENSOR_PANTAI&limit=1000" + (f"&cursor={cursor}" if cursor else ""))
        for e in page["items"]:
            out[e["source_ref_id"]] = 1
        cursor = page.get("next_cursor")
        if not cursor:
            return out


if __name__ == "__main__":
    main()
