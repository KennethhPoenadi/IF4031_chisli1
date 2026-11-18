#!/usr/bin/env python3
"""Bukti Problem 2 (dua nakhoda, satu kapal) yang dapat dijalankan pada satu cluster.

Subperintah (tiap subperintah menyimpan JSON bukti dan mengembalikan kode keluar 0 bila lolos):

  once      Kriteria 2: tiap sumber diambil tepat sekali. Laju permintaan ke mock sama dengan satu instance
            (bukan dikali jumlah replika) dan ID event BMKG/PVMBG berurutan tanpa duplikat maupun celah.
  failover  Kriteria 3: leader dimatikan paksa; instance lain melanjutkan <= 60 detik, tanpa celah dan duplikat.
  freeze    Kriteria 5: container leader dibekukan melewati lease lalu dilepas. Leader lama tidak boleh menyimpan
            apa pun sesudah bangun: laju permintaan tetap satu instance dan data tetap tanpa duplikat/celah.
  rolling   Kriteria 8: ganti versi Aggregator (rolling restart) saat beban berjalan; tidak boleh ada permintaan gagal.

Lingkungan: KUBECONFIG, AGG_URL (Audit API), AUDIT_TOKEN, DASHBOARD_TOKEN. Subperintah freeze memakai
`docker exec <node kind>` untuk membekukan container, sehingga hanya berlaku untuk cluster kind lokal.
Membunuh atau membekukan leader menghentikan polling sesaat: aman, kursor tersimpan.

  python3 tests/p2/p2.py once
  python3 tests/p2/p2.py failover
"""
import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import http.client
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "p1"))
import p1  # noqa: E402

NS = os.environ.get("APP_NAMESPACE", "sistem-informasi-kebencanaan-bnpb")
LEASE = "aggregator-upstream-poller"
KIND_NODE = os.environ.get("KIND_NODE", "xops-control-plane")
MOCKS = {"BMKG": ("bmkg-mock", "seismic-events"), "PVMBG": ("pvmbg-mock", "volcanic-reports")}
LEASE_SECONDS = 15


def sh(*cmd, check=True, timeout=300):
    r = subprocess.run(list(cmd), capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise SystemExit(f"{' '.join(cmd)} gagal: {r.stderr.strip()}")
    return r.stdout.strip()


def kubectl(*args, check=True):
    return sh("kubectl", "-n", NS, *args, check=check)


def lease():
    raw = kubectl("get", "lease", LEASE, "-o", "json", check=False)
    if not raw:
        return None
    s = json.loads(raw)["spec"]
    return {"holder": s.get("holderIdentity"), "transitions": s.get("leaseTransitions", 0)}


def aggregator_pods():
    return kubectl("get", "pods", "-l", "app.kubernetes.io/name=aggregator", "-o",
                   "jsonpath={.items[*].metadata.name}").split()


def mock_requests(since_seconds):
    """Waktu setiap permintaan polling ke mock (dari log mock), per sumber."""
    out = {}
    for src, (app, path) in MOCKS.items():
        times = []
        for pod in kubectl("get", "pods", "-l", f"app.kubernetes.io/name={app}", "-o",
                           "jsonpath={.items[*].metadata.name}").split():
            for line in kubectl("logs", pod, f"--since={since_seconds}s", check=False).splitlines():
                if f'"{"/" + path}"' in line or f"/{path}" in line:
                    m = re.search(r'"timestamp":"([^"]+)"', line)
                    if m:
                        times.append(datetime.fromisoformat(m.group(1).replace("Z", "+00:00")))
        out[src] = sorted(times)
    return out


def refs(c, source):
    items, cursor = [], ""
    while True:
        page = c.audit_get(f"/internal/audit/events?source={source}&limit=1000" + (f"&cursor={cursor}" if cursor else ""))
        items += [e["source_ref_id"] for e in page["items"]]
        cursor = page.get("next_cursor")
        if not cursor:
            return items


def continuity(ids):
    """Periksa ID berurutan (prefix-NNNNNN): tanpa duplikat dan tanpa celah di antara terkecil dan terbesar."""
    nums = []
    for i in ids:
        m = re.search(r"(\d+)$", i)
        if m:
            nums.append(int(m.group(1)))
    if not nums:
        return {"jumlah": 0, "duplikat": 0, "celah": 0}
    uniq = set(nums)
    missing = [n for n in range(min(uniq), max(uniq) + 1) if n not in uniq]
    return {"jumlah": len(nums), "unik": len(uniq), "duplikat": len(nums) - len(uniq), "celah": len(missing),
            "contoh_celah": missing[:10], "rentang": [min(uniq), max(uniq)]}


def rate_per_minute(times, seconds):
    return round(len(times) * 60.0 / seconds, 1) if seconds else 0


def report(name, result, ok):
    result["lolos"] = ok
    d = p1.out_dir(argparse.Namespace(out=os.environ.get("OUT_DIR")))
    p1.save(d, f"p2_{name}.json", result)
    print(f"\nHASIL {name}: {'LOLOS' if ok else 'GAGAL'}\nbukti disimpan di {d}")
    return 0 if ok else 1


def cmd_once(args):
    c = p1.Client()
    pods = aggregator_pods()
    holder = (lease() or {}).get("holder")
    print(f"replika aggregator: {len(pods)}, leader: {holder}")
    print(f"mengukur {args.seconds} detik permintaan ke mock...")
    time.sleep(args.seconds)
    reqs = mock_requests(args.seconds)
    rates = {s: rate_per_minute(t, args.seconds) for s, t in reqs.items()}
    cont = {s: continuity(refs(c, s)) for s in MOCKS}
    # Laju satu instance = 60 / interval polling. Bila semua replika ikut polling, lajunya dikali jumlah replika.
    interval = float(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
    single = 60.0 / interval
    ok = all(rates[s] <= single * 1.5 for s in rates) and all(v["duplikat"] == 0 and v["celah"] == 0 for v in cont.values())
    print(f"laju permintaan per menit: {rates} (satu instance = {single:.0f}, {len(pods)} instance = {single * len(pods):.0f})")
    print("kesinambungan ID:", {s: (v["jumlah"], f"duplikat {v['duplikat']}", f"celah {v['celah']}") for s, v in cont.items()})
    return report("once", {"replika": len(pods), "leader": holder, "laju_per_menit": rates,
                           "laju_satu_instance": single, "kesinambungan": cont}, ok)


def wait_for(fn, timeout, step=0.5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return None


def next_request_after(t0, timeout):
    """Detik sejak t0 sampai permintaan polling berikutnya muncul di mock mana pun."""
    def seen():
        reqs = mock_requests(int(time.time() - t0.timestamp()) + 5)
        later = [t for ts in reqs.values() for t in ts if t > t0]
        return min(later) if later else None
    got = wait_for(seen, timeout, step=1.0)
    return (got - t0).total_seconds() if got else None


def cmd_failover(args):
    c = p1.Client()
    before = {s: continuity(refs(c, s)) for s in MOCKS}
    l0 = lease()
    if not l0 or not l0["holder"]:
        raise SystemExit("tidak ada leader")
    print(f"leader awal: {l0['holder']} (transisi {l0['transitions']})")
    t_kill = datetime.now(timezone.utc)
    kubectl("delete", "pod", l0["holder"], "--grace-period=0", "--force", "--wait=false")
    print("leader dimatikan paksa (tanpa shutdown bersih)")
    new = wait_for(lambda: (lambda l: l if l and l["holder"] and l["holder"] != l0["holder"] else None)(lease()), args.timeout)
    t_lead = (datetime.now(timezone.utc) - t_kill).total_seconds() if new else None
    gap = next_request_after(t_kill, args.timeout) if new else None
    print(f"leader baru: {new['holder'] if new else '-'} setelah {t_lead:.1f} dtk" if new else "tidak ada leader baru")
    print(f"permintaan polling berikutnya ke mock: {gap:.1f} dtk setelah kill" if gap else "polling tidak berlanjut")
    time.sleep(args.settle)
    after = {s: continuity(refs(c, s)) for s in MOCKS}
    ok = bool(new and t_lead is not None and gap is not None and gap <= 60
              and all(v["duplikat"] == 0 and v["celah"] == 0 for v in after.values()))
    print("kesinambungan ID sesudah:", {s: (v["jumlah"], f"duplikat {v['duplikat']}", f"celah {v['celah']}") for s, v in after.items()})
    return report("failover", {"leader_awal": l0, "leader_baru": new, "detik_sampai_leader_baru": t_lead,
                               "detik_sampai_polling_berlanjut": gap, "batas_detik": 60,
                               "kesinambungan_sebelum": before, "kesinambungan_sesudah": after}, ok)


def container_id(pod):
    raw = sh("docker", "exec", KIND_NODE, "crictl", "ps", "--name", "aggregator", "-o", "json")
    for ctr in json.loads(raw)["containers"]:
        if ctr["labels"].get("io.kubernetes.pod.name") == pod:
            return ctr["id"]
    raise SystemExit(f"container aggregator untuk pod {pod} tidak ditemukan")


def ctr(action, cid):
    return sh("docker", "exec", KIND_NODE, "ctr", "-n", "k8s.io", "tasks", action, cid, check=False)


def cmd_freeze(args):
    c = p1.Client()
    l0 = lease()
    cid = container_id(l0["holder"])
    print(f"leader: {l0['holder']} (container {cid[:12]}), transisi {l0['transitions']}")
    ctr("pause", cid)
    t_freeze = time.time()
    print(f"container dibekukan, menunggu {args.freeze_seconds} dtk (> lease {LEASE_SECONDS} dtk)...")
    new = wait_for(lambda: (lambda l: l if l and l["holder"] != l0["holder"] else None)(lease()), args.freeze_seconds)
    time.sleep(max(0, args.freeze_seconds - (time.time() - t_freeze)))
    ctr("resume", cid)
    print("container dilepas; mengukur 60 dtk sesudah bangun...")
    time.sleep(60)
    final = lease()
    reqs = mock_requests(60)
    interval = float(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
    rates = {s: rate_per_minute(t, 60) for s, t in reqs.items()}
    after = {s: continuity(refs(c, s)) for s in MOCKS}
    logs = kubectl("logs", l0["holder"], "--since=3m", check=False)
    lost = len(re.findall(r"leadership lost|kepemimpinan hilang|not leader|fenced", logs, re.I))
    ok = bool(new and final and final["holder"] != l0["holder"]
              and all(r <= (60.0 / interval) * 1.5 for r in rates.values())
              and all(v["duplikat"] == 0 and v["celah"] == 0 for v in after.values()))
    print(f"leader sesudah bangun: {final['holder'] if final else '-'} (leader lama {l0['holder']})")
    print(f"laju permintaan per menit setelah bangun: {rates} (satu instance = {60.0 / interval:.0f})")
    print("kesinambungan ID:", {s: (v["jumlah"], f"duplikat {v['duplikat']}", f"celah {v['celah']}") for s, v in after.items()})
    return report("freeze", {"leader_lama": l0, "leader_saat_beku": new, "leader_sesudah": final,
                             "laju_per_menit_sesudah_bangun": rates, "kesinambungan": after,
                             "baris_log_leader_lama_terkait_kehilangan_kepemimpinan": lost}, ok)


class Load:
    def __init__(self, host, token, workers):
        self.host, self.token, self.workers = host, token, workers
        self.stop, self.lock, self.codes, self.lat = threading.Event(), threading.Lock(), {}, []

    def _run(self):
        conn = None
        while not self.stop.is_set():
            t = time.monotonic()
            try:
                if conn is None:
                    conn = http.client.HTTPConnection("127.0.0.1", 80, timeout=10)
                conn.request("GET", "/dashboard/api/summary?range=1h", headers={
                    "Host": self.host, "Authorization": "Bearer " + self.token})
                resp = conn.getresponse()
                resp.read()
                code = resp.status
            except Exception:
                code, conn = 0, None
            with self.lock:
                self.codes[code] = self.codes.get(code, 0) + 1
                self.lat.append(time.monotonic() - t)
            time.sleep(0.02)

    def __enter__(self):
        self.threads = [threading.Thread(target=self._run, daemon=True) for _ in range(self.workers)]
        for t in self.threads:
            t.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        for t in self.threads:
            t.join(timeout=15)


def cmd_rolling(args):
    token = os.environ.get("DASHBOARD_TOKEN", "")
    if not token:
        raise SystemExit("DASHBOARD_TOKEN wajib")
    old = kubectl("get", "deploy", "aggregator", "-o", "jsonpath={.spec.template.spec.containers[0].image}")
    with Load("ops.prod.localtest.me", token, args.workers) as ld:
        time.sleep(10)
        print("beban berjalan; memulai rolling restart aggregator...")
        t0 = time.monotonic()
        kubectl("rollout", "restart", "deploy/aggregator")
        kubectl("rollout", "status", "deploy/aggregator", "--timeout=600s")
        took = time.monotonic() - t0
        print(f"rollout selesai dalam {took:.0f} dtk; beban dipertahankan 20 dtk lagi...")
        time.sleep(20)
    total = sum(ld.codes.values())
    bad = sum(n for code, n in ld.codes.items() if code != 200)
    lat = sorted(ld.lat)
    p95 = lat[int(0.95 * (len(lat) - 1))] if lat else None
    print(f"{total} permintaan, kode {ld.codes}, p95 {p95:.3f} dtk")
    return report("rolling", {"image": old, "detik_rollout": round(took, 1), "permintaan": total, "kode": ld.codes,
                              "gagal": bad, "p95_detik": round(p95, 3) if p95 else None}, bad == 0 and total > 0)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    o = sub.add_parser("once"); o.add_argument("--seconds", type=int, default=90)
    f = sub.add_parser("failover"); f.add_argument("--timeout", type=int, default=120); f.add_argument("--settle", type=int, default=30)
    z = sub.add_parser("freeze"); z.add_argument("--freeze-seconds", type=int, default=40)
    r = sub.add_parser("rolling"); r.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()
    sys.exit({"once": cmd_once, "failover": cmd_failover, "freeze": cmd_freeze, "rolling": cmd_rolling}[args.cmd](args))


if __name__ == "__main__":
    main()
