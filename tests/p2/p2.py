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
pod pembeku (privileged, hostPID) pada node leader yang mengirim SIGSTOP/SIGCONT ke proses container. FREEZE_MODE=kind memakai `docker exec` pada node kind.
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
        # Hanya ID yang diambil dari mock (bmkg-NNNNNN, pvmbg-NNNNNN). Event sintetis dari tes lain (katalog, lonjakan,
        # uji kehilangan unit) memakai prefix sendiri dan penomorannya bertabrakan, bukan tanda polling ganda.
        items += [e["source_ref_id"] for e in page["items"]
                  if re.match(rf"^{re.escape(source.lower())}-\d+$", e["source_ref_id"])]
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


FREEZER_NS = os.environ.get("FREEZER_NAMESPACE", "default")
_freezer = {"pod": None}


def container_id(pod, cname="aggregator"):
    """ID container aggregator milik pod. Pada k3s node-nya bisa mana saja, jadi pod pembeku dipasang di node itu."""
    if os.environ.get("FREEZE_MODE") == "kind":
        raw = sh("docker", "exec", KIND_NODE, "crictl", "ps", "--name", "aggregator", "-o", "json")
        for c in json.loads(raw)["containers"]:
            if c["labels"].get("io.kubernetes.pod.name") == pod:
                return c["id"]
        raise SystemExit(f"container aggregator untuk pod {pod} tidak ditemukan")
    info = json.loads(kubectl("get", "pod", pod, "-o", "json"))
    cid = next((c["containerID"].split("://")[-1] for c in info["status"]["containerStatuses"] if c["name"] == cname), "")
    if not cid:
        raise SystemExit(f"container aggregator untuk pod {pod} tidak ditemukan")
    node = info["spec"]["nodeName"]
    name = f"freezer-{node}"[:50]
    sh("kubectl", "-n", FREEZER_NS, "delete", "pod", name, "--now", "--ignore-not-found", check=False)
    overrides = json.dumps({"spec": {"nodeName": node, "hostPID": True, "terminationGracePeriodSeconds": 0, "containers": [{
        "name": "freezer", "image": "busybox:1.36", "command": ["sleep", "900"], "securityContext": {"privileged": True}}]}})
    sh("kubectl", "-n", FREEZER_NS, "run", name, "--image=busybox:1.36", "--restart=Never", f"--overrides={overrides}")
    sh("kubectl", "-n", FREEZER_NS, "wait", "--for=condition=Ready", f"pod/{name}", "--timeout=120s")
    _freezer["pod"] = name
    print(f"pod pembeku {name} di node {node}")
    return cid


def ctr(action, cid):
    """pause/resume: kirim SIGSTOP/SIGCONT ke semua proses di cgroup container (setara membekukan container)."""
    if os.environ.get("FREEZE_MODE") == "kind":
        return sh("docker", "exec", KIND_NODE, "ctr", "-n", "k8s.io", "tasks", action, cid, check=False)
    sig = "STOP" if action == "pause" else "CONT"
    script = (f"n=0; for f in $(grep -l {cid} /proc/[0-9]*/cgroup 2>/dev/null); do p=$(echo $f | cut -d/ -f3); "
              f"kill -{sig} $p && n=$((n+1)); done; echo proses=$n")
    out = sh("kubectl", "-n", FREEZER_NS, "exec", _freezer["pod"], "--", "sh", "-c", script, check=False)
    print(f"  {action}: {out}")
    return out


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
        self.events = []  # (waktu kirim, kode); permintaan yang sudah terkirim ke pod beku sebelum dikeluarkan boleh gagal, yang dikirim sesudahnya tidak
        self.errors = []  # (waktu kirim, jenis galat) untuk permintaan tanpa respons HTTP (kode 0)

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
            except Exception as e:
                code, conn = 0, None
                with self.lock:
                    self.errors.append((time.time() - (time.monotonic() - t), type(e).__name__))
            with self.lock:
                self.codes[code] = self.codes.get(code, 0) + 1
                self.lat.append(time.monotonic() - t)
                self.events.append((time.time() - (time.monotonic() - t), code))  # waktu permintaan DIKIRIM
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


def cmd_version(args):
    """Kriteria 8: versi Aggregator diganti selama load test, lewat jalur deploy sungguhan (push aplikasi -> CI membangun image
    dengan tag baru -> CI menulis tag ke repo infra -> Argo CD melakukan RollingUpdate). Tes tidak mengganti image sendiri; ia
    menjalankan beban, menunggu image Deployment berubah, lalu menunggu rollout selesai. Lolos bila tidak ada permintaan gagal."""
    token = os.environ.get("DASHBOARD_TOKEN", "")
    if not token:
        raise SystemExit("DASHBOARD_TOKEN wajib")
    image = lambda: kubectl("get", "deploy", "aggregator", "-o", "jsonpath={.spec.template.spec.containers[0].image}")
    old = image()
    print(f"versi aggregator saat mulai: {old}")
    new, took = old, None
    with Load("ops.prod.localtest.me", token, args.workers) as ld:
        time.sleep(10)
        print("beban berjalan; menunggu versi baru dari CI dan Argo CD (push commit aplikasi sekarang)...")
        t_wait = time.monotonic()
        last_refresh = 0.0
        while new == old and time.monotonic() - t_wait < args.wait:
            if time.monotonic() - last_refresh > 30:
                # Argo CD memeriksa repo tiap ~3 menit; refresh hanya mempercepat deteksi commit tag, bukan mengubah isi deploy.
                sh("kubectl", "-n", "argocd", "annotate", "application", "aggregator-prod",
                   "argocd.argoproj.io/refresh=normal", "--overwrite", check=False)
                last_refresh = time.monotonic()
            time.sleep(5)
            new = image()
        if new == old:
            print(f"versi tidak berubah dalam {args.wait} dtk")
        else:
            t_change = time.time()
            print(f"versi baru terdeteksi setelah {time.monotonic() - t_wait:.0f} dtk menunggu: {new}")
            t0 = time.monotonic()
            kubectl("rollout", "status", "deploy/aggregator", "--timeout=900s")
            took = time.monotonic() - t0
            t_done = time.time()
            print(f"rollout versi baru selesai dalam {took:.0f} dtk; beban dipertahankan 20 dtk lagi...")
            time.sleep(20)
    total = sum(ld.codes.values())
    bad = sum(n for code, n in ld.codes.items() if code != 200)
    lat = sorted(ld.lat)
    p95 = lat[int(0.95 * (len(lat) - 1))] if lat else None
    during = [code for t, code in ld.events if took is not None and t_change <= t <= t_done]
    pods = kubectl("get", "pods", "-l", "app.kubernetes.io/name=aggregator",
                   "-o", "jsonpath={range .items[*]}{.metadata.name} {.spec.containers[0].image}{\"\\n\"}{end}")
    print(f"{total} permintaan selama uji, kode {ld.codes}, p95 {p95:.3f} dtk")
    print(f"selama rollout versi baru: {len(during)} permintaan, gagal {sum(1 for c in during if c != 200)}")
    fails = [(t, f"HTTP {c}") for t, c in ld.events if c not in (0, 200)] + list(ld.errors)
    for t, kind in sorted(fails):
        print(f"   gagal {time.strftime('%H:%M:%S', time.localtime(t))}  {kind}")
    print("pod aggregator sesudah uji:")
    print(pods)
    ok = new != old and bad == 0 and total > 0
    return report("version", {"image_lama": old, "image_baru": new, "detik_rollout": round(took, 1) if took else None,
                              "permintaan": total, "kode": ld.codes, "gagal": bad,
                              "permintaan_selama_rollout": len(during),
                              "gagal_selama_rollout": sum(1 for c in during if c != 200),
                              "p95_detik": round(p95, 3) if p95 else None, "pod_sesudah": pods.split("\n"),
                              "rincian_gagal": [[time.strftime("%H:%M:%S", time.localtime(t)), k] for t, k in sorted(fails)]}, ok)


def ready_endpoints():
    """IP pod aggregator yang siap menerima lalu lintas menurut Endpoints Service (yang dipakai titik masuk)."""
    raw = kubectl("get", "endpoints", "aggregator", "-o", "jsonpath={.subsets[*].addresses[*].ip}", check=False)
    return set(raw.split())


def cmd_unhealthy(args):
    """Kriteria 1: >= 3 instance di node berbeda di belakang satu titik masuk yang berhenti mengarahkan permintaan ke instance
    yang tidak sehat. Satu pod non-leader dibekukan (readiness probe gagal) selama beban lewat ingress berjalan."""
    token = os.environ.get("DASHBOARD_TOKEN", "")
    if not token:
        raise SystemExit("DASHBOARD_TOKEN wajib")
    info = json.loads(kubectl("get", "pods", "-l", "app.kubernetes.io/name=aggregator", "-o", "json"))["items"]
    placement = {p["metadata"]["name"]: (p["spec"]["nodeName"], p["status"].get("podIP", "")) for p in info}
    nodes = sorted({n for n, _ in placement.values()})
    print(f"instance aggregator: {len(placement)} di node {nodes}")
    for name, (node, ip) in sorted(placement.items()):
        print(f"   {name}  {node}  {ip}")
    before = ready_endpoints()
    print(f"endpoint siap di titik masuk sebelum uji: {len(before)}")
    holder = (lease() or {}).get("holder")
    victim = next((n for n in sorted(placement) if n != holder), None)
    if victim is None:
        raise SystemExit("tidak ada pod non-leader")
    v_node, v_ip = placement[victim]
    print(f"pod yang dibekukan: {victim} ({v_node}, {v_ip}); leader {holder} tidak disentuh")
    ok_layout = len(placement) >= 3 and len(nodes) >= 2 and v_ip in before

    with Load("ops.prod.localtest.me", token, args.workers) as ld:
        time.sleep(10)
        base_total = len(ld.events)
        cid = container_id(victim)
        ctr("pause", cid)
        t_freeze = time.time()
        print(f"{victim} dibekukan; memantau Endpoints sampai pod itu dikeluarkan...")
        t_removed = None
        while time.time() - t_freeze < args.timeout:
            if v_ip not in ready_endpoints():
                t_removed = time.time()
                break
            time.sleep(0.5)
        if t_removed:
            print(f"pod dikeluarkan dari endpoint titik masuk setelah {t_removed - t_freeze:.1f} dtk")
            time.sleep(args.after)
        ctr("resume", cid)
        t_resume = time.time()
        back = False
        for _ in range(int(args.timeout * 2)):
            if v_ip in ready_endpoints():
                back = True
                break
            time.sleep(0.5)
        print(f"pod dilepas; kembali ke endpoint: {'ya' if back else 'tidak'} ({time.time() - t_resume:.1f} dtk)")
        time.sleep(5)

    ev = list(ld.events)
    def bad(rows):
        return sum(1 for _, c in rows if c != 200)
    window_before = [e for e in ev if e[0] < t_freeze]
    window_gap = [e for e in ev if t_removed and t_freeze <= e[0] < t_removed + 2]
    window_after = [e for e in ev if t_removed and t_removed + 2 <= e[0] < (t_removed + args.after)]
    print(f"permintaan sebelum pembekuan: {len(window_before)}, gagal {bad(window_before)}")
    print(f"permintaan antara pembekuan dan pod dikeluarkan (+2 dtk): {len(window_gap)}, gagal {bad(window_gap)}")
    print(f"permintaan setelah pod dikeluarkan ({args.after} dtk): {len(window_after)}, gagal {bad(window_after)}")
    ok = bool(ok_layout and t_removed and (t_removed - t_freeze) <= 60 and window_after and bad(window_after) == 0 and back)
    return report("unhealthy", {
        "instance": len(placement), "node": nodes, "pod_dibekukan": victim, "node_pod_dibekukan": v_node,
        "detik_sampai_dikeluarkan": round(t_removed - t_freeze, 1) if t_removed else None,
        "gagal_sebelum": bad(window_before), "permintaan_sebelum": len(window_before),
        "gagal_antara": bad(window_gap), "permintaan_antara": len(window_gap),
        "gagal_sesudah_dikeluarkan": bad(window_after), "permintaan_sesudah_dikeluarkan": len(window_after),
        "kembali_ke_endpoint": back, "kode_semua": ld.codes}, ok)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    o = sub.add_parser("once"); o.add_argument("--seconds", type=int, default=90)
    f = sub.add_parser("failover"); f.add_argument("--timeout", type=int, default=120); f.add_argument("--settle", type=int, default=30)
    z = sub.add_parser("freeze"); z.add_argument("--freeze-seconds", type=int, default=40)
    r = sub.add_parser("rolling"); r.add_argument("--workers", type=int, default=8)
    v = sub.add_parser("version"); v.add_argument("--workers", type=int, default=8)
    v.add_argument("--wait", type=int, default=1500, help="batas menunggu versi baru (detik)")
    u = sub.add_parser("unhealthy"); u.add_argument("--workers", type=int, default=8); u.add_argument("--timeout", type=int, default=90); u.add_argument("--after", type=int, default=25)
    args = ap.parse_args()
    sys.exit({"once": cmd_once, "failover": cmd_failover, "freeze": cmd_freeze, "rolling": cmd_rolling, "version": cmd_version, "unhealthy": cmd_unhealthy}[args.cmd](args))


if __name__ == "__main__":
    main()
