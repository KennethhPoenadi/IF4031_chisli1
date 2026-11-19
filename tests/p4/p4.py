#!/usr/bin/env python3
"""P4 (Atap Seng di Siang Bolong): bukti pada cluster k3s 2 node.

  python3 tests/p4/p4.py live --menit 5        aliran feed live diproses; offset maju, kebakaran WILDFIRE terbentuk, lag terpantau
  python3 tests/p4/p4.py crash                 pemroses (satellite-ingestor) dibunuh saat aliran berjalan; setelah pulih tanpa
                                               kebakaran hilang/ganda dan offset melanjutkan dari checkpoint (kriteria 5 dan 8)
  python3 tests/p4/p4.py reference FILE        bandingkan WILDFIRE tersimpan dengan hasil referensi asisten (kriteria 6)

Lingkungan: KUBECONFIG, AGG_URL (port-forward aggregator, bawaan http://localhost:8090), AUDIT_TOKEN, AGGREGATOR_API_KEY.
"""
import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "p1"))
import p1  # noqa: E402

NS = os.environ.get("APP_NAMESPACE", "sistem-informasi-kebencanaan-bnpb")
SOURCE = "SATELIT"


def kubectl(*args, check=True):
    r = subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True, timeout=120)
    if check and r.returncode:
        raise SystemExit(f"kubectl {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout


def ingestor_pod(exclude=""):
    """Nama pod pemroses yang aktif (bukan yang sedang dihentikan, bukan `exclude`)."""
    items = json.loads(kubectl("get", "pods", "-l", "app.kubernetes.io/name=satellite-ingestor", "-o", "json", check=False) or "{}").get("items", [])
    for p in items:
        if not p["metadata"].get("deletionTimestamp") and p["metadata"]["name"] != exclude:
            return p["metadata"]["name"]
    return ""


def ingestor_health(exclude=""):
    """Keadaan pemroses lewat port-forward ke pod (jalur kubelet, tetap jalan saat pod ada di node worker); None bila belum menjawab."""
    import urllib.request
    pod = ingestor_pod(exclude)
    if not pod:
        return None
    pf = subprocess.Popen(["kubectl", "-n", NS, "port-forward", f"pod/{pod}", "18095:8095"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(10):
            time.sleep(0.5)
            try:
                with urllib.request.urlopen("http://127.0.0.1:18095/health", timeout=5) as r:
                    return json.loads(r.read())
            except Exception:
                continue
        return None
    finally:
        pf.terminate()
        pf.wait()


def fires(c):
    """Semua HazardEvent WILDFIRE tersimpan: {source_ref_id: event}."""
    out, cursor = {}, ""
    while True:
        page = c.audit_get(f"/internal/audit/events?source={SOURCE}&limit=1000" + (f"&cursor={cursor}" if cursor else ""))
        for e in page["items"]:
            out[e["source_ref_id"]] = e
        cursor = page.get("next_cursor")
        if not cursor:
            return out


def dk(e):
    return set((e.get("attributes") or {}).get("detection_keys") or [])


def lost(before, after):
    """Kebakaran sebelumnya yang deteksinya tidak ada di kebakaran mana pun sesudahnya. ID berganti saat kebakaran menyatu, jadi
    yang dihitung adalah deteksi. Engine juga sah menghapus deteksi (update feed menurunkan confidence jadi low, penghapusan feed)."""
    covered = set().union(*(dk(e) for e in after.values())) if after else set()
    return sorted(sid for sid, e in before.items() if sid not in after and dk(e) - covered)


def restarts():
    pods = json.loads(kubectl("get", "pods", "-o", "json"))["items"]
    return {p["metadata"]["name"]: sum(s["restartCount"] for s in (p["status"].get("containerStatuses") or [])) for p in pods}


def report(name, result, ok):
    result["lolos"] = ok
    d = p1.out_dir(argparse.Namespace(out=os.environ.get("OUT_DIR")))
    p1.save(d, f"p4_{name}.json", result)
    print(f"\nHASIL {name}: {'LOLOS' if ok else 'GAGAL'}")
    print("bukti disimpan di", d)
    return 0 if ok else 1


def cmd_live(args):
    c = p1.Client()
    samples = []
    t_end = time.time() + args.menit * 60
    print(f"memantau aliran feed live selama {args.menit} menit (tiap {args.interval} dtk)")
    print(f"{'waktu':>8} {'ready':>5} {'offset':>9} {'terbaru':>9} {'tertinggal':>10} {'kebakaran':>9}  p95 latensi")
    t0 = time.time()
    while time.time() < t_end:
        h = ingestor_health() or {}
        f = fires(c)
        st = h.get("stats") or {}
        committed, latest = h.get("committed_offset", -1), h.get("latest_offset", -1)
        samples.append({"t": round(time.time() - t0), "ready": h.get("ready"), "committed": committed, "latest": latest,
                        "kebakaran": len(f), "stats": st, "latensi": h.get("ingest_to_store")})
        lt = h.get("ingest_to_store") or {}
        p95 = f"{lt['p95_s']:.3f}s" if lt.get("p95_s") is not None else "-"
        print(f"{int(time.time() - t0):>7}s {str(h.get('ready')):>5} {committed:>9} {latest:>9} {max(latest - committed, 0):>10} {len(f):>9}  p95 masuk-tersimpan={p95} (n={lt.get('samples', 0)})")
        time.sleep(args.interval)
    lts = [s["latensi"] for s in samples if s.get("latensi") and s["latensi"].get("p95_s") is not None]
    if lts:
        print(f"p95 latensi data masuk -> HazardEvent tersimpan (sampel terakhir {lts[-1]['samples']}): {lts[-1]['p95_s']:.3f} dtk; p99 {lts[-1]['p99_s']:.3f}; maks {lts[-1]['max_s']:.3f}")
    maxlag = max((max(s["latest"] - s["committed"], 0) for s in samples if isinstance(s["latest"], int) and s["latest"] >= 0), default=0)
    print(f"tertinggal terbesar selama pemantauan: {maxlag} kejadian")
    advanced = [s["committed"] for s in samples if isinstance(s["committed"], int) and s["committed"] >= 0]
    ok = bool(samples and all(s["ready"] for s in samples[1:]) and len(advanced) > 1 and advanced[-1] > advanced[0]
              and samples[-1]["kebakaran"] > 0)
    return report("live", {"menit": args.menit, "sampel": samples, "offset_awal": advanced[0] if advanced else None, "p95_masuk_tersimpan_dtk": lts[-1]["p95_s"] if lts else None, "tertinggal_maks": maxlag,
                           "offset_akhir": advanced[-1] if advanced else None}, ok)


def cmd_crash(args):
    c = p1.Client()
    h0 = ingestor_health() or {}
    if not h0.get("ready"):
        raise SystemExit("pemroses belum Ready; tes tidak bermakna")
    # Kontrol: jendela yang sama panjang tanpa crash. Penghapusan sah oleh engine terjadi juga tanpa crash; crash tidak boleh menambah.
    ctl0 = fires(c)
    tc = time.time()
    time.sleep(args.kontrol)
    before = fires(c)
    kontrol_hilang = lost(ctl0, before)
    print(f"kontrol tanpa crash {time.time() - tc:.0f} dtk: {len(kontrol_hilang)} kebakaran kehilangan deteksi karena pembaruan feed")
    r0 = restarts()
    h0 = ingestor_health() or h0
    print(f"sebelum: offset {h0['committed_offset']}, kebakaran tersimpan {len(before)}")
    pod = ingestor_pod()
    print(f"membunuh pemroses {pod} saat aliran berjalan")
    kubectl("delete", "pod", pod, "--wait=false")
    t0 = time.time()
    h1 = None
    while time.time() - t0 < args.timeout:
        time.sleep(3)
        h = ingestor_health(exclude=pod)
        if h and h.get("ready") and h.get("committed_offset", 0) > 0:
            h1 = h
            break
    if not h1:
        return report("crash", {"pulih": False}, False)
    print(f"pemroses Ready lagi setelah {time.time() - t0:.0f} dtk; offset {h1['committed_offset']}")
    time.sleep(args.after)
    h2 = ingestor_health() or {}
    after = fires(c)
    ids_before, ids_after = set(before), set(after)
    # ID kebakaran = hash deteksi paling awal, jadi ID berganti saat kebakaran menyatu/bertambah. Kehilangan yang sebenarnya adalah
    # deteksi yang sebelumnya ada dalam suatu kebakaran dan sesudah pulih tidak ada di kebakaran mana pun.
    hilang = lost(before, after)
    id_berganti = len(ids_before - ids_after) - len(hilang)
    baru = len(ids_after - ids_before)
    r1 = restarts()
    pods_crash = {p: r1[p] - r0.get(p, 0) for p in r1 if p in r0 and r1[p] > r0[p]}
    print(f"kebakaran dengan deteksi hilang setelah pulih: {len(hilang)} {hilang[:5]}; ID berganti karena menyatu: {id_berganti}; kebakaran baru: {baru}; offset sekarang {h2.get('committed_offset')}")
    print(f"restart komponen lain: {pods_crash or 'tidak ada'}")
    # ID kebakaran adalah kunci penyimpanan, jadi ganda hanya bisa berupa kebakaran yang sama dengan ID berbeda: periksa himpunan deteksi.
    keys = {}
    dobel = 0
    for sid, e in after.items():
        ks = tuple(sorted((e.get("attributes") or {}).get("detection_keys") or []))
        if ks and ks in keys:
            dobel += 1
        keys[ks] = sid
    print(f"kebakaran ganda (himpunan deteksi sama): {dobel}")
    print(f"pembanding: tanpa crash {len(kontrol_hilang)} dalam {args.kontrol} dtk; dengan crash {len(hilang)} dalam {time.time() - t0:.0f} dtk")
    ok = bool(len(hilang) <= len(kontrol_hilang) * max(1.0, (time.time() - t0) / args.kontrol) and dobel == 0 and (h2.get("committed_offset") or -1) >= h0["committed_offset"] and not pods_crash)
    return report("crash", {"pod": pod, "detik_pulih": round(time.time() - t0 - args.after), "offset_sebelum": h0["committed_offset"],
                            "offset_sesudah": h2.get("committed_offset"), "kebakaran_sebelum": len(before),
                            "kebakaran_sesudah": len(after), "hilang": hilang, "hilang_rinci": {k: before[k] for k in hilang}, "kontrol_hilang": kontrol_hilang, "baru": baru, "ganda": dobel,
                            "id_berganti_tanpa_kehilangan": id_berganti, "restart_lain": pods_crash}, ok)


def cmd_stats(args):
    """Kriteria 2 dan 3: hitungan mesin pengelompokan dari aggregator (deteksi diterima, duplikat, ditarik/diperbarui, terlambat, rusak)."""
    c = p1.Client()
    st = c.audit_get("/internal/satellite/stats")
    s = st.get("stats") or {}
    print(f"offset tersimpan: {st.get('offset')}; kebakaran aktif: {st.get('active_fires')}; diperbarui: {st.get('updated_at')}")
    for k in sorted(s):
        print(f"  {k:<28} {s[k]:>10}")
    late = s.get("late", 0)
    print(f"deteksi melewati batas keterlambatan (dilaporkan, tidak dibuang diam-diam): {late}")
    f = fires(c)
    print(f"HazardEvent WILDFIRE tersimpan: {len(f)}")
    ok = bool(st.get("offset", 0) > 0 and s.get("accepted", 0) > 0 and s.get("retracted", 0) > 0 and s.get("duplicate", 0) > 0 and f)
    return report("stats", {"offset": st.get("offset"), "stats": s, "terlambat": late, "wildfire_tersimpan": len(f)}, ok)


FEED_RANGES = ["104.21.0.0/16", "172.67.0.0/16"]  # Cloudflare di depan satellite.naufarrel.dev; hanya berlaku di network namespace pemroses
CUT_POD = "bukti-putus-feed"


def cut_start(pod):
    """Putuskan jalur pemroses ke feed: pod privileged di node yang sama memasang rute blackhole di network namespace pemroses.
    (NetworkPolicy tidak ditegakkan di worker sehingga tidak dipakai.) Mengembalikan (pid, fungsi-pulihkan)."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "p2"))
    import p2
    node = json.loads(kubectl("get", "pod", pod, "-o", "json"))["spec"]["nodeName"]
    ov = json.dumps({"spec": {"nodeName": node, "hostPID": True, "terminationGracePeriodSeconds": 0, "containers": [{
        "name": "freezer", "image": "busybox:1.36", "command": ["sleep", "900"], "securityContext": {"privileged": True}}]}})
    subprocess.run(["kubectl", "-n", "default", "delete", "pod", CUT_POD, "--now", "--ignore-not-found"], capture_output=True)
    subprocess.run(["kubectl", "-n", "default", "run", CUT_POD, "--image=busybox:1.36", "--restart=Never", f"--overrides={ov}"], check=True, capture_output=True)
    subprocess.run(["kubectl", "-n", "default", "wait", "--for=condition=Ready", f"pod/{CUT_POD}", "--timeout=120s"], check=True, capture_output=True)
    cid = p2.container_id(pod, "satellite-ingestor")
    pid = subprocess.run(["kubectl", "-n", "default", "exec", CUT_POD, "--", "sh", "-c", f"grep -l {cid} /proc/[0-9]*/cgroup 2>/dev/null | head -1 | cut -d/ -f3"],
                         capture_output=True, text=True).stdout.strip()
    if not pid:
        raise SystemExit("proses pemroses tidak ditemukan di node")

    def route(op):
        for r in FEED_RANGES:
            subprocess.run(["kubectl", "-n", "default", "exec", CUT_POD, "--", "nsenter", "-t", pid, "-n", "ip", "route", op, "blackhole", r],
                           capture_output=True)

    def restore():
        route("del")
        subprocess.run(["kubectl", "-n", "default", "delete", "pod", CUT_POD, "--now", "--wait=false"], capture_output=True)

    route("add")
    return pid, restore


def cmd_reconnect(args):
    """Kriteria 8: koneksi ke feed diputus (NetworkPolicy egress hanya ke dalam cluster) lalu disambung kembali. Pemroses harus
    melanjutkan dari posisi tepat tanpa kehilangan dan tanpa menggandakan."""
    c = p1.Client()
    h0 = ingestor_health() or {}
    if not h0.get("ready"):
        raise SystemExit("pemroses belum Ready; tes tidak bermakna")
    pod = ingestor_pod()
    ctl0 = fires(c)
    time.sleep(args.kontrol)
    before = fires(c)
    kontrol_hilang = lost(ctl0, before)
    print(f"kontrol tanpa putus {args.kontrol} dtk: {len(kontrol_hilang)} kebakaran kehilangan deteksi karena pembaruan feed")
    h0 = ingestor_health() or h0
    r0 = restarts()
    t_cut = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"sebelum: offset {h0['committed_offset']}, terbaru di feed {h0.get('latest_offset')}, kebakaran {len(before)}")
    pid, restore = cut_start(pod)
    try:
        print(f"koneksi pemroses ke feed DIPUTUS selama {args.putus} dtk (rute blackhole ke {', '.join(FEED_RANGES)} di namespace jaringan pemroses)")
        serie = []
        t_hold = time.time()
        while time.time() - t_hold < args.putus:
            time.sleep(10)
            serie.append((round(time.time() - t_hold), (ingestor_health() or {}).get("committed_offset")))
        mid = ingestor_health() or {}
        print("offset tersimpan selama putus (dtk, offset):", serie)
    finally:
        restore()
    t0 = time.time()
    print("koneksi DISAMBUNG kembali; menunggu pemroses menyusul")
    h1 = None
    while time.time() - t0 < args.timeout:
        time.sleep(5)
        h = ingestor_health() or {}
        if h.get("ready") and h.get("committed_offset", 0) > h0["committed_offset"] and h.get("latest_offset", 0) - h.get("committed_offset", 0) < 5000:
            h1 = h
            break
    logs = kubectl("logs", pod, "--since-time", t_cut, check=False)
    putus_log = [ln for ln in logs.splitlines() if "koneksi Feed Satelit terputus" in ln]
    lompat = [ln for ln in logs.splitlines() if "lompatan offset" in ln]
    tertahan = len({o for _, o in serie[-3:]}) == 1
    for ln in putus_log[:2]:
        print("log:", ln[:240])
    after = fires(c)
    hilang = lost(before, after)
    keys, dobel = {}, 0
    for sid, e in after.items():
        ks = tuple(sorted(dk(e)))
        if ks and ks in keys:
            dobel += 1
        keys[ks] = sid
    r1 = restarts()
    restart_lain = {k: r1[k] - r0.get(k, 0) for k in r1 if k in r0 and r1[k] > r0[k]}
    print(f"offset tertahan saat putus: {mid.get('committed_offset')}; setelah sambung: {(h1 or {}).get('committed_offset')} "
          f"(menyusul dalam {time.time() - t0:.0f} dtk)")
    print(f"kebakaran dengan deteksi hilang: {len(hilang)} (kontrol tanpa putus: {len(kontrol_hilang)}); ganda: {dobel}; "
          f"pod pemroses sama: {ingestor_pod() == pod}; restart: {restart_lain or 'tidak ada'}")
    print(f"offset tertahan selama putus: {tertahan}; lompatan offset (data terlewat) di log: {len(lompat)}")
    ok = bool(h1 and putus_log and tertahan and not lompat and dobel == 0 and len(hilang) <= len(kontrol_hilang) * max(1.0, (time.time() - t0 + args.putus) / args.kontrol)
              and not restart_lain)
    return report("reconnect", {"pod": pod, "putus_dtk": args.putus, "offset_sebelum": h0["committed_offset"], "offset_saat_putus": mid.get("committed_offset"),
                                "offset_sesudah": (h1 or {}).get("committed_offset"), "log_putus": [ln[:300] for ln in putus_log[:3]], "offset_selama_putus": serie, "lompatan_offset": len(lompat),
                                "hilang": hilang, "kontrol_hilang": kontrol_hilang, "ganda": dobel, "restart": restart_lain}, ok)


def cmd_reference(args):
    ref = json.load(open(args.file))
    ref = ref.get("items", ref) if isinstance(ref, dict) else ref

    def keys(m):
        raw = m.get("detection_keys") or (m.get("attributes") or {}).get("detection_keys") or []
        return frozenset(raw)

    r_sets = {keys(m) for m in ref if keys(m)}
    ours = {keys(e) for e in fires(p1.Client()).values() if keys(e)}
    only_ref, only_ours = r_sets - ours, ours - r_sets
    print(f"referensi: {len(r_sets)} kebakaran; sistem: {len(ours)}; sama persis: {len(r_sets & ours)}")
    print(f"hanya di referensi: {len(only_ref)}; hanya di sistem: {len(only_ours)}")
    ok = not only_ref and not only_ours
    return report("reference", {"referensi": len(r_sets), "sistem": len(ours), "sama": len(r_sets & ours),
                                "hanya_referensi": len(only_ref), "hanya_sistem": len(only_ours)}, ok)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("live"); a.add_argument("--menit", type=float, default=5); a.add_argument("--interval", type=int, default=10)
    b = sub.add_parser("crash"); b.add_argument("--timeout", type=int, default=180); b.add_argument("--after", type=int, default=45); b.add_argument("--kontrol", type=int, default=100)
    sub.add_parser("stats")
    q = sub.add_parser("reconnect"); q.add_argument("--putus", type=int, default=60); q.add_argument("--kontrol", type=int, default=100); q.add_argument("--timeout", type=int, default=240)
    r = sub.add_parser("reference"); r.add_argument("file")
    args = ap.parse_args()
    sys.exit({"live": cmd_live, "crash": cmd_crash, "reference": cmd_reference, "stats": cmd_stats, "reconnect": cmd_reconnect}[args.cmd](args))


if __name__ == "__main__":
    main()
