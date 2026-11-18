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


def ingestor_pod():
    out = kubectl("get", "pod", "-l", "app.kubernetes.io/name=satellite-ingestor", "-o", "jsonpath={.items[0].metadata.name}", check=False)
    return out.strip()


def ingestor_health():
    """Keadaan pemroses lewat proxy API server (tanpa port-forward); None bila pod belum menjawab."""
    pod = ingestor_pod()
    if not pod:
        return None
    r = subprocess.run(["kubectl", "get", "--raw", f"/api/v1/namespaces/{NS}/pods/{pod}:8095/proxy/health"],
                       capture_output=True, text=True, timeout=30)
    try:
        return json.loads(r.stdout)
    except ValueError:
        return None


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
    print(f"{'waktu':>8} {'ready':>5} {'offset':>9} {'terbaru':>9} {'tertinggal':>10} {'kebakaran':>9}  keadaan mesin")
    t0 = time.time()
    while time.time() < t_end:
        h = ingestor_health() or {}
        f = fires(c)
        st = h.get("stats") or {}
        committed, latest = h.get("committed_offset", -1), h.get("latest_offset", -1)
        samples.append({"t": round(time.time() - t0), "ready": h.get("ready"), "committed": committed, "latest": latest,
                        "kebakaran": len(f), "stats": st})
        brief = ", ".join(f"{k}={v}" for k, v in sorted(st.items())[:5])
        print(f"{int(time.time() - t0):>7}s {str(h.get('ready')):>5} {committed:>9} {latest:>9} {max(latest - committed, 0):>10} {len(f):>9}  {brief}")
        time.sleep(args.interval)
    advanced = [s["committed"] for s in samples if isinstance(s["committed"], int) and s["committed"] >= 0]
    ok = bool(samples and all(s["ready"] for s in samples[1:]) and len(advanced) > 1 and advanced[-1] > advanced[0]
              and samples[-1]["kebakaran"] > 0)
    return report("live", {"menit": args.menit, "sampel": samples, "offset_awal": advanced[0] if advanced else None,
                           "offset_akhir": advanced[-1] if advanced else None}, ok)


def cmd_crash(args):
    c = p1.Client()
    h0 = ingestor_health() or {}
    if not h0.get("ready"):
        raise SystemExit("pemroses belum Ready; tes tidak bermakna")
    before = fires(c)
    r0 = restarts()
    print(f"sebelum: offset {h0['committed_offset']}, kebakaran tersimpan {len(before)}")
    pod = ingestor_pod()
    print(f"membunuh pemroses {pod} saat aliran berjalan")
    kubectl("delete", "pod", pod, "--wait=false")
    t0 = time.time()
    h1 = None
    while time.time() - t0 < args.timeout:
        time.sleep(3)
        h = ingestor_health()
        if h and h.get("ready"):
            h1 = h
            break
    if not h1:
        return report("crash", {"pulih": False}, False)
    print(f"pemroses Ready lagi setelah {time.time() - t0:.0f} dtk; offset {h1['committed_offset']}")
    time.sleep(args.after)
    h2 = ingestor_health() or {}
    after = fires(c)
    ids_before, ids_after = set(before), set(after)
    hilang = sorted(ids_before - ids_after)
    baru = len(ids_after - ids_before)
    r1 = restarts()
    pods_crash = {p: r1[p] - r0.get(p, 0) for p in r1 if p in r0 and r1[p] > r0[p]}
    print(f"kebakaran hilang setelah pulih: {len(hilang)} {hilang[:5]}; kebakaran baru: {baru}; offset sekarang {h2.get('committed_offset')}")
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
    ok = bool(not hilang and dobel == 0 and (h2.get("committed_offset") or -1) >= h0["committed_offset"] and not pods_crash)
    return report("crash", {"pod": pod, "detik_pulih": round(time.time() - t0 - args.after), "offset_sebelum": h0["committed_offset"],
                            "offset_sesudah": h2.get("committed_offset"), "kebakaran_sebelum": len(before),
                            "kebakaran_sesudah": len(after), "hilang": hilang, "baru": baru, "ganda": dobel,
                            "restart_lain": pods_crash}, ok)


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
    b = sub.add_parser("crash"); b.add_argument("--timeout", type=int, default=180); b.add_argument("--after", type=int, default=45)
    r = sub.add_parser("reference"); r.add_argument("file")
    args = ap.parse_args()
    sys.exit({"live": cmd_live, "crash": cmd_crash, "reference": cmd_reference}[args.cmd](args))


if __name__ == "__main__":
    main()
