#!/usr/bin/env python3
"""P5 (Pelampung yang Berbohong): bukti pada cluster k3s 2 node.

  python3 tests/p5/p5.py critical      kriteria 1: tiap pengamatan critical dari Sensor Pantai sampai ke notifier Tim Lapangan,
                                       0 hilang, p95 latensi <= 5 dtk
  python3 tests/p5/p5.py quarantine    kriteria 2: data yang melanggar kontrak tidak menjadi HazardEvent, disisihkan dengan alasan
  python3 tests/p5/p5.py unhealthy     kriteria 3 dan 4: sensor dibekukan; sumber lain tetap cepat, permintaan ke sensor tidak
                                       melonjak (circuit breaker), client dijawab cepat, sensor pulih tanpa intervensi
  python3 tests/p5/p5.py priority      kriteria 5: beban Media berlebih (1000 VU) tidak mengganggu Tim Lapangan (k6 p5-priority.js)

Lingkungan: KUBECONFIG, AGG_URL (port-forward aggregator, bawaan http://localhost:8090), AUDIT_TOKEN, DASHBOARD_TOKEN,
AUTH_CLIENT_FIELD_*/AUTH_CLIENT_MEDIA_* (untuk priority). Token Sensor Pantai dibaca dari Secret di dalam proses (tidak dicetak)
hanya untuk membaca kebenaran dasar dari sensor; tanpa itu tes critical memakai Audit API saja dan melaporkannya.
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "p1"))
sys.path.insert(0, os.path.join(HERE, "..", "p2"))
import p1  # noqa: E402

NS = os.environ.get("APP_NAMESPACE", "sistem-informasi-kebencanaan-bnpb")
SOURCE = "SENSOR_PANTAI"


def kubectl(*args, check=True, timeout=120):
    r = subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True, timeout=timeout)
    if check and r.returncode:
        raise SystemExit(f"kubectl {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout


def report(name, result, ok):
    result["lolos"] = ok
    d = p1.out_dir(argparse.Namespace(out=os.environ.get("OUT_DIR")))
    p1.save(d, f"p5_{name}.json", result)
    print(f"\nHASIL {name}: {'LOLOS' if ok else 'GAGAL'}")
    print("bukti disimpan di", d)
    return 0 if ok else 1


def pct(values, q):
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(round(q * (len(v) - 1))))]


def events(c, source=SOURCE):
    out, cursor = {}, ""
    while True:
        page = c.audit_get(f"/internal/audit/events?source={source}&limit=1000" + (f"&cursor={cursor}" if cursor else ""))
        for e in page["items"]:
            out[e["source_ref_id"]] = e
        cursor = page.get("next_cursor")
        if not cursor:
            return out


def unprocessed(c, source=SOURCE):
    out, cursor = [], ""
    while True:
        page = c.audit_get(f"/internal/audit/unprocessed?source={source}&limit=1000" + (f"&cursor={cursor}" if cursor else ""))
        out += page["items"]
        cursor = page.get("next_cursor")
        if not cursor:
            return out


def aggregator_logs(since_s):
    pods = json.loads(kubectl("get", "pods", "-l", "app.kubernetes.io/name=aggregator", "-o", "json"))["items"]
    lines = []
    for p in pods:
        lines += kubectl("logs", p["metadata"]["name"], f"--since={int(since_s)}s", check=False, timeout=180).splitlines()
    return lines


def notifier_alerts(since_s):
    """hazard_id -> age_ms (detik dari event disimpan sampai notifikasi diproses) untuk ALERT COASTAL."""
    pods = json.loads(kubectl("get", "pods", "-l", "app.kubernetes.io/name=notifier", "-o", "json"))["items"]
    out = {}
    for p in pods:
        for ln in kubectl("logs", p["metadata"]["name"], f"--since={int(since_s)}s", check=False, timeout=180).splitlines():
            if "ALERT dispatched" not in ln or '"COASTAL"' not in ln:
                continue
            try:
                d = json.loads(ln)
            except ValueError:
                continue
            out.setdefault(d["hazard_id"], d["age_ms"] / 1000.0)
    return out


def sensor_ground_truth():
    """Seluruh pengamatan dari Sensor Pantai (lewat port-forward). Token dibaca dari Secret dan tidak dicetak. None bila tidak bisa."""
    try:
        raw = kubectl("get", "secret", "app-secrets", "-o", "jsonpath={.data.PANTAI_AUTH_TOKEN}")
        token = base64.b64decode(raw).decode()
    except Exception:
        return None
    pf = subprocess.Popen(["kubectl", "-n", NS, "port-forward", "svc/sensor-pantai", "18080:8080"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(3)
        obs, cursor = [], ""
        for _ in range(400):
            url = "http://127.0.0.1:18080/v1/observations?limit=500" + (f"&cursor={cursor}" if cursor else "")
            req = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
            with urllib.request.urlopen(req, timeout=20) as r:
                page = json.loads(r.read())
            obs += page.get("items", [])
            cursor = page.get("next_cursor")
            if not cursor:
                break
        return obs
    except Exception as exc:
        print("kebenaran dasar dari sensor tidak terbaca:", type(exc).__name__)
        return None
    finally:
        pf.terminate()


def cmd_critical(args):
    c = p1.Client()
    stored = events(c)
    quarantined = {i.get("source_ref_id") or i["message_id"] for i in unprocessed(c)}
    truth = sensor_ground_truth()
    if truth is None:
        print("PERINGATAN: kebenaran dasar sensor tidak tersedia; memakai event critical yang tersimpan di Audit API")
        crit_ids = [rid for rid, e in stored.items() if (e.get("attributes") or {}).get("critical") is True]
        basis = "audit"
    else:
        crit_ids = [o["observation_id"] for o in truth if o.get("critical") is True]
        basis = "sensor"
    alerts = notifier_alerts(args.since)
    hid = {rid: stored[rid]["hazard_id"] for rid in stored}
    delivered = [r for r in crit_ids if r in hid and hid[r] in alerts]
    karantina = [r for r in crit_ids if r in quarantined]
    lost = [r for r in crit_ids if r not in set(delivered) and r not in set(karantina)]
    lat = [alerts[hid[r]] for r in delivered]
    p95 = pct(lat, 0.95)
    print(f"dasar penghitungan: {basis}; pengamatan critical: {len(crit_ids)}; sampai ke notifier: {len(delivered)}; "
          f"disisihkan karena melanggar kontrak: {len(karantina)}; hilang: {len(lost)}")
    print(f"latensi event tersimpan -> notifikasi: p50={pct(lat, 0.5)} p95={p95} maks={max(lat) if lat else None} detik")
    if lost:
        print("contoh hilang:", lost[:5])
    ok = bool(crit_ids and not lost and p95 is not None and p95 <= 5.0)
    return report("critical", {"dasar": basis, "critical_total": len(crit_ids), "sampai_notifier": len(delivered),
                               "disisihkan_kontrak": len(karantina), "hilang": len(lost), "contoh_hilang": lost[:10],
                               "latensi_p50_dtk": pct(lat, 0.5), "latensi_p95_dtk": p95, "latensi_maks_dtk": max(lat) if lat else None,
                               "catatan": "latensi dihitung dari event disimpan oleh Aggregator sampai notifier memproses (age_ms)"}, ok)


def cmd_quarantine(args):
    c = p1.Client()
    stored = events(c)
    items = unprocessed(c)
    reasons = {}
    for i in items:
        reasons[(i.get("reason") or "")[:70]] = reasons.get((i.get("reason") or "")[:70], 0) + 1
    complete = all(i.get("reason") and i.get("received_at") and i.get("source") for i in items)
    leaked = [i for i in items if (i.get("source_ref_id") or "") in stored]
    print(f"pengamatan yang disisihkan: {len(items)}; HazardEvent SENSOR_PANTAI tersimpan: {len(stored)}")
    for r, n in sorted(reasons.items(), key=lambda kv: -kv[1])[:8]:
        print(f"   {n:>5}  {r}")
    print(f"semua punya alasan, sumber, dan waktu: {complete}; yang justru tersimpan sebagai HazardEvent: {len(leaked)}")
    ok = bool(items and complete and not leaked)
    return report("quarantine", {"disisihkan": len(items), "alasan": reasons, "lengkap": complete,
                                 "bocor_jadi_event": len(leaked), "hazard_event_tersimpan": len(stored)}, ok)


def cmd_unhealthy(args):
    import p2
    token = os.environ.get("DASHBOARD_TOKEN", "")
    if not token:
        raise SystemExit("DASHBOARD_TOKEN wajib")
    c = p1.Client()
    before = len(events(c))
    pod = kubectl("get", "pod", "-l", "app.kubernetes.io/name=sensor-pantai", "-o", "jsonpath={.items[0].metadata.name}").strip()
    cid = p2.container_id(pod, "sensor-pantai")
    with p2.Load("ops.prod.localtest.me", token, args.workers) as ld:
        time.sleep(10)
        base = len(ld.events)
        p2.ctr("pause", cid)
        t0 = time.time()
        print(f"sensor {pod} dibekukan (tidak sehat) selama {args.window} dtk; beban sumber lain tetap berjalan")
        time.sleep(args.window)
        t1 = time.time()
        p2.ctr("resume", cid)
        print("sensor dilepas; menunggu sistem memakainya lagi tanpa intervensi")
        resumed, t_back = False, None
        while time.time() - t1 < 90:
            time.sleep(5)
            if len(events(c)) > before:
                resumed, t_back = True, time.time() - t1
                break
    win = [(t, code) for t, code in ld.events if t0 <= t < t1]
    lat_bad = sum(1 for _, code in win if code != 200)
    lines = aggregator_logs(args.window + 60)
    fails = [ln for ln in lines if "Sensor Pantai" in ln and ("fetch failed" in ln or "station fetch failed" in ln)]
    suppressed = [ln for ln in lines if "circuit breaker open" in ln]
    lat = sorted(c2 for c2 in ld.lat)
    p95 = pct(lat, 0.95)
    print(f"permintaan sumber lain selama gangguan: {len(win)}, gagal {lat_bad}; p95 latensi keseluruhan {p95:.3f} dtk")
    print(f"permintaan nyata ke sensor yang gagal: {len(fails)}; ditahan circuit breaker: {len(suppressed)}; "
          f"tanpa breaker akan ~{args.window} (1 per dtk)")
    print(f"sistem memakai sensor lagi: {resumed}" + (f" setelah {t_back:.0f} dtk" if t_back else ""))
    limit = args.window // 3
    ok = bool(win and lat_bad == 0 and p95 is not None and p95 <= 2.0 and len(fails) <= limit and resumed)
    return report("unhealthy", {"detik_gangguan": args.window, "permintaan_lain": len(win), "gagal_lain": lat_bad,
                                "p95_dtk": p95, "permintaan_ke_sensor_gagal": len(fails), "ditahan_breaker": len(suppressed),
                                "batas_permintaan_ke_sensor": limit, "pulih_sendiri": resumed, "detik_pulih": t_back}, ok)


def cmd_priority(args):
    env = dict(os.environ)
    for src, dst in (("AUTH_CLIENT_FIELD_ID", "FIELD_CLIENT_ID"), ("AUTH_CLIENT_FIELD_SECRET", "FIELD_CLIENT_SECRET"),
                     ("AUTH_CLIENT_MEDIA_ID", "MEDIA_CLIENT_ID"), ("AUTH_CLIENT_MEDIA_SECRET", "MEDIA_CLIENT_SECRET")):
        if src in env:
            env.setdefault(dst, env[src])
    env.setdefault("BASE_URL", "http://api.prod.localtest.me")
    env.setdefault("AUTH_BASE_URL", "http://auth.prod.localtest.me")
    env["DURATION"] = args.durasi
    script = os.path.join(HERE, "..", "k6", "p5-priority.js")
    print(f"k6 p5-priority.js: {env.get('MEDIA_VUS', '1000')} VU Media + {env.get('FIELD_VUS', '50')} VU Tim Lapangan, {args.durasi}")
    k6 = os.path.expanduser("~/bin/k6") if os.path.exists(os.path.expanduser("~/bin/k6")) else "k6"
    r = subprocess.run([k6, "run", "--quiet", "--summary-export", "/tmp/p5-priority-summary.json", script], env=env)
    summary = {}
    try:
        summary = json.load(open("/tmp/p5-priority-summary.json"))["metrics"]
    except Exception:
        pass
    get = lambda m, k: (summary.get(m) or {}).get(k)
    return report("priority", {"k6_exit": r.returncode, "field_success": get("field_success_rate", "value"),
                               "field_429": get("field_429_rate", "value"), "field_p95_ms": get("field_latency", "p(95)"),
                               "media_429": get("media_controlled_429_rate", "value")}, r.returncode == 0)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("critical"); a.add_argument("--since", type=int, default=6 * 3600)
    sub.add_parser("quarantine")
    u = sub.add_parser("unhealthy"); u.add_argument("--window", type=int, default=60); u.add_argument("--workers", type=int, default=8)
    p = sub.add_parser("priority"); p.add_argument("--durasi", default="45s")
    args = ap.parse_args()
    sys.exit({"critical": cmd_critical, "quarantine": cmd_quarantine, "unhealthy": cmd_unhealthy, "priority": cmd_priority}[args.cmd](args))


if __name__ == "__main__":
    main()
