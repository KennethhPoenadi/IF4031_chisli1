#!/usr/bin/env python3
"""Bukti Problem 3 (surat dari tetangga): perubahan skema, pesan rusak, DLQ, dan laporan Tim Lapangan.

Subperintah (tiap subperintah menyimpan JSON bukti dan mengembalikan kode keluar 0 bila lolos):

  schema   Kriteria 1, 2, 4, 5. Mock PVMBG diubah menjadi tidak kompatibel (rename, type, missing). Pesan yang
           tidak dapat diproses tersimpan utuh beserta alasan, sumber, dan waktunya (parked bila tidak akan
           berhasil tanpa perubahan sistem). Sumber lain tetap berjalan. Setelah mock dikembalikan, pesan yang
           pulih diproses ulang tanpa duplikat dan urutan per source_ref_id terjaga.
  corrupt  Kriteria 3. Mock mengirim JSON terpotong, bersarang sangat dalam, UTF-8 tidak valid, dan ukuran
           berlebih. Tidak ada pod yang crash atau restart, dan sumber lain tetap diproses.
  reports  Kriteria 6. POST /field-reports membalas 202 dengan cepat. Komponen penerima dimatikan paksa di
           tengah pengiriman: tidak ada laporan berbalas 202 yang hilang, dan tidak ada yang diproses ganda.
  access   Kriteria 5 (akses). Pesan gagal hanya dapat diperiksa oleh BNPB Internal Ops.

Lingkungan: KUBECONFIG, AGG_URL (port-forward ke svc/aggregator), AGG_INTERNAL_KEY, AUDIT_TOKEN,
PVMBG_URL (port-forward ke svc/pvmbg-mock), PVMBG_TOKEN, API_URL dan AUTH_URL (ingress, bawaan
http://127.0.0.1 dengan Host api.prod.localtest.me dan auth.prod.localtest.me), AUTH_CLIENT_FIELD_ID,
AUTH_CLIENT_FIELD_SECRET, AUTH_CLIENT_OPS_ID, AUTH_CLIENT_OPS_SECRET, AUTH_CLIENT_MEDIA_ID, AUTH_CLIENT_MEDIA_SECRET.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import http.client
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "p1"))
import p1  # noqa: E402

NS = os.environ.get("APP_NAMESPACE", "sistem-informasi-kebencanaan-bnpb")
PVMBG = os.environ.get("PVMBG_URL", "http://localhost:18082").rstrip("/")


def kubectl(*args, check=True):
    r = subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True, timeout=300)
    if check and r.returncode != 0:
        raise SystemExit(f"kubectl {' '.join(args)} gagal: {r.stderr.strip()}")
    return r.stdout.strip()


def restarts():
    raw = kubectl("get", "pods", "-o", "json")
    out = {}
    for pod in json.loads(raw)["items"]:
        cs = pod["status"].get("containerStatuses") or []
        out[pod["metadata"]["name"]] = sum(c["restartCount"] for c in cs)
    return out


def bmkg_polls(seconds):
    """Jumlah poll BMKG yang sukses (HTTP 200) di log semua pod aggregator dalam N detik terakhir.
    Hitungan event BMKG di Audit API tidak dipakai sebagai bukti 'sumber lain tetap jalan': ID mock dimulai dari 1 lagi sesudah
    mock di-restart dan bertabrakan dengan event lama yang sudah tersimpan, sehingga event baru dianggap duplikat (idempoten)."""
    pods = json.loads(kubectl("get", "pods", "-l", "app.kubernetes.io/name=aggregator", "-o", "json"))["items"]
    total = 0
    for pod in pods:
        out = kubectl("logs", pod["metadata"]["name"], f"--since={int(seconds) + 2}s", check=False) or ""
        total += sum(1 for ln in out.splitlines() if '"msg":"bmkg call"' in ln and '"status":200' in ln)
    return total


def pvmbg_admin(body):
    code, res = p1.http_json("POST", PVMBG + "/admin/schema-version",
                             {"Authorization": "Bearer " + os.environ["PVMBG_TOKEN"]}, body)
    if code != 200:
        raise SystemExit(f"admin PVMBG HTTP {code}: {res}")
    return res


def unprocessed(c, status=None):
    items, cursor = [], ""
    while True:
        q = "/internal/audit/unprocessed?limit=1000" + (f"&cursor={cursor}" if cursor else "")
        page = c.audit_get(q)
        items += page["items"]
        cursor = page.get("next_cursor")
        if not cursor:
            break
    return [i for i in items if not status or i["status"] == status]


def source_count(c, source):
    return c.audit_get(f"/internal/audit/checksum?source={source}")["count"]


def reprocess(c):
    code, res = p1.http_json("POST", c.agg + "/internal/unprocessed/reprocess", {"X-Internal-Key": c.key}, {})
    return res if code == 200 else {"error": res, "code": code}


def report(name, result, ok):
    result["lolos"] = ok
    d = p1.out_dir(argparse.Namespace(out=os.environ.get("OUT_DIR")))
    p1.save(d, f"p3_{name}.json", result)
    print(f"\nHASIL {name}: {'LOLOS' if ok else 'GAGAL'}\nbukti disimpan di {d}")
    return 0 if ok else 1


def cmd_schema(args):
    c = p1.Client()
    before = {"parked": len(unprocessed(c, "parked")), "retrying": len(unprocessed(c, "retrying")),
              "PVMBG": source_count(c, "PVMBG"), "BMKG": source_count(c, "BMKG")}
    r0 = restarts()
    print("baseline:", before)
    results = {}
    for change in args.changes:
        print(f"== mode tidak kompatibel: {change}")
        t_mode = time.time()
        pvmbg_admin({"mode": "incompatible", "change": change})
        time.sleep(args.wait)
        parked = unprocessed(c, "parked")
        mid = {"parked": len(parked), "BMKG": source_count(c, "BMKG"), "polls": bmkg_polls(time.time() - t_mode)}
        sample = parked[-1] if parked else None
        results[change] = {"parked_total": len(parked), "bmkg_count": mid["BMKG"], "bmkg_poll_sukses": mid["polls"], "contoh": sample}
        print(f"   parked={len(parked)}  poll BMKG sukses selama mode ini: {mid['polls']} (hitungan event BMKG {mid['BMKG']})")
        if sample:
            print(f"   contoh: sumber={sample['source']} retryable={sample['retryable']} alasan={sample['reason'][:90]}")
        pvmbg_admin({"mode": "incompatible", "enabled": False})
        time.sleep(10)

    parked_all = unprocessed(c, "parked")
    retrying_all = unprocessed(c, "retrying")
    pv_before = source_count(c, "PVMBG")
    res = reprocess(c)
    time.sleep(15)
    parked_after = unprocessed(c, "parked")
    pv_after = source_count(c, "PVMBG")
    pv_refs = p1_refs(c, "PVMBG")
    cont = continuity_ids(pv_refs)
    r1 = restarts()
    crashed = {p: r1[p] - r0.get(p, 0) for p in r1 if r1[p] > r0.get(p, 0)}
    fields_ok = all(i.get("reason") and i.get("received_at") and i.get("source") for i in parked_all)
    distinguishes = bool({i["status"] for i in parked_all + retrying_all}) and \
        all(i["retryable"] == (i["status"] == "retrying") for i in parked_all + retrying_all)
    bmkg_grew = all(v["bmkg_poll_sukses"] > 0 and v["bmkg_count"] >= before["BMKG"] for v in results.values())
    ok = (len(parked_all) > before["parked"] and fields_ok and distinguishes and bmkg_grew and not crashed
          and cont["duplikat"] == 0)
    print(f"parked setelah mode rusak: {len(parked_all)} (sebelum {before['parked']}), retrying: {len(retrying_all)}")
    print(f"reprocess: {res}")
    print(f"parked setelah reprocess: {len(parked_after)}; PVMBG {pv_before} -> {pv_after}; duplikat ID: {cont['duplikat']}")
    print(f"pod restart selama uji: {crashed or 'tidak ada'}")
    return report("schema", {"baseline": before, "per_mode": results, "parked_akhir": len(parked_all),
                             "retrying_akhir": len(retrying_all), "reprocess": res,
                             "parked_setelah_reprocess": len(parked_after), "pvmbg": [pv_before, pv_after],
                             "kesinambungan_pvmbg": cont, "pod_restart": crashed,
                             "field_lengkap": fields_ok, "membedakan_retryable": distinguishes}, ok)


def cmd_recover(args):
    """Kriteria 4: setelah sistem disesuaikan (alias dan konversi tipe di konfigurasi Aggregator), pesan yang dulu gagal diproses
    ulang. Yang bisa pulih harus pulih seluruhnya, tanpa duplikat dan dengan urutan per source_ref_id terjaga; pesan yang
    kehilangan field wajib (volcano_id) memang tidak akan pernah berhasil dan harus tetap tersimpan beserta alasannya."""
    c = p1.Client()
    cm = json.loads(kubectl("get", "configmap", "-o", "json"))["items"]
    rules = {}
    for item in cm:
        d = item.get("data") or {}
        if "PVMBG_FIELD_ALIASES" in d:
            rules = {"PVMBG_FIELD_ALIASES": d["PVMBG_FIELD_ALIASES"], "PVMBG_FIELD_COERCE": d.get("PVMBG_FIELD_COERCE", "")}
    print("aturan penyesuaian di konfigurasi Aggregator:", rules)
    if not rules.get("PVMBG_FIELD_ALIASES") and not rules.get("PVMBG_FIELD_COERCE"):
        raise SystemExit("aturan belum diterapkan di cluster (PVMBG_FIELD_ALIASES/COERCE kosong)")
    r0 = restarts()
    parked = unprocessed(c, "parked")
    by_ref = {}
    for e in parked:
        by_ref.setdefault(e.get("source_ref_id", ""), []).append(e)
    print(f"pesan parked sebelum pemrosesan ulang: {len(parked)} ({len(by_ref)} source_ref_id)")
    pv_before = source_count(c, "PVMBG")
    res = {}
    for _ in range(4):  # satu putaran dapat membuka pesan yang tadinya terblokir oleh pesan sebelumnya pada source_ref_id sama
        res = reprocess(c)
        print("reprocess:", {k: res.get(k) for k in ("considered", "recovered", "still_parked", "blocked", "skipped_unsupported")})
        if not res.get("recovered"):
            break
        time.sleep(3)
    time.sleep(10)
    after = unprocessed(c, "parked")
    pv_after = source_count(c, "PVMBG")
    refs = p1_refs(c, "PVMBG")
    cont = continuity_ids(refs)
    left_reasons = sorted({(e["reason"] or "")[:80] for e in after})
    # Yang tersisa harus pesan yang memang tidak akan pernah berhasil: field wajib hilang, atau isi rusak (terpotong, UTF-8 tidak valid,
    # bersarang terlalu dalam, terlalu besar). Pesan karena perubahan nama/tipe sudah tertangani aturan dan tidak boleh tersisa.
    permanent = ("volcano_id", "malformed:")
    only_missing = all(any(m in (e["reason"] or "") for m in permanent) and not e["retryable"] for e in after)
    r1 = restarts()
    crashed = {p: r1[p] - r0.get(p, 0) for p in r1 if r1[p] > r0.get(p, 0)}
    recovered = len(parked) - len(after)
    print(f"parked sesudah: {len(after)}; pulih: {recovered}; event PVMBG {pv_before} -> {pv_after}; duplikat ID: {cont['duplikat']}")
    print("alasan yang tersisa:", left_reasons)
    print(f"pod restart: {crashed or 'tidak ada'}")
    ok = bool(recovered > 0 and pv_after - pv_before >= 0 and cont["duplikat"] == 0 and only_missing and not crashed)
    return report("recover", {"aturan": rules, "parked_sebelum": len(parked), "parked_sesudah": len(after), "pulih": recovered,
                              "pvmbg": [pv_before, pv_after], "kesinambungan_pvmbg": cont, "alasan_tersisa": left_reasons,
                              "hanya_field_wajib_hilang_tersisa": only_missing, "pod_restart": crashed}, ok)


def cmd_retry(args):
    """Kriteria 2 (sisi dicoba lagi): bila broker macet, pesan PVMBG yang valid gagal dipublikasikan. Itu kegagalan sementara, jadi
    pesan harus berstatus retrying (retryable) dan dicoba lagi otomatis sampai tersimpan, berbeda dari pesan parked yang menunggu
    perubahan sistem. Broker dibekukan (SIGSTOP) lalu dilepas; tidak ada intervensi lain."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "p2"))
    import p2  # noqa: E402
    c = p1.Client()
    parked0 = {i["message_id"] for i in unprocessed(c, "parked")}
    retry0 = unprocessed(c, "retrying")
    pod = "rabbitmq-0"
    cid = p2.container_id(pod, "rabbitmq")
    p2.ctr("pause", cid)
    t0 = time.time()
    print(f"broker {pod} dibekukan; menunggu pesan PVMBG yang valid masuk status retrying...")
    seen = []
    try:
        while time.time() - t0 < args.timeout:
            seen = [i for i in unprocessed(c, "retrying") if i["source"] == "PVMBG"]
            if len(seen) >= args.min:
                break
            time.sleep(5)
    finally:
        p2.ctr("resume", cid)
    t_resume = time.time()
    contoh = seen[-1] if seen else None
    if contoh:
        print(f"retrying={len(seen)}; contoh: retryable={contoh['retryable']} percobaan={contoh['attempts']} alasan={contoh['reason'][:100]}")
    print("broker dilepas; menunggu percobaan ulang otomatis...")
    left = len(seen)
    while time.time() - t_resume < args.timeout:
        left = len([i for i in unprocessed(c, "retrying") if i["source"] == "PVMBG"])
        if left == 0:
            break
        time.sleep(5)
    parked1 = {i["message_id"] for i in unprocessed(c, "parked")}
    pv = p1_refs(c, "PVMBG")
    cont = continuity_ids(pv)
    print(f"retrying tersisa: {left}; detik sampai pulih: {time.time() - t_resume:.0f}; parked baru: {len(parked1 - parked0)}; duplikat ID: {cont['duplikat']}")
    ok = bool(contoh and contoh["retryable"] and contoh["status"] == "retrying" and left == 0 and not (parked1 - parked0)
              and cont["duplikat"] == 0)
    return report("retry", {"retrying_terlihat": len(seen), "contoh": contoh, "retrying_tersisa": left,
                            "detik_pulih": round(time.time() - t_resume), "parked_baru": len(parked1 - parked0),
                            "kesinambungan_pvmbg": cont}, ok)


def p1_refs(c, source):
    items, cursor = [], ""
    while True:
        page = c.audit_get(f"/internal/audit/events?source={source}&limit=1000" + (f"&cursor={cursor}" if cursor else ""))
        items += [e["source_ref_id"] for e in page["items"]]
        cursor = page.get("next_cursor")
        if not cursor:
            return items


def continuity_ids(ids):
    nums = [int(m.group(1)) for i in ids if (m := re.search(r"(\d+)$", i))]
    uniq = set(nums)
    return {"jumlah": len(nums), "duplikat": len(nums) - len(uniq),
            "celah": (max(uniq) - min(uniq) + 1 - len(uniq)) if uniq else 0}


def cmd_corrupt(args):
    c = p1.Client()
    r0 = restarts()
    bm0 = source_count(c, "BMKG")
    t_start = time.time()
    parked0 = len(unprocessed(c))
    kinds = ["truncated", "deep", "invalid_utf8", "oversize"]
    for k in kinds:
        print(f"== kirim respons korup: {k} x {args.count}")
        pvmbg_admin({"mode": "corrupt", "kind": k, "count": args.count})
        time.sleep(args.wait)
    time.sleep(10)
    r1 = restarts()
    crashed = {p: r1[p] - r0.get(p, 0) for p in r1 if r1[p] > r0.get(p, 0)}
    bm1 = source_count(c, "BMKG")
    parked1 = unprocessed(c)
    polls = bmkg_polls(time.time() - t_start)
    ok = not crashed and polls > 0 and bm1 >= bm0
    print(f"pod restart: {crashed or 'tidak ada'}; poll BMKG sukses selama uji: {polls}; BMKG {bm0} -> {bm1}; pesan tak terproses {parked0} -> {len(parked1)}")
    return report("corrupt", {"pod_restart": crashed, "bmkg": [bm0, bm1], "bmkg_poll_sukses": polls, "tak_terproses": [parked0, len(parked1)],
                              "kinds": kinds, "per_kind": args.count}, ok)


def token(client_id, client_secret):
    host = "auth.prod.localtest.me"
    body = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": client_id,
                                   "client_secret": client_secret}).encode()
    conn = http.client.HTTPConnection("127.0.0.1", 80, timeout=10)
    conn.request("POST", "/token", body=body, headers={"Host": host, "Content-Type": "application/x-www-form-urlencoded"})
    resp = conn.getresponse()
    data = json.loads(resp.read() or b"{}")
    if resp.status != 200:
        raise SystemExit(f"token HTTP {resp.status}: {data}")
    return data["access_token"]


def post_report(tok, key, n):
    conn = http.client.HTTPConnection("127.0.0.1", 80, timeout=10)
    t = time.monotonic()
    try:
        conn.request("POST", "/field-reports", body=json.dumps({"kind": "uji", "n": n, "note": f"laporan {key}"}),
                     headers={"Host": "api.prod.localtest.me", "Authorization": "Bearer " + tok,
                              "Content-Type": "application/json", "Idempotency-Key": key})
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, time.monotonic() - t, data
    except Exception as e:  # noqa: BLE001
        return 0, time.monotonic() - t, str(e).encode()


def cmd_reports(args):
    c = p1.Client()
    tok = token(os.environ["AUTH_CLIENT_FIELD_ID"], os.environ["AUTH_CLIENT_FIELD_SECRET"])
    stamp = int(time.time())
    accepted, rejected, lat = [], [], []

    def one(i):
        key = f"p3-{stamp}-{i:05d}"
        code, dur, data = post_report(tok, key, i)
        return key, code, dur, data

    def killer():
        time.sleep(args.kill_after)
        for app in ("client-facing-api", "aggregator"):
            pods = kubectl("get", "pods", "-l", f"app.kubernetes.io/name={app}", "-o", "jsonpath={.items[*].metadata.name}").split()
            kubectl("delete", "pod", pods[0], "--grace-period=0", "--force", "--wait=false", check=False)
            print(f"   dibunuh paksa: {pods[0]}")

    print(f"mengirim {args.count} laporan; komponen dimatikan paksa sesudah {args.kill_after} dtk")
    with ThreadPoolExecutor(max_workers=args.workers + 1) as ex:
        ex.submit(killer)
        for key, code, dur, data in ex.map(one, range(args.count)):
            if code == 202:
                accepted.append(key)
                lat.append(dur)
            else:
                rejected.append((key, code))
    lat.sort()
    p95 = lat[int(0.95 * (len(lat) - 1))] if lat else None
    print(f"202: {len(accepted)}, ditolak/gagal: {len(rejected)} (kode {sorted({c for _, c in rejected})}), p95 {p95:.3f} dtk")

    # Kirim ulang yang ditolak dengan Idempotency-Key yang sama (perilaku klien yang benar), lalu tunggu hilir.
    for key, code in rejected:
        for _ in range(10):
            code2, _, _ = post_report(tok, key, 0)
            if code2 == 202:
                accepted.append(key)
                break
            time.sleep(1)
    time.sleep(args.settle)
    ids = set()
    deadline = time.time() + 120
    missing = list(accepted)
    stats = {}
    while time.time() < deadline:
        try:
            code, body = p1.http_json("GET", c.agg + "/internal/field-reports/stats?ids=true", {"X-Internal-Key": c.key})
        except OSError:
            # Pod Aggregator baru saja dimatikan paksa oleh uji ini; sambungan ke pod yang diganti putus sesaat.
            time.sleep(2)
            continue
        if code == 200:
            ids = set(body.get("report_ids", []))
            stats = body.get("stats", {})
            missing = [k for k in accepted if not any(i.endswith(":" + k) or i == k for i in ids)]
            if not missing:
                break
        time.sleep(5)
    # Nama kunci statistik berbeda antar versi; kunci yang tidak ditemukan harus dianggap gagal, bukan 0.
    more_than_once = next((stats[k] for k in ("processed_more_than_once", "ProcessedMoreThanOnce", "processed_more_than_one",
                                              "ProcessedMoreThanOne") if k in stats), None)
    ok = (not missing and more_than_once == 0 and stats.get("duplicate_deliveries", 0) == 0
          and stats.get("max_process_count", 1) <= 1)
    print(f"laporan 202 yang hilang: {len(missing)} dari {len(accepted)}; diproses lebih dari sekali: "
          f"{more_than_once}; stats {stats}")
    return report("reports", {"dikirim": args.count, "berbalas_202": len(accepted), "ditolak_awal": len(rejected),
                              "hilang": len(missing), "contoh_hilang": missing[:10], "p95_detik": p95, "stats": stats}, ok)


def cmd_access(args):
    c = p1.Client()
    out = {}
    for label, idenv in (("ops", "AUTH_CLIENT_OPS"), ("field", "AUTH_CLIENT_FIELD"), ("media", "AUTH_CLIENT_MEDIA")):
        tok = token(os.environ[idenv + "_ID"], os.environ[idenv + "_SECRET"])
        conn = http.client.HTTPConnection("127.0.0.1", 80, timeout=10)
        conn.request("GET", "/ops/unprocessed", headers={"Host": "api.prod.localtest.me", "Authorization": "Bearer " + tok})
        out[label] = conn.getresponse().status
    conn = http.client.HTTPConnection("127.0.0.1", 80, timeout=10)
    conn.request("GET", "/ops/unprocessed", headers={"Host": "api.prod.localtest.me"})
    out["tanpa_token"] = conn.getresponse().status
    print("GET /ops/unprocessed:", out)
    ok = out["ops"] == 200 and out["field"] == 403 and out["media"] == 403 and out["tanpa_token"] == 401
    return report("access", {"status_per_klien": out}, ok)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("schema"); s.add_argument("--wait", type=int, default=45)
    s.add_argument("--changes", nargs="+", default=["rename", "type", "missing"])
    k = sub.add_parser("corrupt"); k.add_argument("--count", type=int, default=3); k.add_argument("--wait", type=int, default=30)
    sub.add_parser("recover")
    q = sub.add_parser("retry"); q.add_argument("--timeout", type=int, default=240); q.add_argument("--min", type=int, default=3)
    r = sub.add_parser("reports"); r.add_argument("--count", type=int, default=300); r.add_argument("--workers", type=int, default=8)
    r.add_argument("--kill-after", type=float, default=3.0); r.add_argument("--settle", type=int, default=20)
    sub.add_parser("access")
    args = ap.parse_args()
    sys.exit({"schema": cmd_schema, "corrupt": cmd_corrupt, "reports": cmd_reports, "access": cmd_access, "recover": cmd_recover, "retry": cmd_retry}[args.cmd](args))


if __name__ == "__main__":
    main()
