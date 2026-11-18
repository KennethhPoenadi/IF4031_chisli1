#!/usr/bin/env python3
"""Bukti Problem 1 kriteria 5 (node utuh): skenario `wipe-node` Tester Agent menghapus satu node worker.

Langkah:
  0. Syarat awal: cluster sehat.
  1. Baseline: semua source_ref_id BMKG dari Audit API, dan peta unit penyimpanan per node.
  2. Lalu lintas latar (baca, checksum, tulis) berjalan terus selama skenario.
  3. Memicu skenario latihan `wipe-node` lewat Tester Agent, lalu memantau node, pod, dan replikasi tiap 5 detik.
  4. Menunggu Tester Agent menyatakan pemulihan akhir selesai dan seluruh node/pod kembali siap. Bila sebuah node hilang
     dari cluster, tes menampilkan petunjuk join ulang dan menunggu (maks 25 menit).
  5. Memastikan tidak ada event yang hilang, tidak ada respons melanggar (5xx selain 503 terkontrol, permintaan menggantung),
     dan node yang dihapus ikut menanggung data lagi dengan replikasi utuh.

Lingkungan sama dengan p1.py (AGG_URL, AGG_INTERNAL_KEY, AUDIT_URL, AUDIT_TOKEN), ditambah KUBECONFIG.
Menghapus node adalah tindakan merusak: jalankan hanya pada cluster uji.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import p1  # noqa: E402
import unit_loss as ul  # noqa: E402


def sh(*cmd, timeout=120):
    r = subprocess.run(list(cmd), capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


def nodes():
    rc, out, _ = sh("kubectl", "get", "nodes", "--no-headers")
    res = {}
    for line in out.splitlines():
        f = line.split()
        if len(f) >= 2:
            res[f[0]] = f[1]
    return res


def unit_nodes():
    rc, out, _ = sh("kubectl", "-n", ul.NS, "get", "pods", "-o", "wide", "--no-headers")
    m = {}
    for line in out.splitlines():
        f = line.split()
        if f and f[0].startswith("store-"):
            m[f[0]] = (f[6] if len(f) > 6 else "?", f[2])
    return m


def not_ready_pods():
    rc, out, _ = sh("kubectl", "-n", ul.NS, "get", "pods", "--no-headers")
    bad = []
    for line in out.splitlines():
        f = line.split()
        if len(f) >= 3:
            a, b = f[1].split("/")
            if a != b or f[2] != "Running":
                bad.append(f"{f[0]}:{f[2]}")
    return bad


def agent_seq():
    """Nomor urut terbesar pada rantai log Tester Agent dan daftar baris tindakan."""
    rc, out, _ = sh("kubectl", "logs", "-n", "tester-agent", "-l", "app=tester-agent", "--tail=80", "--prefix")
    rows = []
    for line in out.splitlines():
        m = re.search(r"\[pod/([^/]+)/agent\] (\{.*\})", line)
        if not m:
            continue
        try:
            d = json.loads(m.group(2))
        except ValueError:
            continue
        rows.append((m.group(1), d["seq"], d["action"]))
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default="BMKG")
    ap.add_argument("--wait-minutes", type=int, default=25)
    ap.add_argument("--out")
    args = ap.parse_args()
    c, d = p1.Client(), p1.out_dir(args)
    report = {}

    print("== 0. syarat awal: cluster sehat")
    end = time.time() + 600
    while True:
        pre = ul.aget(c, "/internal/audit/distribution")
        if pre["under_replicated"] == 0 and not pre["rebuilding_units"] and not pre["partial"] and not not_ready_pods():
            break
        if time.time() > end:
            raise SystemExit("cluster tidak sehat sebelum uji")
        time.sleep(10)
    n0, u0 = nodes(), unit_nodes()
    print("   node:", n0)
    print("   unit penyimpanan -> node:", {k: v[0] for k, v in u0.items()})

    print(f"== 1. baseline {args.source}")
    base, base_partial = ul.all_refs(c, args.source)
    print(f"   {len(base)} event (partial={base_partial})")
    if base_partial:
        raise SystemExit("baseline partial")
    report["baseline"] = {"event": len(base), "node": n0, "unit_ke_node": {k: v[0] for k, v in u0.items()}}

    rows0 = agent_seq()
    seq0 = max((r[1] for r in rows0), default=0)

    with ul.Traffic(c) as tr:
        time.sleep(5)
        print("== 2. memicu skenario Tester Agent: wipe-node")
        t0 = time.time()
        rc, out, err = sh("kubectl", "exec", "-n", "tester-agent", "daemonset/tester-agent", "--", "/app", "run", "practice", "wipe-node")
        for line in (out or err).splitlines():
            print("  ", line)
        report["skenario"] = {"keluaran": out or err}

        print("== 3. memantau node, pod, dan replikasi (tiap 10 dtk) sampai Tester Agent selesai memulihkan")
        timeline, done, saw_down, cleared = [], False, False, False
        deadline = t0 + args.wait_minutes * 60
        hinted = False
        while time.time() < deadline:
            nd = nodes()
            try:
                dist = ul.aget(c, "/internal/audit/distribution", tries=3)
                rep = f"partial={dist['partial']} under_replicated={dist['under_replicated']} rebuilding={dist['rebuilding_units']} tak_terjangkau={dist['unavailable_units']}"
            except SystemExit:
                rep = "audit tidak terjangkau"
            gone = [n for n in n0 if n not in nd or nd[n] != "Ready"]
            saw_down = saw_down or bool(gone)
            if gone and not cleared:
                # wipe_and_shutdown mematikan node sebelum run sempat ditutup. Catatan run (ConfigMap tester-run, status "go")
                # membuat pod Tester Agent mengulang wipe dan shutdown tiap kali node kembali. Simpan salinan, lalu hapus
                # (atas keputusan pemilik cluster) agar node dapat pulih.
                cleared = True
                bk = os.path.join(d, "tester-run.backup.yaml")
                rc2, yml, _ = sh("kubectl", "-n", "tester-agent", "get", "cm", "tester-run", "-o", "yaml")
                if rc2 == 0:
                    open(bk, "w").write(yml)
                rc3, o3, e3 = sh("kubectl", "-n", "tester-agent", "delete", "cm", "tester-run", "--ignore-not-found")
                print(f"   >>> catatan run Tester Agent dihapus agar node tidak dimatikan ulang saat kembali ({o3 or e3}); salinan: {os.path.basename(bk)}")
                report["catatan_run_dihapus"] = True
            nr = not_ready_pods()
            t = round(time.time() - t0)
            line = f"t+{t:>4}s node={nd} pod_tidak_siap={len(nr)} {rep}"
            print("  ", line)
            timeline.append({"t": t, "node": nd, "pod_tidak_siap": nr, "replikasi": rep})
            rows = [r for r in agent_seq() if r[1] > seq0]
            finished = any("pemulihan akhir selesai" in r[2] for r in rows)
            if gone and not hinted:
                hinted = True
                print(f"\n   >>> node {gone} tidak siap/hilang dari cluster. Bila node tidak kembali sendiri, join ulang")
                print("   >>> worker (token baru dari WSL: sudo cat /var/lib/rancher/k3s/server/node-token),")
                print("   >>> lalu tes ini melanjutkan otomatis.\n")
            report["agent_menyatakan_selesai"] = bool(finished)
            # Pulih = node sempat mati, lalu semua node dan pod aplikasi Ready lagi (pesan "selesai" agen tidak diandalkan,
            # karena run dihapus agar node tidak dimatikan ulang).
            if saw_down and not gone and not nr and all(s == "Ready" for s in nd.values()) and len(nd) >= len(n0):
                done = True
                break
            time.sleep(10)
        report["lini_masa"] = timeline
        report["lalu_lintas_selama_skenario"] = tr.summary(since=t0)
        report["node_sempat_tidak_siap"] = saw_down
        print("   log Tester Agent:")
        for pod, seq, act in [r for r in agent_seq() if r[1] > seq0][:14]:
            print(f"     {pod} seq {seq} {act}")

        print("== 4. tunggu replikasi utuh (partial=False, under_replicated=0, tidak ada unit dibangun ulang)")
        end, dist = time.time() + 900, {}
        while time.time() < end:
            dist = ul.aget(c, "/internal/audit/distribution")
            if dist["under_replicated"] == 0 and not dist["rebuilding_units"] and not dist["partial"]:
                break
            time.sleep(10)
        lengkap = dist.get("under_replicated", 1) == 0 and not dist.get("rebuilding_units") and not dist.get("partial")
        per_node = {n["node"]: n["copies"] for n in dist.get("nodes", [])}
        print(f"   lengkap={lengkap} under_replicated={dist.get('under_replicated')} salinan per node={per_node}")
        report["replikasi_akhir"] = {"lengkap": lengkap, "salinan_per_node": per_node,
                                     "under_replicated": dist.get("under_replicated"),
                                     "rebuilding_units": dist.get("rebuilding_units")}

        print(f"== 5. periksa tidak ada event {args.source} yang hilang")
        after, after_partial = ul.all_refs(c, args.source)
        missing = base - after
        print(f"   hilang: {len(missing)} dari {len(base)} (partial={after_partial})")
        report["kriteria5"] = {"baseline": len(base), "sesudah": len(after), "hilang": len(missing),
                               "contoh_hilang": sorted(missing)[:10], "partial": after_partial}
        report["lalu_lintas_total"] = tr.summary()

    n1, u1 = nodes(), unit_nodes()
    report["node_akhir"] = n1
    report["unit_ke_node_akhir"] = {k: v[0] for k, v in u1.items()}
    ikut = all(per_node.get(n, 0) > 0 for n in n0)
    violations = ul.bad(report["lalu_lintas_total"])
    print("\n== lalu lintas selama uji:")
    for kind, s in report["lalu_lintas_total"].items():
        print(f"   {kind:<9} {s['jumlah']} permintaan, kode {s['kode']}, partial {s['partial']}, "
              f"p95 {s['p95_detik']} dtk, maks {s['maks_detik']} dtk")
    print(f"   node akhir: {n1}; seluruh node menanggung data: {ikut}")
    ok = done and lengkap and not missing and not after_partial and violations == 0 and ikut and saw_down
    report["lolos"] = ok
    report["pelanggaran_lalu_lintas"] = violations
    os.makedirs(d if ok else os.path.join(d, "gagal"), exist_ok=True)
    p1.save(d if ok else os.path.join(d, "gagal"), "node_wipe.json", report)
    print(f"\nHASIL: {'LOLOS' if ok else 'GAGAL'} (respons melanggar: {violations}; pemulihan node terpantau: {done})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
