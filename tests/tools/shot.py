#!/usr/bin/env python3
"""Merender keluaran terminal asli menjadi gambar PNG untuk bukti (selain JSON).

Gambar BUKAN tangkapan layar desktop: ini render teks dari berkas log yang dihasilkan tes yang sebenarnya, lengkap
dengan perintah dan waktu. Isi log tidak diubah (hanya kode warna ANSI dibuang). Memakai Microsoft Edge/Chrome headless
di Windows lewat WSL.

  python3 tests/tools/shot.py --log docs/bukti/p1/2node/logs/unit_loss.log \
      --title "P1 kriteria 4-6: kehilangan unit penyimpanan" --cmd "python3 tests/p1/unit_loss.py ..." \
      --out docs/bukti/p1/2node/img/unit_loss.png
"""
import argparse
import html
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime

BROWSERS = [
    "/mnt/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",
    "/mnt/c/Program Files/Microsoft/Edge/Application/msedge.exe",
    "/mnt/c/Program Files/Google/Chrome/Application/chrome.exe",
]
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LINE_PX, WIDTH = 19, 1280


def browser():
    for b in BROWSERS:
        if os.path.exists(b):
            return b
    sys.exit("Edge/Chrome tidak ditemukan di Windows")


def verdict_class(line):
    up = line.upper()
    if "HASIL" in up and "LOLOS" in up:
        return "ok"
    if "HASIL" in up and "GAGAL" in up:
        return "bad"
    return ""


def build(args, lines):
    rows = []
    for ln in lines:
        cls = verdict_class(ln)
        rows.append(f'<div class="{cls}">{html.escape(ln) or "&nbsp;"}</div>')
    stamp = args.time or datetime.fromtimestamp(os.path.getmtime(args.log)).strftime("%Y-%m-%d %H:%M:%S")
    return f"""<!doctype html><html><head><meta charset="utf-8"><style>
body{{margin:0;background:#0d1117;color:#c9d1d9;font:13px/{LINE_PX}px 'Cascadia Mono',Consolas,monospace}}
.bar{{background:#161b22;padding:10px 18px;border-bottom:1px solid #30363d}}
.t{{color:#fff;font:600 15px 'Segoe UI',sans-serif}}.m{{color:#8b949e;font:12px 'Segoe UI',sans-serif;margin-top:3px}}
.cmd{{color:#79c0ff;padding:10px 18px 0}}pre,.o{{padding:6px 18px 14px}}.ok{{color:#3fb950;font-weight:700}}.bad{{color:#f85149;font-weight:700}}
.f{{color:#6e7681;font:11px 'Segoe UI',sans-serif;padding:8px 18px;border-top:1px solid #30363d}}
</style></head><body>
<div class="bar"><div class="t">{html.escape(args.title)}</div>
<div class="m">{html.escape(args.where)} &middot; {stamp}</div></div>
<div class="cmd">$ {html.escape(args.cmd)}</div>
<div class="o">{''.join(rows)}</div>
<div class="f">Render dari keluaran asli tes (berkas: {html.escape(os.path.basename(args.log))}); isi tidak diubah.</div>
</body></html>"""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--title", required=True)
    ap.add_argument("--cmd", default="")
    ap.add_argument("--where", default="cluster k3s 2 node: chisli-control (WSL, amd64) + chisli-worker-1 (Mac, arm64)")
    ap.add_argument("--time", default="")
    ap.add_argument("--max-lines", type=int, default=70)
    args = ap.parse_args()

    raw = ANSI.sub("", open(args.log, errors="replace").read()).rstrip("\n").split("\n")
    lines = raw if len(raw) <= args.max_lines else raw[: args.max_lines // 3] + ["... (dipotong, lihat berkas log lengkap) ..."] + raw[-(args.max_lines * 2 // 3):]
    height = 150 + LINE_PX * (len(lines) + 1)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with tempfile.TemporaryDirectory(dir="/mnt/c/Users/kenne/AppData/Local/Temp") as tmp:
        page = os.path.join(tmp, "p.html")
        open(page, "w", encoding="utf-8").write(build(args, lines))
        win_tmp = subprocess.check_output(["wslpath", "-w", tmp], text=True).strip()
        png_win = win_tmp + "\\out.png"
        url = "file:///" + (win_tmp + "\\p.html").replace("\\", "/")
        subprocess.run([browser(), "--headless=new", "--disable-gpu", "--hide-scrollbars", "--force-device-scale-factor=1.25",
                        f"--window-size={WIDTH},{height}", f"--screenshot={png_win}", url],
                       capture_output=True, timeout=120)
        src = os.path.join(tmp, "out.png")
        if not os.path.exists(src):
            sys.exit("render gagal: PNG tidak terbentuk")
        with open(src, "rb") as f, open(args.out, "wb") as g:
            g.write(f.read())
    print(f"gambar: {args.out} ({os.path.getsize(args.out) // 1024} KB)")


if __name__ == "__main__":
    main()
