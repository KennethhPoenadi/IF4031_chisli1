#!/usr/bin/env python3
"""Menampilkan log tes yang panjang secara ringkas supaya muat dalam satu layar (untuk screenshot).

Isi log TIDAK diubah: semua baris selain lini masa ("   t+ NNNs ...") ditampilkan apa adanya. Baris lini masa hanya disaring
(satu baris per sekitar INTERVAL detik, ditambah baris pertama, terakhir, dan setiap perubahan status node); jumlah baris yang
dilewati dinyatakan di layar. Log dibagi dua bagian pada baris "== 4." agar tiap bagian muat dalam satu layar:

  --part head   judul, langkah 0-3 dan lini masa
  --part tail   langkah 4 dan seterusnya (replikasi utuh, event hilang, lalu lintas, HASIL)

  python3 tests/tools/log_summary.py docs/bukti/p1/2node/logs/node_wipe.log --part head
"""
import argparse
import os
import re
import sys
from datetime import datetime

TL = re.compile(r"^\s+t\+\s*(\d+)s node=\{'chisli-control': '(\w+)', 'chisli-worker-1': '(\w+)'\}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("log")
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--part", choices=["all", "head", "tail"], default="all")
    ap.add_argument("--split", default="== 4.", help="awal baris yang memisahkan bagian head dan tail")
    ap.add_argument("--note", default="", help="catatan tambahan yang dicetak di akhir tampilan (pisahkan dengan |)")
    args = ap.parse_args()
    lines = open(args.log, errors="replace").read().rstrip("\n").split("\n")
    cut = next((i for i, l in enumerate(lines) if l.startswith(args.split)), len(lines))
    lo, hi = {"all": (0, len(lines)), "head": (0, cut), "tail": (cut, len(lines))}[args.part]

    tl_idx = [i for i in range(lo, hi) if TL.match(lines[i])]
    keep, last_t, last_state = set(), -10**9, None
    for n, i in enumerate(tl_idx):
        m = TL.match(lines[i])
        t, state = int(m.group(1)), (m.group(2), m.group(3))
        if n == 0 or n == len(tl_idx) - 1 or t - last_t >= args.interval or state != last_state:
            keep.add(i)
            last_t = t
        last_state = state

    mt = datetime.fromtimestamp(os.path.getmtime(args.log)).strftime("%Y-%m-%d %H:%M:%S")
    label = {"all": "", "head": " [bagian 1: langkah dan lini masa]", "tail": " [bagian 2: hasil]"}[args.part]
    print(f"# ringkasan layar dari log lengkap: {os.path.basename(args.log)}{label} ({len(lines)} baris, ditulis {mt})")
    if tl_idx:
        print(f"# lini masa: {len(keep)} dari {len(tl_idx)} baris ditampilkan (satu per ~{args.interval} dtk dan tiap perubahan status node)")
    for i in range(lo, hi):
        if i in tl_idx and i not in keep:
            continue
        print(lines[i][:215])
    if args.note:
        print()
        for part in args.note.split("|"):
            print("# catatan:", part.strip())


if __name__ == "__main__":
    sys.exit(main())
