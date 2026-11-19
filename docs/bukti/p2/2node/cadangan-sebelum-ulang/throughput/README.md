# Throughput 3 instance Aggregator vs 1 instance (kriteria P2)

Hasil: **LOLOS**, rasio 3,71x (batas 2,0x). 1 instance 35,9 rps, 3 instance 133,1 rps (permintaan sukses 2xx per detik).

Cara ukur: replika Aggregator dikunci (HPA min=max) 1 lalu 3, beban sama (600 VU, 40 detik, `tests/tools/throughput_3x.sh`).
Kapasitas hanya dapat diukur pada beban yang melampaui kemampuan; karena itu beban 600 VU membuat tiap tahap berada di titik jenuh.

Kueri sengaja bervariasi (100 nilai `limit`). client-facing-api menggabungkan permintaan identik yang tiba bersamaan, sehingga dengan dua URL
tetap satu Aggregator sudah melayani 654 rps dan yang membatasi throughput bukan lagi jumlah Aggregator (lihat
`../temuan-throughput-kueri-sama`: rasio 1,52x pada kueri yang sama). Kueri bervariasi tidak dapat digabung dan benar-benar
menguji tier Aggregator.

Catatan: baris `HASIL: GAGAL` per tahap pada screenshot adalah pemeriksaan galat < 1% milik pembangkit beban untuk uji tunggal. Pada titik
jenuh galat (timeout 5xx) memang melebihi 1%; itu bukan bagian kriteria throughput, yang hanya membandingkan permintaan sukses per detik.
Ukuran pada 40 VU (tidak jenuh, 1,42x karena dibatasi jumlah VU) ada di `../temuan-throughput-40vu-tidak-jenuh`.
