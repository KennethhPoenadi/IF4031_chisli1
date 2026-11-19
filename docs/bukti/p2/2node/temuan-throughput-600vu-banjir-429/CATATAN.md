# Throughput 3 vs 1 instance pada build sekarang (19 Nov)

| Run | 1 instance | 3 instance | Rasio |
|---|---|---|---|
| 600 VU (skrip bawaan) | 566 rps | 452 rps | 0,80x |
| 120 VU (eksplorasi) | 1.266 rps | 1.898 rps | 1,50x |

Temuan dari pengukuran CPU per pod (`cpu-per-pod-120vu.log`):

1. Pada 600 VU hampir semua permintaan ditolak 429 oleh client-facing-api (518 ribu dari 541 ribu) dan langsung diulang,
   sehingga CPU laptop habis untuk banjir 429; throughput 2xx dibatasi laptop, bukan aggregator.
2. Dengan 3 aggregator hanya SATU pod yang menerima beban (354m), dua lainnya 6m: koneksi keep-alive dari client-facing-api ke
   aggregator dibuat sebelum skala naik dan terus dipakai, sedangkan kube-proxy menyeimbangkan per koneksi. Instance baru tidak
   menerima lalu lintas. Diperbaiki: aggregator menutup koneksi yang berumur > 30 dtk (`AGGREGATOR_MAX_CONN_AGE_SECONDS`).
3. Dengan 1 aggregator, aggregator memakai 482m dari limit 1 core, sementara store-0 (MongoDB) 1.002m dan node control 78%:
   setelah optimasi pembacaan lintas unit, biaya utama ada di MongoDB dan CPU laptop yang dipakai bersama mesin penguji.

Bukti utama `../throughput/` (3,71x) diambil pada build sebelumnya, ketika aggregator memang menjadi leher botol.
