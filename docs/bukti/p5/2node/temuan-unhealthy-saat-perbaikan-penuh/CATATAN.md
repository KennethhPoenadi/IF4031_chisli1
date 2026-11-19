# Temuan: P5 unhealthy gagal saat perbaikan penyimpanan penuh sedang berjalan

Run 19 November 2026 19:36 WIB, image `47c24bf5`.

## Hasil

| Kriteria | Hasil |
|---|---|
| Permintaan sumber lain selama gangguan gagal | 0 dari 180 (lolos) |
| Permintaan nyata ke sensor yang gagal | 9, batas 20 (lolos, 71 ditahan circuit breaker) |
| Sensor dipakai lagi tanpa intervensi | ya, setelah 20 dtk (lolos) |
| p95 latensi beban sumber lain | **10,01 dtk, batas 2 dtk (gagal)** |

## Penyebab

Run ini dimulai 11 menit setelah uji P2 rolling me-restart semua aggregator. Leader baru selalu menjalankan satu perbaikan
penyimpanan penuh. Agar memori tetap kecil, perbaikan dibagi menjadi 8 bagian (`repairEntriesPerPass` = 40.000 dari
sekitar 320.000 event), dan setiap bagian memindai seluruh koleksi `hazard_events` di ketiga unit.

Log `store-1` (di worker MacBook) menunjukkan pemindaian penuh tanpa filter (`COLLSCAN`, sekitar 90.000 dokumen per
batch) berulang kira-kira tiap menit, dengan durasi 4 sampai 46 detik per batch. Pada saat yang sama CPU worker 73 sampai 83%
dan `store-1` memakai 450 sampai 770m. Beban dashboard dalam uji ini (`/dashboard/api/summary`) ikut membaca `store-1`,
sehingga latensinya naik sampai batas waktu 10 detik. Lonjakan CPU juga membuat HPA menaikkan aggregator menjadi 5 replika
dan leader berganti lagi pada 19:39, yang memulai perbaikan penuh berikutnya.

Saat cluster tidak sedang memperbaiki, endpoint yang sama menjawab dalam 0,2 sampai 1,1 detik.

## Tindak lanjut

Uji diulang setelah perbaikan selesai dan replika aggregator kembali stabil. Fungsi yang diuji P5 kriteria 3 dan 4
(circuit breaker dan pemulihan tanpa intervensi) sudah bekerja pada run ini; yang gagal hanya latensi akibat beban perbaikan.

## Run kedua (19:58 WIB, `run2-1958/`)

Diulang setelah perbaikan penuh selesai (log leader 19:52: `perbaikan penyimpanan selesai`, 449.883 kunci dipindai).
Hasilnya lebih buruk: 45 dari 61 permintaan dashboard gagal (batas waktu 10 dtk). Jadi penyebab utamanya bukan perbaikan,
melainkan `/dashboard/api/summary` pada build ini tidak tahan 8 permintaan serentak saat `store-1` (worker MacBook yang sedang
dipakai) sibuk. Bukti yang lolos di folder utama diambil 19 November 02:01 WIB, sebelum perubahan waktu analitik dashboard
(`f8806cc`, 13:27 WIB); saat itu p95 beban yang sama 1,02 dtk dengan 0 gagal.
