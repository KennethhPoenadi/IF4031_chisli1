# P4 kriteria 4: lag pemrosesan terlihat di dashboard

- `img/dashboard_satelit.png`: kartu "Aliran satelit (lag pemrosesan)" di Dashboard Analitik (`/dashboard`), dibuka di Chrome lewat
  ingress Traefik. Ditangkap dari tab Chrome yang sedang terbuka dengan DevTools `Page.captureScreenshot` (render asli peramban,
  tanpa bingkai jendela), karena jendela Chrome di belakang jendela lain tidak digambar ulang untuk `PrintWindow`.
- `p4_dashboard_satelit.json`: isi `/dashboard/api/satellite` pada saat yang sama.
- Kartu menampilkan: jumlah kejadian tertinggal dari feed, lag waktu virtual, p95 latensi data masuk hingga HazardEvent tersimpan,
  offset tersimpan dibanding offset terbaru di feed, kebakaran aktif, deteksi diterima/ditarik/duplikat, dan yang melewati batas
  keterlambatan. Badge "Mutakhir" / "Mengejar" / "Pemroses tidak memperbarui".

Catatan jujur tentang angka p95 pada tangkapan ini (14 dtk): p95 dihitung dari 5.000 sampel terakhir, dan pod pemroses baru saja
diganti oleh rollout (13.13 WIB), sehingga jendela sampel masih memuat masa mengejar sesudah pemulihan (p50 0,57 dtk, p99 22 dtk).
p95 pada kondisi stabil terukur 1,67 dtk (`p4_live.json`, tertinggal maksimum 0 kejadian).

Perilaku feed yang teramati: bila diminta dari offset terbaru, server mengirim kejadian yang tersedia lalu menutup stream (EOF).
Pemroses menyambung ulang tiap sekitar 2 dtk dan melanjutkan dari offset terakhir (lihat juga `p4_reconnect.json`).
