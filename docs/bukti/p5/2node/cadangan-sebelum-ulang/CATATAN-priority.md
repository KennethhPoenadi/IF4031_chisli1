# P5 kriteria 5: Tim Lapangan tetap dilayani saat Media dibanjiri

Bukti utama: `img/priority.png`, `logs/priority.log`, `p5_priority.json` (1000 VU Media + 50 VU Tim Lapangan, 45 dtk).
Konfirmasi: `priority-konfirmasi/` (run kedua, konfigurasi sama).

| Run | Tim Lapangan sukses | p95 | p99 | Jawaban stale |
|---|---|---|---|---|
| bukti | 100% (7.181/7.181) | 0,63 dtk | 0,84 dtk | 0% |
| konfirmasi | 100% (9.536/9.536) | 0,45 dtk | 0,65 dtk | 0% |

Perjalanan perbaikan (setiap run gagal disimpan di folder `temuan-priority-*` dan `sebelum-*`):

1. Batas in-flight per pod disesuaikan terhadap kapasitas aggregator lintas replika (72-80% -> 85%).
2. Pembacaan lintas unit penyimpanan selesai begitu unit yang menjawab mencakup seluruh data (RF=2), tanpa menunggu unit
   lambat di seberang Tailscale (85% -> 97%).
3. Hedging panggilan kelas prioritas, log 429 disampel, dan batas tunggu Tim Lapangan 2 dtk dengan jawaban terakhir bertanda
   stale (maks 30 dtk) sebagai degradasi terkendali.
4. Akar masalah terakhir: Traefik hanya ada di node control, tetapi pod API tersebar ke worker, sehingga setengah lalu lintas
   ingress menyeberang Tailscale yang jenuh oleh banjir Media (pod di control p99 108 ms, pod di worker p99 2,6 dtk, 378 dari
   1.980 permintaan tidak pernah sampai ke pod). Pod API kini diutamakan di node ingress -> 100%.
5. Percobaan nativelb + trafficDistribution PreferSameNode menghasilkan 502 dari Traefik di bawah beban, jadi dicabut
   (`temuan-priority-nativelb`).

Cache 1 dtk untuk kelas Media sempat dicoba dan memperburuk hasil (Media lebih banyak dilayani sehingga beban ingress naik);
kodenya tetap ada tetapi dimatikan (`temuan-priority-run4-cache`).
