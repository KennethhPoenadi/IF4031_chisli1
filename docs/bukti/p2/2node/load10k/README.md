# Uji beban 10.000 VU, dua mesin, replika Aggregator 3 -> 5 -> 2

Hasil: **LOLOS**. Gagal bukan-429 gabungan 0,775% (batas 1%); laptop 0,76% dan Mac 0,84%, keduanya juga di bawah 1%.

| Sisi | VU | Permintaan | Gagal | % |
|---|---|---|---|---|
| Laptop (WSL, node control) | 8.000 | 577.068 | 4.412 | 0,76% |
| Mac (VM Colima, node worker) | 2.000 | 88.967 | 750 | 0,84% |
| Gabungan | 10.000 | 666.035 | 5.162 | 0,775% |

"Gagal" = 5xx, 401, kode lain, dan galat jaringan. 429 adalah penolakan terkontrol dan tidak dihitung gagal (sesuai spek).

## Cara uji
- Pembangkit beban: `tests/loadgen` (Go, satu goroutine per VU, token client_credentials per VU diperbarui sebelum kedaluwarsa 60 detik).
  k6 tidak muat di memori mesin penguji untuk 10.000 VU (sekitar 0,27-0,39 MB per VU; pembangkit Go sekitar 0,065 MB per VU).
- Dua mesin dimulai serempak lewat penanda `GO` (`tests/tools/ten_k.sh` di laptop; di Mac perintah `loadgen` menunggu `GO`).
- Replika Aggregator diganti 3 -> 5 -> 2 lewat HPA selama beban (`scale-timeline.txt`, `replicas.txt`); HPA dikembalikan ke 3..5 setelah uji.
- Bukti: `img/laptop.png` (konsol laptop), `img/mac.png` (terminal Mac; baris perintah disensor karena memuat kredensial uji,
  keluaran di bawahnya tidak diubah), `loadgen-laptop.json`, `logs/laptop.log`, `combined.json`.
  Angka Mac pada `combined.json` dicatat dari `img/mac.png`.

## Yang diperbaiki sampai lolos (setiap langkah diukur)
| Perubahan | Efek terukur |
|---|---|
| Penggabungan permintaan identik di client-facing-api (kelas prioritas dipisah) | gagal laptop 2,79% -> 1,86%, sukses 2,4x |
| Batas serentak 50 per replika, pool Mongo dipanaskan, timeout upstream 5 dtk | menghentikan kegagalan berantai (unit ditandai mati) |
| client-facing-api 6-10 replika, auth-service 4-6, probe liveness TCP sabar | menghapus OOMKilled dan pod yang dimatikan probe |
| Traefik: batas memori 1400Mi, idle backend 45 dtk (< 60 dtk server) | memori ingress stabil, 502 dari koneksi keep-alive basi berkurang |
| Argo CD abaikan selisih min/maxReplicas HPA | perubahan replika saat beban tidak dibatalkan selfHeal |

## Catatan jujur
- Uji sebelumnya yang gagal dan penyebabnya ada di riwayat commit infra. Dua uji awal tidak serempak (penanda GO lama belum dihapus);
  sudah diperbaiki dan hasil yang dipakai di sini dimulai serempak (log server berkas mencatat Mac menerima GO pada detik yang sama).
- Batas beban pada perangkat ini: laptop 8 core berbagi CPU dengan cluster dan ingress; di atas ini margin tipis
  (lonjakan sesaat 1-3,6% pada beberapa interval 5 detik; rata-rata keseluruhan di bawah 1%).
