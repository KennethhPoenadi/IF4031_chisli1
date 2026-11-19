# P5 kriteria 5 (priority): diagnosis

Tim Lapangan (50 VU) dengan Media dibanjiri, k6 dari laptop yang sama dengan control node, lewat Traefik di 127.0.0.1:80.

| Run | Konfigurasi | Tim Lapangan sukses | Catatan |
|---|---|---|---|
| awal | MAX_INFLIGHT=50 | 72-80% | aggregator CPU 229% |
| run3 | MAX_INFLIGHT=25 | 85,5% | p95 4,87 dtk |
| run4 | MAX_INFLIGHT=25 + cache reguler 1 dtk | 73,3% | cache memperburuk: Media dilayani (429 turun ke 68%), lebih banyak respons 10-19 KB lewat ingress |

Pembanding (MAX_INFLIGHT=25, k6 30 dtk):

| Beban | Tim Lapangan sukses |
|---|---|
| Tim Lapangan 50 VU sendirian | 99,5% (p95 0,98 dtk) |
| Tim Lapangan 10 VU sendirian | 100% |
| + Media 250 VU | 97,0% |
| + Media 500 VU | 96,2% |
| + Media 1000 VU | 85,5% |

Pengukuran CPU laptop control selama beban (top): traefik ~90% satu core, k6 ~78%, tailscaled ~39%, laptop secara keseluruhan masih ~56% idle.
Aggregator hampir menganggur (19-75m CPU). Traefik hanya 1 pod (hostPort 80 pada control node), jadi seluruh lalu lintas
Media dan Tim Lapangan melewati satu proses ingress yang berbagi laptop dengan mesin penguji.

Kesimpulan: pada konfigurasi satu laptop ini, jalur Tim Lapangan terdegradasi oleh jenuhnya ingress dan CPU laptop, bukan oleh logika prioritas
di client-facing-api (kelas prioritas mempunyai slot cadangan 10 per pod dan tidak digabung dengan kelas reguler). Kriteria 99% belum tercapai pada beban 1000 VU Media.
