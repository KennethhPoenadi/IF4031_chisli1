# Topologi Arsitektur Cluster (Milestone 2)

Dokumen ini menggambarkan cluster yang dipakai untuk semua bukti di `docs/bukti/p1..p5/2node`. Angka diambil langsung dari cluster
(`kubectl get nodes`, `kubectl get pods -o wide`, `lscpu`, `free`) pada 19 November 2026, bukan dari rencana.

## 1. Ringkasan

Cluster **k3s dua node** yang berjalan di dua laptop berbeda dan dihubungkan lewat jaringan overlay **Tailscale**.
Semua beban kerja berada di satu namespace, `sistem-informasi-kebencanaan-bnpb`. Penyebaran aplikasi dikelola dengan **GitOps** (Argo CD).

```
                        Internet / feed Satelit
                  (https://satellite.naufarrel.dev, Cloudflare)
                                   ^
                                   | HTTPS (pemroses satelit)
 +---------------------------------+--------------------------------------+        Tailscale (WireGuard)        +----------------------------------+
 |  NODE 1: chisli-control  (control-plane + worker)                       | <==================================> |  NODE 2: chisli-worker-1 (worker) |
 |  Laptop Windows (i7-12650H, 16 GB), WSL2 Ubuntu 26.04, amd64           |   100.66.57.46      100.109.151.22   |  MacBook Air M1 8 GB, VM Colima   |
 |  8 vCPU (i7-12650H), 9,7 GiB RAM terlihat oleh WSL                      |   (flannel antar node lewat tailnet) |  3 vCPU, 4,8 GiB RAM, arm64       |
 |                                                                         |                                      |                                   |
 |  k3s server, Traefik (ingress), Argo CD, KEDA, Kyverno                  |                                      |  k3s agent                        |
 |  store-0, store-2 (MongoDB)   rabbitmq-0                                |                                      |  store-1 (MongoDB)                |
 |  aggregator x3, client-facing-api x5, auth-service x1                   |                                      |  aggregator x2, client-facing-api |
 |  satellite-ingestor, sensor-pantai, dashboard-updater,                  |                                      |  x5, auth-service x3, notifier    |
 |  situation-analyst, bmkg-mock, pvmbg-mock, tester-agent                 |                                      |  tester-agent                     |
 +-------------------------------------------------------------------------+                                      +----------------------------------+
        ^
        | 127.0.0.1:80  (ingress Traefik; api./auth./ops.prod.localtest.me)
   k6, skrip bukti (tests/), peramban dashboard
```

## 2. Node

| | chisli-control | chisli-worker-1 |
|---|---|---|
| Peran | control-plane + menjalankan beban kerja | worker |
| Host fisik | Laptop Windows: Intel Core i7-12650H, RAM 16 GB | MacBook Air M1 (2020): Apple M1, RAM 8 GB, SSD 512 GB |
| Jenis node | VM WSL2 | VM Colima |
| CPU node | 8 vCPU | 3 vCPU |
| RAM node | 9,7 GiB (batas WSL2 10 GB) | 4,8 GiB (VM Colima) |
| Arsitektur | amd64 | **arm64** |
| OS | Ubuntu 26.04.1 LTS | Ubuntu 24.04.4 LTS |
| Kernel | 6.18.58.1-microsoft-standard-WSL2 | 6.8.0-117-generic |
| Container runtime | containerd 2.3.7 (k3s) | containerd 2.3.7 (k3s) |
| Kubernetes | v1.37.2+k3s1 | v1.37.2+k3s1 |
| Alamat Tailscale | 100.66.57.46 | 100.109.151.22 |

Catatan penting:

- **Dua arsitektur CPU (amd64 dan arm64).** Image yang dipakai tersedia untuk kedua arsitektur, sehingga pod bisa dijadwalkan di kedua node.
- Worker adalah **VM di dalam MacBook Air M1 (8 GB RAM)**, bukan seluruh MacBook: VM Colima mendapat 3 vCPU dan 4,8 GiB, sisanya untuk
  macOS. Node control juga VM (WSL2) dengan batas 10 GB dari RAM laptop 16 GB.
- Control node adalah laptop yang sama dengan mesin penguji (k6, skrip bukti, peramban). Beban uji dan cluster **berbagi CPU dan RAM**
  laptop itu, sehingga pada beban tinggi hasil uji ikut dibatasi oleh kapasitas laptop (memori control node sering di atas 80%).
- Kedua node berkomunikasi lewat Tailscale (WireGuard), bukan LAN langsung, sehingga lalu lintas antar node (misalnya ke `store-1`)
  memiliki latensi lebih tinggi daripada lalu lintas di dalam satu node.

## 3. Komponen platform

| Komponen | Namespace | Fungsi |
|---|---|---|
| k3s | kube-system | Kubernetes ringan (server di control, agent di worker), flannel untuk jaringan pod |
| Traefik | traefik | Ingress. Host: `api.prod.localtest.me`, `auth.prod.localtest.me`, `ops.prod.localtest.me` (127.0.0.1:80) |
| Argo CD | argocd | GitOps: CI menulis tag image ke repo infra, Argo CD yang menerapkannya |
| KEDA | keda | Autoscaling berbasis antrean untuk `notifier` dan `dashboard-updater` (1 sampai 5 replika) |
| Kyverno | kyverno | Kebijakan: image hanya boleh dari registry GitHub Container Registry grup |
| Tester Agent | tester-agent | DaemonSet milik asisten untuk skenario gangguan latihan (satu pod per node) |

Observabilitas (Alloy/Grafana) **tidak terpasang** di cluster ini; log diambil dengan `kubectl logs`.

## 4. Beban kerja aplikasi

Penempatan pod pada saat dokumen ini ditulis. Scheduler dapat memindahkan pod, jadi jumlah per node bisa berubah.

| Layanan | Replika (min-maks) | CPU request / limit | RAM request / limit | Penempatan |
|---|---|---|---|---|
| aggregator | 3-5 (HPA CPU 70%) | 100m / 1 | 256Mi / 512Mi | tersebar di kedua node (3 control, 2 worker) |
| client-facing-api | 6-10 (HPA CPU 70%) | 100m / 500m | 128Mi / 256Mi | tersebar (5 control, 5 worker) |
| auth-service | 4-6 (HPA CPU 70%) | 100m / 500m | 128Mi / 256Mi | tersebar (1 control, 3 worker) |
| store (MongoDB 7) | 3 (StatefulSet) | 200m / 1,5 | 512Mi / 1Gi | store-0, store-2 di control; **store-1 di worker** |
| rabbitmq | 1 (StatefulSet) | 100m / 500m | 512Mi / 1Gi | control |
| notifier | 1-5 (KEDA) | 100m / 500m | 128Mi / 256Mi | worker |
| dashboard-updater | 1-5 (KEDA) | 100m / 500m | 128Mi / 256Mi | control |
| satellite-ingestor | 1 | 100m / 500m | 128Mi / 256Mi | control |
| sensor-pantai | 1 | 100m / 500m | 128Mi / 256Mi | control |
| situation-analyst | 1 | 100m / 500m | 128Mi / 256Mi | control |
| bmkg-mock, pvmbg-mock | 1 | 100m / 500m | 64Mi / 128Mi | control |

Setiap unit penyimpanan memakai volume persisten 2 GiB.

## 5. Alur data

```
 BMKG mock ----\
 PVMBG mock -----+--> aggregator (leader election, satu instance mengambil sumber) --> shardstore --> store-0/1/2 (MongoDB)
 Sensor Pantai -/        ^  ^                                                              |   replikasi faktor 2
 Feed Satelit --> satellite-ingestor --(POST batch /internal/ingest/satellite)             |
                                                                                           v
 Klien (Media, Tim Lapangan, Ops) --> Traefik --> client-facing-api --> aggregator --> baca lintas unit
                                                       |
                                                       +--> RabbitMQ --> notifier, dashboard-updater, situation-analyst
```

- **Penyimpanan terdistribusi (P1):** data HazardEvent disebar ke tiga unit MongoDB dengan replikasi faktor 2. Satu unit berada di worker, dua di control,
  jadi kehilangan satu node membuat sebagian data hanya tersedia dari salinan lain.
- **Pengambilan data (P2):** hanya satu instance aggregator yang mengambil tiap sumber (lease dengan fencing token); instance lain menunggu.
- **Pesan tidak terbaca (P3):** disimpan di DLQ dengan status `parked` (permanen) atau `retrying` (sementara). Hanya Ops yang dapat memeriksanya.
- **Pemroses satelit (P4):** satellite-ingestor membaca aliran SSE feed, mengelompokkan deteksi menjadi kebakaran, lalu mengirim batch ke aggregator.
- **Jalur Tim Lapangan (P5):** kelas prioritas di client-facing-api menyisihkan 10 slot per pod untuk Tim Lapangan; kelas reguler (Media) dibatasi
  dan ditolak dengan 429 bila penuh.

## 6. Konfigurasi yang relevan untuk pengujian

| Parameter | Nilai | Keterangan |
|---|---|---|
| `CLIENT_API_MAX_INFLIGHT` | 25 per pod | Total permintaan serentak = batas x replika |
| `CLIENT_API_PRIORITY_RESERVED` | 10 per pod | Slot khusus Tim Lapangan |
| `CLIENT_API_REGULAR_CACHE_TTL_MS` | 1000 | Cache singkat untuk kelas reguler saja |
| `CLIENT_API_AGGREGATOR_TIMEOUT_MS` | 5000 | Batas tunggu ke aggregator |
| `STORE_UNIT_TIMEOUT_MS` | 5000 | Batas tunggu per unit penyimpanan |
| `DOC_TOMBSTONE_GRACE_SECONDS` | 7200 (bawaan) | Masa tenggang pembersihan tombstone |
| `SATELLITE_LATENESS_MINUTES` | 360 | Batas keterlambatan deteksi satelit |

## 7. Keterbatasan topologi yang perlu diketahui pembaca laporan

1. **Dua node saja, di dua laptop.** Pada beban sangat tinggi (misalnya 10.000 VU atau 1000 VU Media) mesin penguji dan control node berbagi CPU/RAM,
   sehingga angka throughput dan latensi ikut dibatasi oleh laptop, bukan hanya oleh rancangan sistem.
2. **Jalur Tailscale antar node** menambah latensi ke `store-1` dan membuat node worker lebih mudah timeout dibanding node control.
3. **Control node ikut menjalankan beban kerja** dan menampung dua dari tiga unit penyimpanan, sehingga kehilangan control node jauh lebih berat
   daripada kehilangan worker.
4. **Worker lebih kecil** (3 vCPU, 4,8 GiB) dan berarsitektur berbeda (arm64).
5. **Tanpa stack observabilitas**, sehingga metrik seperti lag satelit dan p95 latensi dibaca dari endpoint `/health` ingestor dan log, bukan dari dashboard metrik.
