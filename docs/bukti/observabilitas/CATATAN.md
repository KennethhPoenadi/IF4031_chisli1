# Observabilitas (kontrak platform M2 dan P5 kriteria 7-8)

Stack di namespace `observability`: Prometheus (metrik, aturan SLO), Grafana (dashboard), Tempo (trace), Loki (log), Alloy
(DaemonSet di kedua node: OTLP trace -> Tempo, log pod -> Loki). Prometheus dan Grafana di node control agar dashboard tetap hidup
saat node worker dimatikan; Loki dan Tempo di worker untuk membagi memori. Dipasang dengan `scripts/observability.sh` (repo infra).
Grafana: `http://grafana.localtest.me` lewat ingress (pengguna admin, kata sandi di Secret `observability/grafana-admin`).

Tangkapan (Chrome headless asli yang merender Grafana live, DevTools Page.captureScreenshot), rentang 30 menit yang mencakup game day
P5 pukul 14.39-14.46 WIB:

| Berkas | Isi |
|---|---|
| `grafana_services_red.png` | Dashboard metrik per service: throughput, error rate 5xx, penolakan terkontrol 429, respons per kode, latensi p50/p95/p99 |
| `grafana_jalur_peringatan.png` | Jalur peringatan Tim Lapangan: p95 latensi notifikasi, SLO pesan diproses <= 5 dtk (100%), event masuk/gagal, per sumber |
| `grafana_cluster.png` | Node siap, CPU dan memori per node, pod per node dan per role, restart container |
| `grafana_trace_lintas_layanan.png` | Satu permintaan `GET /hazard-events`: client-facing-api -> aggregator -> 3 x store.query, durasi tiap langkah |
| `grafana_log_correlation.png` | Log terstruktur client-facing-api dan aggregator untuk permintaan yang sama: correlation_id dan trace_id sama |

Membaca game day dari dashboard (P5 kriteria 7): pada "Event masuk per sumber" SENSOR_PANTAI turun ke nol sekitar 14.41 (sensor
dibekukan) lalu pulih; "Restart container" naik sekitar 14.41-14.42 (sensor di-restart liveness saat beku, lalu Tester Agent
kill-store); latensi notifikasi tetap di bawah 1 dtk dan SLO <= 5 dtk tetap 100%.

Catatan jujur:
- Pada trace contoh, satu span store.query bertanda error: satu unit penyimpanan menjawab gagal sementara dua unit lain sudah
  mencakup data (RF=2), sehingga permintaan tetap 200.
- Penyesuaian WSL: node-exporter tanpa mount root host (`/` di WSL bukan mount shared), Alloy di control diberi limit 1 CPU/512Mi.
- Aturan SLO memakai `le=~"5(\.0)?"` karena Prometheus 3 menormalisasi label bucket menjadi `5.0`.
