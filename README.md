# IF4031_chisli1

Platform koordinasi kebencanaan BNPB untuk Milestone 1 IF4031. Aggregator BNPB mengambil data gempa dari BMKG dan data gunung api dari PVMBG (keduanya mock), menyatukannya ke format HazardEvent, menyimpannya di MongoDB, lalu menyebarkannya lewat RabbitMQ ke dua consumer dan lewat API ke client.

## Service dan port

| Service | Port di host | Keterangan |
|---|---|---|
| client-facing-api | 8080 | endpoint untuk client, `GET /hazard-events` |
| bmkg-mock | 8081 | header `X-BMKG-Key` |
| pvmbg-mock | 8082 | header `Authorization: Bearer` |
| aggregator | 127.0.0.1:8090 | polling, mapping, Canonical Store, hanya dipakai antar service |
| auth-service | 8091 | penerbit token |
| rabbitmq | 127.0.0.1:5672 dan 15672 | 15672 untuk UI manajemen |
| notifier, dashboard-updater | tidak dibuka ke host | consumer, health di 8093 dan 8092 di dalam jaringan Docker |
| mongo | tidak dibuka ke host | hanya bisa dijangkau aggregator |

Port BMKG, PVMBG, dan client-facing-api bisa diganti lewat `BMKG_PORT`, `PVMBG_PORT`, dan `CLIENT_API_PORT`.

## Menjalankan

Butuh Docker dengan Compose v2. Untuk menjalankan test dibutuhkan Go, untuk load test dibutuhkan k6 dan jq.

```
cp .env.example .env
openssl rand -base64 32
```

Tempel hasil `openssl` ke `AUTH_SIGNING_SEED` di `.env`, lalu:

```
docker compose up -d --build
docker compose ps
```

Semua service sehat setelah beberapa detik. Mematikan:

```
docker compose down
```

Tambahkan `-v` untuk ikut menghapus data Mongo dan RabbitMQ.

## Konfigurasi

Daftar lengkap ada di `.env.example`. Yang paling sering diubah:

| Variabel | Default | Fungsi |
|---|---|---|
| `AGGREGATOR_POLL_INTERVAL_SECONDS` | 5 | interval polling ke BMKG dan PVMBG |
| `AGGREGATOR_BMKG_TIMEOUT_MS`, `AGGREGATOR_PVMBG_TIMEOUT_MS` | 2000, 4000 | timeout per sumber |
| `AGGREGATOR_PVMBG_STALE_AFTER_SECONDS` | 30 | batas data vulkanik ditandai stale |
| `BMKG_DELAY_MS` | 100 | delay tetap BMKG, boleh 50 sampai 150 |
| `PVMBG_DELAY_MIN_MS`, `PVMBG_DELAY_MAX_MS` | 500, 3000 | rentang delay acak PVMBG |
| `BMKG_GENERATION_INTERVAL_SECONDS`, `PVMBG_GENERATION_INTERVAL_SECONDS` | 10 | jarak kemunculan data baru di mock |
| `AUTH_ACCESS_TTL_SECONDS`, `AUTH_REFRESH_TTL_SECONDS` | 60, 600 | umur token |
| `CLIENT_API_MAX_INFLIGHT` | 200 | batas request bersamaan, selebihnya dijawab 429 |
| `CLIENT_API_REVOCATION_POLL_MS` | 250 | jeda sinkron daftar token yang dicabut |
| `NOTIFIER_MIN_SEVERITY`, `NOTIFIER_BINDING_KEYS` | WASPADA, `hazard.#` | batas alert dan filter routing key |

Kredensial demo di `.env.example` dipakai pada contoh di bawah. Service menolak start kalau secret wajib kosong.

## Skenario

Ambil token untuk dipakai di contoh berikutnya. Nilainya berlaku 60 detik, ulangi perintah ini kalau sudah kedaluwarsa.

```
TOKEN=$(curl -s -X POST localhost:8091/token -d grant_type=client_credentials -d client_id=ops-demo -d client_secret=ops-demo-secret | jq -r .access_token)
curl -s -H "Authorization: Bearer $TOKEN" "localhost:8080/hazard-events?limit=5" | jq
```

### P1, perubahan skema tanpa restart

Awalnya `confidence_level` belum ada. Nyalakan di PVMBG, tunggu satu siklus polling, lalu lihat atribut event vulkanik:

```
curl -s -X POST localhost:8082/admin/schema-version -H "Authorization: Bearer pvmbg-demo-token" -H "Content-Type: application/json" -d '{"enabled":true}'
curl -s -H "Authorization: Bearer $TOKEN" "localhost:8080/hazard-events?hazard_type=VOLCANIC&limit=5" | jq '.data[] | {hazard_id, attributes}'
```

Event baru punya `confidence_level` di `attributes`, event lama tidak, dan keduanya terbaca. Field yang tidak dikenal selalu diteruskan ke `attributes`. Perubahan yang hanya menambah field didukung, sedangkan hapus, ganti nama, atau ganti tipe field yang sudah dikenal tidak didukung dan event itu dilewati.

### P2, konkurensi dan degradasi

Untuk uji dengan delay PVMBG 3 detik, set `PVMBG_DELAY_MIN_MS=3000` dan `PVMBG_DELAY_MAX_MS=3000` di `.env`. Untuk load test naikkan juga `AUTH_ACCESS_TTL_SECONDS=300` karena token 60 detik habis di tengah test. Terapkan:

```
docker compose up -d pvmbg-mock auth-service
```

Lalu jalankan k6:

```
TOKEN=$(curl -s -X POST localhost:8091/token -d grant_type=client_credentials -d client_id=ops-demo -d client_secret=ops-demo-secret | jq -r .access_token)
API_TOKEN=$TOKEN k6 run tests/k6/p2-isolation.js
API_TOKEN=$TOKEN k6 run tests/k6/p2-sustained.js
API_TOKEN=$TOKEN PVMBG_TOKEN=pvmbg-demo-token k6 run tests/k6/p2-outage.js
```

`p2-isolation.js` membandingkan latensi request BMKG saja dengan request PVMBG, `p2-sustained.js` memakai 50 koneksi selama 60 detik (ubah dengan `VUS` dan `TEST_DURATION`), `p2-outage.js` menyalakan dan mematikan outage sendiri.

Outage juga bisa dinyalakan manual. Selama outage data seismik tetap normal dan data vulkanik dijawab dari Mongo dengan `meta.sources.pvmbg.stale_since`:

```
curl -s -X POST localhost:8082/admin/outage -H "Authorization: Bearer pvmbg-demo-token" -H "Content-Type: application/json" -d '{"enabled":true}'
curl -s -H "Authorization: Bearer $TOKEN" "localhost:8080/hazard-events?hazard_type=VOLCANIC&limit=1" | jq .meta
curl -s -X POST localhost:8082/admin/outage -H "Authorization: Bearer pvmbg-demo-token" -H "Content-Type: application/json" -d '{"enabled":false}'
```

### P3, kredensial dan hak akses

Kredensial silang antar instansi harus ditolak, keduanya menghasilkan 403:

```
curl -i -H "X-BMKG-Key: pvmbg-demo-token" localhost:8081/seismic-events
curl -i -H "Authorization: Bearer bmkg-demo-key" localhost:8082/volcanic-reports
```

Client Media hanya menerima field ringkasan, dan permintaan eksplisit ke field mentah dijawab 403:

```
MEDIA=$(curl -s -X POST localhost:8091/token -d grant_type=client_credentials -d client_id=media-demo -d client_secret=media-demo-secret | jq -r .access_token)
curl -s -H "Authorization: Bearer $MEDIA" "localhost:8080/hazard-events?limit=1" | jq
curl -i -H "Authorization: Bearer $MEDIA" "localhost:8080/hazard-events?fields=latitude"
```

Refresh token, memakai client Tim Lapangan:

```
LOGIN=$(curl -s -X POST localhost:8091/token -d grant_type=client_credentials -d client_id=field-demo -d client_secret=field-demo-secret)
OLD=$(echo $LOGIN | jq -r .access_token)
RT=$(echo $LOGIN | jq -r .refresh_token)
curl -s -X POST localhost:8091/token -d grant_type=refresh_token -d refresh_token=$RT | jq
```

Tunggu sekitar 2 detik lalu pakai `$OLD` ke `/hazard-events`, hasilnya 401 `token_revoked`. Kalau menunggu lebih dari 60 detik sebelum refresh, hasilnya 401 `token_expired`, dan refresh token tetap berlaku sampai 600 detik. Memakai refresh token yang sama dua kali mencabut seluruh sesi.

### P4, container independen dan penyimpanan

Build ulang satu service tanpa menyentuh yang lain, sambil memeriksa bahwa client API tetap menjawab:

```
docker compose up -d --build --no-deps aggregator
curl -s localhost:8080/health
```

Mongo hanya terjangkau dari aggregator, perintah ini harus gagal:

```
docker compose exec client-facing-api wget -T 2 -qO- http://mongo:27017
```

### P5, pub-sub

Matikan satu consumer, biarkan beberapa event terbit, lalu nyalakan lagi. Event yang terlewat dikirim ulang karena queue-nya durable:

```
docker compose stop notifier
docker compose start notifier
docker compose logs notifier
docker compose exec dashboard-updater wget -qO- localhost:8092/state
```

Menambah consumer baru tanpa mengubah aggregator, cukup dengan queue baru:

```
docker compose run -d --no-deps --name consumer3 -e DASHBOARD_UPDATER_QUEUE=consumer3-queue -e DASHBOARD_UPDATER_HEALTH_PORT=8094 dashboard-updater
docker logs consumer3
docker rm -f consumer3
```

Pengiriman bersifat at-least-once, jadi consumer membuang duplikat berdasarkan `hazard_id` dan severity.

### Log

Semua service menulis log JSON ke stdout dengan `correlation_id`. Satu alur dilacak dengan:

```
docker compose logs | grep <correlation_id>
```

Log tidak disimpan permanen, simpan dulu dengan `docker compose logs --no-color > log.txt` sebelum container dihapus.

## Test

```
for s in services/*/
do
  (cd $s && go test ./...)
done
```

## Kontribusi anggota

| NIM | Nama | Kontribusi |
|---|---|---|
| 13523008 | Varel Tiara | Mock BMKG dan PVMBG, load testing dengan k6, Problem 4 (service terpisah dalam container, MongoDB sebagai Canonical Store, Docker Compose), daftar teknologi, penyatuan laporan |
| 13523029 | Bryan Ho | Problem 3 (auth-service, scope client, refresh token, penegakan hak akses di client-facing-api), pendahuluan, asumsi, petunjuk menjalankan, penulisan laporan |
| 13523040 | Kenneth Poenadi | Problem 1, 2, dan 5 (aggregator, polling, pemetaan, degradasi saat sumber mati, RabbitMQ dan consumer), arsitektur sistem, penulisan laporan |

Seluruh anggota ikut berkontribusi pada perancangan, implementasi, dan penulisan laporan.
