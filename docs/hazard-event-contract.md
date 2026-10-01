# HazardEvent JSON Contract

Source of truth untuk bentuk JSON `HazardEvent` yang dikirim lewat RabbitMQ
(`hazard_events` exchange) dan dibalikin Aggregator lewat `GET /hazard-events`.

Struct Go didefinisikan independen di tiga module (tidak ada shared Go
package, karena masing-masing punya `go.mod`/Docker build context sendiri):

- `services/aggregator/internal/hazard/hazard.go` (punya tag `bson` juga,
  karena ini yang nulis ke Mongo)
- `services/dashboard-updater/internal/hazard/hazard.go`
- `services/notifier/internal/hazard/hazard.go`

Field dan tipe JSON di ketiganya **wajib identik**. Kalau field ini berubah
(nama, tipe, atau ada field baru), update ketiga file di atas sekaligus,
jangan cuma salah satu.

## Bentuk JSON

```json
{
  "hazard_id": "hz-bmkg-000123",
  "source": "BMKG",
  "source_ref_id": "bmkg-000123",
  "hazard_type": "SEISMIC",
  "severity": "WASPADA",
  "area_name": "Selat Sunda",
  "latitude": -7.5,
  "longitude": 110.0,
  "occurred_at": "2026-09-25T14:11:50Z",
  "ingested_at": "2026-09-25T14:11:50Z",
  "attributes": {
    "magnitude": 5.4,
    "depth_km": 10.2,
    "potential_tsunami": false
  }
}
```

| Field | Tipe JSON | Enum nilai |
|---|---|---|
| `hazard_id` | string | `hz-<source_ref_id>` |
| `source` | string | `BMKG`, `PVMBG` |
| `source_ref_id` | string | - |
| `hazard_type` | string | `SEISMIC`, `VOLCANIC` |
| `severity` | string | `NORMAL`, `WASPADA`, `SIAGA`, `AWAS` |
| `area_name` | string | - |
| `latitude`, `longitude` | number | - |
| `occurred_at`, `ingested_at` | string (RFC3339) | - |
| `attributes` | object (map bebas) | field spesifik sumber, termasuk field baru yang belum ada padanan kanonik (mis. `confidence_level`) |

## Kenapa diduplikasi, bukan shared Go package

Tiap service (`aggregator`, `dashboard-updater`, `notifier`) punya
`go.mod` dan Docker build context sendiri-sendiri. Shared Go package lintas
module nambah kompleksitas build (perlu Go workspace atau module path
khusus) yang tidak sepadan buat scope M1 — field-nya kecil dan jarang
berubah. Kontrak JSON yang terdokumentasi di sini dianggap cukup.

## Yang bukan bagian kontrak sinkron

- Tag `bson` — cuma relevan di `aggregator` (satu-satunya yang nulis ke
  Mongo), service lain tidak perlu menambahkannya.
- Method tambahan seperti `Severity.Rank()` di `notifier` — boleh beda per
  service, bukan bagian kontrak yang harus disalin ke semua.
