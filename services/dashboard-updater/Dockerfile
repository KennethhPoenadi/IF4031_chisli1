FROM golang:1.26 AS builder
# Konteks build adalah folder services/ (lihat docker-compose.yml), karena modul telemetry dipakai bersama
# oleh semua service lewat `replace telemetry => ../telemetry`.
WORKDIR /src
COPY telemetry/go.mod telemetry/go.sum telemetry/
COPY dashboard-updater/go.mod dashboard-updater/go.sum dashboard-updater/
WORKDIR /src/dashboard-updater
RUN go mod download

COPY telemetry/ /src/telemetry/
COPY dashboard-updater/ /src/dashboard-updater/
RUN CGO_ENABLED=0 GOOS=linux go build -o dashboard-updater ./cmd/dashboard-updater

FROM alpine:3.22

RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app

WORKDIR /app

COPY --from=builder /src/dashboard-updater/dashboard-updater .

USER 65532:65532

CMD ["./dashboard-updater"]
