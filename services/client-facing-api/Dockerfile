FROM golang:1.26 AS builder
# Konteks build adalah folder services/ (lihat docker-compose.yml), karena modul telemetry dipakai bersama
# oleh semua service lewat `replace telemetry => ../telemetry`.
WORKDIR /src
COPY telemetry/go.mod telemetry/go.sum telemetry/
COPY client-facing-api/go.mod client-facing-api/go.sum client-facing-api/
WORKDIR /src/client-facing-api
RUN go mod download

COPY telemetry/ /src/telemetry/
COPY client-facing-api/ /src/client-facing-api/
RUN CGO_ENABLED=0 GOOS=linux go build -o client-facing-api ./cmd/client-facing-api

FROM alpine:3.22

RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app

WORKDIR /app

COPY --from=builder /src/client-facing-api/client-facing-api .

USER 65532:65532

CMD ["./client-facing-api"]
