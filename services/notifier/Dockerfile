FROM golang:1.26 AS builder
# Konteks build adalah folder services/ (lihat docker-compose.yml), karena modul telemetry dipakai bersama
# oleh semua service lewat `replace telemetry => ../telemetry`.
WORKDIR /src
COPY telemetry/go.mod telemetry/go.sum telemetry/
COPY notifier/go.mod notifier/go.sum notifier/
WORKDIR /src/notifier
RUN go mod download

COPY telemetry/ /src/telemetry/
COPY notifier/ /src/notifier/
RUN CGO_ENABLED=0 GOOS=linux go build -o notifier ./cmd/notifier

FROM alpine:3.22

RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app

WORKDIR /app

COPY --from=builder /src/notifier/notifier .

USER 65532:65532

CMD ["./notifier"]
