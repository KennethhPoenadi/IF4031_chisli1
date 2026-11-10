FROM golang:1.26 AS builder
# Konteks build adalah folder services/ (lihat docker-compose.yml), karena modul telemetry dipakai bersama
# oleh semua service lewat `replace telemetry => ../telemetry`.
WORKDIR /src
COPY telemetry/go.mod telemetry/go.sum telemetry/
COPY aggregator/go.mod aggregator/go.sum aggregator/
WORKDIR /src/aggregator
RUN go mod download

COPY telemetry/ /src/telemetry/
COPY aggregator/ /src/aggregator/
RUN CGO_ENABLED=0 GOOS=linux go build -o aggregator ./cmd/aggregator

FROM alpine:3.22

RUN apk add --no-cache wget

RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app

WORKDIR /app

COPY --from=builder /src/aggregator/aggregator .

EXPOSE 8090

USER 65532:65532

CMD ["./aggregator"]
