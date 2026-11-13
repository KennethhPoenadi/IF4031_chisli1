FROM golang:1.26 AS builder
WORKDIR /src
COPY telemetry/go.mod telemetry/go.sum telemetry/
COPY satellite-ingestor/go.mod satellite-ingestor/go.sum satellite-ingestor/
WORKDIR /src/satellite-ingestor
RUN go mod download

COPY telemetry/ /src/telemetry/
COPY satellite-ingestor/ /src/satellite-ingestor/
RUN CGO_ENABLED=0 GOOS=linux go build -o satellite-ingestor ./cmd/satellite-ingestor

FROM alpine:3.22

RUN apk add --no-cache ca-certificates && addgroup -S -g 65532 app && adduser -S -u 65532 -G app app

WORKDIR /app

COPY --from=builder /src/satellite-ingestor/satellite-ingestor .

USER 65532:65532

CMD ["./satellite-ingestor"]
