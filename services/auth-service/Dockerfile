FROM golang:1.26 AS builder
WORKDIR /src
COPY telemetry/go.mod telemetry/go.sum telemetry/
COPY auth-service/go.mod auth-service/go.sum auth-service/
WORKDIR /src/auth-service
RUN go mod download

COPY telemetry/ /src/telemetry/
COPY auth-service/ /src/auth-service/
RUN CGO_ENABLED=0 GOOS=linux go build -o auth-service ./cmd/auth-service

FROM alpine:3.22

RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app

WORKDIR /app

COPY --from=builder /src/auth-service/auth-service .

USER 65532:65532

CMD ["./auth-service"]
