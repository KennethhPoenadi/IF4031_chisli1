FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o auth-service ./cmd/auth-service

FROM alpine:3.20

RUN addgroup -S app && adduser -S -G app app

WORKDIR /app

COPY --from=builder /app/auth-service .

USER app

CMD ["./auth-service"]
