FROM golang:1.22 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o auth-service ./cmd/auth-service

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/auth-service .

CMD ["./auth-service"]
