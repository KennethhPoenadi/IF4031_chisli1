FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o dashboard-updater ./cmd/dashboard-updater

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/dashboard-updater .

CMD ["./dashboard-updater"]
