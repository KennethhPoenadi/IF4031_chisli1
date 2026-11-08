FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o dashboard-updater ./cmd/dashboard-updater

FROM alpine:3.22

RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app

WORKDIR /app

COPY --from=builder /app/dashboard-updater .

USER 65532:65532

CMD ["./dashboard-updater"]
