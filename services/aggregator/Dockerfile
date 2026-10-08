FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o aggregator ./cmd/aggregator

FROM alpine:3.20

RUN apk add --no-cache wget

RUN addgroup -S app && adduser -S -G app app

WORKDIR /app

COPY --from=builder /app/aggregator .

EXPOSE 8090

USER app

CMD ["./aggregator"]
