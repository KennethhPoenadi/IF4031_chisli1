FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o aggregator ./cmd/aggregator

FROM alpine:3.22

RUN apk add --no-cache wget

RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app

WORKDIR /app

COPY --from=builder /app/aggregator .

EXPOSE 8090

USER 65532:65532

CMD ["./aggregator"]
