FROM golang:1.26 AS builder
WORKDIR /src
COPY telemetry/go.mod telemetry/go.sum telemetry/
COPY situation-analyst/go.mod situation-analyst/go.sum situation-analyst/
WORKDIR /src/situation-analyst
RUN go mod download

COPY telemetry/ /src/telemetry/
COPY situation-analyst/ /src/situation-analyst/
RUN CGO_ENABLED=0 GOOS=linux go build -o situation-analyst ./cmd/situation-analyst

FROM alpine:3.22

RUN apk add --no-cache ca-certificates wget

RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app

WORKDIR /app

COPY --from=builder /src/situation-analyst/situation-analyst .

EXPOSE 8094

USER 65532:65532

CMD ["./situation-analyst"]
