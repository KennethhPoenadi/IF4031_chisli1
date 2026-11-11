FROM golang:1.26-alpine AS build
WORKDIR /src
COPY telemetry/go.mod telemetry/go.sum telemetry/
COPY pvmbg-mock/go.mod pvmbg-mock/go.sum pvmbg-mock/
WORKDIR /src/pvmbg-mock
RUN go mod download

COPY telemetry/ /src/telemetry/
COPY pvmbg-mock/ /src/pvmbg-mock/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pvmbg-mock ./cmd/server

FROM alpine:3.22
RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app
COPY --from=build /out/pvmbg-mock /usr/local/bin/pvmbg-mock
USER 65532:65532
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/pvmbg-mock"]
