FROM golang:1.26-alpine AS build
# Konteks build adalah folder services/ (lihat docker-compose.yml), karena modul telemetry dipakai bersama
# oleh semua service lewat `replace telemetry => ../telemetry`.
WORKDIR /src
COPY telemetry/go.mod telemetry/go.sum telemetry/
COPY bmkg-mock/go.mod bmkg-mock/go.sum bmkg-mock/
WORKDIR /src/bmkg-mock
RUN go mod download

COPY telemetry/ /src/telemetry/
COPY bmkg-mock/ /src/bmkg-mock/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bmkg-mock ./cmd/server

FROM alpine:3.22
RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app
COPY --from=build /out/bmkg-mock /usr/local/bin/bmkg-mock
USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/bmkg-mock"]
