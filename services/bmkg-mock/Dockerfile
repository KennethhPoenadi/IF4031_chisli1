FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bmkg-mock ./cmd/server

FROM alpine:3.22
RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app
COPY --from=build /out/bmkg-mock /usr/local/bin/bmkg-mock
USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/bmkg-mock"]
