FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pvmbg-mock ./cmd/server

FROM alpine:3.22
RUN addgroup -S -g 65532 app && adduser -S -u 65532 -G app app
COPY --from=build /out/pvmbg-mock /usr/local/bin/pvmbg-mock
USER 65532:65532
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/pvmbg-mock"]
