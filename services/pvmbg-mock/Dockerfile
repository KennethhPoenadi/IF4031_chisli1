FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pvmbg-mock ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/pvmbg-mock /usr/local/bin/pvmbg-mock
USER app
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/pvmbg-mock"]
