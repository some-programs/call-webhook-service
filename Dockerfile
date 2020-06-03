from golang:1.14 as builder
add . /src
workdir /src
run go build .

from alpine:latest as certs
run apk --update add ca-certificates

from gcr.io/distroless/base
copy --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
copy --from=builder /src/call-webhook-service /call-webhook-service
entrypoint ["/call-webhook-service"]
