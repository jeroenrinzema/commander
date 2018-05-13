# Builder
FROM golang:1.10-alpine AS builder

WORKDIR /go/src/webservice
COPY . .

# Install librdkafka
RUN apk update && \
    apk add libressl-libcrypto libressl-libssl --update-cache --repository http://nl.alpinelinux.org/alpine/edge/main && \
    apk add librdkafka --update-cache --repository http://nl.alpinelinux.org/alpine/edge/community && \
    apk add librdkafka-dev --update-cache --repository http://nl.alpinelinux.org/alpine/edge/community && \
    apk add git openssh openssl yajl-dev zlib-dev cyrus-sasl-dev openssl-dev build-base coreutils

RUN go get -d -v .
RUN go build -o build .

# Final
FROM alpine

# Install librdkafka
RUN apk update && \
    apk add libressl-libcrypto libressl-libssl --update-cache --repository http://nl.alpinelinux.org/alpine/edge/main && \
    apk add librdkafka --update-cache --repository http://nl.alpinelinux.org/alpine/edge/community && \
    apk add openssl

COPY --from=builder /go/src/webservice/build ./
ENTRYPOINT ["./build"]
