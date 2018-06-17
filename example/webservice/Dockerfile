# Builder
FROM golang:1.10-alpine AS builder

ENV GO_PROJECT_PATH=webservice
ENV LIBRESSL_VERSION=2.7
ENV LIBRDKAFKA_VERSION=0.11.4-r1

WORKDIR /go/src/${GO_PROJECT_PATH}
COPY . .

# Install librdkafka
RUN apk update && \
    apk add libressl${LIBRESSL_VERSION}-libcrypto libressl${LIBRESSL_VERSION}-libssl --update-cache --repository http://nl.alpinelinux.org/alpine/edge/main && \
    apk add librdkafka=${LIBRDKAFKA_VERSION} --update-cache --repository http://nl.alpinelinux.org/alpine/edge/community && \
    apk add librdkafka-dev=${LIBRDKAFKA_VERSION} --update-cache --repository http://nl.alpinelinux.org/alpine/edge/community && \
    apk add git openssh openssl yajl-dev zlib-dev cyrus-sasl-dev openssl-dev build-base coreutils

RUN go get -u github.com/golang/dep/cmd/dep
RUN dep ensure
RUN go build -o build .

# Final
FROM alpine

ENV GO_PROJECT_PATH=webservice
ENV LIBRESSL_VERSION=2.7
ENV LIBRDKAFKA_VERSION=0.11.4-r1

# Install librdkafka
RUN apk update && \
    apk add libressl${LIBRESSL_VERSION}-libcrypto libressl${LIBRESSL_VERSION}-libssl --update-cache --repository http://nl.alpinelinux.org/alpine/edge/main && \
    apk add librdkafka=${LIBRDKAFKA_VERSION} --update-cache --repository http://nl.alpinelinux.org/alpine/edge/community

COPY --from=builder /go/src/${GO_PROJECT_PATH}/build ./
ENTRYPOINT ["./build"]
