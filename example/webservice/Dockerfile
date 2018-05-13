FROM golang:1.10-alpine

ARG LIBRESSL_VERSION=2.7
ARG LIBRDKAFKA_VERSION=0.11.4-r1

# Install librdkafka
RUN apk update && \
    apk add libressl${LIBRESSL_VERSION}-libcrypto libressl${LIBRESSL_VERSION}-libssl --update-cache --repository http://nl.alpinelinux.org/alpine/edge/main && \
    apk add librdkafka=${LIBRDKAFKA_VERSION} --update-cache --repository http://nl.alpinelinux.org/alpine/edge/community && \
    apk add librdkafka-dev=${LIBRDKAFKA_VERSION} --update-cache --repository http://nl.alpinelinux.org/alpine/edge/community && \
    apk add git openssh openssl yajl-dev zlib-dev cyrus-sasl-dev openssl-dev build-base coreutils

WORKDIR /go/src/webservice
COPY . .

RUN go get -d -v .
RUN go build -o build .

EXPOSE 80
CMD ["/go/src/webservice/build"]
