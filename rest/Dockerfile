# Build stage
# =====================

FROM golang:alpine AS build-env

ARG LIBRESSL_VERSION=2.5
ARG LIBRDKAFKA_VERSION=0.11.0-r0

# Install librdkafka
RUN apk update && \
    apk add libressl${LIBRESSL_VERSION}-libcrypto libressl${LIBRESSL_VERSION}-libssl --update-cache --repository http://nl.alpinelinux.org/alpine/edge/main && \
    apk add librdkafka=${LIBRDKAFKA_VERSION} --update-cache --repository http://nl.alpinelinux.org/alpine/edge/community && \
    apk add librdkafka-dev=${LIBRDKAFKA_VERSION} --update-cache --repository http://nl.alpinelinux.org/alpine/edge/community && \
    apk add git openssh openssl yajl-dev zlib-dev cyrus-sasl-dev openssl-dev build-base coreutils

WORKDIR /go/src/
ADD . .
RUN go get -d -v .
RUN go build -o rest

# Final stage
# =====================
FROM alpine
WORKDIR /app
COPY --from=build-env /go/src/rest /app/
ENTRYPOINT ./rest
EXPOSE 8080
