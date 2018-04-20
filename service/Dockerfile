FROM golang:1.8

WORKDIR /go/src/service
COPY . .

RUN go get -d -v ./
RUN go install -v ./

RUN go build

CMD ["app"]
