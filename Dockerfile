FROM golang:1.12-alpine as builder
MAINTAINER siddontang

RUN apk add --no-cache \
    make \
    git

COPY . /go/src/github.com/pingcap/pd
WORKDIR /go/src/github.com/pingcap/pd

RUN make

FROM alpine:3.5

COPY --from=builder /go/src/github.com/pingcap/pd/bin/pd-server /pd-server

EXPOSE 2379 2380

ENTRYPOINT ["/pd-server"]
