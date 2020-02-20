FROM golang:1.13-alpine as builder
MAINTAINER siddontang

RUN apk add --no-cache \
    make \
    git \
    bash \
    curl \
    gcc \
    g++

# Install jq for pd-ctl
RUN cd / && \
    wget https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 -O jq && \
    chmod +x jq

RUN mkdir -p /go/src/github.com/pingcap/pd
WORKDIR /go/src/github.com/pingcap/pd

# Cache dependencies
COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

RUN make

FROM alpine:3.5

COPY --from=builder /go/src/github.com/pingcap/pd/bin/pd-server /pd-server
COPY --from=builder /go/src/github.com/pingcap/pd/bin/pd-ctl /pd-ctl
COPY --from=builder /jq /usr/local/bin/jq

EXPOSE 2379 2380

ENTRYPOINT ["/pd-server"]
