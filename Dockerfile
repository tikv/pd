FROM golang:1.23-alpine as builder

RUN apk add --no-cache \
    make \
    git \
    bash \
    curl \
    gcc \
    g++ \
    binutils-gold

# Install jq for pd-ctl
RUN cd / && \
    wget https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 -O jq && \
    chmod +x jq

RUN mkdir -p /go/src/github.com/tikv/pd
WORKDIR /go/src/github.com/tikv/pd

# Cache dependencies
COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

# Workaround sqlite3 and alpine 3.19 incompatibility
# https://github.com/mattn/go-sqlite3/issues/1164
RUN CGO_CFLAGS="-D_LARGEFILE64_SOURCE" make

FROM alpine:3.17

COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-server /pd-server
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-ctl /pd-ctl
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-recover /pd-recover
COPY --from=builder /jq /usr/local/bin/jq

RUN apk add --no-cache \
    curl

EXPOSE 2379 2380

ENTRYPOINT ["/pd-server"]
