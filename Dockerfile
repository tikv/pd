FROM golang:1.6

MAINTAINER siddontang

COPY . /go/src/github.com/pingcap/pd

RUN cd /go/src/github.com/pingcap/pd/ && \
    rm -rf vendor && ln -s _vendor/vendor vendor && \
    go build -o bin/pd-server cmd/pd-server/main.go && \
    cp -f ./bin/pd-server /go/bin/pd-server && \
    cp -rf ./templates /go/templates

EXPOSE 1234 9090 2379 2380

ENTRYPOINT ["pd-server"]

