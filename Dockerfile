FROM golang:1.9.2

MAINTAINER siddontang

COPY . /go/src/github.com/pingcap/pd

RUN cd /go/src/github.com/pingcap/pd/ && \
    make && \
    cp -f ./bin/pd-server /go/bin/pd-server && \
    rm -rf ../pd

EXPOSE 2379 2380

ENTRYPOINT ["pd-server"]

