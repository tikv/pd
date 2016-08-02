GO=GO15VENDOREXPERIMENT="1" go

default: build

all: dev install

dev: build check test

build: 
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GO) build -o bin/pd-server cmd/pd-server/main.go
	rm -rf vendor

install: 
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GO) install ./...
	rm -rf vendor

test: 
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GO) test --race ./pd-client ./server ./server/api
	rm -rf vendor

check:
	go get github.com/golang/lint/golint

	go tool vet . 2>&1 | grep -vE 'vendor|render.Delims' | awk '{print} END{if(NR>0) {exit 1}}'
	go tool vet --shadow . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	golint ./... 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	gofmt -s -l . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'

deps:
	# see https://github.com/coreos/etcd/blob/master/scripts/updatedep.sh
	rm -rf Godeps vendor
	mkdir -p _vendor/vendor
	ln -s _vendor/vendor vendor
	godep save ./...
	rm -rf _vendor/Godeps
	rm vendor
	mv Godeps _vendor/

update_kvproto:
	rm -rf Godeps vendor
	ln -s _vendor/Godeps Godeps
	ln -s _vendor/vendor vendor
	# Guarantee executing OK.
	go get -u -v -d github.com/pingcap/kvproto/pkg || true
	godep update github.com/pingcap/kvproto/pkg/...
	rm Godeps vendor

	