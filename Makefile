all: build check test 

GO=GO15VENDOREXPERIMENT="1" go

PWD=$(shell pwd)

build:
	@rm -rf vendor | ln -sf $(PWD)/cmd/vendor $(PWD)/vendor
	$(GO) build -o bin/pd-server cmd/pd-server/main.go

install:
	@rm -rf vendor | ln -sf $(PWD)/cmd/vendor $(PWD)/vendor
	$(GO) install ./...

test:
	@rm -rf vendor | ln -sf $(PWD)/cmd/vendor $(PWD)/vendor
	$(GO) test --race ./pd-server ./pd-client ./server

check:
	go get github.com/golang/lint/golint

	go tool vet . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	go tool vet --shadow . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	golint ./... 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	gofmt -s -l . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'

deps:
	# see https://github.com/coreos/etcd/blob/master/scripts/updatedep.sh
	rm -rf Godeps vendor
	ln -s $(PWD)/cmd/vendor $(PWD)/vendor
	godep save ./...
	rm -rf cmd/Godeps
	rm vendor
	mv Godeps cmd/
