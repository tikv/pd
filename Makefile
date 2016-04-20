GO=GO15VENDOREXPERIMENT="1" go

all: build check test 

build: 
	rm -rf vendor && ln -s cmd/vendor vendor
	$(GO) build -o bin/pd-server cmd/pd-server/main.go
	rm -rf vendor

install: 
	rm -rf vendor && ln -s cmd/vendor vendor
	$(GO) install ./...
	rm -rf vendor

test: 
	rm -rf vendor && ln -s cmd/vendor vendor
	$(GO) test --race ./pd-client ./server
	rm -rf vendor

check:
	go get github.com/golang/lint/golint

	go tool vet . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	go tool vet --shadow . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	golint ./... 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	gofmt -s -l . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	

deps:
	# see https://github.com/coreos/etcd/blob/master/scripts/updatedep.sh
	rm -rf Godeps vendor
	mkdir -p cmd/vendor
	ln -s cmd/vendor vendor
	godep save ./...
	rm -rf cmd/Godeps
	rm vendor
	mv Godeps cmd/
