all: build check test 

GO=GO15VENDOREXPERIMENT="1" go

build:
	$(GO) build -o bin/pd-server pd-server/main.go

install:
	$(GO) install ./...

test:
	$(GO) test --race ./pd-server ./pd-client ./server

check:
	go get github.com/golang/lint/golint

	go tool vet . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	go tool vet --shadow . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	golint ./... 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	gofmt -s -l . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'