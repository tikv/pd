GO=GO15VENDOREXPERIMENT="1" go

LDFLAGS += -X "github.com/pingcap/pd/server.PDBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/pd/server.PDGitHash=$(shell git rev-parse HEAD)"

default: build

all: dev install

dev: build check test

build:
	mv _vendor/vendor vendor
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
	mkdir -p _vendor
	mv vendor _vendor/vendor

install: 
	mv _vendor/vendor vendor
	$(GO) install ./...
	mv vendor _vendor/vendor

test: 
	mv _vendor/vendor vendor
	$(GO) test --race ./pd-client ./server ./server/api
	mv vendor _vendor/vendor

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

update:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
	mv _vendor/vendor vendor
	rm -rf _vendor
ifdef PKG
	glide --verbose get --strip-vendor --skip-test ${PKG}
else
	glide --verbose update --strip-vendor --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests
	mkdir -p _vendor
	mv vendor _vendor/vendor

update_kvproto:
	make update PKG=github.com/pingcap/kvproto/pkg

clean:
	# clean unix socket
	find . -type s | xargs -r rm

.PHONY: update clean
