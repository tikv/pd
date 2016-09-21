GO=GO15VENDOREXPERIMENT="1" go

PACKAGES := $$(go list ./...| grep -vE 'vendor')

GOFILTER := grep -vE 'vendor|render.Delims|bindata_assetfs|testutil'
GOCHECKER := $(GOFILTER) | awk '{ print } END { if (NR > 0) { exit 1 } }'

LDFLAGS += -X "github.com/pingcap/pd/server.PDBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/pd/server.PDGitHash=$(shell git rev-parse HEAD)"

default: build

all: dev install

dev: build check test

build-fe:
	go get github.com/jteeuwen/go-bindata/...
	go get github.com/elazarl/go-bindata-assetfs/...
	cd server/api && go-bindata-assetfs -pkg api templates/... && cd -

build:
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
	rm -rf vendor

install:
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GO) install ./...
	rm -rf vendor

test:
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GO) test --race $(PACKAGES)
	rm -rf vendor

check:
	go get github.com/golang/lint/golint

	@echo "vet"
	@ go tool vet . 2>&1 | $(GOCHECKER)
	@ go tool vet --shadow . 2>&1 | $(GOCHECKER)
	@echo "golint"
	@ golint ./... 2>&1 | $(GOCHECKER)
	@echo "gofmt"
	@ gofmt -s -l . 2>&1 | $(GOCHECKER)

update:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
	rm -r vendor && mv _vendor/vendor vendor || true
	rm -rf _vendor
ifdef PKG
	glide get --strip-vendor --skip-test ${PKG}
else
	glide update --strip-vendor --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests
	mkdir -p _vendor
	mv vendor _vendor/vendor

clean:
	# clean unix socket
	find . -type s -exec rm {} \;

.PHONY: update clean
