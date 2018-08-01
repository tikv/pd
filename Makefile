PD_PKG := github.com/pingcap/pd

TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     uniq | sed -e "s/^\./github.com\/pingcap\/pd/")
BASIC_TEST_PKGS := $(filter-out github.com/pingcap/pd/pkg/integration_test,$(TEST_PKGS))

PACKAGES := go list ./...
PACKAGE_DIRECTORIES := $(PACKAGES) | sed 's|github.com/pingcap/pd/||'
GOCHECKER := awk '{ print } END { if (NR > 0) { exit 1 } }'

LDFLAGS += -X "$(PD_PKG)/server.PDReleaseVersion=$(shell git describe --tags --dirty)"
LDFLAGS += -X "$(PD_PKG)/server.PDBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(PD_PKG)/server.PDGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(PD_PKG)/server.PDGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

# Ignore following files's coverage.
#
# See more: https://godoc.org/path/filepath#Match
COVERIGNORE := "cmd/*/*,pdctl/*,pdctl/*/*,server/api/bindata_assetfs.go"

default: build

all: dev

dev: build check test

ci: build check basic_test

build:
ifeq ("$(WITH_RACE)", "1")
	CGO_ENABLED=1 go build -race -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
else
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
endif
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o bin/pd-ctl cmd/pd-ctl/main.go
	CGO_ENABLED=0 go build -o bin/pd-tso-bench cmd/pd-tso-bench/main.go
	CGO_ENABLED=0 go build -o bin/pd-recover cmd/pd-recover/main.go

test:
	# testing..
	CGO_ENABLED=1 go test -race -cover $(TEST_PKGS)

basic_test:
	go test $(BASIC_TEST_PKGS)

tool-install: check-setup
	# tool environment
	# check runner
	retool add gopkg.in/alecthomas/gometalinter.v2 v2.0.5
	# check spelling
	retool add github.com/client9/misspell/cmd/misspell v0.3.4
	# checks correctness
	retool add github.com/gordonklaus/ineffassign 7bae11eba15a3285c75e388f77eb6357a2d73ee2
	retool add honnef.co/go/tools/cmd/megacheck master
	retool add github.com/dnephin/govet 4a96d43e39d340b63daa8bc5576985aa599885f6
	# slow checks
	retool add github.com/kisielk/errcheck v1.1.0
	# linter
	retool add github.com/mgechev/revive 7773f47324c2bf1c8f7a5500aff2b6c01d3ed73b
	retool add github.com/securego/gosec/cmd/gosec 1.0.0

# These need to be fixed before they can be ran regularly
check-fail:
	CGO_ENABLED=0 retool do gometalinter.v2 --disable-all \
	  --enable errcheck \
	  $$($(PACKAGE_DIRECTORIES))
	CGO_ENABLED=0 retool do gosec $$($(PACKAGE_DIRECTORIES))

check-all: static lint
	@echo "checking"

check-setup:
	@which retool >/dev/null 2>&1 || go get github.com/twitchtv/retool
	@retool sync

check: check-setup check-all

static:
	@ # Not running vet and fmt through metalinter becauase it ends up looking at vendor
	gofmt -s -l $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)
	retool do govet --shadow $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)

	CGO_ENABLED=0 retool do gometalinter.v2 --disable-all --deadline 120s \
	  --enable misspell \
	  --enable megacheck \
	  --enable ineffassign \
	  $$($(PACKAGE_DIRECTORIES))

lint:
	@echo "linting"
	CGO_ENABLED=0 retool do revive -formatter friendly -config revive.toml $$($(PACKAGES))

travis_coverage:
ifeq ("$(TRAVIS_COVERAGE)", "1")
	GOPATH=$(VENDOR) $(HOME)/gopath/bin/goveralls -service=travis-ci -ignore $(COVERIGNORE)
else
	@echo "coverage only runs in travis."
endif

update:
	which dep 2>/dev/null || go get -u github.com/golang/dep/cmd/dep
ifdef PKG
	dep ensure -add ${PKG}
else
	dep ensure -update
endif
	@echo "removing test files"
	dep prune
	bash ./hack/clean_vendor.sh

simulator:
	CGO_ENABLED=0 go build -o bin/simulator cmd/simulator/main.go
	bin/simulator

.PHONY: update clean tool-install
