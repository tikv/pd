PD_PKG := github.com/pingcap/pd

TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     sort -u | sed -e "s/^\./github.com\/pingcap\/pd/")
INTEGRATION_TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     sort -u | sed -e "s/^\./github.com\/pingcap\/pd/" | grep -E "tests")
BASIC_TEST_PKGS := $(filter-out $(INTEGRATION_TEST_PKGS),$(TEST_PKGS))

PACKAGES := go list ./...
PACKAGE_DIRECTORIES := $(PACKAGES) | sed 's|github.com/pingcap/pd/||'
GOCHECKER := awk '{ print } END { if (NR > 0) { exit 1 } }'
RETOOL := ./scripts/retool
OVERALLS := overalls

FAILPOINT_ENABLE  := $$(find $$PWD/ -type d | grep -vE "(\.git|\.retools)" | xargs ./scripts/retool do failpoint-ctl enable)
FAILPOINT_DISABLE := $$(find $$PWD/ -type d | grep -vE "(\.git|\.retools)" | xargs ./scripts/retool do failpoint-ctl disable)

DEADLOCK_ENABLE := $$(\
						find . -name "*.go" | grep -vE "(vendor|\.retools)" \
						| xargs -n 1 sed -i.bak 's/sync\.RWMutex/deadlock.RWMutex/;s/sync\.Mutex/deadlock.Mutex/' && \
						find . -name "*.go" | grep -vE "(vendor|\.retools)" | xargs grep -lE "(deadlock\.RWMutex|deadlock\.Mutex)" \
						| xargs ./scripts/retool do goimports -w)
DEADLOCK_DISABLE := $$(\
						find . -name "*.go" | grep -vE "(vendor|\.retools)" \
						| xargs -n 1 sed -i.bak 's/deadlock\.RWMutex/sync.RWMutex/;s/deadlock\.Mutex/sync.Mutex/' && \
						find . -name "*.go" | grep -vE "(vendor|\.retools)" | xargs grep -lE "(sync\.RWMutex|sync\.Mutex)" \
						| xargs ./scripts/retool do goimports -w && \
						find . -name "*.bak" | grep -vE "(vendor|\.retools)" | xargs rm && \
						go mod tidy)

LDFLAGS += -X "$(PD_PKG)/server.PDReleaseVersion=$(shell git describe --tags --dirty)"
LDFLAGS += -X "$(PD_PKG)/server.PDBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(PD_PKG)/server.PDGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(PD_PKG)/server.PDGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

GOVER_MAJOR := $(shell go version | sed -E -e "s/.*go([0-9]+)[.]([0-9]+).*/\1/")
GOVER_MINOR := $(shell go version | sed -E -e "s/.*go([0-9]+)[.]([0-9]+).*/\2/")
GO111 := $(shell [ $(GOVER_MAJOR) -gt 1 ] || [ $(GOVER_MAJOR) -eq 1 ] && [ $(GOVER_MINOR) -ge 11 ]; echo $$?)
ifeq ($(GO111), 1)
$(error "go below 1.11 does not support modules")
endif

default: build

all: dev

dev: build check test

ci: build check basic-test

build: pd-server pd-ctl pd-tso-bench pd-recover
pd-server: export GO111MODULE=on
pd-server:
ifeq ("$(WITH_RACE)", "1")
	CGO_ENABLED=1 go build -race -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
else
	CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
endif

pd-ctl: export GO111MODULE=on
pd-ctl:
	CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o bin/pd-ctl tools/pd-ctl/main.go
pd-tso-bench: export GO111MODULE=on
pd-tso-bench:
	CGO_ENABLED=0 go build -o bin/pd-tso-bench tools/pd-tso-bench/main.go
pd-recover: export GO111MODULE=on
pd-recover:
	CGO_ENABLED=0 go build -o bin/pd-recover tools/pd-recover/main.go

test: retool-setup deadlock-setup
	# testing...
	@$(DEADLOCK_ENABLE)
	@$(FAILPOINT_ENABLE)
	CGO_ENABLED=1 GO111MODULE=on go test -race -cover $(TEST_PKGS) || { $(FAILPOINT_DISABLE); $(DEADLOCK_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(DEADLOCK_DISABLE)

basic-test:
	@$(FAILPOINT_ENABLE)
	GO111MODULE=on go test $(BASIC_TEST_PKGS) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

# These need to be fixed before they can be ran regularly
check-fail:
	CGO_ENABLED=0 ./scripts/retool do gometalinter.v2 --disable-all \
	  --enable errcheck \
	  $$($(PACKAGE_DIRECTORIES))
	CGO_ENABLED=0 ./scripts/retool do gosec $$($(PACKAGE_DIRECTORIES))

check-all: static lint tidy
	@echo "checking"

retool-setup: export GO111MODULE=off
retool-setup: 
	@which retool >/dev/null 2>&1 || go get github.com/twitchtv/retool
	@./scripts/retool sync

check: retool-setup check-all

static: export GO111MODULE=on
static:
	@ # Not running vet and fmt through metalinter becauase it ends up looking at vendor
	gofmt -s -l $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)
	./scripts/retool do govet --shadow $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)

	CGO_ENABLED=0 ./scripts/retool do golangci-lint run --disable-all --deadline 120s \
	  --enable misspell \
	  --enable staticcheck \
	  --enable ineffassign \
	  $$($(PACKAGE_DIRECTORIES))

lint:
	@echo "linting"
	CGO_ENABLED=0 ./scripts/retool do revive -formatter friendly -config revive.toml $$($(PACKAGES))

tidy:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	git diff --quiet go.mod go.sum

travis_coverage: export GO111MODULE=on
travis_coverage:
ifeq ("$(TRAVIS_COVERAGE)", "1")
	@$(FAILPOINT_ENABLE)
	CGO_ENABLED=1 ./scripts/retool do $(OVERALLS) -concurrency=8 -project=github.com/pingcap/pd -covermode=count -ignore='.git,vendor' -- -coverpkg=./... || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
else
	@echo "coverage only runs in travis."
endif

simulator: export GO111MODULE=on
simulator:
	CGO_ENABLED=0 go build -o bin/pd-simulator tools/pd-simulator/main.go

regions-dump: export GO111MODULE=on
regions-dump:
	CGO_ENABLED=0 go build -o bin/regions-dump tools/regions-dump/main.go

clean-test:
	rm -rf /tmp/test_pd*
	rm -rf /tmp/pd-tests*
	rm -rf /tmp/test_etcd*

deadlock-setup: export GO111MODULE=off
deadlock-setup:
	go get github.com/sasha-s/go-deadlock

deadlock-enable: deadlock-setup
	@$(DEADLOCK_ENABLE)

deadlock-disable:
	@$(DEADLOCK_DISABLE)

failpoint-enable: retool-setup
	# Converting failpoints...
	@$(FAILPOINT_ENABLE)

failpoint-disable:
	# Restoring failpoints...
	@$(FAILPOINT_DISABLE)

.PHONY: all ci vendor clean-test tidy
