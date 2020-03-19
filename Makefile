PD_PKG := github.com/pingcap/pd/v4

TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     sort -u | sed -e "s/^\./github.com\/pingcap\/pd\/v4/")
INTEGRATION_TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     sort -u | sed -e "s/^\./github.com\/pingcap\/pd\/v4/" | grep -E "tests")
BASIC_TEST_PKGS := $(filter-out $(INTEGRATION_TEST_PKGS),$(TEST_PKGS))

IGNORE := grep -v 'dashboard/uiserver'
PACKAGES := go list ./... | $(IGNORE)
PACKAGE_DIRECTORIES := $(PACKAGES) | sed 's|$(PD_PKG)/||'
GOCHECKER := $(IGNORE) | awk '{ print } END { if (NR > 0) { exit 1 } }'
OVERALLS := overalls

TOOL_BIN_PATH := $(shell pwd)/.tools/bin
export GOBIN := $(TOOL_BIN_PATH)
export PATH := $(TOOL_BIN_PATH):$(PATH)

FAILPOINT_ENABLE  := $$(find $$PWD/ -type d | grep -vE "\.git" | xargs failpoint-ctl enable)
FAILPOINT_DISABLE := $$(find $$PWD/ -type d | grep -vE "\.git" | xargs failpoint-ctl disable)

DEADLOCK_ENABLE := $$(\
						find . -name "*.go" \
						| xargs -n 1 sed -i.bak 's/sync\.RWMutex/deadlock.RWMutex/;s/sync\.Mutex/deadlock.Mutex/' && \
						find . -name "*.go" | xargs grep -lE "(deadlock\.RWMutex|deadlock\.Mutex)" \
						| xargs goimports -w)
DEADLOCK_DISABLE := $$(\
						find . -name "*.go" \
						| xargs -n 1 sed -i.bak 's/deadlock\.RWMutex/sync.RWMutex/;s/deadlock\.Mutex/sync.Mutex/' && \
						find . -name "*.go" | xargs grep -lE "(sync\.RWMutex|sync\.Mutex)" \
						| xargs goimports -w && \
						find . -name "*.bak" | xargs rm && \
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

dev: build tools check test

ci: build check basic-test

build: pd-server pd-ctl
tools: pd-tso-bench pd-recover pd-analysis pd-heartbeat-bench
pd-server: export GO111MODULE=on
pd-server:
ifneq ($(OS),Windows_NT)
	./scripts/embed-dashboard-ui.sh
endif
ifeq ("$(WITH_RACE)", "1")
	CGO_ENABLED=1 go build -race -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
else
	CGO_ENABLED=1 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
endif

# Tools
pd-ctl: export GO111MODULE=on
pd-ctl:
	CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o bin/pd-ctl tools/pd-ctl/main.go
pd-tso-bench: export GO111MODULE=on
pd-tso-bench:
	CGO_ENABLED=0 go build -o bin/pd-tso-bench tools/pd-tso-bench/main.go
pd-recover: export GO111MODULE=on
pd-recover:
	CGO_ENABLED=0 go build -o bin/pd-recover tools/pd-recover/main.go
pd-analysis: export GO111MODULE=on
pd-analysis:
	CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o bin/pd-analysis tools/pd-analysis/main.go
pd-heartbeat-bench: export GO111MODULE=on
pd-heartbeat-bench:
	CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o bin/pd-heartbeat-bench tools/pd-heartbeat-bench/main.go

test: install-tools deadlock-setup
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
	CGO_ENABLED=0 golangci-lint run --disable-all \
	  --enable errcheck \
	  $$($(PACKAGE_DIRECTORIES))
	CGO_ENABLED=0 gosec $$($(PACKAGE_DIRECTORIES))

check-all: static lint tidy
	@echo "checking"

install-tools: export GO111MODULE=on
install-tools: golangci-lint-setup
	mkdir -p $(TOOL_BIN_PATH)
	grep '_' tools.go | sed 's/"//g' | awk '{print $$2}' | xargs go install

golangci-lint-setup:
	@mkdir -p $(TOOL_BIN_PATH)
	@which golangci-lint >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(TOOL_BIN_PATH) v1.23.7

check: install-tools check-all check-plugin

check-plugin:
	@echo "checking plugin"
	cd ./plugin/scheduler_example && make evictLeaderPlugin.so && rm evictLeaderPlugin.so

static: export GO111MODULE=on
static:
	@ # Not running vet and fmt through metalinter becauase it ends up looking at vendor
	gofmt -s -l $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)

	CGO_ENABLED=0 golangci-lint run $$($(PACKAGE_DIRECTORIES))

lint:
	@echo "linting"
	CGO_ENABLED=0 revive -formatter friendly -config revive.toml $$($(PACKAGES))

tidy:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	git diff --quiet go.mod go.sum

travis_coverage: export GO111MODULE=on
travis_coverage:
ifeq ("$(TRAVIS_COVERAGE)", "1")
	@$(FAILPOINT_ENABLE)
	CGO_ENABLED=1 $(OVERALLS) -concurrency=8 -project=github.com/pingcap/pd -covermode=count -ignore='.git,vendor' -- -coverpkg=./... || { $(FAILPOINT_DISABLE); exit 1; }
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

failpoint-enable: install-tools
	# Converting failpoints...
	@$(FAILPOINT_ENABLE)

failpoint-disable:
	# Restoring failpoints...
	@$(FAILPOINT_DISABLE)

.PHONY: all ci vendor clean-test tidy
