PD_PKG := github.com/tikv/pd

TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     sort -u | sed -e "s/^\./github.com\/tikv\/pd/")
INTEGRATION_TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     sort -u | sed -e "s/^\./github.com\/tikv\/pd/" | grep -E "tests")
TSO_INTEGRATION_TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     sort -u | sed -e "s/^\./github.com\/tikv\/pd/" | grep -E "server/tso")
BASIC_TEST_PKGS := $(filter-out $(INTEGRATION_TEST_PKGS),$(TEST_PKGS))

PACKAGES := go list ./...
PACKAGE_DIRECTORIES := $(PACKAGES) | sed 's|$(PD_PKG)/||'
GOCHECKER := awk '{ print } END { if (NR > 0) { exit 1 } }'
OVERALLS := overalls

TASK_COUNT=1
TASK_ID=1

BUILD_BIN_PATH := $(shell pwd)/bin
GO_TOOLS_BIN_PATH := $(shell pwd)/.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)
SHELL := env PATH='$(PATH)' GOBIN='$(GO_TOOLS_BIN_PATH)' $(shell which bash)

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

BUILD_FLAGS ?=
BUILD_TAGS ?=
BUILD_CGO_ENABLED := 0
PD_EDITION ?= Community
# Ensure PD_EDITION is set to Community or Enterprise before running build process.
ifneq "$(PD_EDITION)" "Community"
ifneq "$(PD_EDITION)" "Enterprise"
  $(error Please set the correct environment variable PD_EDITION before running `make`)
endif
endif

ifneq ($(SWAGGER), 0)
	BUILD_TAGS += swagger_server
endif

ifeq ($(DASHBOARD), 0)
	BUILD_TAGS += without_dashboard
else
	BUILD_CGO_ENABLED := 1
endif

ifeq ("$(WITH_RACE)", "1")
	BUILD_FLAGS += -race
	BUILD_CGO_ENABLED := 1
endif

LDFLAGS += -X "$(PD_PKG)/server/versioninfo.PDReleaseVersion=$(shell git describe --tags --dirty --always)"
LDFLAGS += -X "$(PD_PKG)/server/versioninfo.PDBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(PD_PKG)/server/versioninfo.PDGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(PD_PKG)/server/versioninfo.PDGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "$(PD_PKG)/server/versioninfo.PDEdition=$(PD_EDITION)"

ifneq ($(DASHBOARD), 0)
	# Note: LDFLAGS must be evaluated lazily for these scripts to work correctly
	LDFLAGS += -X "github.com/pingcap/tidb-dashboard/pkg/utils/version.InternalVersion=$(shell scripts/describe-dashboard.sh internal-version)"
	LDFLAGS += -X "github.com/pingcap/tidb-dashboard/pkg/utils/version.Standalone=No"
	LDFLAGS += -X "github.com/pingcap/tidb-dashboard/pkg/utils/version.PDVersion=$(shell git describe --tags --dirty --always)"
	LDFLAGS += -X "github.com/pingcap/tidb-dashboard/pkg/utils/version.BuildTime=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
	LDFLAGS += -X "github.com/pingcap/tidb-dashboard/pkg/utils/version.BuildGitHash=$(shell scripts/describe-dashboard.sh git-hash)"
endif

GOVER_MAJOR := $(shell go version | sed -E -e "s/.*go([0-9]+)[.]([0-9]+).*/\1/")
GOVER_MINOR := $(shell go version | sed -E -e "s/.*go([0-9]+)[.]([0-9]+).*/\2/")
GO111 := $(shell [ $(GOVER_MAJOR) -gt 1 ] || [ $(GOVER_MAJOR) -eq 1 ] && [ $(GOVER_MINOR) -ge 11 ]; echo $$?)
ifeq ($(GO111), 1)
  $(error "go below 1.11 does not support modules")
endif

default: build

all: dev

dev: build check tools test

ci: build check basic-test

build: pd-server pd-ctl pd-recover

tools: pd-tso-bench pd-analysis pd-heartbeat-bench

PD_SERVER_DEP :=
ifneq ($(SWAGGER), 0)
	PD_SERVER_DEP += swagger-spec
endif
ifneq ($(DASHBOARD_DISTRIBUTION_DIR),)
	BUILD_TAGS += dashboard_distro
	PD_SERVER_DEP += dashboard-replace-distro-info
endif
PD_SERVER_DEP += dashboard-ui

pd-server: export GO111MODULE=on
pd-server: ${PD_SERVER_DEP}
	CGO_ENABLED=$(BUILD_CGO_ENABLED) go build $(BUILD_FLAGS) -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags "$(BUILD_TAGS)" -o $(BUILD_BIN_PATH)/pd-server cmd/pd-server/main.go

pd-server-basic: export GO111MODULE=on
pd-server-basic:
	SWAGGER=0 DASHBOARD=0 $(MAKE) pd-server

# dependent
install-all-tools: export GO111MODULE=on
install-all-tools:
	@mkdir -p $(GO_TOOLS_BIN_PATH)
	@which golangci-lint >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GO_TOOLS_BIN_PATH) v1.43.0
	@grep '_' tools.go | sed 's/"//g' | awk '{print $$2}' | xargs go install

install-golangci-lint:
	@mkdir -p $(GO_TOOLS_BIN_PATH)
	@which golangci-lint >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GO_TOOLS_BIN_PATH) v1.43.0

install-go-tools: export GO111MODULE=on
install-go-tools:
	@mkdir -p $(GO_TOOLS_BIN_PATH)
	@grep '_' tools.go | sed 's/"//g' | awk '{print $$2}' | xargs go install

swagger-spec: export GO111MODULE=on
swagger-spec: install-go-tools
	go mod vendor
	swag init --parseVendor -generalInfo server/api/router.go --exclude vendor/github.com/pingcap/tidb-dashboard --output docs/swagger
	go mod tidy
	rm -rf vendor

dashboard-ui: export GO111MODULE=on
dashboard-ui:
	./scripts/embed-dashboard-ui.sh

dashboard-replace-distro-info:
	rm -f pkg/dashboard/distro/distro_info.go
	cp $(DASHBOARD_DISTRIBUTION_DIR)/distro_info.go pkg/dashboard/distro/distro_info.go

# Tools
pd-ctl: export GO111MODULE=on
pd-ctl:
	CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/pd-ctl tools/pd-ctl/main.go
pd-tso-bench: export GO111MODULE=on
pd-tso-bench:
	CGO_ENABLED=0 go build -o $(BUILD_BIN_PATH)/pd-tso-bench tools/pd-tso-bench/main.go
pd-recover: export GO111MODULE=on
pd-recover:
	CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/pd-recover tools/pd-recover/main.go
pd-analysis: export GO111MODULE=on
pd-analysis:
	CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/pd-analysis tools/pd-analysis/main.go
pd-heartbeat-bench: export GO111MODULE=on
pd-heartbeat-bench:
	CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/pd-heartbeat-bench tools/pd-heartbeat-bench/main.go

test: install-go-tools
	# testing all pkgs...
	@$(DEADLOCK_ENABLE)
	@$(FAILPOINT_ENABLE)
	CGO_ENABLED=1 GO111MODULE=on go test -tags tso_function_test -timeout 20m -race -cover $(TEST_PKGS) || { $(FAILPOINT_DISABLE); $(DEADLOCK_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(DEADLOCK_DISABLE)

basic-test:
	# testing basic pkgs...
	@$(FAILPOINT_ENABLE)
	GO111MODULE=on go test $(BASIC_TEST_PKGS) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

test-with-cover: install-go-tools dashboard-ui
	# testing all pkgs (expect TSO consistency test) with converage...
	@$(FAILPOINT_ENABLE)
	for PKG in $(TEST_PKGS); do\
		set -euo pipefail;\
		CGO_ENABLED=1 GO111MODULE=on go test -race -covermode=atomic -coverprofile=coverage.tmp -coverpkg=./... $$PKG 2>&1 | grep -v "no packages being tested" && tail -n +2 coverage.tmp >> covprofile || { $(FAILPOINT_DISABLE); rm coverage.tmp && exit 1;}; \
		rm coverage.tmp;\
	done
	@$(FAILPOINT_DISABLE)

# The command should be used in daily CI，it will split some tasks to run parallel.
# It should retain report.xml,coverage,coverage.xml and package.list to analyze.
test-with-cover-parallel: install-go-tools dashboard-ui split
	@$(FAILPOINT_ENABLE)
	set -euo pipefail;\
	CGO_ENABLED=1 GO111MODULE=on gotestsum --junitfile report.xml -- -v --race -covermode=atomic -coverprofile=coverage $(shell cat package.list)  2>&1 || { $(FAILPOINT_DISABLE); }; \
	gocov convert coverage | gocov-xml >> coverage.xml;\
	@$(FAILPOINT_DISABLE)

test-tso-function: install-go-tools
	# testing TSO function...
	@$(DEADLOCK_ENABLE)
	@$(FAILPOINT_ENABLE)
	CGO_ENABLED=1 GO111MODULE=on go test -race -tags without_dashboard,tso_function_test $(TSO_INTEGRATION_TEST_PKGS) || { $(FAILPOINT_DISABLE); $(DEADLOCK_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(DEADLOCK_DISABLE)

test-tso-consistency: install-go-tools
	# testing TSO consistency...
	@$(DEADLOCK_ENABLE)
	@$(FAILPOINT_ENABLE)
	CGO_ENABLED=1 GO111MODULE=on go test -race -tags without_dashboard,tso_consistency_test $(TSO_INTEGRATION_TEST_PKGS) || { $(FAILPOINT_DISABLE); $(DEADLOCK_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(DEADLOCK_DISABLE)

check: install-all-tools check-all check-plugin errdoc check-testing-t docker-build-test

check-all: static lint tidy
	@echo "checking"

check-plugin:
	@echo "checking plugin"
	cd ./plugin/scheduler_example && $(MAKE) evictLeaderPlugin.so && rm evictLeaderPlugin.so

static: export GO111MODULE=on
static: install-golangci-lint
	@ # Not running vet and fmt through metalinter becauase it ends up looking at vendor
	gofmt -s -l -d $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)
	golangci-lint run $$($(PACKAGE_DIRECTORIES))

lint: install-go-tools
	@echo "linting"
	revive -formatter friendly -config revive.toml $$($(PACKAGES))

tidy:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	git diff --quiet go.mod go.sum

errdoc: install-go-tools
	@echo "generator errors.toml"
	./scripts/check-errdoc.sh

docker-build-test:
	$(eval DOCKER_PS_EXIT_CODE=$(shell docker ps > /dev/null 2>&1 ; echo $$?))
	@if [ $(DOCKER_PS_EXIT_CODE) -ne 0 ]; then \
	echo "Encountered problem while invoking docker cli. Is the docker daemon running?"; \
	fi
	docker build --no-cache -t tikv/pd .

check-testing-t:
	./scripts/check-testing-t.sh

simulator: export GO111MODULE=on
simulator:
	CGO_ENABLED=0 go build -o $(BUILD_BIN_PATH)/pd-simulator tools/pd-simulator/main.go

regions-dump: export GO111MODULE=on
regions-dump:
	CGO_ENABLED=0 go build -o $(BUILD_BIN_PATH)/regions-dump tools/regions-dump/main.go

stores-dump: export GO111MODULE=on
stores-dump:
	CGO_ENABLED=0 go build -o $(BUILD_BIN_PATH)/stores-dump tools/stores-dump/main.go

clean-test:
	# Cleaning test tmp...
	rm -rf /tmp/test_pd*
	rm -rf /tmp/pd-tests*
	rm -rf /tmp/test_etcd*
	go clean -testcache

clean-build:
	# Cleaning building files...
	rm -rf .dashboard_asset_cache/
	rm -rf $(BUILD_BIN_PATH)
	rm -rf $(GO_TOOLS_BIN_PATH)

deadlock-enable: install-go-tools
	# Enabling deadlock...
	@$(DEADLOCK_ENABLE)

deadlock-disable: install-go-tools
	# Disabling deadlock...
	@$(DEADLOCK_DISABLE)

failpoint-enable: install-go-tools
	# Converting failpoints...
	@$(FAILPOINT_ENABLE)

failpoint-disable: install-go-tools
	# Restoring failpoints...
	@$(FAILPOINT_DISABLE)

split:
# todo: it will remove server/api,/tests and tso packages after daily CI integrate all verify CI.
	go list ./... | grep -v -E  "github.com/tikv/pd/server/api|github.com/tikv/pd/tests/client|github.com/tikv/pd/tests/server/tso" > packages.list;\
	split packages.list -n r/${TASK_COUNT} packages_unit_ -a 1 --numeric-suffixes=1;\
	cat packages_unit_${TASK_ID} |tr "\n" " " >package.list;\
	rm packages*;

clean: failpoint-disable deadlock-disable clean-test clean-build

.PHONY: all ci vendor tidy clean-test clean-build clean
