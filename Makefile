# Build pd-server, pd-ctl, pd-recover
default: build

# Development validation.
all: dev
dev: build check tools test

# Lightweight development validation.
dev-basic: build check basic-test

.PHONY: default all dev dev-basic

#### Build ####

BUILD_FLAGS ?=
BUILD_TAGS ?=
BUILD_CGO_ENABLED := 0
BUILD_TOOL_CGO_ENABLED := 0
BUILD_GOEXPERIMENT ?=
PD_EDITION ?= Community
# Ensure PD_EDITION is set to Community or Enterprise before running build process.
ifneq "$(PD_EDITION)" "Community"
ifneq "$(PD_EDITION)" "Enterprise"
  $(error Please set the correct environment variable PD_EDITION before running `make`)
endif
endif

ifeq ($(SWAGGER), 1)
	BUILD_TAGS += swagger_server
endif

ifeq ($(DASHBOARD), 0)
	BUILD_TAGS += without_dashboard
else
	BUILD_CGO_ENABLED := 1
endif

ifeq ($(FAILPOINT), 1)
	BUILD_TAGS += with_fail
endif

ifeq ("$(WITH_RACE)", "1")
	BUILD_FLAGS += -race
	BUILD_CGO_ENABLED := 1
endif

ifeq ($(PLUGIN), 1)
	BUILD_TAGS += with_plugin
endif

ifeq ($(ENABLE_FIPS), 1)
	BUILD_TAGS+=boringcrypto
	BUILD_GOEXPERIMENT=boringcrypto
	BUILD_CGO_ENABLED := 1
	BUILD_TOOL_CGO_ENABLED := 1
endif

RELEASE_VERSION ?= $(shell git describe --tags --dirty --always)
ifeq ($(RUN_CI), 1)
	RELEASE_VERSION := None
endif

LDFLAGS += -X "$(PD_PKG)/pkg/versioninfo.PDReleaseVersion=$(RELEASE_VERSION)"
LDFLAGS += -X "$(PD_PKG)/pkg/versioninfo.PDBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(PD_PKG)/pkg/versioninfo.PDGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(PD_PKG)/pkg/versioninfo.PDGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "$(PD_PKG)/pkg/versioninfo.PDEdition=$(PD_EDITION)"

ifneq ($(DASHBOARD), 0)
	# Note: LDFLAGS must be evaluated lazily for these scripts to work correctly
	LDFLAGS += -X "github.com/pingcap/tidb-dashboard/pkg/utils/version.InternalVersion=$(shell scripts/describe-dashboard.sh internal-version)"
	LDFLAGS += -X "github.com/pingcap/tidb-dashboard/pkg/utils/version.Standalone=No"
	LDFLAGS += -X "github.com/pingcap/tidb-dashboard/pkg/utils/version.PDVersion=$(shell git describe --tags --dirty --always)"
	LDFLAGS += -X "github.com/pingcap/tidb-dashboard/pkg/utils/version.BuildTime=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
	LDFLAGS += -X "github.com/pingcap/tidb-dashboard/pkg/utils/version.BuildGitHash=$(shell scripts/describe-dashboard.sh git-hash)"
endif

ROOT_PATH := $(shell pwd)
BUILD_BIN_PATH := $(ROOT_PATH)/bin

build: pd-server pd-ctl pd-recover

tools: pd-tso-bench pd-heartbeat-bench regions-dump stores-dump pd-api-bench pd-ut

PD_SERVER_DEP :=
ifeq ($(SWAGGER), 1)
	PD_SERVER_DEP += swagger-spec
endif
ifneq ($(DASHBOARD_DISTRIBUTION_DIR),)
	BUILD_TAGS += dashboard_distro
	PD_SERVER_DEP += dashboard-replace-distro-info
endif
PD_SERVER_DEP += dashboard-ui

pre-build: ${PD_SERVER_DEP}

pd-server: pre-build
	GOEXPERIMENT=$(BUILD_GOEXPERIMENT) CGO_ENABLED=$(BUILD_CGO_ENABLED) go build $(BUILD_FLAGS) -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags "$(BUILD_TAGS)" -o $(BUILD_BIN_PATH)/pd-server cmd/pd-server/main.go

pd-server-failpoint:
	@$(FAILPOINT_ENABLE)
	FAILPOINT=1 $(MAKE) pd-server || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

pd-server-basic:
	SWAGGER=0 DASHBOARD=0 $(MAKE) pd-server

.PHONY: pre-build build tools pd-server pd-server-basic

# Tools
pd-ctl:
	cd tools && GOEXPERIMENT=$(BUILD_GOEXPERIMENT) CGO_ENABLED=$(BUILD_TOOL_CGO_ENABLED) go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/pd-ctl pd-ctl/main.go
pd-tso-bench:
	cd tools && CGO_ENABLED=0 go build -o $(BUILD_BIN_PATH)/pd-tso-bench pd-tso-bench/main.go
pd-api-bench:
	cd tools && CGO_ENABLED=0 go build -o $(BUILD_BIN_PATH)/pd-api-bench pd-api-bench/main.go
pd-recover:
	cd tools && GOEXPERIMENT=$(BUILD_GOEXPERIMENT) CGO_ENABLED=$(BUILD_TOOL_CGO_ENABLED) go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/pd-recover pd-recover/main.go
pd-analysis:
	cd tools && CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/pd-analysis pd-analysis/main.go
pd-heartbeat-bench:
	cd tools && CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/pd-heartbeat-bench pd-heartbeat-bench/main.go
simulator:
	cd tools && GOEXPERIMENT=$(BUILD_GOEXPERIMENT) CGO_ENABLED=$(BUILD_CGO_ENABLED) go build $(BUILD_FLAGS) -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/pd-simulator pd-simulator/main.go
regions-dump:
	cd tools && CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/regions-dump regions-dump/main.go
stores-dump:
	cd tools && CGO_ENABLED=0 go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/stores-dump stores-dump/main.go
pd-ut: pd-xprog
	cd tools && GOEXPERIMENT=$(BUILD_GOEXPERIMENT) CGO_ENABLED=$(BUILD_TOOL_CGO_ENABLED) go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/pd-ut pd-ut/ut.go pd-ut/coverProfile.go
pd-xprog:
	cd tools && GOEXPERIMENT=$(BUILD_GOEXPERIMENT) CGO_ENABLED=$(BUILD_TOOL_CGO_ENABLED) go build -tags xprog -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/xprog pd-ut/xprog.go

.PHONY: pd-ctl pd-tso-bench pd-recover pd-analysis pd-heartbeat-bench simulator regions-dump stores-dump pd-api-bench pd-ut

#### Docker image ####

docker-image:
	$(eval DOCKER_PS_EXIT_CODE=$(shell docker ps > /dev/null 2>&1 ; echo $$?))
	@if [ $(DOCKER_PS_EXIT_CODE) -ne 0 ]; then \
	echo "Encountered problem while invoking docker cli. Is the docker daemon running?"; \
	fi
	docker build --no-cache -t tikv/pd .

.PHONY: docker-image

#### Build utils ###

swagger-spec: install-tools
	swag init --parseDependency --parseInternal --parseDepth 1 --dir server --generalInfo api/router.go --output docs/swagger
	swag fmt --dir server

dashboard-ui:
	./scripts/embed-dashboard-ui.sh

dashboard-replace-distro-info:
	rm -f pkg/dashboard/distro/distro_info.go
	cp $(DASHBOARD_DISTRIBUTION_DIR)/distro_info.go pkg/dashboard/distro/distro_info.go

.PHONY: swagger-spec dashboard-ui dashboard-replace-distro-info

#### Static tools ####

PD_PKG := github.com/tikv/pd
PACKAGES := $(shell go list ./...)

GO_TOOLS_BIN_PATH := $(ROOT_PATH)/.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)
SHELL := env PATH='$(PATH)' GOBIN='$(GO_TOOLS_BIN_PATH)' $(shell which bash)

install-tools:
	@mkdir -p $(GO_TOOLS_BIN_PATH)
	@which golangci-lint >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GO_TOOLS_BIN_PATH) v1.61.0
	@grep '_' tools.go | sed 's/"//g' | awk '{print $$2}' | xargs go install

.PHONY: install-tools

#### Static checks ####

check: tidy static generate-errdoc

static: install-tools pre-build
	@ echo "gofmt ..."
	@ gofmt -s -l -d $(PACKAGE_DIRECTORIES) 2>&1 | awk '{ print } END { if (NR > 0) { exit 1 } }'
	@ echo "golangci-lint ..."
	@ golangci-lint run --verbose $(PACKAGE_DIRECTORIES) --allow-parallel-runners
	@ for mod in $(SUBMODULES); do cd $$mod && $(MAKE) static && cd $(ROOT_PATH) > /dev/null; done

# Because CI downloads the dashboard code and runs gofmt, we can't add this check into static now.
fmt:
	@ echo "gofmt ..."
	@ gofmt -s -l -w -r 'interface{} -> any' -d $(PACKAGE_DIRECTORIES) 2>&1 | awk '{ print } END { if (NR > 0) { exit 1 } }'

tidy:
	@ go mod tidy
	git diff go.mod go.sum | cat
	git diff --quiet go.mod go.sum

	@ for mod in $(SUBMODULES); do cd $$mod && $(MAKE) tidy && cd $(ROOT_PATH) > /dev/null; done

generate-errdoc: install-tools
	@echo "generating errors.toml..."
	./scripts/generate-errdoc.sh

check-plugin:
	@echo "checking plugin..."
	cd ./plugin/scheduler_example && $(MAKE) evictLeaderPlugin.so && rm evictLeaderPlugin.so

.PHONY: check static tidy generate-errdoc check-plugin

#### Test utils ####

FAILPOINT_ENABLE  := $$(find $$PWD/ -type d | grep -vE "\.git" | xargs failpoint-ctl enable)
FAILPOINT_DISABLE := $$(find $$PWD/ -type d | grep -vE "\.git" | xargs failpoint-ctl disable)

failpoint-enable: install-tools
	# Converting failpoints...
	@$(FAILPOINT_ENABLE)

failpoint-disable: install-tools
	# Restoring failpoints...
	@$(FAILPOINT_DISABLE)

.PHONY: failpoint-enable failpoint-disable

#### Test ####

ut: pd-ut
	@$(FAILPOINT_ENABLE)
	# only run unit tests
	./bin/pd-ut run --ignore tests --race --junitfile ./junitfile
	@$(CLEAN_UT_BINARY)
	@$(FAILPOINT_DISABLE)

PACKAGE_DIRECTORIES := $(subst $(PD_PKG)/,,$(PACKAGES))
TEST_PKGS := $(filter $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     sort -u | sed -e "s/^\./github.com\/tikv\/pd/"),$(PACKAGES))
BASIC_TEST_PKGS := $(filter-out $(PD_PKG)/tests%,$(TEST_PKGS))

SUBMODULES := $(filter $(shell find . -iname "go.mod" -exec dirname {} \;),\
				$(filter-out .,$(shell find . -iname "Makefile" -exec dirname {} \;)))

test: install-tools
	# testing all pkgs...
	@$(FAILPOINT_ENABLE)
	CGO_ENABLED=1 go test -tags tso_function_test,deadlock -timeout 20m -race -cover $(TEST_PKGS) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

basic-test: install-tools
	# testing basic pkgs...
	@$(FAILPOINT_ENABLE)
	go test $(BASIC_TEST_PKGS) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

ci-test-job: install-tools dashboard-ui pd-ut
	@$(FAILPOINT_ENABLE)
	./scripts/ci-subtask.sh $(JOB_INDEX) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

TSO_INTEGRATION_TEST_PKGS := $(PD_PKG)/tests/server/tso

test-tso: install-tools
	# testing TSO function & consistency...
	@$(FAILPOINT_ENABLE)
	CGO_ENABLED=1 go test -race -tags without_dashboard,tso_full_test,deadlock $(TSO_INTEGRATION_TEST_PKGS) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

test-tso-function: install-tools
	# testing TSO function...
	@$(FAILPOINT_ENABLE)
	CGO_ENABLED=1 go test -race -tags without_dashboard,tso_function_test,deadlock $(TSO_INTEGRATION_TEST_PKGS) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

test-tso-consistency: install-tools
	# testing TSO consistency...
	@$(FAILPOINT_ENABLE)
	CGO_ENABLED=1 go test -race -tags without_dashboard,tso_consistency_test,deadlock $(TSO_INTEGRATION_TEST_PKGS) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

REAL_CLUSTER_TEST_PATH := $(ROOT_PATH)/tests/integrations/realcluster

test-real-cluster:
	@ rm -rf ~/.tiup/data/pd_real_cluster_test
	# testing with the real cluster...
	cd $(REAL_CLUSTER_TEST_PATH) && $(MAKE) check

.PHONY: test basic-test test-with-cover test-tso test-tso-function test-tso-consistency test-real-cluster

#### Daily CI coverage analyze  ####

TASK_COUNT=1
TASK_ID=1

# The command should be used in daily CI, it will split some tasks to run parallel.
# It should retain report.xml,coverage,coverage.xml and package.list to analyze.
test-with-cover-parallel: install-tools dashboard-ui split
	@$(FAILPOINT_ENABLE)
	set -euo pipefail;\
	CGO_ENABLED=1 GO111MODULE=on gotestsum --junitfile report.xml -- -v --race -covermode=atomic -coverprofile=coverage $(shell cat package.list)  2>&1 || { $(FAILPOINT_DISABLE); }; \
	gocov convert coverage | gocov-xml >> coverage.xml;\
	@$(FAILPOINT_DISABLE)

split:
# todo: it will remove server/api,/tests and tso packages after daily CI integrate all verify CI.
	go list ./... | grep -v -E  "github.com/tikv/pd/server/api|github.com/tikv/pd/tests/client|github.com/tikv/pd/tests/server/tso" > packages.list;\
	split packages.list -n r/${TASK_COUNT} packages_unit_ -a 1 --numeric-suffixes=1;\
	cat packages_unit_${TASK_ID} |tr "\n" " " >package.list;\
	rm packages*;

#### Clean up ####

clean: failpoint-disable clean-test clean-build

CLEAN_UT_BINARY := find . -name '*.test.bin'| xargs rm -f

clean-test:
	# Cleaning test tmp...
	rm -rf /tmp/pd_tests*
	rm -f $(REAL_CLUSTER_TEST_PATH)/playground.log
	go clean -testcache
	@$(CLEAN_UT_BINARY)

clean-build:
	# Cleaning building files...
	rm -rf .dashboard_download_cache/
	rm -rf .dashboard_build_temp/
	rm -rf $(BUILD_BIN_PATH)
	rm -rf $(GO_TOOLS_BIN_PATH)

.PHONY: clean clean-test clean-build
