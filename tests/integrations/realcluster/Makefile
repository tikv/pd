# Copyright 2023 TiKV Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ROOT_PATH := $(shell pwd)/../../..
GO_TOOLS_BIN_PATH := $(ROOT_PATH)/.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)
SHELL := env PATH='$(PATH)' GOBIN='$(GO_TOOLS_BIN_PATH)' $(shell which bash)

static: install-tools
	@ echo "gofmt ..."
	@ gofmt -s -l -d . 2>&1 | awk '{ print } END { if (NR > 0) { exit 1 } }'
	@ echo "golangci-lint ..."
	@ golangci-lint run -c $(ROOT_PATH)/.golangci.yml --verbose ./... --allow-parallel-runners
	@ echo "modernize ..."
	@ go run golang.org/x/tools/go/analysis/passes/modernize/cmd/modernize@v0.52.0 -test ./...

tidy:
	@ go mod tidy
	git diff go.mod go.sum | cat
	git diff --quiet go.mod go.sum

check: tiup test

tiup:
	@echo "==> Checking tiup installation"
	@TIUP_DIR="$${TIUP_HOME:-$(HOME)/.tiup}"; \
	if [ -x "$$TIUP_DIR/bin/tiup" ]; then \
		TIUP_BIN="$$TIUP_DIR/bin/tiup"; \
	elif [ -x "$(HOME)/.tiup/bin/tiup" ]; then \
		TIUP_BIN="$(HOME)/.tiup/bin/tiup"; \
	else \
		TIUP_BIN="$$TIUP_DIR/bin/tiup"; \
	fi; \
	PG_BIN=""; \
	if [ -x "$$TIUP_BIN" ]; then \
		PG_BIN="$$("$$TIUP_BIN" --binary playground 2>/dev/null || true)"; \
	fi; \
	if [ -x "$$TIUP_BIN" ] && [ -n "$$PG_BIN" ] && [ -x "$$PG_BIN" ]; then \
		echo "tiup already installed"; \
		echo "tiup version: $$("$$TIUP_BIN" --version)"; \
	else \
		if [ ! -x "$$TIUP_BIN" ] || [ ! -f "$$TIUP_DIR/bin/root.json" ]; then \
			echo "Installing tiup..."; \
			curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh; \
			TIUP_BIN="$$TIUP_DIR/bin/tiup"; \
		else \
			echo "tiup already installed"; \
		fi; \
		echo "tiup version: $$("$$TIUP_BIN" --version)"; \
		PG_BIN="$$("$$TIUP_BIN" --binary playground 2>/dev/null || true)"; \
		if [ -z "$$PG_BIN" ] || [ ! -x "$$PG_BIN" ]; then \
			if [ -n "$$PG_BIN" ]; then \
				PG_VER_DIR="$$(dirname "$$PG_BIN")"; \
				PG_PARENT="$$(cd "$$(dirname "$$PG_VER_DIR")" 2>/dev/null && pwd -P || true)"; \
				PG_ROOT="$$(cd "$$TIUP_DIR/components/playground" 2>/dev/null && pwd -P || true)"; \
				if [ -n "$$PG_PARENT" ] && [ "$$PG_PARENT" = "$$PG_ROOT" ] && [ "$$(basename "$$PG_VER_DIR")" != "." ] && [ "$$(basename "$$PG_VER_DIR")" != ".." ]; then \
					echo "Removing incomplete tiup playground directory $$PG_VER_DIR..."; \
					rm -rf "$$PG_VER_DIR"; \
				fi; \
			fi; \
			echo "Installing tiup playground..."; \
			"$$TIUP_BIN" install playground; \
			PG_BIN="$$("$$TIUP_BIN" --binary playground 2>/dev/null || true)"; \
			if [ -z "$$PG_BIN" ] || [ ! -x "$$PG_BIN" ]; then \
				echo "tiup playground binary is missing or not executable after install: $$PG_BIN" >&2; \
				exit 1; \
			fi; \
		fi; \
	fi

deploy: kill_cluster deploy_only

deploy_only:
	@ echo "deploying..."
	./deploy.sh
	@ echo "wait cluster ready..."
	./wait_tiup.sh 15 20
	@ echo "check cluster status..."
	@ pid=$$(ps -ef | grep 'playground' | grep -v grep | awk '{print $$2}' | head -n 1); \
	echo $$pid;

kill_cluster:
	@ echo "kill cluster..."
	@ pid=$$(ps -ef | grep 'playground' | grep -v grep | awk '{print $$2}' | head -n 1); \
	if [ ! -z "$$pid" ]; then \
		echo $$pid; \
		kill $$pid; \
		echo "waiting for cluster to exit..."; \
		sleep 30; \
	fi

test: tiup
	CGO_ENABLED=1 go test ./... -v -tags deadlock -race -cover -timeout 20m || (\
	echo "The following is pd-0 log" ; \
	cat ~/.tiup/data/pd_real_cluster_test/pd-0/pd.log ; \
	echo "The following is pd-1 log" ; \
	cat ~/.tiup/data/pd_real_cluster_test/pd-1/pd.log ; \
	echo "The following is pd-2 log" ; \
	cat ~/.tiup/data/pd_real_cluster_test/pd-2/pd.log ; \
	exit 1)

install-tools:
	cd $(ROOT_PATH) && $(MAKE) install-tools

.PHONY: tidy static check tiup deploy deploy_only kill_cluster test install-tools
