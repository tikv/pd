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

tidy:
	@ go mod tidy
	git diff go.mod go.sum | cat
	git diff --quiet go.mod go.sum

.PHONY: check
check: tiup test

.PHONY: tiup
tiup:
	@echo "==> Checking tiup installation"
	@if ! which tiup > /dev/null 2>&1; then \
		echo "Installing tiup..."; \
		curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh; \
	else \
		echo "tiup already installed"; \
	fi
	@echo "tiup version: $$(tiup --version)"

.PHONY: deploy
deploy: kill_cluster deploy_only

.PHONY: deploy_only
deploy_only:
	@echo "==> Deploying cluster..."
	@./deploy.sh
	@echo "==> Waiting for cluster to be ready..."
	@./wait_tiup.sh 15 20
	@echo "==> Checking cluster status..."
	@if pid=$$(pgrep -f "tiup.*playground.*--tag"); then \
		echo "Playground process running with pid: $$pid"; \
		tiup playground display; \
	else \
		echo "ERROR: Playground process not found"; \
		exit 1; \
	fi

.PHONY: kill_cluster
kill_cluster:
	@echo "==> Killing cluster..."
	@if pid=$$(pgrep -f "tiup.*playground.*--tag"); then \
		echo "Found playground process(es):"; \
		ps -fp $$pid; \
		echo "Sending SIGTERM..."; \
		kill $$pid; \
		echo "Waiting for process to exit (30s)..."; \
		for i in $$(seq 1 30); do \
			if ! kill -0 $$pid 2>/dev/null; then \
				echo "Process exited"; \
				exit 0; \
			fi; \
			sleep 1; \
		done; \
		echo "Process still running, sending SIGKILL..."; \
		kill -9 $$pid || true; \
	else \
		echo "No playground process found"; \
	fi

.PHONY: test
test:
	@echo "==> Running integration tests..."
	@CGO_ENABLED=1 go test ./... -v -tags deadlock -race -cover || (\
		echo "==> Test failed. Collecting logs..."; \
		$(MAKE) collect_logs; \
		exit 1 \
	)

.PHONY: collect_logs
collect_logs:
	@echo "==> Collecting cluster logs..."
	@for pd in 0 1 2; do \
		echo "### PD-$$pd logs ###"; \
		cat ~/.tiup/data/pd_real_cluster_test/pd-$$pd/pd.log 2>/dev/null || echo "No log file found"; \
		echo; \
	done
	@echo "==> Collecting playground logs..."
	@find /tmp/real_cluster/playground -type f -name "*.log" -exec sh -c 'echo "### $$(basename {}) ###"; cat {}; echo' \;

.PHONY: clean
clean:
	@echo "==> Cleaning up..."
	@rm -rf /tmp/real_cluster/playground/*
	@echo "==> Cleaned playground logs"

install-tools:
	cd $(ROOT_PATH) && $(MAKE) install-tools
