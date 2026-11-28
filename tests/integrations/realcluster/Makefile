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
	@set -eu; \
	TIUP_DIR="$${TIUP_HOME:-$(HOME)/.tiup}"; \
	mkdir -p "$$TIUP_DIR"; \
	TIUP_DIR="$$(cd "$$TIUP_DIR" && pwd -P)"; \
	if [ -n "$${TIUP_HOME:-}" ]; then \
		export TIUP_HOME="$$TIUP_DIR"; \
	fi; \
	resolve_tiup_bin() { \
		if [ -x "$$TIUP_DIR/bin/tiup" ]; then \
			printf '%s\n' "$$TIUP_DIR/bin/tiup"; \
		elif [ -x "$$HOME/.tiup/bin/tiup" ]; then \
			printf '%s\n' "$$HOME/.tiup/bin/tiup"; \
		else \
			printf '%s\n' "$$TIUP_DIR/bin/tiup"; \
		fi; \
	}; \
	lookup_playground_bin() { \
		local tiup_bin="$$1"; \
		local probe_timeout="$${TIUP_PROBE_TIMEOUT:-30}"; \
		if ! command -v go >/dev/null 2>&1; then \
			echo "go is required to verify tiup playground binary via $$tiup_bin" >&2; \
			return 1; \
		fi; \
		if ! GOWORK=off CGO_ENABLED=0 go run ./internal/playgroundprobe/cmd --lookup "$$tiup_bin" "$$probe_timeout" 9>&-; then \
			echo "failed to look up tiup playground binary via $$tiup_bin" >&2; \
			return 1; \
		fi; \
	}; \
	is_playground_usable() { \
		local pg_bin="$$1"; \
		local probe_timeout="$${TIUP_PROBE_TIMEOUT:-30}"; \
		if [ -z "$$pg_bin" ]; then \
			return 1; \
		fi; \
		if ! command -v go >/dev/null 2>&1; then \
			echo "go is required to verify tiup playground binary: $$pg_bin" >&2; \
			exit 1; \
		fi; \
		local probe_result; \
		if ! probe_result="$$(GOWORK=off CGO_ENABLED=0 go run ./internal/playgroundprobe/cmd "$$pg_bin" "$$probe_timeout" 9>&-)"; then \
			echo "failed to verify tiup playground binary: $$pg_bin" >&2; \
			exit 1; \
		fi; \
		case "$$probe_result" in \
			usable) return 0 ;; \
			unusable) return 1 ;; \
			*) echo "unexpected tiup playground probe result '$$probe_result' for $$pg_bin" >&2; exit 1 ;; \
		esac; \
	}; \
	TIUP_BIN="$$(resolve_tiup_bin)"; \
	PG_BIN=""; \
	if [ -x "$$TIUP_BIN" ]; then \
		PG_BIN="$$(lookup_playground_bin "$$TIUP_BIN")" || exit 1; \
	fi; \
	if [ -x "$$TIUP_BIN" ] && is_playground_usable "$$PG_BIN"; then \
		echo "tiup already installed"; \
		echo "tiup version: $$("$$TIUP_BIN" --version)"; \
	else \
		LOCK_FILE="$$TIUP_DIR/.realcluster-playground-install.lock"; \
		run_install() { \
			set -eu; \
			TIUP_BIN="$$(resolve_tiup_bin)"; \
			if [ ! -x "$$TIUP_BIN" ] || [ ! -f "$$TIUP_DIR/bin/root.json" ]; then \
				echo "Installing tiup..."; \
				curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh; \
				TIUP_BIN="$$(resolve_tiup_bin)"; \
			else \
				echo "tiup already installed"; \
			fi; \
			echo "tiup version: $$("$$TIUP_BIN" --version)"; \
			PG_BIN="$$(lookup_playground_bin "$$TIUP_BIN")" || exit 1; \
			if ! is_playground_usable "$$PG_BIN"; then \
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
				PG_BIN="$$(lookup_playground_bin "$$TIUP_BIN")" || exit 1; \
				if ! is_playground_usable "$$PG_BIN"; then \
					echo "tiup playground binary is missing or not runnable after install: $$PG_BIN" >&2; \
					exit 1; \
				fi; \
			fi; \
		}; \
		if command -v flock >/dev/null 2>&1; then \
			(set -eu; exec 9>>"$$LOCK_FILE" && flock -x 9 && run_install); \
		elif command -v perl >/dev/null 2>&1; then \
			export -f resolve_tiup_bin lookup_playground_bin is_playground_usable run_install; \
			export TIUP_DIR; \
			perl -MFcntl=:flock -e 'open my $$fh, ">>", shift @ARGV or die $$!; flock($$fh, LOCK_EX) or die $$!; my $$rc = system(@ARGV); exit($$rc == -1 ? 127 : ($$rc & 127 ? 128 + ($$rc & 127) : $$rc >> 8))' "$$LOCK_FILE" bash -c 'run_install'; \
		else \
			echo "Neither flock nor perl is available to lock $$LOCK_FILE" >&2; \
			exit 1; \
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
