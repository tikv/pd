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

check: tiup test

tiup:
	# if tiup binary not exist, download it
	if ! which tiup > /dev/null 2>&1; then \
        curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh; \
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

test:
	CGO_ENABLED=1 go test ./... -v -tags deadlock -race -cover || (\
	echo "follow is pd-0 log\n" ; \
	cat ~/.tiup/data/pd_real_cluster_test/pd-0/pd.log ; \
	echo "follow is pd-1 log\n" ; \
	cat ~/.tiup/data/pd_real_cluster_test/pd-1/pd.log ; \
	echo "follow is pd-2 log\n" ; \
	cat ~/.tiup/data/pd_real_cluster_test/pd-2/pd.log ; \
	exit 1)

install-tools:
	cd $(ROOT_PATH) && $(MAKE) install-tools
