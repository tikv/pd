# Copyright 2022 TiKV Project Authors.
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

GO_TOOLS_BIN_PATH := $(shell pwd)/../.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)
SHELL := env PATH='$(PATH)' GOBIN='$(GO_TOOLS_BIN_PATH)' $(shell which bash)

test:
	CGO_ENABLE=1 go test -race -cover

basic-test:
	CGO_ENABLE=1 go test

ci-test-job:
	CGO_ENABLED=1 go test -race -covermode=atomic -coverprofile=covprofile -coverpkg=../... github.com/tikv/pd/client

install-tools:
	cd .. && $(MAKE) install-tools

static: install-tools
	@ gofmt -s -l -d . 2>&1 | awk '{ print } END { if (NR > 0) { exit 1 } }'
	@ golangci-lint run -c ../.golangci.yml ./...
	@ revive -formatter friendly -config ../revive.toml .

tidy:
	@ go mod tidy
	git diff --quiet go.mod go.sum

