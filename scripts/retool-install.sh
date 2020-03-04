#!/usr/bin/env bash
set -euo pipefail

# This script generates tools.json
# It helps record what releases/branches are being used

cd "$(dirname "$0")/.."
command -v retool >/dev/null || go get github.com/twitchtv/retool

# tool environment
# check runner
./scripts/retool add gopkg.in/alecthomas/gometalinter.v2 v2.0.5
# linter
./scripts/retool add github.com/mgechev/revive 7773f47324c2bf1c8f7a5500aff2b6c01d3ed73b
# go fail
./scripts/retool add github.com/pingcap/failpoint/failpoint-ctl master
# deadlock detection
./scripts/retool add golang.org/x/tools/cmd/goimports 04b5d21e00f1f47bd824a6ade581e7189bacde87
# bindata
./scripts/retool add github.com/kevinburke/go-bindata/go-bindata v3.16.0
# overall
./scripts/retool add github.com/go-playground/overalls 22ec1a223b7c9a2e56355bd500b539cba3784238
