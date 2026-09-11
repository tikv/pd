#!/usr/bin/env bash

# Copyright 2026 TiKV Project Authors.
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

set -euo pipefail

temp_root=${TMPDIR:-/tmp}
test_tmp_dir=$(mktemp -d "${temp_root%/}/pd_tests.XXXXXX")

cleanup() {
	if [[ -n ${test_tmp_dir:-} && -d $test_tmp_dir ]]; then
		rm -rf -- "$test_tmp_dir"
	fi
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

TMPDIR=$test_tmp_dir "$@"
