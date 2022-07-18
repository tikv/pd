#!/usr/bin/env bash
# Copyright 2020 TiKV Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.
#
# set is used to set the environment variables.
#  -e: exit immediately when a command returning a non-zero exit code.
#  -u: treat unset variables as an error.
#  -o pipefail: sets the exit code of a pipeline to that of the rightmost command to exit with a non-zero status,
#       or to zero if all commands of the pipeline exit successfully.
set -euo pipefail

cd -P .

cp errors.toml errors.toml.before
errdoc-gen --source . --module github.com/tikv/pd --output errors.toml --ignore ./client/errs
diff -q errors.toml errors.toml.before
rm errors.toml.before
