// Copyright 2025 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Notes: it's a copy from tidb

package logutil

import (
	"go.uber.org/zap"

	"github.com/pingcap/log"
)

// BgLogger is alias of `logutil.BgLogger()`. It's initialized in tidb-server's
// main function. Don't use it in `init` or equivalent functions otherwise it
// will print to stdout.
func BgLogger() *zap.Logger {
	return log.L().With()
}
