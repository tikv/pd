// Copyright 2023 TiKV Project Authors.
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

package gogc

import (
	"os"
	"runtime/debug"
	"strconv"
	"sync/atomic"
)

var gogcPercent int64

func init() {
	gogcPercent = 100
	if val, err := strconv.Atoi(os.Getenv("GOGC")); err == nil {
		gogcPercent = int64(val)
	}
}

// SetGCPercent update GOGC percent and related metrics.
func SetGCPercent(val int) {
	if val <= 0 {
		val = 100
	}
	debug.SetGCPercent(val)
	atomic.StoreInt64(&gogcPercent, int64(val))
}

// GetGCPercent returns the current value of GOGC percent.
func GetGCPercent() int {
	return int(atomic.LoadInt64(&gogcPercent))
}
