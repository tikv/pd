// Copyright 2017 TiKV Project Authors.
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

package systimemon

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestSystimeMonitor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var jumpForward int32

	triggered := false
	go StartMonitor(ctx,
		func() time.Time {
			if !triggered {
				triggered = true
				return time.Now()
			}

			return time.Now().Add(-2 * time.Second)
		}, func() {
			atomic.StoreInt32(&jumpForward, 1)
		})

	time.Sleep(time.Second)

	if atomic.LoadInt32(&jumpForward) != 1 {
		t.Error("should detect time error")
	}
}
