// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package systimemon

import (
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func TestSize(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSystimeMonitorSuite{})

type testSystimeMonitorSuite struct{}

func (s *testSystimeMonitorSuite) TestSystimeMonitor(c *C) {
	jumpForward := new(int32)
	atomic.StoreInt32(jumpForward, 0)
	trigged := false
	go StartTimeMonitor(
		func() time.Time {
			if !trigged {
				trigged = true
				return time.Now()
			}

			return time.Now().Truncate(2 * time.Second)
		}, func() bool {
			atomic.AddInt32(jumpForward, 1)
			return false
		})

	time.Sleep(1 * time.Second)
	c.Assert(atomic.LoadInt32(jumpForward), Equals, int32(1))
}
