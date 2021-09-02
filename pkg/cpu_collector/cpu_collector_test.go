// Copyright 2021 TiKV Project Authors.
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

package collector_test

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	collector "github.com/tikv/pd/pkg/cpu_collector"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCollectorSuite{})

type testCollectorSuite struct {
}

const defaultCPUCollecttingInterval = time.Second

func (s *testCollectorSuite) Test(c *C) {
	cpuCollector := collector.NewCPUCollector(defaultCPUCollecttingInterval)
	cpuCollector.Start(context.Background())
	time.Sleep(defaultCPUCollecttingInterval * 3)
	c.Assert(cpuCollector.GetCPUUsage(), Greater, 0.0)
}
