// Copyright 2018 PingCAP, Inc.
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

package core

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testRollingStats{})

type testRollingStats struct{}

func (t *testRollingStats) TestRollingMedian(c *C) {
	data := []float64{2, 4, 2, 800, 600, 6, 3}
	expected := []float64{2, 3, 2, 3, 4, 6, 6}
	stats := NewRollingStats(5)
	for i, e := range data {
		stats.Add(e)
		c.Assert(stats.Median(), Equals, expected[i])
	}
}

var _ = Suite(&testRollingStoreStats{})

type testRollingStoreStats struct{}

func (t *testRollingStoreStats) TestObserve(c *C) {
	s := newRollingStoreStats()
	now := time.Now()
	stats := &pdpb.StoreStats{
		StoreId:      1,
		BytesWritten: 512 * 1024 * 60,
		Interval: &pdpb.TimeInterval{
			StartTimestamp: uint64(now.Add(-60*time.Second).UnixNano() / int64(time.Millisecond)),
			EndTimestamp:   uint64(now.UnixNano() / int64(time.Millisecond)),
		},
	}
	s.Observe(stats)
	c.Assert(s.GetBytesWriteRate(), Equals, 512.0*1024)
}
