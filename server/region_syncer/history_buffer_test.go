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

package syncer

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testHistoryBuffer{})

type testHistoryBuffer struct{}

func Test(t *testing.T) {
	TestingT(t)
}

func (t *testHistoryBuffer) TestBufferSize(c *C) {
	var regions []*core.RegionInfo
	for i := 0; i <= 100; i++ {
		regions = append(regions, core.NewRegionInfo(&metapb.Region{Id: uint64(i)}, nil))
	}

	// size equals 1
	h := newHistoryBuffer(1, core.NewMemoryKV())
	c.Assert(h.len(), Equals, 0)
	for _, r := range regions {
		h.record(r)
	}
	c.Assert(h.len(), Equals, 1)
	c.Assert(h.get(100), Equals, regions[h.nextIndex()-1])
	c.Assert(h.get(99), IsNil)

	// size equals 2
	h = newHistoryBuffer(2, core.NewMemoryKV())
	for _, r := range regions {
		h.record(r)
	}
	c.Assert(h.len(), Equals, 2)
	c.Assert(h.get(100), Equals, regions[h.nextIndex()-1])
	c.Assert(h.get(99), Equals, regions[h.nextIndex()-2])
	c.Assert(h.get(98), IsNil)

	// size eqauls 100
	kv := core.NewMemoryKV()
	h1 := newHistoryBuffer(100, kv)
	for i := 0; i < 6; i++ {
		h1.record(regions[i])
	}
	c.Assert(h1.len(), Equals, 6)
	c.Assert(h1.nextIndex(), Equals, uint64(6))
	h1.persist()

	// restart the buffer
	h2 := newHistoryBuffer(100, kv)
	c.Assert(h2.nextIndex(), Equals, uint64(6))
	c.Assert(h2.get(h.nextIndex()-1), IsNil)
	c.Assert(h2.len(), Equals, 0)
	for _, r := range regions {
		index := h2.nextIndex()
		h2.record(r)
		c.Assert(h2.get(uint64(index)), Equals, r)
	}

	c.Assert(h2.nextIndex(), Equals, uint64(107))
	c.Assert(h2.get(h2.nextIndex()), IsNil)
	s, err := h2.kv.Load(historyKey)
	c.Assert(err, IsNil)
	// flush in index 106
	c.Assert(s, Equals, "106")
}
