// copyright 2018 pingcap, inc.
//
// licensed under the apache license, version 2.0 (the "license");
// you may not use this file except in compliance with the license.
// you may obtain a copy of the license at
//
//     http://www.apache.org/licenses/license-2.0
//
// unless required by applicable law or agreed to in writing, software
// distributed under the license is distributed on an "as is" basis,
// see the license for the specific language governing permissions and
// limitations under the license.

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
	for i := 0; i < 100; i++ {
		regions = append(regions, core.NewRegionInfo(&metapb.Region{Id: uint64(i)}, nil))
	}

	// size equal 1
	h := newHistoryBuffer(1, core.NewMemoryKV())
	c.Assert(h.len(), Equals, 0)
	for _, r := range regions {
		h.record(r)
	}
	c.Assert(h.len(), Equals, 1)
	c.Assert(h.get(100), Equals, regions[h.lastIndex()-1])
	c.Assert(h.get(99), IsNil)

	// size equal 2
	h = newHistoryBuffer(2, core.NewMemoryKV())
	for _, r := range regions {
		h.record(r)
	}
	c.Assert(h.len(), Equals, 2)
	c.Assert(h.get(100), Equals, regions[h.lastIndex()-1])
	c.Assert(h.get(99), Equals, regions[h.lastIndex()-2])
	c.Assert(h.get(98), IsNil)

	// size eqaul 100
	h = newHistoryBuffer(100, core.NewMemoryKV())
	for _, r := range regions {
		h.record(r)
	}
	c.Assert(h.len(), Equals, 100)
	for i, r := range regions {
		// index start with 1
		index := i + 1
		c.Assert(h.get(uint64(index)), Equals, r)
	}
	c.Assert(h.get(0), IsNil)
	s, err := h.kv.Load(historyKey)
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "100")
}
