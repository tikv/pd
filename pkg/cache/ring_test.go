package cache

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testRingSuite{})

type testRingSuite struct {
}

func (r *testRingSuite) TestRingPutItem(c *C) {
	ring := NewRing(10)
	ring.Put(newSimpleRingItem([]byte("002"), []byte("100")))
	c.Assert(ring.tree.Len(), Equals, 1)
	ring.Put(newSimpleRingItem([]byte("100"), []byte("200")))
	c.Assert(ring.tree.Len(), Equals, 2)

	// init key range: [001,100], [100,200]
	c.Assert(ring.GetRange(newSimpleRingItem([]byte("000"), []byte("001"))), HasLen, 0)
	c.Assert(ring.GetRange(newSimpleRingItem([]byte("000"), []byte("009"))), HasLen, 1)
	c.Assert(ring.GetRange(newSimpleRingItem([]byte("010"), []byte("090"))), HasLen, 1)
	c.Assert(ring.GetRange(newSimpleRingItem([]byte("010"), []byte("110"))), HasLen, 2)
	c.Assert(ring.GetRange(newSimpleRingItem([]byte("200"), []byte("300"))), HasLen, 0)

	// test1： insert one keyrange, the old overlaps will retain like split buckets.
	// key range: [001,010],[010,090],[090,100],[100,200]
	ring.Put(newSimpleRingItem([]byte("010"), []byte("090")))
	c.Assert(ring.tree.Len(), Equals, 4)
	c.Assert(ring.GetRange(newSimpleRingItem([]byte("010"), []byte("090"))), HasLen, 1)

	// test2: insert one keyrange, the old overlaps will retain like merge .
	// key range: [001,080], [080,090],[090,100],[100,200]
	ring.Put(newSimpleRingItem([]byte("001"), []byte("080")))
	c.Assert(ring.tree.Len(), Equals, 4)
	c.Assert(ring.GetRange(newSimpleRingItem([]byte("010"), []byte("090"))), HasLen, 2)

	// test2: insert one keyrange, the old overlaps will retain like merge .
	// key range: [001,120],[120,200]
	ring.Put(newSimpleRingItem([]byte("001"), []byte("120")))
	c.Assert(ring.tree.Len(), Equals, 2)
	c.Assert(ring.GetRange(newSimpleRingItem([]byte("010"), []byte("090"))), HasLen, 1)
}

func (r *testRingSuite) TestRingItemAdjust(c *C) {
	ringItem := newSimpleRingItem([]byte("010"), []byte("090"))
	var overlaps []RingItem
	overlaps = ringItem.Debris([]byte("000"), []byte("100"))
	c.Assert(overlaps, HasLen, 0)
	overlaps = ringItem.Debris([]byte("000"), []byte("080"))
	c.Assert(overlaps, HasLen, 1)
	overlaps = ringItem.Debris([]byte("020"), []byte("080"))
	c.Assert(overlaps, HasLen, 2)
	overlaps = ringItem.Debris([]byte("010"), []byte("090"))
	c.Assert(overlaps, HasLen, 0)
	overlaps = ringItem.Debris([]byte("010"), []byte("100"))
	c.Assert(overlaps, HasLen, 0)
	overlaps = ringItem.Debris([]byte("100"), []byte("200"))
	c.Assert(overlaps, HasLen, 0)
}
