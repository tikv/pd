// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/pkg/mock/mockid"
	"github.com/pingcap/pd/v4/server/id"
)

func TestCore(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRegionMapSuite{})

type testRegionMapSuite struct{}

func (s *testRegionMapSuite) TestRegionMap(c *C) {
	var empty *regionMap
	c.Assert(empty.Len(), Equals, 0)
	c.Assert(empty.Get(1), IsNil)

	rm := newRegionMap()
	s.check(c, rm)
	rm.Put(s.regionInfo(1))
	s.check(c, rm, 1)

	rm.Put(s.regionInfo(2))
	rm.Put(s.regionInfo(3))
	s.check(c, rm, 1, 2, 3)

	rm.Put(s.regionInfo(3))
	rm.Delete(4)
	s.check(c, rm, 1, 2, 3)

	rm.Delete(3)
	rm.Delete(1)
	s.check(c, rm, 2)

	rm.Put(s.regionInfo(3))
	s.check(c, rm, 2, 3)
}

func (s *testRegionMapSuite) regionInfo(id uint64) *RegionInfo {
	return &RegionInfo{
		meta: &metapb.Region{
			Id: id,
		},
		approximateSize: int64(id),
		approximateKeys: int64(id),
	}
}

func (s *testRegionMapSuite) check(c *C, rm *regionMap, ids ...uint64) {
	// Check Get.
	for _, id := range ids {
		c.Assert(rm.Get(id).GetID(), Equals, id)
	}
	// Check Len.
	c.Assert(rm.Len(), Equals, len(ids))
	// Check id set.
	expect := make(map[uint64]struct{})
	for _, id := range ids {
		expect[id] = struct{}{}
	}
	set1 := make(map[uint64]struct{})
	for _, r := range rm.m {
		set1[r.GetID()] = struct{}{}
	}
	c.Assert(set1, DeepEquals, expect)
	// Check region size.
	var total int64
	for _, id := range ids {
		total += int64(id)
	}
	c.Assert(rm.TotalSize(), Equals, total)
}

var _ = Suite(&testRegionKey{})

type testRegionKey struct{}

func (*testRegionKey) TestRegionKey(c *C) {
	testCase := []struct {
		key    string
		expect string
	}{
		{`"t\x80\x00\x00\x00\x00\x00\x00\xff!_r\x80\x00\x00\x00\x00\xff\x02\u007fY\x00\x00\x00\x00\x00\xfa"`,
			`7480000000000000FF215F728000000000FF027F590000000000FA`},
		{"\"\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\xff\\x05\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\xf8\"",
			`80000000000000FF0500000000000000F8`},
	}
	for _, t := range testCase {
		got, err := strconv.Unquote(t.key)
		c.Assert(err, IsNil)
		s := fmt.Sprintln(RegionToHexMeta(&metapb.Region{StartKey: []byte(got)}))
		c.Assert(strings.Contains(s, t.expect), IsTrue)

		// start key changed
		orgion := NewRegionInfo(&metapb.Region{EndKey: []byte(got)}, nil)
		region := NewRegionInfo(&metapb.Region{StartKey: []byte(got), EndKey: []byte(got)}, nil)
		s = DiffRegionKeyInfo(orgion, region)
		c.Assert(s, Matches, ".*StartKey Changed.*")
		c.Assert(strings.Contains(s, t.expect), IsTrue)

		// end key changed
		orgion = NewRegionInfo(&metapb.Region{StartKey: []byte(got)}, nil)
		region = NewRegionInfo(&metapb.Region{StartKey: []byte(got), EndKey: []byte(got)}, nil)
		s = DiffRegionKeyInfo(orgion, region)
		c.Assert(s, Matches, ".*EndKey Changed.*")
		c.Assert(strings.Contains(s, t.expect), IsTrue)
	}
}

func (*testRegionKey) TestRegionOverlaps(c *C) {
	regions := NewRegionsInfo()
	for i := 0; i < 3; i++ {
		peer := &metapb.Peer{StoreId: 1, Id: uint64(i + 1)}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer},
			StartKey: []byte(fmt.Sprintf("%20d", i*10)),
			EndKey:   []byte(fmt.Sprintf("%20d", (i+1)*10)),
		}, peer)
		regions.AddRegion(region)
	}
	peer := &metapb.Peer{StoreId: 1, Id: uint64(5)}
	region := NewRegionInfo(&metapb.Region{
		Id:       uint64(5),
		Peers:    []*metapb.Peer{peer},
		StartKey: []byte(fmt.Sprintf("%20d", 50)),
		EndKey:   []byte(fmt.Sprintf("%20d", 100)),
	}, peer)
	regions.AddRegion(region)

	peer = &metapb.Peer{StoreId: 1, Id: uint64(1)}
	region = NewRegionInfo(&metapb.Region{
		Id:       uint64(1),
		Peers:    []*metapb.Peer{peer},
		StartKey: []byte(fmt.Sprintf("%20d", 5)),
		EndKey:   []byte(fmt.Sprintf("%20d", 15)),
	}, peer)
	overlaps, item := regions.GetOverlaps(region)
	c.Assert(0, Equals, bytes.Compare(overlaps[0].GetStartKey(), []byte(fmt.Sprintf("%20d", 0))))
	c.Assert(0, Equals, bytes.Compare(overlaps[1].GetStartKey(), []byte(fmt.Sprintf("%20d", 10))))
	c.Assert(0, Equals, bytes.Compare(item.region.GetStartKey(), []byte(fmt.Sprintf("%20d", 0))))

	region = NewRegionInfo(&metapb.Region{
		Id:       uint64(1),
		Peers:    []*metapb.Peer{peer},
		StartKey: []byte(fmt.Sprintf("%20d", 15)),
		EndKey:   []byte(fmt.Sprintf("%20d", 17)),
	}, peer)
	overlaps, item = regions.GetOverlaps(region)
	c.Assert(len(overlaps), Equals, 1)
	c.Assert(0, Equals, bytes.Compare(overlaps[0].GetStartKey(), []byte(fmt.Sprintf("%20d", 10))))
	c.Assert(0, Equals, bytes.Compare(item.region.GetStartKey(), []byte(fmt.Sprintf("%20d", 10))))

	region = NewRegionInfo(&metapb.Region{
		Id:       uint64(1),
		Peers:    []*metapb.Peer{peer},
		StartKey: []byte(fmt.Sprintf("%20d", 35)),
		EndKey:   []byte(fmt.Sprintf("%20d", 45)),
	}, peer)
	overlaps, item = regions.GetOverlaps(region)
	c.Assert(len(overlaps), Equals, 0)
	c.Assert(item, IsNil)

	region = NewRegionInfo(&metapb.Region{
		Id:       uint64(1),
		Peers:    []*metapb.Peer{peer},
		StartKey: []byte(fmt.Sprintf("%20d", 29)),
		EndKey:   []byte(fmt.Sprintf("%20d", 51)),
	}, peer)
	overlaps, item = regions.GetOverlaps(region)
	c.Assert(len(overlaps), Equals, 2)
	c.Assert(0, Equals, bytes.Compare(overlaps[0].GetStartKey(), []byte(fmt.Sprintf("%20d", 20))))
	c.Assert(0, Equals, bytes.Compare(overlaps[1].GetStartKey(), []byte(fmt.Sprintf("%20d", 50))))
	c.Assert(0, Equals, bytes.Compare(item.region.GetStartKey(), []byte(fmt.Sprintf("%20d", 20))))
}

func (*testRegionKey) TestReplaceOrAddRegion(c *C) {
	regions := NewRegionsInfo()
	for i := 0; i < 100; i++ {
		peer1 := &metapb.Peer{StoreId: uint64(i%5 + 1), Id: uint64(i*5 + 1)}
		peer2 := &metapb.Peer{StoreId: uint64((i+1)%5 + 1), Id: uint64(i*5 + 2)}
		peer3 := &metapb.Peer{StoreId: uint64((i+2)%5 + 1), Id: uint64(i*5 + 3)}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer1, peer2, peer3},
			StartKey: []byte(fmt.Sprintf("%20d", i*10)),
			EndKey:   []byte(fmt.Sprintf("%20d", (i+1)*10)),
		}, peer1)
		regions.ReplaceOrAddRegion(region, nil, nil)
	}

	peer1 := &metapb.Peer{StoreId: uint64(4), Id: uint64(101)}
	peer2 := &metapb.Peer{StoreId: uint64(5), Id: uint64(102)}
	peer3 := &metapb.Peer{StoreId: uint64(1), Id: uint64(103)}
	region := NewRegionInfo(&metapb.Region{
		Id:       uint64(21),
		Peers:    []*metapb.Peer{peer1, peer2, peer3},
		StartKey: []byte(fmt.Sprintf("%20d", 184)),
		EndKey:   []byte(fmt.Sprintf("%20d", 211)),
	}, peer1)
	region.learners = append(region.learners, region.voters[1])
	region.pendingPeers = append(region.pendingPeers, region.voters[2])
	overlaps, item := regions.GetOverlaps(region)
	regions.ReplaceOrAddRegion(region, overlaps, item)
	c.Assert(len(overlaps), Equals, 4)
	c.Assert(item.region.GetID(), Equals, uint64(19))
	c.Assert(regions.leaders[1].length(), Equals, 19)
	c.Assert(regions.leaders[2].length(), Equals, 19)
	c.Assert(regions.leaders[3].length(), Equals, 20)
	c.Assert(regions.leaders[4].length(), Equals, 20)
	c.Assert(regions.leaders[5].length(), Equals, 19)

	c.Assert(regions.followers[1].length(), Equals, 39)
	c.Assert(regions.followers[2].length(), Equals, 38)
	c.Assert(regions.followers[3].length(), Equals, 38)
	c.Assert(regions.followers[4].length(), Equals, 39)
	c.Assert(regions.followers[5].length(), Equals, 40)

	c.Assert(regions.learners[1].length(), Equals, 0)
	c.Assert(regions.learners[2].length(), Equals, 0)
	c.Assert(regions.learners[3].length(), Equals, 0)
	c.Assert(regions.learners[4].length(), Equals, 0)
	c.Assert(regions.learners[5].length(), Equals, 1)

	c.Assert(regions.pendingPeers[1].length(), Equals, 1)
	c.Assert(regions.pendingPeers[2].length(), Equals, 0)
	c.Assert(regions.pendingPeers[3].length(), Equals, 0)
	c.Assert(regions.pendingPeers[4].length(), Equals, 0)
	c.Assert(regions.pendingPeers[5].length(), Equals, 0)
	c.Assert(regions.tree.length(), Equals, 97)
	c.Assert(len(regions.GetRegions()), Equals, 97)

	peer1 = &metapb.Peer{StoreId: uint64(2), Id: uint64(101)}
	peer2 = &metapb.Peer{StoreId: uint64(3), Id: uint64(102)}
	peer3 = &metapb.Peer{StoreId: uint64(1), Id: uint64(103)}
	region = NewRegionInfo(&metapb.Region{
		Id:       uint64(21),
		Peers:    []*metapb.Peer{peer1, peer2, peer3},
		StartKey: []byte(fmt.Sprintf("%20d", 184)),
		EndKey:   []byte(fmt.Sprintf("%20d", 211)),
	}, peer1)
	region.learners = append(region.learners, region.voters[1])
	region.pendingPeers = append(region.pendingPeers, region.voters[2])
	overlaps, item = regions.GetOverlaps(region)
	regions.ReplaceOrAddRegion(region, overlaps, item)
	c.Assert(len(overlaps), Equals, 1)
	c.Assert(isEqualRegion(region, item), IsTrue)
	c.Assert(regions.leaders[1].length(), Equals, 19)
	c.Assert(regions.leaders[2].length(), Equals, 20)
	c.Assert(regions.leaders[3].length(), Equals, 20)
	c.Assert(regions.leaders[4].length(), Equals, 19)
	c.Assert(regions.leaders[5].length(), Equals, 19)

	c.Assert(regions.followers[1].length(), Equals, 39)
	c.Assert(regions.followers[2].length(), Equals, 38)
	c.Assert(regions.followers[3].length(), Equals, 39)
	c.Assert(regions.followers[4].length(), Equals, 39)
	c.Assert(regions.followers[5].length(), Equals, 39)

	c.Assert(regions.learners[1].length(), Equals, 0)
	c.Assert(regions.learners[2].length(), Equals, 0)
	c.Assert(regions.learners[3].length(), Equals, 1)
	c.Assert(regions.learners[4].length(), Equals, 0)
	c.Assert(regions.learners[5].length(), Equals, 0)

	c.Assert(regions.pendingPeers[1].length(), Equals, 1)
	c.Assert(regions.pendingPeers[2].length(), Equals, 0)
	c.Assert(regions.pendingPeers[3].length(), Equals, 0)
	c.Assert(regions.pendingPeers[4].length(), Equals, 0)
	c.Assert(regions.pendingPeers[5].length(), Equals, 0)
	c.Assert(regions.tree.length(), Equals, 97)
	c.Assert(len(regions.GetRegions()), Equals, 97)
}

func (*testRegionKey) TestUpdateRegionToSubTree(c *C) {
	regions := NewRegionsInfo()
	var regionChange *RegionInfo
	for i := 0; i < 100; i++ {
		peer1 := &metapb.Peer{StoreId: uint64(i%5 + 1), Id: uint64(i*5 + 1)}
		peer2 := &metapb.Peer{StoreId: uint64((i+1)%5 + 1), Id: uint64(i*5 + 2)}
		peer3 := &metapb.Peer{StoreId: uint64((i+2)%5 + 1), Id: uint64(i*5 + 3)}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer1, peer2, peer3},
			StartKey: []byte(fmt.Sprintf("%20d", i*10)),
			EndKey:   []byte(fmt.Sprintf("%20d", (i+1)*10)),
		}, peer1)
		regions.ReplaceOrAddRegion(region, nil, nil)
		if i == 40 {
			regionChange = region
		}
	}
	c.Assert(regions.leaders[1].length(), Equals, 20)
	c.Assert(regions.leaders[2].length(), Equals, 20)
	c.Assert(regions.leaders[3].length(), Equals, 20)
	c.Assert(regions.leaders[4].length(), Equals, 20)
	c.Assert(regions.leaders[5].length(), Equals, 20)

	c.Assert(regions.followers[1].length(), Equals, 40)
	c.Assert(regions.followers[2].length(), Equals, 40)
	c.Assert(regions.followers[3].length(), Equals, 40)
	c.Assert(regions.followers[4].length(), Equals, 40)
	c.Assert(regions.followers[5].length(), Equals, 40)

	regions.removeRegionFromSubTree(regionChange)
	c.Assert(regions.leaders[1].length(), Equals, 19)
	c.Assert(regions.leaders[2].length(), Equals, 20)
	c.Assert(regions.leaders[3].length(), Equals, 20)
	c.Assert(regions.leaders[4].length(), Equals, 20)
	c.Assert(regions.leaders[5].length(), Equals, 20)

	c.Assert(regions.followers[1].length(), Equals, 40)
	c.Assert(regions.followers[2].length(), Equals, 39)
	c.Assert(regions.followers[3].length(), Equals, 39)
	c.Assert(regions.followers[4].length(), Equals, 40)
	c.Assert(regions.followers[5].length(), Equals, 40)

	regionChange.voters[0].IsLearner = true
	regionChange.leader.Id = 203
	regionChange.learners = append(regionChange.learners, regionChange.voters[0])
	regionChange.leader = regionChange.voters[1]
	regions.addRegionToSubTree(regionChange)

	c.Assert(regions.leaders[1].length(), Equals, 19)
	c.Assert(regions.leaders[2].length(), Equals, 21)
	c.Assert(regions.leaders[3].length(), Equals, 20)
	c.Assert(regions.leaders[4].length(), Equals, 20)
	c.Assert(regions.leaders[5].length(), Equals, 20)

	c.Assert(regions.followers[1].length(), Equals, 41)
	c.Assert(regions.followers[2].length(), Equals, 39)
	c.Assert(regions.followers[3].length(), Equals, 40)
	c.Assert(regions.followers[4].length(), Equals, 40)
	c.Assert(regions.followers[5].length(), Equals, 40)

	c.Assert(regions.learners[1].length(), Equals, 1)
	c.Assert(regions.learners[2].length(), Equals, 0)
	c.Assert(regions.learners[3].length(), Equals, 0)
	c.Assert(regions.learners[4].length(), Equals, 0)
	c.Assert(regions.learners[5].length(), Equals, 0)
}

func (*testRegionKey) TestIsEqualRegion(c *C) {
	peer1 := &metapb.Peer{StoreId: uint64(1), Id: uint64(1)}
	peer2 := &metapb.Peer{StoreId: uint64(2), Id: uint64(2)}
	peer3 := &metapb.Peer{StoreId: uint64(3), Id: uint64(3)}
	region := NewRegionInfo(&metapb.Region{
		Id:       uint64(1),
		Peers:    []*metapb.Peer{peer1, peer2, peer3},
		StartKey: []byte(fmt.Sprintf("%20d", 10)),
		EndKey:   []byte(fmt.Sprintf("%20d", 20)),
	}, peer1)

	otherRegion := NewRegionInfo(&metapb.Region{
		Id:       uint64(2),
		Peers:    []*metapb.Peer{peer1, peer2, peer3},
		StartKey: []byte(fmt.Sprintf("%20d", 20)),
		EndKey:   []byte(fmt.Sprintf("%20d", 30)),
	}, peer1)
	other := &regionItem{region: otherRegion}

	c.Assert(isEqualRegion(region, nil), Equals, false)
	c.Assert(isEqualRegion(nil, nil), Equals, false)
	c.Assert(isEqualRegion(nil, other), Equals, false)
	c.Assert(isEqualRegion(region, other), Equals, false)
	otherRegion.meta.Id = 1
	otherRegion.meta.StartKey = []byte(fmt.Sprintf("%20d", 10))
	otherRegion.meta.EndKey = []byte(fmt.Sprintf("%20d", 20))
	c.Assert(isEqualRegion(region, other), Equals, true)
	otherRegion.meta.StartKey = []byte(fmt.Sprintf("%20d", 11))
	c.Assert(isEqualRegion(region, other), Equals, true)
	otherRegion.meta.StartKey = []byte(fmt.Sprintf("%20d", 10))
	otherRegion.meta.EndKey = []byte(fmt.Sprintf("%20d", 21))
	c.Assert(isEqualRegion(region, other), Equals, true)
}

func (*testRegionKey) TestIsEqualPeers(c *C) {
	var peers [3]*metapb.Peer
	peers[0] = &metapb.Peer{StoreId: uint64(1), Id: uint64(1)}
	peers[1] = &metapb.Peer{StoreId: uint64(2), Id: uint64(2)}
	peers[2] = &metapb.Peer{StoreId: uint64(3), Id: uint64(3)}
	c.Assert(isEqualPeers(peers[:], nil), Equals, false)
	c.Assert(isEqualPeers(nil, peers[:]), Equals, false)
	c.Assert(isEqualPeers(nil, nil), Equals, true)

	var others [4]*metapb.Peer
	others[0] = &metapb.Peer{StoreId: uint64(1), Id: uint64(1)}
	others[1] = &metapb.Peer{StoreId: uint64(2), Id: uint64(2)}
	others[2] = &metapb.Peer{StoreId: uint64(3), Id: uint64(3)}
	others[3] = &metapb.Peer{StoreId: uint64(3), Id: uint64(4)}
	c.Assert(isEqualPeers(others[:], peers[:]), Equals, false)
	c.Assert(isEqualPeers(peers[:], others[:]), Equals, false)

	var others2 [3]*metapb.Peer
	others2[0] = &metapb.Peer{StoreId: uint64(1), Id: uint64(1)}
	others2[1] = &metapb.Peer{StoreId: uint64(2), Id: uint64(2)}
	others2[2] = &metapb.Peer{StoreId: uint64(3), Id: uint64(4)}
	c.Assert(isEqualPeers(others2[:], peers[:]), Equals, false)
	c.Assert(isEqualPeers(peers[:], others2[:]), Equals, false)

	var others3 [3]*metapb.Peer
	others3[0] = &metapb.Peer{StoreId: uint64(1), Id: uint64(1)}
	others3[1] = &metapb.Peer{StoreId: uint64(4), Id: uint64(2)}
	others3[2] = &metapb.Peer{StoreId: uint64(3), Id: uint64(3)}
	c.Assert(isEqualPeers(others3[:], peers[:]), Equals, false)
	c.Assert(isEqualPeers(peers[:], others3[:]), Equals, false)

	var others4 [4]*metapb.Peer
	others4[2] = &metapb.Peer{StoreId: uint64(1), Id: uint64(1)}
	others4[3] = &metapb.Peer{StoreId: uint64(2), Id: uint64(2)}
	others4[1] = &metapb.Peer{StoreId: uint64(3), Id: uint64(3)}
	c.Assert(isEqualPeers(peers[:], others4[:]), Equals, true)
	c.Assert(isEqualPeers(others4[:], peers[:]), Equals, true)

}

func BenchmarkRandomRegion(b *testing.B) {
	regions := NewRegionsInfo()
	for i := 0; i < 5000000; i++ {
		peer := &metapb.Peer{StoreId: 1, Id: uint64(i + 1)}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer},
			StartKey: []byte(fmt.Sprintf("%20d", i)),
			EndKey:   []byte(fmt.Sprintf("%20d", i+1)),
		}, peer)
		regions.AddRegion(region)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		regions.RandLeaderRegion(1, nil)
	}
}

const keyLength = 100

func randomBytes(n int) []byte {
	bytes := make([]byte, n)
	_, err := rand.Read(bytes)
	if err != nil {
		panic(err)
	}
	return bytes
}

func newRegionInfoID(idAllocator id.Allocator) *RegionInfo {
	var (
		peers  []*metapb.Peer
		leader *metapb.Peer
	)
	for i := 0; i < 3; i++ {
		id, _ := idAllocator.Alloc()
		p := &metapb.Peer{Id: id, StoreId: id}
		if i == 0 {
			leader = p
		}
		peers = append(peers, p)
	}
	regionID, _ := idAllocator.Alloc()
	return NewRegionInfo(
		&metapb.Region{
			Id:       regionID,
			StartKey: randomBytes(keyLength),
			EndKey:   randomBytes(keyLength),
			Peers:    peers,
		},
		leader,
	)
}

func BenchmarkAddRegion(b *testing.B) {
	regions := NewRegionsInfo()
	idAllocator := mockid.NewIDAllocator()
	var items []*RegionInfo
	for i := 0; i < 10000000; i++ {
		items = append(items, newRegionInfoID(idAllocator))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		regions.AddRegion(items[i])
	}
}
