// Copyright 2016 TiKV Project Authors.
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

package core

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestRegionInfo(t *testing.T) {
	re := require.New(t)
	n := uint64(3)

	peers := make([]*metapb.Peer, 0, n)
	for i := range n {
		p := &metapb.Peer{
			Id:      i,
			StoreId: i,
		}
		peers = append(peers, p)
	}
	region := &metapb.Region{
		Peers: peers,
	}
	downPeer, pendingPeer := peers[0], peers[1]

	info := NewRegionInfo(
		region,
		peers[0],
		WithDownPeers([]*pdpb.PeerStats{{Peer: downPeer}}),
		WithPendingPeers([]*metapb.Peer{pendingPeer}))

	r := info.Clone()
	re.Equal(info, r)

	for i := range n {
		re.Equal(r.meta.Peers[i], r.GetPeer(i))
	}
	re.Nil(r.GetPeer(n))
	re.Nil(r.GetDownPeer(n))
	re.Equal(downPeer, r.GetDownPeer(downPeer.GetId()))
	re.Nil(r.GetPendingPeer(n))
	re.Equal(pendingPeer, r.GetPendingPeer(pendingPeer.GetId()))

	for i := range n {
		re.Equal(i, r.GetStorePeer(i).GetStoreId())
	}
	re.Nil(r.GetStorePeer(n))

	removePeer := &metapb.Peer{
		Id:      n,
		StoreId: n,
	}
	r = r.Clone(SetPeers(append(r.meta.Peers, removePeer)))
	re.Regexp("Add peer.*", DiffRegionPeersInfo(info, r))
	re.Regexp("Remove peer.*", DiffRegionPeersInfo(r, info))
	re.Equal(removePeer, r.GetStorePeer(n))
	r = r.Clone(WithRemoveStorePeer(n))
	re.Empty(DiffRegionPeersInfo(r, info))
	re.Nil(r.GetStorePeer(n))
	r = r.Clone(WithStartKey([]byte{0}))
	re.Regexp("StartKey Changed.*", DiffRegionKeyInfo(r, info))
	r = r.Clone(WithEndKey([]byte{1}))
	re.Regexp(".*EndKey Changed.*", DiffRegionKeyInfo(r, info))

	stores := r.GetStoreIDs()
	re.Len(stores, int(n))
	for i := range n {
		_, ok := stores[i]
		re.True(ok)
	}

	followers := r.GetFollowers()
	re.Len(followers, int(n-1))
	for i := uint64(1); i < n; i++ {
		re.Equal(peers[i], followers[peers[i].GetStoreId()])
	}
}

func TestRegionItem(t *testing.T) {
	re := require.New(t)
	item := newRegionItemFromRange([]byte("b"), []byte{})

	re.False(item.Less(newRegionItemFromRange([]byte("a"), []byte{})))
	re.False(item.Less(newRegionItemFromRange([]byte("b"), []byte{})))
	re.True(item.Less(newRegionItemFromRange([]byte("c"), []byte{})))

	re.False(item.getRegion().contain([]byte("a")))
	re.True(item.getRegion().contain([]byte("b")))
	re.True(item.getRegion().contain([]byte("c")))

	item = newRegionItemFromRange([]byte("b"), []byte("d"))
	re.False(item.getRegion().contain([]byte("a")))
	re.True(item.getRegion().contain([]byte("b")))
	re.True(item.getRegion().contain([]byte("c")))
	re.False(item.getRegion().contain([]byte("d")))
}

func newRegionWithStat(start, end string, size, keys int64) *RegionInfo {
	region := NewTestRegionInfo(1, 1, []byte(start), []byte(end))
	region.approximateSize, region.approximateKeys = size, keys
	return region
}

func TestRegionTreeStat(t *testing.T) {
	re := require.New(t)
	tree := newRegionTree()
	re.Equal(int64(0), tree.totalSize)
	updateNewItem(tree, newRegionWithStat("a", "b", 1, 2))
	re.Equal(int64(1), tree.totalSize)
	updateNewItem(tree, newRegionWithStat("b", "c", 3, 4))
	re.Equal(int64(4), tree.totalSize)
	updateNewItem(tree, newRegionWithStat("b", "e", 5, 6))
	re.Equal(int64(6), tree.totalSize)
	tree.remove(newRegionWithStat("a", "b", 2, 2))
	re.Equal(int64(5), tree.totalSize)
	tree.remove(newRegionWithStat("f", "g", 1, 2))
	re.Equal(int64(5), tree.totalSize)
}

func TestRegionTreeMerge(t *testing.T) {
	re := require.New(t)
	tree := newRegionTree()
	updateNewItem(tree, newRegionWithStat("a", "b", 1, 2))
	updateNewItem(tree, newRegionWithStat("b", "c", 3, 4))
	re.Equal(int64(4), tree.totalSize)
	updateNewItem(tree, newRegionWithStat("a", "c", 5, 5))
	re.Equal(int64(5), tree.totalSize)
}

func TestRegionTree(t *testing.T) {
	re := require.New(t)
	tree := newRegionTree()

	re.Nil(tree.search([]byte("a")))

	regionA := NewTestRegionInfo(1, 1, []byte("a"), []byte("b"))
	regionB := NewTestRegionInfo(2, 2, []byte("b"), []byte("c"))
	regionC := NewTestRegionInfo(3, 3, []byte("c"), []byte("d"))
	regionD := NewTestRegionInfo(4, 4, []byte("d"), []byte{})

	updateNewItem(tree, regionA)
	updateNewItem(tree, regionC)
	re.Nil(tree.overlaps(newRegionItemFromRange([]byte("b"), []byte("c"))))
	re.Equal(regionC, tree.overlaps(newRegionItemFromRange([]byte("c"), []byte("d")))[0])
	re.Equal(regionC, tree.overlaps(newRegionItemFromRange([]byte("a"), []byte("cc")))[1])
	re.Nil(tree.search([]byte{}))
	re.Equal(regionA, tree.search([]byte("a")))
	re.Nil(tree.search([]byte("b")))
	re.Equal(regionC, tree.search([]byte("c")))
	re.Nil(tree.search([]byte("d")))

	// search previous region
	re.Nil(tree.searchPrev([]byte("a")))
	re.Nil(tree.searchPrev([]byte("b")))
	re.Nil(tree.searchPrev([]byte("c")))

	updateNewItem(tree, regionB)
	// search previous region
	re.Equal(regionB, tree.searchPrev([]byte("c")))
	re.Equal(regionA, tree.searchPrev([]byte("b")))

	tree.remove(regionC)
	updateNewItem(tree, regionD)
	re.Nil(tree.search([]byte{}))
	re.Equal(regionA, tree.search([]byte("a")))
	re.Equal(regionB, tree.search([]byte("b")))
	re.Nil(tree.search([]byte("c")))
	re.Equal(regionD, tree.search([]byte("d")))

	// check get adjacent regions
	prev, next := tree.getAdjacentRegions(regionA)
	re.Nil(prev)
	re.Equal(regionB, next.getRegion())
	prev, next = tree.getAdjacentRegions(regionB)
	re.Equal(regionA, prev.getRegion())
	re.Equal(regionD, next.getRegion())
	prev, next = tree.getAdjacentRegions(regionC)
	re.Equal(regionB, prev.getRegion())
	re.Equal(regionD, next.getRegion())
	prev, next = tree.getAdjacentRegions(regionD)
	re.Equal(regionB, prev.getRegion())
	re.Nil(next)

	// region with the same range and different region id will not be delete.
	region0 := newRegionItemFromRange([]byte{}, []byte("a")).getRegion()
	updateNewItem(tree, region0)
	re.Equal(region0, tree.search([]byte{}))
	anotherRegion0 := newRegionItemFromRange([]byte{}, []byte("a")).getRegion()
	anotherRegion0.meta.Id = 123
	tree.remove(anotherRegion0)
	re.Equal(region0, tree.search([]byte{}))

	// overlaps with 0, A, B, C.
	region0D := newRegionItemFromRange([]byte(""), []byte("d")).getRegion()
	updateNewItem(tree, region0D)
	re.Equal(region0D, tree.search([]byte{}))
	re.Equal(region0D, tree.search([]byte("a")))
	re.Equal(region0D, tree.search([]byte("b")))
	re.Equal(region0D, tree.search([]byte("c")))
	re.Equal(regionD, tree.search([]byte("d")))

	// overlaps with D.
	regionE := newRegionItemFromRange([]byte("e"), []byte{}).getRegion()
	updateNewItem(tree, regionE)
	re.Equal(region0D, tree.search([]byte{}))
	re.Equal(region0D, tree.search([]byte("a")))
	re.Equal(region0D, tree.search([]byte("b")))
	re.Equal(region0D, tree.search([]byte("c")))
	re.Nil(tree.search([]byte("d")))
	re.Equal(regionE, tree.search([]byte("e")))
}

func updateRegions(re *require.Assertions, tree *regionTree, regions []*RegionInfo) {
	for _, region := range regions {
		updateNewItem(tree, region)
		re.Equal(region, tree.search(region.GetStartKey()))
		if len(region.GetEndKey()) > 0 {
			end := region.GetEndKey()[0]
			re.Equal(region, tree.search([]byte{end - 1}))
			re.NotEqual(region, tree.search([]byte{end + 1}))
		}
	}
}

// snapshotRanges returns the key ranges a snapshot reports, in iteration order.
func snapshotRanges(snap *rootRangeSnapshot) [][2]string {
	var got [][2]string
	snap.scanRange([]byte(""), func(r *RegionInfo) bool {
		got = append(got, [2]string{string(r.GetStartKey()), string(r.GetEndKey())})
		return true
	})
	return got
}

// treeRanges returns the key ranges a live tree reports, in iteration order.
func treeRanges(regions *RegionsInfo) [][2]string {
	var got [][2]string
	regions.tree.scanRange([]byte(""), func(r *RegionInfo) bool {
		got = append(got, [2]string{string(r.GetStartKey()), string(r.GetEndKey())})
		return true
	})
	return got
}

// TestRootRangeSnapshotIsolation checks the contract documented on
// rootRangeSnapshot: the shape is frozen, the values are not.
//
// It applies all three mutation classes after taking the snapshot -- a same-range
// update, a split, and a removal -- and asserts that the snapshot still reports
// the original key ranges in the original order while the live tree reports the
// new ones.
func TestRootRangeSnapshotIsolation(t *testing.T) {
	re := require.New(t)
	regions := NewRegionsInfo()
	put := func(id uint64, start, end string, opts ...RegionCreateOption) {
		region := NewTestRegionInfo(id, 1, []byte(start), []byte(end), opts...)
		_, err := regions.AtomicCheckAndPutRegion(ContextTODO(), region)
		re.NoError(err)
	}

	put(1, "a", "b", SetApproximateSize(10))
	put(2, "b", "c", SetApproximateSize(10))
	put(3, "c", "d", SetApproximateSize(10))
	re.Equal(int64(30), regions.GetRegionSizeByRange([]byte("a"), []byte("d")))

	snap := regions.snapshotRootTree()
	original := [][2]string{{"a", "b"}, {"b", "c"}, {"c", "d"}}
	re.Equal(original, snapshotRanges(snap))

	// 1. Same range, new value.
	put(2, "b", "c", SetApproximateSize(99), SetRegionVersion(2))
	// 2. Split [c, d) into [c, cc) and [cc, d).
	put(3, "c", "cc", SetRegionVersion(2))
	put(4, "cc", "d", SetRegionVersion(2))
	// 3. Remove [a, b).
	regions.RemoveRegionIfExist(1)

	// The shape is frozen: same items, same ranges, same order, same length.
	re.Equal(3, snap.length())
	re.Equal(original, snapshotRanges(snap))

	// The value is not frozen. The same-range update has completed and the item is
	// shared with the live tree, so the snapshot now reports the new size. This is
	// the documented latitude, not a bug.
	var sizeOfB int64
	snap.scanRange([]byte("b"), func(r *RegionInfo) bool {
		sizeOfB = r.GetApproximateSize()
		return false
	})
	re.Equal(int64(99), sizeOfB)

	// The live tree has moved on.
	re.Equal([][2]string{{"b", "c"}, {"c", "cc"}, {"cc", "d"}}, treeRanges(regions))
}

// TestRegionItemRangeImmutability is the direct guard for the invariant
// documented on regionItem: an item that is in the root tree never changes its
// key range.
//
// It holds one snapshot for the whole test and, after every update, checks the
// ordering of both the live tree and that snapshot. An in-place range change
// anywhere on the root path shows up here as a snapshot whose start keys stop
// increasing, which is the state that makes a clone return wrong answers.
func TestRegionItemRangeImmutability(t *testing.T) {
	re := require.New(t)
	regions := NewRegionsInfo()
	const count = 50
	for i := range count {
		region := NewTestRegionInfo(uint64(i+1), 1,
			[]byte(fmt.Sprintf("%04d", i*10)), []byte(fmt.Sprintf("%04d", (i+1)*10)))
		_, err := regions.AtomicCheckAndPutRegion(ContextTODO(), region)
		re.NoError(err)
	}

	snap := regions.snapshotRootTree()
	snapLen := snap.length()

	checkOrdered := func(desc string, scan func(func(*RegionInfo) bool)) int {
		var prev []byte
		n := 0
		scan(func(r *RegionInfo) bool {
			if n > 0 {
				re.Negative(bytes.Compare(prev, r.GetStartKey()),
					"%s: start keys not increasing at %q after %q", desc, r.GetStartKey(), prev)
			}
			prev = r.GetStartKey()
			n++
			return true
		})
		return n
	}

	version := uint64(1)
	for i := range 1000 {
		id := uint64(i%count + 1)
		origin := regions.GetRegion(id)
		re.NotNil(origin)
		version++
		var region *RegionInfo
		if i%2 == 0 {
			// Same range, new value.
			region = origin.Clone(SetApproximateSize(int64(i)), SetRegionVersion(version))
		} else {
			// Range change: toggle the end key between its original value and a
			// shorter one. Appending to the start key keeps the new end key strictly
			// inside the region, so this never overlaps a neighbour and never
			// deletes one.
			fullEnd := []byte(fmt.Sprintf("%04d", int(id)*10))
			shrunkEnd := append(append([]byte{}, origin.GetStartKey()...), '5')
			endKey := shrunkEnd
			if !bytes.Equal(origin.GetEndKey(), fullEnd) {
				endKey = fullEnd
			}
			region = origin.Clone(WithEndKey(endKey), SetRegionVersion(version))
		}
		_, err := regions.AtomicCheckAndPutRegion(ContextTODO(), region)
		re.NoError(err)

		checkOrdered("live tree", func(f func(*RegionInfo) bool) {
			regions.tree.scanRange([]byte(""), f)
		})
		n := checkOrdered("snapshot", func(f func(*RegionInfo) bool) {
			snap.scanRange([]byte(""), f)
		})
		// The snapshot's shape is frozen, so its length cannot drift either.
		re.Equal(snapLen, n)
		re.Equal(snapLen, snap.length())
	}
}

func TestRegionTreeSplitAndMerge(t *testing.T) {
	re := require.New(t)
	tree := newRegionTree()
	regions := []*RegionInfo{newRegionItemFromRange([]byte{}, []byte{}).getRegion()}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for range n {
		regions = SplitRegions(regions)
		updateRegions(re, tree, regions)
	}

	// Merge.
	for range n {
		regions = MergeRegions(regions)
		updateRegions(re, tree, regions)
	}

	// Split twice and merge once.
	for i := range n * 2 {
		if (i+1)%3 == 0 {
			regions = MergeRegions(regions)
		} else {
			regions = SplitRegions(regions)
		}
		updateRegions(re, tree, regions)
	}
}

func TestRandomRegion(t *testing.T) {
	re := require.New(t)
	tree := newRegionTree()
	r := tree.randomRegion(nil)
	re.Nil(r)

	regionA := NewTestRegionInfo(1, 1, []byte(""), []byte("g"))
	updateNewItem(tree, regionA)
	ra := tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("", "")})
	re.Equal(regionA, ra)
	ra = tree.randomRegion(nil)
	re.Equal(regionA, ra)
	ra2 := tree.RandomRegions(2, []keyutil.KeyRange{keyutil.NewKeyRange("", "")})
	re.Equal([]*RegionInfo{regionA, regionA}, ra2)
	ra2 = tree.RandomRegions(2, nil)
	re.Equal([]*RegionInfo{regionA, regionA}, ra2)

	regionB := NewTestRegionInfo(2, 2, []byte("g"), []byte("n"))
	regionC := NewTestRegionInfo(3, 3, []byte("n"), []byte("t"))
	regionD := NewTestRegionInfo(4, 4, []byte("t"), []byte(""))
	updateNewItem(tree, regionB)
	updateNewItem(tree, regionC)
	updateNewItem(tree, regionD)

	rb := tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("g", "n")})
	re.Equal(regionB, rb)
	rc := tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("n", "t")})
	re.Equal(regionC, rc)
	rd := tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("t", "")})
	re.Equal(regionD, rd)

	rf := tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("", "a")})
	re.Nil(rf)
	rf = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("o", "s")})
	re.Nil(rf)
	rf = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("", "a")})
	re.Nil(rf)
	rf = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("z", "")})
	re.Nil(rf)

	checkRandomRegion(re, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, nil)
	checkRandomRegion(re, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, []keyutil.KeyRange{keyutil.NewKeyRange("", "")})
	checkRandomRegion(re, tree, []*RegionInfo{regionA, regionB}, []keyutil.KeyRange{keyutil.NewKeyRange("", "n")})
	checkRandomRegion(re, tree, []*RegionInfo{regionC, regionD}, []keyutil.KeyRange{keyutil.NewKeyRange("n", "")})
	checkRandomRegion(re, tree, []*RegionInfo{}, []keyutil.KeyRange{keyutil.NewKeyRange("h", "s")})
	checkRandomRegion(re, tree, []*RegionInfo{regionB, regionC}, []keyutil.KeyRange{keyutil.NewKeyRange("a", "z")})
}

func TestRandomRegionDiscontinuous(t *testing.T) {
	re := require.New(t)
	tree := newRegionTree()
	r := tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("c", "f")})
	re.Nil(r)

	// test for single region
	regionA := NewTestRegionInfo(1, 1, []byte("c"), []byte("f"))
	updateNewItem(tree, regionA)
	ra := tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("c", "e")})
	re.Nil(ra)
	ra = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("c", "f")})
	re.Equal(regionA, ra)
	ra = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("c", "g")})
	re.Equal(regionA, ra)
	ra = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("a", "e")})
	re.Nil(ra)
	ra = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("a", "f")})
	re.Equal(regionA, ra)
	ra = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("a", "g")})
	re.Equal(regionA, ra)

	regionB := NewTestRegionInfo(2, 2, []byte("n"), []byte("x"))
	updateNewItem(tree, regionB)
	rb := tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("g", "x")})
	re.Equal(regionB, rb)
	rb = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("g", "y")})
	re.Equal(regionB, rb)
	rb = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("n", "y")})
	re.Equal(regionB, rb)
	rb = tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("o", "y")})
	re.Nil(rb)

	regionC := NewTestRegionInfo(3, 3, []byte("z"), []byte(""))
	updateNewItem(tree, regionC)
	rc := tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("y", "")})
	re.Equal(regionC, rc)
	regionD := NewTestRegionInfo(4, 4, []byte(""), []byte("a"))
	updateNewItem(tree, regionD)
	rd := tree.randomRegion([]keyutil.KeyRange{keyutil.NewKeyRange("", "b")})
	re.Equal(regionD, rd)

	checkRandomRegion(re, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, nil)
	checkRandomRegion(re, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, []keyutil.KeyRange{keyutil.NewKeyRange("", "")})
}

func TestStoreRegionCount(t *testing.T) {
	re := require.New(t)
	regions := NewRegionsInfo()
	i := uint64(1)
	voterFn := func() *metapb.Peer {
		i++
		return &metapb.Peer{
			StoreId: 2,
			Id:      i,
			Role:    metapb.PeerRole_Voter,
		}
	}
	learnerFn := func() *metapb.Peer {
		i++
		return &metapb.Peer{
			StoreId: 3,
			Id:      i,
			Role:    metapb.PeerRole_Learner,
		}
	}

	regions.CheckAndPutRegion(NewTestRegionInfo(1, 1, []byte("a"), []byte("c"), WithAddPeer(voterFn()), WithAddPeer(learnerFn())))
	regions.CheckAndPutRegion(NewTestRegionInfo(2, 1, []byte("e"), []byte("g"), WithAddPeer(voterFn()), WithAddPeer(learnerFn())))
	regions.CheckAndPutRegion(NewTestRegionInfo(3, 1, []byte("g"), []byte("i"), WithAddPeer(voterFn()), WithAddPeer(learnerFn())))
	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f"), []byte("g"), []byte("h"), []byte("")} {
		count := regions.GetRegionCount([]byte("a"), key)
		scanCount := len(regions.ScanRegions([]byte("a"), key, 100))
		re.Equal(count, scanCount, "endKey: %s", key)
		storeCount := regions.GetStoreLeaderCountByRange(uint64(1), []byte("a"), key)
		re.Equal(count, storeCount, "endKey: %s", key)
		learnerStoreCount := regions.GetStoreLearnerCountByRange(uint64(3), []byte("a"), key)
		re.Equal(count, learnerStoreCount, "endKey: %s", key)
		for _, storeID := range []uint64{1, 2, 3} {
			storePeerCount := regions.GetStorePeerCountByRange(storeID, []byte("a"), key)
			re.Equal(count, storePeerCount, "endKey: %s", key)
		}
	}
}

func updateNewItem(tree *regionTree, region *RegionInfo) {
	item := newRegionItem(region)
	tree.update(item, false)
}

func checkRandomRegion(re *require.Assertions, tree *regionTree, regions []*RegionInfo, ranges []keyutil.KeyRange) {
	keys := make(map[string]struct{})
	for i := 0; i < 10000 && len(keys) < len(regions); i++ {
		re := tree.randomRegion(ranges)
		if re == nil {
			continue
		}
		k := string(re.GetStartKey())
		if _, ok := keys[k]; !ok {
			keys[k] = struct{}{}
		}
	}
	for _, region := range regions {
		_, ok := keys[string(region.GetStartKey())]
		re.True(ok)
	}
	re.Len(keys, len(regions))
}

func newRegionItemFromRange(start, end []byte) *regionItem {
	return newRegionItem(NewTestRegionInfo(1, 1, start, end))
}

type mockRegionTreeData struct {
	tree  *regionTree
	items []*RegionInfo
}

func (m *mockRegionTreeData) clearTree() *mockRegionTreeData {
	m.tree = newRegionTree()
	return m
}

func (m *mockRegionTreeData) shuffleItems() *mockRegionTreeData {
	for i := 0; i < len(m.items); i++ {
		j := rand.IntN(i + 1)
		m.items[i], m.items[j] = m.items[j], m.items[i]
	}
	return m
}

func mock1MRegionTree() *mockRegionTreeData {
	data := &mockRegionTreeData{newRegionTree(), make([]*RegionInfo, 1000000)}
	for i := range 1_000_000 {
		region := &RegionInfo{meta: &metapb.Region{Id: uint64(i), StartKey: []byte(fmt.Sprintf("%20d", i)), EndKey: []byte(fmt.Sprintf("%20d", i+1))}}
		updateNewItem(data.tree, region)
		data.items[i] = region
	}
	return data
}

const MaxCount = 1_000_000

func BenchmarkRegionTreeSequentialInsert(b *testing.B) {
	tree := newRegionTree()
	for i := range b.N {
		item := &RegionInfo{meta: &metapb.Region{StartKey: []byte(fmt.Sprintf("%20d", i)), EndKey: []byte(fmt.Sprintf("%20d", i+1))}}
		updateNewItem(tree, item)
	}
}

func BenchmarkRegionTreeRandomInsert(b *testing.B) {
	data := mock1MRegionTree().clearTree().shuffleItems()
	b.ResetTimer()
	for i := range b.N {
		index := i % MaxCount
		updateNewItem(data.tree, data.items[index])
	}
}

func BenchmarkRegionTreeRandomOverlapsInsert(b *testing.B) {
	tree := newRegionTree()
	var items []*RegionInfo
	for range MaxCount {
		var startKey, endKey int
		key1 := rand.IntN(MaxCount)
		key2 := rand.IntN(MaxCount)
		if key1 < key2 {
			startKey = key1
			endKey = key2
		} else {
			startKey = key2
			endKey = key1
		}
		items = append(items, &RegionInfo{meta: &metapb.Region{StartKey: []byte(fmt.Sprintf("%20d", startKey)), EndKey: []byte(fmt.Sprintf("%20d", endKey))}})
	}
	b.ResetTimer()
	for i := range b.N {
		index := i % MaxCount
		updateNewItem(tree, items[index])
	}
}

func BenchmarkRegionTreeRandomUpdate(b *testing.B) {
	data := mock1MRegionTree().shuffleItems()
	b.ResetTimer()
	for i := range b.N {
		index := i % MaxCount
		updateNewItem(data.tree, data.items[index])
	}
}

func BenchmarkRegionTreeSequentialLookUpRegion(b *testing.B) {
	data := mock1MRegionTree()
	b.ResetTimer()
	for i := range b.N {
		index := i % MaxCount
		data.tree.find(newRegionItem(data.items[index]))
	}
}

func BenchmarkRegionTreeRandomLookUpRegion(b *testing.B) {
	data := mock1MRegionTree().shuffleItems()
	b.ResetTimer()
	for i := range b.N {
		index := i % MaxCount
		data.tree.find(newRegionItem(data.items[index]))
	}
}

func BenchmarkRegionTreeScan(b *testing.B) {
	data := mock1MRegionTree().shuffleItems()
	b.ResetTimer()
	for range b.N {
		data.tree.scanRanges()
	}
}
