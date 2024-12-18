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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
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
	re.Equal("", DiffRegionPeersInfo(r, info))
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
	item := newRegionItem([]byte("b"), []byte{})

	re.False(item.Less(newRegionItem([]byte("a"), []byte{})))
	re.False(item.Less(newRegionItem([]byte("b"), []byte{})))
	re.True(item.Less(newRegionItem([]byte("c"), []byte{})))

	re.False(item.contain([]byte("a")))
	re.True(item.contain([]byte("b")))
	re.True(item.contain([]byte("c")))

	item = newRegionItem([]byte("b"), []byte("d"))
	re.False(item.contain([]byte("a")))
	re.True(item.contain([]byte("b")))
	re.True(item.contain([]byte("c")))
	re.False(item.contain([]byte("d")))
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
	re.Nil(tree.overlaps(newRegionItem([]byte("b"), []byte("c"))))
	re.Equal(regionC, tree.overlaps(newRegionItem([]byte("c"), []byte("d")))[0])
	re.Equal(regionC, tree.overlaps(newRegionItem([]byte("a"), []byte("cc")))[1])
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
	re.Equal(regionB, next.RegionInfo)
	prev, next = tree.getAdjacentRegions(regionB)
	re.Equal(regionA, prev.RegionInfo)
	re.Equal(regionD, next.RegionInfo)
	prev, next = tree.getAdjacentRegions(regionC)
	re.Equal(regionB, prev.RegionInfo)
	re.Equal(regionD, next.RegionInfo)
	prev, next = tree.getAdjacentRegions(regionD)
	re.Equal(regionB, prev.RegionInfo)
	re.Nil(next)

	// region with the same range and different region id will not be delete.
	region0 := newRegionItem([]byte{}, []byte("a")).RegionInfo
	updateNewItem(tree, region0)
	re.Equal(region0, tree.search([]byte{}))
	anotherRegion0 := newRegionItem([]byte{}, []byte("a")).RegionInfo
	anotherRegion0.meta.Id = 123
	tree.remove(anotherRegion0)
	re.Equal(region0, tree.search([]byte{}))

	// overlaps with 0, A, B, C.
	region0D := newRegionItem([]byte(""), []byte("d")).RegionInfo
	updateNewItem(tree, region0D)
	re.Equal(region0D, tree.search([]byte{}))
	re.Equal(region0D, tree.search([]byte("a")))
	re.Equal(region0D, tree.search([]byte("b")))
	re.Equal(region0D, tree.search([]byte("c")))
	re.Equal(regionD, tree.search([]byte("d")))

	// overlaps with D.
	regionE := newRegionItem([]byte("e"), []byte{}).RegionInfo
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

func TestRegionTreeSplitAndMerge(t *testing.T) {
	re := require.New(t)
	tree := newRegionTree()
	regions := []*RegionInfo{newRegionItem([]byte{}, []byte{}).RegionInfo}

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
	ra := tree.randomRegion([]KeyRange{NewKeyRange("", "")})
	re.Equal(regionA, ra)
	ra = tree.randomRegion(nil)
	re.Equal(regionA, ra)
	ra2 := tree.RandomRegions(2, []KeyRange{NewKeyRange("", "")})
	re.Equal([]*RegionInfo{regionA, regionA}, ra2)
	ra2 = tree.RandomRegions(2, nil)
	re.Equal([]*RegionInfo{regionA, regionA}, ra2)

	regionB := NewTestRegionInfo(2, 2, []byte("g"), []byte("n"))
	regionC := NewTestRegionInfo(3, 3, []byte("n"), []byte("t"))
	regionD := NewTestRegionInfo(4, 4, []byte("t"), []byte(""))
	updateNewItem(tree, regionB)
	updateNewItem(tree, regionC)
	updateNewItem(tree, regionD)

	rb := tree.randomRegion([]KeyRange{NewKeyRange("g", "n")})
	re.Equal(regionB, rb)
	rc := tree.randomRegion([]KeyRange{NewKeyRange("n", "t")})
	re.Equal(regionC, rc)
	rd := tree.randomRegion([]KeyRange{NewKeyRange("t", "")})
	re.Equal(regionD, rd)

	rf := tree.randomRegion([]KeyRange{NewKeyRange("", "a")})
	re.Nil(rf)
	rf = tree.randomRegion([]KeyRange{NewKeyRange("o", "s")})
	re.Nil(rf)
	rf = tree.randomRegion([]KeyRange{NewKeyRange("", "a")})
	re.Nil(rf)
	rf = tree.randomRegion([]KeyRange{NewKeyRange("z", "")})
	re.Nil(rf)

	checkRandomRegion(re, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, nil)
	checkRandomRegion(re, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, []KeyRange{NewKeyRange("", "")})
	checkRandomRegion(re, tree, []*RegionInfo{regionA, regionB}, []KeyRange{NewKeyRange("", "n")})
	checkRandomRegion(re, tree, []*RegionInfo{regionC, regionD}, []KeyRange{NewKeyRange("n", "")})
	checkRandomRegion(re, tree, []*RegionInfo{}, []KeyRange{NewKeyRange("h", "s")})
	checkRandomRegion(re, tree, []*RegionInfo{regionB, regionC}, []KeyRange{NewKeyRange("a", "z")})
}

func TestRandomRegionDiscontinuous(t *testing.T) {
	re := require.New(t)
	tree := newRegionTree()
	r := tree.randomRegion([]KeyRange{NewKeyRange("c", "f")})
	re.Nil(r)

	// test for single region
	regionA := NewTestRegionInfo(1, 1, []byte("c"), []byte("f"))
	updateNewItem(tree, regionA)
	ra := tree.randomRegion([]KeyRange{NewKeyRange("c", "e")})
	re.Nil(ra)
	ra = tree.randomRegion([]KeyRange{NewKeyRange("c", "f")})
	re.Equal(regionA, ra)
	ra = tree.randomRegion([]KeyRange{NewKeyRange("c", "g")})
	re.Equal(regionA, ra)
	ra = tree.randomRegion([]KeyRange{NewKeyRange("a", "e")})
	re.Nil(ra)
	ra = tree.randomRegion([]KeyRange{NewKeyRange("a", "f")})
	re.Equal(regionA, ra)
	ra = tree.randomRegion([]KeyRange{NewKeyRange("a", "g")})
	re.Equal(regionA, ra)

	regionB := NewTestRegionInfo(2, 2, []byte("n"), []byte("x"))
	updateNewItem(tree, regionB)
	rb := tree.randomRegion([]KeyRange{NewKeyRange("g", "x")})
	re.Equal(regionB, rb)
	rb = tree.randomRegion([]KeyRange{NewKeyRange("g", "y")})
	re.Equal(regionB, rb)
	rb = tree.randomRegion([]KeyRange{NewKeyRange("n", "y")})
	re.Equal(regionB, rb)
	rb = tree.randomRegion([]KeyRange{NewKeyRange("o", "y")})
	re.Nil(rb)

	regionC := NewTestRegionInfo(3, 3, []byte("z"), []byte(""))
	updateNewItem(tree, regionC)
	rc := tree.randomRegion([]KeyRange{NewKeyRange("y", "")})
	re.Equal(regionC, rc)
	regionD := NewTestRegionInfo(4, 4, []byte(""), []byte("a"))
	updateNewItem(tree, regionD)
	rd := tree.randomRegion([]KeyRange{NewKeyRange("", "b")})
	re.Equal(regionD, rd)

	checkRandomRegion(re, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, nil)
	checkRandomRegion(re, tree, []*RegionInfo{regionA, regionB, regionC, regionD}, []KeyRange{NewKeyRange("", "")})
}

func updateNewItem(tree *regionTree, region *RegionInfo) {
	item := &regionItem{RegionInfo: region}
	tree.update(item, false)
}

func checkRandomRegion(re *require.Assertions, tree *regionTree, regions []*RegionInfo, ranges []KeyRange) {
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

func newRegionItem(start, end []byte) *regionItem {
	return &regionItem{RegionInfo: NewTestRegionInfo(1, 1, start, end)}
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
		j := rand.Intn(i + 1)
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
	for i := 0; i < b.N; i++ {
		item := &RegionInfo{meta: &metapb.Region{StartKey: []byte(fmt.Sprintf("%20d", i)), EndKey: []byte(fmt.Sprintf("%20d", i+1))}}
		updateNewItem(tree, item)
	}
}

func BenchmarkRegionTreeRandomInsert(b *testing.B) {
	data := mock1MRegionTree().clearTree().shuffleItems()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := i % MaxCount
		updateNewItem(data.tree, data.items[index])
	}
}

func BenchmarkRegionTreeRandomOverlapsInsert(b *testing.B) {
	tree := newRegionTree()
	var items []*RegionInfo
	for range MaxCount {
		var startKey, endKey int
		key1 := rand.Intn(MaxCount)
		key2 := rand.Intn(MaxCount)
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
	for i := 0; i < b.N; i++ {
		index := i % MaxCount
		updateNewItem(tree, items[index])
	}
}

func BenchmarkRegionTreeRandomUpdate(b *testing.B) {
	data := mock1MRegionTree().shuffleItems()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := i % MaxCount
		updateNewItem(data.tree, data.items[index])
	}
}

func BenchmarkRegionTreeSequentialLookUpRegion(b *testing.B) {
	data := mock1MRegionTree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := i % MaxCount
		data.tree.find(&regionItem{RegionInfo: data.items[index]})
	}
}

func BenchmarkRegionTreeRandomLookUpRegion(b *testing.B) {
	data := mock1MRegionTree().shuffleItems()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := i % MaxCount
		data.tree.find(&regionItem{RegionInfo: data.items[index]})
	}
}

func BenchmarkRegionTreeScan(b *testing.B) {
	data := mock1MRegionTree().shuffleItems()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data.tree.scanRanges()
	}
}
