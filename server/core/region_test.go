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
	"crypto/rand"
	"fmt"
	"math"
	mrand "math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/id"
)

func TestNeedMerge(t *testing.T) {
	re := require.New(t)
	mererSize, mergeKeys := int64(20), int64(200000)
	testdata := []struct {
		size   int64
		keys   int64
		expect bool
	}{{
		size:   20,
		keys:   200000,
		expect: true,
	}, {
		size:   20 - 1,
		keys:   200000 - 1,
		expect: true,
	}, {
		size:   20,
		keys:   200000 - 1,
		expect: true,
	}, {
		size:   20,
		keys:   200000 + 1,
		expect: false,
	}, {
		size:   20 + 1,
		keys:   200000 + 1,
		expect: false,
	}}
	for _, v := range testdata {
		r := RegionInfo{
			approximateSize: v.size,
			approximateKeys: v.keys,
		}
		re.Equal(v.expect, r.NeedMerge(mererSize, mergeKeys))
	}
}

func TestSortedEqual(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		idsA    []int
		idsB    []int
		isEqual bool
	}{
		{
			[]int{},
			[]int{},
			true,
		},
		{
			[]int{},
			[]int{1, 2},
			false,
		},
		{
			[]int{1, 2},
			[]int{1, 2},
			true,
		},
		{
			[]int{1, 2},
			[]int{2, 1},
			true,
		},
		{
			[]int{1, 2},
			[]int{1, 2, 3},
			false,
		},
		{
			[]int{1, 2, 3},
			[]int{2, 3, 1},
			true,
		},
		{
			[]int{1, 3},
			[]int{1, 2},
			false,
		},
	}
	meta := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{
				Id:      1,
				StoreId: 10,
			},
			{
				Id:      3,
				StoreId: 30,
			},
			{
				Id:      2,
				StoreId: 20,
				Role:    metapb.PeerRole_Learner,
			},
			{
				Id:      4,
				StoreId: 40,
				Role:    metapb.PeerRole_IncomingVoter,
			},
		},
	}
	pickPeers := func(ids []int) []*metapb.Peer {
		peers := make([]*metapb.Peer, 0, len(ids))
		for _, i := range ids {
			peers = append(peers, meta.Peers[i])
		}
		return peers
	}
	pickPeerStats := func(ids []int) []*pdpb.PeerStats {
		peers := make([]*pdpb.PeerStats, 0, len(ids))
		for _, i := range ids {
			peers = append(peers, &pdpb.PeerStats{Peer: meta.Peers[i]})
		}
		return peers
	}
	// test NewRegionInfo
	for _, testCase := range testCases {
		regionA := NewRegionInfo(&metapb.Region{Id: 100, Peers: pickPeers(testCase.idsA)}, nil)
		regionB := NewRegionInfo(&metapb.Region{Id: 100, Peers: pickPeers(testCase.idsB)}, nil)
		re.Equal(testCase.isEqual, SortedPeersEqual(regionA.GetVoters(), regionB.GetVoters()))
		re.Equal(testCase.isEqual, SortedPeersEqual(regionA.GetVoters(), regionB.GetVoters()))
	}

	// test RegionFromHeartbeat
	for _, testCase := range testCases {
		regionA := RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
			Region:       &metapb.Region{Id: 100, Peers: pickPeers(testCase.idsA)},
			DownPeers:    pickPeerStats(testCase.idsA),
			PendingPeers: pickPeers(testCase.idsA),
		})
		regionB := RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
			Region:       &metapb.Region{Id: 100, Peers: pickPeers(testCase.idsB)},
			DownPeers:    pickPeerStats(testCase.idsB),
			PendingPeers: pickPeers(testCase.idsB),
		})
		re.Equal(testCase.isEqual, SortedPeersEqual(regionA.GetVoters(), regionB.GetVoters()))
		re.Equal(testCase.isEqual, SortedPeersEqual(regionA.GetVoters(), regionB.GetVoters()))
		re.Equal(testCase.isEqual, SortedPeersEqual(regionA.GetPendingPeers(), regionB.GetPendingPeers()))
		re.Equal(testCase.isEqual, SortedPeersStatsEqual(regionA.GetDownPeers(), regionB.GetDownPeers()))
	}

	// test Clone
	region := NewRegionInfo(meta, meta.Peers[0])
	for _, testCase := range testCases {
		downPeersA := pickPeerStats(testCase.idsA)
		downPeersB := pickPeerStats(testCase.idsB)
		pendingPeersA := pickPeers(testCase.idsA)
		pendingPeersB := pickPeers(testCase.idsB)

		regionA := region.Clone(WithDownPeers(downPeersA), WithPendingPeers(pendingPeersA))
		regionB := region.Clone(WithDownPeers(downPeersB), WithPendingPeers(pendingPeersB))
		re.Equal(testCase.isEqual, SortedPeersStatsEqual(regionA.GetDownPeers(), regionB.GetDownPeers()))
		re.Equal(testCase.isEqual, SortedPeersEqual(regionA.GetPendingPeers(), regionB.GetPendingPeers()))
	}
}

func TestInherit(t *testing.T) {
	re := require.New(t)
	// size in MB
	// case for approximateSize
	testCases := []struct {
		originExists bool
		originSize   uint64
		size         uint64
		expect       uint64
	}{
		{false, 0, 0, 1},
		{false, 0, 2, 2},
		{true, 0, 2, 2},
		{true, 1, 2, 2},
		{true, 2, 0, 2},
	}
	for _, testCase := range testCases {
		var origin *RegionInfo
		if testCase.originExists {
			origin = NewRegionInfo(&metapb.Region{Id: 100}, nil)
			origin.approximateSize = int64(testCase.originSize)
		}
		r := NewRegionInfo(&metapb.Region{Id: 100}, nil)
		r.approximateSize = int64(testCase.size)
		r.Inherit(origin, false)
		re.Equal(int64(testCase.expect), r.approximateSize)
	}

	// bucket
	data := []struct {
		originBuckets *metapb.Buckets
		buckets       *metapb.Buckets
	}{
		{nil, nil},
		{nil, &metapb.Buckets{RegionId: 100, Version: 2}},
		{&metapb.Buckets{RegionId: 100, Version: 2}, &metapb.Buckets{RegionId: 100, Version: 3}},
		{&metapb.Buckets{RegionId: 100, Version: 2}, nil},
	}
	for _, d := range data {
		origin := NewRegionInfo(&metapb.Region{Id: 100}, nil, SetBuckets(d.originBuckets))
		r := NewRegionInfo(&metapb.Region{Id: 100}, nil)
		r.Inherit(origin, true)
		re.Equal(d.originBuckets, r.GetBuckets())
		// region will not inherit bucket keys.
		if origin.GetBuckets() != nil {
			newRegion := NewRegionInfo(&metapb.Region{Id: 100}, nil)
			newRegion.Inherit(origin, false)
			re.NotEqual(d.originBuckets, newRegion.GetBuckets())
		}
	}
}

func TestRegionRoundingFlow(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		flow   uint64
		digit  int
		expect uint64
	}{
		{10, 0, 10},
		{13, 1, 10},
		{11807, 3, 12000},
		{252623, 4, 250000},
		{258623, 4, 260000},
		{258623, 64, 0},
		{252623, math.MaxInt64, 0},
		{252623, math.MinInt64, 252623},
	}
	for _, testCase := range testCases {
		r := NewRegionInfo(&metapb.Region{Id: 100}, nil, WithFlowRoundByDigit(testCase.digit))
		r.readBytes = testCase.flow
		r.writtenBytes = testCase.flow
		re.Equal(testCase.expect, r.GetRoundBytesRead())
	}
}

func TestRegionWriteRate(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		bytes           uint64
		keys            uint64
		interval        uint64
		expectBytesRate float64
		expectKeysRate  float64
	}{
		{0, 0, 0, 0, 0},
		{10, 3, 0, 0, 0},
		{0, 0, 1, 0, 0},
		{10, 3, 1, 0, 0},
		{0, 0, 5, 0, 0},
		{10, 3, 5, 2, 0.6},
		{0, 0, 500, 0, 0},
		{10, 3, 500, 0, 0},
	}
	for _, testCase := range testCases {
		r := NewRegionInfo(&metapb.Region{Id: 100}, nil, SetWrittenBytes(testCase.bytes), SetWrittenKeys(testCase.keys), SetReportInterval(0, testCase.interval))
		bytesRate, keysRate := r.GetWriteRate()
		re.Equal(testCase.expectBytesRate, bytesRate)
		re.Equal(testCase.expectKeysRate, keysRate)
	}
}

func TestNeedSync(t *testing.T) {
	re := require.New(t)
	RegionGuide := GenerateRegionGuideFunc(false)
	meta := &metapb.Region{
		Id:          1000,
		StartKey:    []byte("a"),
		EndKey:      []byte("z"),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 100, Version: 100},
		Peers: []*metapb.Peer{
			{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 12, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 13, StoreId: 1, Role: metapb.PeerRole_Voter},
		},
	}
	region := NewRegionInfo(meta, meta.Peers[0])

	testCases := []struct {
		optionsA []RegionCreateOption
		optionsB []RegionCreateOption
		needSync bool
	}{
		{
			optionsB: []RegionCreateOption{WithLeader(nil)},
			needSync: true,
		},
		{
			optionsB: []RegionCreateOption{WithLeader(meta.Peers[1])},
			needSync: true,
		},
		{
			optionsB: []RegionCreateOption{WithPendingPeers(meta.Peers[1:2])},
			needSync: true,
		},
		{
			optionsB: []RegionCreateOption{WithDownPeers([]*pdpb.PeerStats{{Peer: meta.Peers[1], DownSeconds: 600}})},
			needSync: true,
		},
		{
			optionsA: []RegionCreateOption{SetWrittenBytes(200), WithFlowRoundByDigit(2)},
			optionsB: []RegionCreateOption{SetWrittenBytes(300), WithFlowRoundByDigit(2)},
			needSync: true,
		},
		{
			optionsA: []RegionCreateOption{SetWrittenBytes(250), WithFlowRoundByDigit(2)},
			optionsB: []RegionCreateOption{SetWrittenBytes(349), WithFlowRoundByDigit(2)},
			needSync: false,
		},
		{
			optionsA: []RegionCreateOption{SetWrittenBytes(200), WithFlowRoundByDigit(4)},
			optionsB: []RegionCreateOption{SetWrittenBytes(300), WithFlowRoundByDigit(4)},
			needSync: false,
		},
		{
			optionsA: []RegionCreateOption{SetWrittenBytes(100000), WithFlowRoundByDigit(4)},
			optionsB: []RegionCreateOption{SetWrittenBytes(200), WithFlowRoundByDigit(2)},
			needSync: true,
		},
		{
			optionsA: []RegionCreateOption{SetWrittenBytes(100000), WithFlowRoundByDigit(127)},
			optionsB: []RegionCreateOption{SetWrittenBytes(0), WithFlowRoundByDigit(2)},
			needSync: false,
		},
		{
			optionsA: []RegionCreateOption{SetWrittenBytes(0), WithFlowRoundByDigit(2)},
			optionsB: []RegionCreateOption{SetWrittenBytes(100000), WithFlowRoundByDigit(127)},
			needSync: true,
		},
	}

	for _, testCase := range testCases {
		regionA := region.Clone(testCase.optionsA...)
		regionB := region.Clone(testCase.optionsB...)
		_, _, _, needSync := RegionGuide(regionA, regionB)
		re.Equal(testCase.needSync, needSync)
	}
}

func TestRegionMap(t *testing.T) {
	re := require.New(t)
	rm := make(map[uint64]*regionItem)
	checkMap(re, rm)
	rm[1] = &regionItem{RegionInfo: regionInfo(1)}
	checkMap(re, rm, 1)

	rm[2] = &regionItem{RegionInfo: regionInfo(2)}
	rm[3] = &regionItem{RegionInfo: regionInfo(3)}
	checkMap(re, rm, 1, 2, 3)

	rm[3] = &regionItem{RegionInfo: regionInfo(3)}
	delete(rm, 4)
	checkMap(re, rm, 1, 2, 3)

	delete(rm, 3)
	delete(rm, 1)
	checkMap(re, rm, 2)

	rm[3] = &regionItem{RegionInfo: regionInfo(3)}
	checkMap(re, rm, 2, 3)
}

func regionInfo(id uint64) *RegionInfo {
	return &RegionInfo{
		meta: &metapb.Region{
			Id: id,
		},
		approximateSize: int64(id),
		approximateKeys: int64(id),
	}
}

func checkMap(re *require.Assertions, rm map[uint64]*regionItem, ids ...uint64) {
	// Check Get.
	for _, id := range ids {
		re.Equal(id, rm[id].GetID())
	}
	// Check Len.
	re.Equal(len(ids), len(rm))
	// Check id set.
	expect := make(map[uint64]struct{})
	for _, id := range ids {
		expect[id] = struct{}{}
	}
	set1 := make(map[uint64]struct{})
	for _, r := range rm {
		set1[r.GetID()] = struct{}{}
	}
	re.Equal(expect, set1)
}

func TestRegionKey(t *testing.T) {
	re := require.New(t)
	testCase := []struct {
		key    string
		expect string
	}{
		{`"t\x80\x00\x00\x00\x00\x00\x00\xff!_r\x80\x00\x00\x00\x00\xff\x02\u007fY\x00\x00\x00\x00\x00\xfa"`,
			`7480000000000000FF215F728000000000FF027F590000000000FA`},
		{"\"\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\xff\\x05\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\xf8\"",
			`80000000000000FF0500000000000000F8`},
	}
	for _, test := range testCase {
		got, err := strconv.Unquote(test.key)
		re.NoError(err)
		s := fmt.Sprintln(RegionToHexMeta(&metapb.Region{StartKey: []byte(got)}))
		re.Contains(s, test.expect)

		// start key changed
		origin := NewRegionInfo(&metapb.Region{EndKey: []byte(got)}, nil)
		region := NewRegionInfo(&metapb.Region{StartKey: []byte(got), EndKey: []byte(got)}, nil)
		s = DiffRegionKeyInfo(origin, region)
		re.Regexp(".*StartKey Changed.*", s)
		re.Contains(s, test.expect)

		// end key changed
		origin = NewRegionInfo(&metapb.Region{StartKey: []byte(got)}, nil)
		region = NewRegionInfo(&metapb.Region{StartKey: []byte(got), EndKey: []byte(got)}, nil)
		s = DiffRegionKeyInfo(origin, region)
		re.Regexp(".*EndKey Changed.*", s)
		re.Contains(s, test.expect)
	}
}

func TestSetRegion(t *testing.T) {
	re := require.New(t)
	regions := NewRegionsInfo()
	for i := 0; i < 100; i++ {
		peer1 := &metapb.Peer{StoreId: uint64(i%5 + 1), Id: uint64(i*5 + 1)}
		peer2 := &metapb.Peer{StoreId: uint64((i+1)%5 + 1), Id: uint64(i*5 + 2)}
		peer3 := &metapb.Peer{StoreId: uint64((i+2)%5 + 1), Id: uint64(i*5 + 3)}
		if i%3 == 0 {
			peer2.IsWitness = true
		}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer1, peer2, peer3},
			StartKey: []byte(fmt.Sprintf("%20d", i*10)),
			EndKey:   []byte(fmt.Sprintf("%20d", (i+1)*10)),
		}, peer1)
		origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(region)
		regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
	}

	peer1 := &metapb.Peer{StoreId: uint64(4), Id: uint64(101)}
	peer2 := &metapb.Peer{StoreId: uint64(5), Id: uint64(102), Role: metapb.PeerRole_Learner}
	peer3 := &metapb.Peer{StoreId: uint64(1), Id: uint64(103)}
	region := NewRegionInfo(&metapb.Region{
		Id:       uint64(21),
		Peers:    []*metapb.Peer{peer1, peer2, peer3},
		StartKey: []byte(fmt.Sprintf("%20d", 184)),
		EndKey:   []byte(fmt.Sprintf("%20d", 211)),
	}, peer1)
	region.pendingPeers = append(region.pendingPeers, peer3)
	origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(region)
	regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
	checkRegions(re, regions)
	re.Equal(97, regions.tree.length())
	re.Len(regions.GetRegions(), 97)

	origin, overlaps, rangeChanged = regions.SetRegionWithUpdate(region)
	regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
	peer1 = &metapb.Peer{StoreId: uint64(2), Id: uint64(101)}
	peer2 = &metapb.Peer{StoreId: uint64(3), Id: uint64(102), Role: metapb.PeerRole_Learner}
	peer3 = &metapb.Peer{StoreId: uint64(1), Id: uint64(103)}
	region = NewRegionInfo(&metapb.Region{
		Id:       uint64(21),
		Peers:    []*metapb.Peer{peer1, peer2, peer3},
		StartKey: []byte(fmt.Sprintf("%20d", 184)),
		EndKey:   []byte(fmt.Sprintf("%20d", 212)),
	}, peer1)
	region.pendingPeers = append(region.pendingPeers, peer3)
	origin, overlaps, rangeChanged = regions.SetRegionWithUpdate(region)
	regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
	checkRegions(re, regions)
	re.Equal(97, regions.tree.length())
	re.Len(regions.GetRegions(), 97)

	// Test remove overlaps.
	region = region.Clone(WithStartKey([]byte(fmt.Sprintf("%20d", 175))), WithNewRegionID(201))
	re.NotNil(regions.GetRegion(21))
	re.NotNil(regions.GetRegion(18))
	origin, overlaps, rangeChanged = regions.SetRegionWithUpdate(region)
	regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
	checkRegions(re, regions)
	re.Equal(96, regions.tree.length())
	re.Len(regions.GetRegions(), 96)
	re.NotNil(regions.GetRegion(201))
	re.Nil(regions.GetRegion(21))
	re.Nil(regions.GetRegion(18))

	// Test update keys and size of region.
	region = region.Clone(
		SetApproximateKeys(20),
		SetApproximateSize(30),
		SetWrittenBytes(40),
		SetWrittenKeys(10),
		SetReportInterval(0, 5))
	origin, overlaps, rangeChanged = regions.SetRegionWithUpdate(region)
	regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
	checkRegions(re, regions)
	re.Equal(96, regions.tree.length())
	re.Len(regions.GetRegions(), 96)
	re.NotNil(regions.GetRegion(201))
	re.Equal(int64(30), regions.tree.TotalSize())
	bytesRate, keysRate := regions.tree.TotalWriteRate()
	re.Equal(float64(8), bytesRate)
	re.Equal(float64(2), keysRate)
}

func TestShouldRemoveFromSubTree(t *testing.T) {
	re := require.New(t)
	peer1 := &metapb.Peer{StoreId: uint64(1), Id: uint64(1)}
	peer2 := &metapb.Peer{StoreId: uint64(2), Id: uint64(2)}
	peer3 := &metapb.Peer{StoreId: uint64(3), Id: uint64(3)}
	peer4 := &metapb.Peer{StoreId: uint64(3), Id: uint64(3)}
	region := NewRegionInfo(&metapb.Region{
		Id:       uint64(1),
		Peers:    []*metapb.Peer{peer1, peer2, peer4},
		StartKey: []byte(fmt.Sprintf("%20d", 10)),
		EndKey:   []byte(fmt.Sprintf("%20d", 20)),
	}, peer1)

	origin := NewRegionInfo(&metapb.Region{
		Id:       uint64(1),
		Peers:    []*metapb.Peer{peer1, peer2, peer3},
		StartKey: []byte(fmt.Sprintf("%20d", 10)),
		EndKey:   []byte(fmt.Sprintf("%20d", 20)),
	}, peer1)
	re.True(region.peersEqualTo(origin))

	region.leader = peer2
	re.False(region.peersEqualTo(origin))

	region.leader = peer1
	region.pendingPeers = append(region.pendingPeers, peer4)
	re.False(region.peersEqualTo(origin))

	region.pendingPeers = nil
	region.learners = append(region.learners, peer2)
	re.False(region.peersEqualTo(origin))

	origin.learners = append(origin.learners, peer2, peer3)
	region.learners = append(region.learners, peer4)
	re.True(region.peersEqualTo(origin))

	region.voters[2].StoreId = 4
	re.False(region.peersEqualTo(origin))
}

func checkRegions(re *require.Assertions, regions *RegionsInfo) {
	leaderMap := make(map[uint64]uint64)
	followerMap := make(map[uint64]uint64)
	learnerMap := make(map[uint64]uint64)
	witnessMap := make(map[uint64]uint64)
	pendingPeerMap := make(map[uint64]uint64)
	for _, item := range regions.GetRegions() {
		leaderMap[item.leader.StoreId]++
		for _, follower := range item.GetFollowers() {
			followerMap[follower.StoreId]++
		}
		for _, learner := range item.GetLearners() {
			learnerMap[learner.StoreId]++
		}
		for _, witness := range item.GetWitnesses() {
			witnessMap[witness.StoreId]++
		}
		for _, pendingPeer := range item.GetPendingPeers() {
			pendingPeerMap[pendingPeer.StoreId]++
		}
	}
	for key, value := range regions.leaders {
		re.Equal(int(leaderMap[key]), value.length())
	}
	for key, value := range regions.followers {
		re.Equal(int(followerMap[key]), value.length())
	}
	for key, value := range regions.learners {
		re.Equal(int(learnerMap[key]), value.length())
	}
	for key, value := range regions.witnesses {
		re.Equal(int(witnessMap[key]), value.length())
	}
	for key, value := range regions.pendingPeers {
		re.Equal(int(pendingPeerMap[key]), value.length())
	}
}

func BenchmarkUpdateBuckets(b *testing.B) {
	region := NewTestRegionInfo([]byte{}, []byte{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buckets := &metapb.Buckets{RegionId: 0, Version: uint64(i)}
		region.UpdateBuckets(buckets, region.GetBuckets())
	}
	if region.GetBuckets().GetVersion() != uint64(b.N-1) {
		b.Fatal("update buckets failed")
	}
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
		origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(region)
		regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		regions.RandLeaderRegion(1, nil)
	}
}

func BenchmarkRandomSetRegion(b *testing.B) {
	regions := NewRegionsInfo()
	var items []*RegionInfo
	for i := 0; i < 1000000; i++ {
		peer := &metapb.Peer{StoreId: 1, Id: uint64(i + 1)}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer},
			StartKey: []byte(fmt.Sprintf("%20d", i)),
			EndKey:   []byte(fmt.Sprintf("%20d", i+1)),
		}, peer)
		origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(region)
		regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
		items = append(items, region)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := items[i%len(items)]
		item.approximateKeys = int64(200000)
		item.approximateSize = int64(20)
		origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(item)
		regions.UpdateSubTree(item, origin, overlaps, rangeChanged)
	}
}

func TestGetRegionSizeByRange(t *testing.T) {
	regions := NewRegionsInfo()
	nums := 1000010
	for i := 0; i < nums; i++ {
		peer := &metapb.Peer{StoreId: 1, Id: uint64(i + 1)}
		endKey := []byte(fmt.Sprintf("%20d", i+1))
		if i == nums-1 {
			endKey = []byte("")
		}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer},
			StartKey: []byte(fmt.Sprintf("%20d", i)),
			EndKey:   endKey,
		}, peer, SetApproximateSize(10))
		origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(region)
		regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
	}
	totalSize := regions.GetRegionSizeByRange([]byte(""), []byte(""))
	require.Equal(t, int64(nums*10), totalSize)
	for i := 1; i < 10; i++ {
		verifyNum := nums / i
		endKey := fmt.Sprintf("%20d", verifyNum)
		totalSize := regions.GetRegionSizeByRange([]byte(""), []byte(endKey))
		require.Equal(t, int64(verifyNum*10), totalSize)
	}
}

func BenchmarkRandomSetRegionWithGetRegionSizeByRange(b *testing.B) {
	regions := NewRegionsInfo()
	var items []*RegionInfo
	for i := 0; i < 1000000; i++ {
		peer := &metapb.Peer{StoreId: 1, Id: uint64(i + 1)}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer},
			StartKey: []byte(fmt.Sprintf("%20d", i)),
			EndKey:   []byte(fmt.Sprintf("%20d", i+1)),
		}, peer, SetApproximateSize(10))
		origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(region)
		regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
		items = append(items, region)
	}
	b.ResetTimer()
	go func() {
		for {
			regions.GetRegionSizeByRange([]byte(""), []byte(""))
			time.Sleep(time.Millisecond)
		}
	}()
	for i := 0; i < b.N; i++ {
		item := items[i%len(items)]
		item.approximateKeys = int64(200000)
		origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(item)
		regions.UpdateSubTree(item, origin, overlaps, rangeChanged)
	}
}

func BenchmarkRandomSetRegionWithGetRegionSizeByRangeParallel(b *testing.B) {
	regions := NewRegionsInfo()
	var items []*RegionInfo
	for i := 0; i < 1000000; i++ {
		peer := &metapb.Peer{StoreId: 1, Id: uint64(i + 1)}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer},
			StartKey: []byte(fmt.Sprintf("%20d", i)),
			EndKey:   []byte(fmt.Sprintf("%20d", i+1)),
		}, peer)
		origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(region)
		regions.UpdateSubTree(region, origin, overlaps, rangeChanged)
		items = append(items, region)
	}
	b.ResetTimer()
	go func() {
		for {
			regions.GetRegionSizeByRange([]byte(""), []byte(""))
			time.Sleep(time.Millisecond)
		}
	}()

	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				item := items[mrand.Intn(len(items))]
				n := item.Clone(SetApproximateSize(20))
				origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(n)
				regions.UpdateSubTree(item, origin, overlaps, rangeChanged)
			}
		},
	)
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
		origin, overlaps, rangeChanged := regions.SetRegionWithUpdate(items[i])
		regions.UpdateSubTree(items[i], origin, overlaps, rangeChanged)
	}
}
