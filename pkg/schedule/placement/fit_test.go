// Copyright 2019 TiKV Project Authors.
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

package placement

import (
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

func makeStores() StoreSet {
	stores := core.NewStoresInfo()
	now := time.Now()
	for zone := 1; zone <= 5; zone++ {
		for rack := 1; rack <= 5; rack++ {
			for host := 1; host <= 5; host++ {
				for x := 1; x <= 5; x++ {
					id := uint64(zone*1000 + rack*100 + host*10 + x)
					labels := map[string]string{
						"zone": fmt.Sprintf("zone%d", zone),
						"rack": fmt.Sprintf("rack%d", rack),
						"host": fmt.Sprintf("host%d", host),
						"id":   fmt.Sprintf("id%d", x),
					}
					if x == 5 {
						labels["engine"] = "tiflash"
					}
					if id == 1111 || id == 2111 || id == 3111 {
						labels["disk"] = "ssd"
					}
					stores.PutStore(core.NewStoreInfoWithLabel(id, labels).Clone(core.SetLastHeartbeatTS(now)))
				}
			}
		}
	}
	return stores
}

// example: "1111_leader,1234,2111_learner"
func makeRegion(def string) (*core.RegionInfo, error) {
	var regionMeta metapb.Region
	var leader *metapb.Peer
	for peerDef := range strings.SplitSeq(def, ",") {
		role, idStr := Follower, peerDef
		if strings.Contains(peerDef, "_") {
			splits := strings.Split(peerDef, "_")
			idStr, role = splits[0], PeerRoleType(splits[1])
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, err
		}
		peer := &metapb.Peer{Id: uint64(id), StoreId: uint64(id), Role: role.MetaPeerRole()}
		regionMeta.Peers = append(regionMeta.Peers, peer)
		if role == Leader {
			leader = peer
		}
	}
	return core.NewRegionInfo(&regionMeta, leader), nil
}

// example: "3/voter/zone=zone1+zone2,rack=rack2/zone,rack,host"
// count role constraints location_labels
func makeRule(def string) (*Rule, error) {
	var (
		rule Rule
		err  error
	)
	splits := strings.Split(def, "/")
	rule.Count, err = strconv.Atoi(splits[0])
	if err != nil {
		return nil, err
	}

	rule.Role = PeerRoleType(splits[1])
	// only support k=v type constraint
	for c := range strings.SplitSeq(splits[2], ",") {
		if c == "" {
			break
		}
		kv := strings.Split(c, "=")
		rule.LabelConstraints = append(rule.LabelConstraints, LabelConstraint{
			Key:    kv[0],
			Op:     "in",
			Values: strings.Split(kv[1], "+"),
		})
	}
	rule.LocationLabels = strings.Split(splits[3], ",")
	return &rule, nil
}

func checkPeerMatch(peers []*metapb.Peer, expect string) bool {
	if len(peers) == 0 && expect == "" {
		return true
	}

	m := make(map[string]struct{})
	for _, p := range peers {
		m[strconv.Itoa(int(p.Id))] = struct{}{}
	}
	expects := strings.Split(expect, ",")
	if len(expects) != len(m) {
		return false
	}
	for _, p := range expects {
		delete(m, p)
	}
	return len(m) == 0
}

func TestReplace(t *testing.T) {
	re := require.New(t)
	stores := makeStores()

	testCases := []struct {
		region     string
		rules      []string
		srcStoreID uint64
		dstStoreID uint64
		ok         bool
	}{
		{"1111,2111,3111", []string{"3/voter//zone"}, 1111, 4111, true},
		// replace failed when the target store doesn't match the rule.
		{"1111,2111,3111", []string{"3/voter/zone=zone1+zone2+zone3/zone"}, 1111, 4111, false},
		// replace failed when the isolation level decrease.
		{"1111,2111,3111", []string{"3/voter//zone"}, 1111, 2113, false},
		{"1111,2111,3111,1115_learner", []string{"3/voter//zone", "1/learner/engine=tiflash/host"}, 1115, 2115, true},
		// replace failed when the target store is not tiflash
		{"1111,2111,3111,1115_learner", []string{"3/voter//zone", "1/learner/engine=tiflash/host"}, 1115, 1112, false},
		{"1111_leader,2111,3111", []string{"1/leader/zone=zone1/zone", "2/voter//zone"}, 1111, 1112, true},
		// replace failed when the leader is not match the leader constraint.
		{"1111_leader,2111,3111", []string{"1/leader/zone=zone1/zone", "2/voter//zone"}, 1111, 2112, false},
		// transfer leader
		{"1111_leader,1121,1131", []string{"1/leader/host=host1+host2/host", "2/voter//host"}, 1111, 1121, true},
		// replace failed when the leader is not match the leader constraint.
		{"1111_leader,1121,1131", []string{"1/leader/host=host1+host2/host", "2/voter//host"}, 1111, 1131, false},

		// transfer leader success with different rule when the role is the same.
		{"1111_leader,1121,1131", []string{"1/voter/host=host1/host", "1/voter/host=host2/host", "1/voter/host=host3/host"}, 1111, 1121, true},
		//// transfer leader failed with different rule, but the role isn't same.
		{"1111_leader,1121,1131", []string{"1/leader/host=host1/host", "1/voter/host=host2/host", "1/voter/host=host3/host"}, 1111, 1121, false},
	}
	for _, tc := range testCases {
		region, err := makeRegion(tc.region)
		re.NoError(err)
		rules := make([]*Rule, 0, len(tc.rules))
		for _, r := range tc.rules {
			rule, err := makeRule(r)
			re.NoError(err)
			rules = append(rules, rule)
		}
		rf := fitRegion(stores.GetStores(), region, rules, false)
		re.True(rf.IsSatisfied())
		rf.regionStores = stores.GetStores()
		re.Equal(rf.Replace(tc.srcStoreID, stores.GetStore(tc.dstStoreID)), tc.ok)
	}
}

func TestFitRegion(t *testing.T) {
	re := require.New(t)
	stores := makeStores()

	testCases := []struct {
		region   string
		rules    []string
		fitPeers string
	}{
		// test count
		{"1111,1112,1113", []string{"1/voter//"}, "1111"},
		{"1111,1112,1113", []string{"2/voter//"}, "1111,1112"},
		{"1111,1112,1113", []string{"3/voter//"}, "1111,1112,1113"},
		{"1111,1112,1113", []string{"5/voter//"}, "1111,1112,1113"},
		// best location
		{"1111,1112,1113,2111,2222,3222,3333", []string{"3/voter//zone,rack,host"}, "1111,2111,3222"},
		{"1111,1121,1211,2111,2211", []string{"3/voter//zone,rack,host"}, "1111,1211,2111"},
		{"1111,1211,1311,1411,2111,2211,2311,3111", []string{"5/voter//zone,rack,host"}, "1111,1211,2111,2211,3111"},
		// test role match
		{"1111_learner,1112,1113", []string{"1/voter//"}, "1112"},
		{"1111_learner,1112,1113", []string{"2/voter//"}, "1112,1113"},
		{"1111_learner,1112,1113", []string{"3/voter//"}, "1111,1112,1113"},
		{"1111,1112_learner,1121_learner,1122_learner,1131_learner,1132,1141,1142", []string{"3/follower//zone,rack,host"}, "1111,1132,1141"},
		// test 2 rule
		{"1111,1112,1113,1114", []string{"3/voter//", "1/voter/id=id1/"}, "1112,1113,1114/1111"},
		{"1111,2211,3111,3112", []string{"3/voter//zone", "1/voter/rack=rack2/"}, "1111,2211,3111//3112"},
		{"1111,2211,3111,3112", []string{"1/voter/rack=rack2/", "3/voter//zone"}, "2211/1111,3111,3112"},
		{"1111_leader,2111,3111", []string{"3/voter//", "3/learner/disk=ssd/"}, "1111,2111,3111/"},
		{"1111_leader,2111,3111,4111", []string{"3/voter//", "3/learner/disk=ssd/"}, "1111,2111,4111/3111"},
		{"1111_leader,2111,3111,4111_learner", []string{"3/voter//", "3/learner/disk=ssd/"}, "1111,2111,3111//4111"},
		// Improving the first rule must discard the later rule's old assignment,
		// even when no peer remains eligible for that later rule.
		{"1111_learner,1112_leader", []string{"1/voter//", "1/voter/id=id2/"}, "1112//1111"},
	}

	for _, testCase := range testCases {
		region, err := makeRegion(testCase.region)
		re.NoError(err)
		rules := make([]*Rule, 0, len(testCase.rules))
		for _, r := range testCase.rules {
			rule, err := makeRule(r)
			re.NoError(err)
			rules = append(rules, rule)
		}
		rf := fitRegion(stores.GetStores(), region, rules, false)
		expects := strings.Split(testCase.fitPeers, "/")
		for i, f := range rf.RuleFits {
			re.True(checkPeerMatch(f.Peers, expects[i]))
		}
		if len(rf.RuleFits) < len(expects) {
			re.True(checkPeerMatch(rf.OrphanPeers, expects[len(rf.RuleFits)]))
		}
	}
}

func TestIsolationScore(t *testing.T) {
	as := assert.New(t)
	stores := makeStores()
	testCases := []struct {
		checker func(any, any, ...any) bool
		peers1  []uint64
		peers2  []uint64
	}{
		{as.Less, []uint64{1111, 1112}, []uint64{1111, 1121}},
		{as.Less, []uint64{1111, 1211}, []uint64{1111, 2111}},
		{as.Less, []uint64{1111, 1211, 1311, 2111, 3111}, []uint64{1111, 1211, 2111, 2211, 3111}},
		{as.Equal, []uint64{1111, 1211, 2111, 2211, 3111}, []uint64{1111, 2111, 2211, 3111, 3211}},
		{as.Greater, []uint64{1111, 1211, 2111, 2211, 3111}, []uint64{1111, 1121, 2111, 2211, 3111}},
	}

	makePeers := func(ids []uint64) []*fitPeer {
		var peers []*fitPeer
		for _, id := range ids {
			peers = append(peers, &fitPeer{
				Peer:  &metapb.Peer{StoreId: id},
				store: stores.GetStore(id),
			})
		}
		return peers
	}

	for _, testCase := range testCases {
		peers1, peers2 := makePeers(testCase.peers1), makePeers(testCase.peers2)
		score1 := isolationScore(peers1, []string{"zone", "rack", "host"})
		score2 := isolationScore(peers2, []string{"zone", "rack", "host"})
		testCase.checker(score1, score2)
	}
}

func TestFitRegionLargePeerSets(t *testing.T) {
	for _, peerCount := range []int{bits.UintSize - 1, bits.UintSize, bits.UintSize + 1} {
		t.Run(fmt.Sprintf("all-%d", peerCount), func(t *testing.T) {
			region := mockRegion(peerCount, 0)
			stores := newMockStoresSet(peerCount).GetStores()
			rule := &Rule{Role: Voter, Count: peerCount, LocationLabels: []string{"zone"}}
			fit := fitRegion(stores, region, []*Rule{rule}, false)
			require.True(t, fit.IsSatisfied())
			require.Equal(t, region.GetPeers(), fit.RuleFits[0].Peers)
			require.Empty(t, fit.OrphanPeers)
		})
	}

	t.Run("isolation-beyond-word-size", func(t *testing.T) {
		peerCount := bits.UintSize + 1
		region := mockRegion(peerCount, 0)
		stores := make([]*core.StoreInfo, peerCount)
		for i := range stores {
			zone := "zone1"
			if i >= peerCount-2 {
				zone = fmt.Sprintf("zone%d", i-peerCount+4)
			}
			stores[i] = core.NewStoreInfoWithLabel(uint64(i+1), map[string]string{"zone": zone})
		}
		rule := &Rule{Role: Voter, Count: 3, LocationLabels: []string{"zone"}}
		fit := fitRegion(stores, region, []*Rule{rule}, false)
		require.Len(t, fit.RuleFits, 1)
		require.True(t, fit.RuleFits[0].IsSatisfied())
		require.Equal(t, []*metapb.Peer{region.GetPeers()[0], region.GetPeers()[peerCount-2], region.GetPeers()[peerCount-1]}, fit.RuleFits[0].Peers)
		require.Equal(t, float64(3), fit.RuleFits[0].IsolationScore)
		require.Equal(t, region.GetPeers()[1:peerCount-2], fit.OrphanPeers)
		require.False(t, fit.IsSatisfied())
	})

	t.Run("empty-candidates", func(t *testing.T) {
		region := mockRegion(3, 0)
		stores := newMockStoresSet(3).GetStores()
		rule := &Rule{Role: Voter, Count: 3, LabelConstraints: []LabelConstraint{{Key: "zone", Op: In, Values: []string{"missing"}}}}
		fit := fitRegion(stores, region, []*Rule{rule}, false)
		require.Len(t, fit.RuleFits, 1)
		require.Empty(t, fit.RuleFits[0].Peers)
		require.Equal(t, region.GetPeers(), fit.OrphanPeers)
		require.False(t, fit.IsSatisfied())
	})
}

func TestFitRegionEquivalentPeers(t *testing.T) {
	for _, zones := range []int{1, 3} {
		t.Run(fmt.Sprintf("zones-%d", zones), func(t *testing.T) {
			region := mockRegion(36, 0)
			stores := make([]*core.StoreInfo, 36)
			for i := range stores {
				stores[i] = core.NewStoreInfoWithLabel(uint64(i+1), map[string]string{"zone": strconv.Itoa(i % zones)})
			}
			w := newFitWorker(stores, region, []*Rule{{Role: Voter, Count: 18, LocationLabels: []string{"zone"}}}, false)
			w.run()
			require.Equal(t, region.GetPeers()[:18], w.bestFit.RuleFits[0].Peers)
			require.Equal(t, region.GetPeers()[18:], w.bestFit.OrphanPeers)
			score := float64(0)
			if zones == 3 {
				score = 108 // Six peers per zone, with 6*6 pairs between each pair of zones.
			}
			require.Equal(t, score, w.bestFit.RuleFits[0].IsolationScore)
			for _, peer := range w.peers {
				require.False(t, peer.selected)
			}
		})
	}
}

func TestFitRegionEquivalentPeersAcrossRules(t *testing.T) {
	region := mockRegion(10, 0)
	stores := newMockStoresSet(10).GetStores()
	rules := make([]*Rule, 10)
	for i := range rules {
		rules[i] = &Rule{Role: Voter, Count: 1, LocationLabels: []string{"zone"}}
	}
	w := newFitWorker(stores, region, rules, false)
	w.run()
	require.True(t, w.bestFit.IsSatisfied())
	for i, rf := range w.bestFit.RuleFits {
		require.Equal(t, region.GetPeers()[i:i+1], rf.Peers)
	}
	for _, peer := range w.peers {
		require.False(t, peer.selected)
	}
}

func TestFitRegionUnusedRuleSuffix(t *testing.T) {
	for _, distinctLastPeer := range []bool{false, true} {
		for _, cumulative := range []bool{false, true} {
			t.Run(fmt.Sprintf("distinct-last-peer=%t/cumulative=%t", distinctLastPeer, cumulative), func(t *testing.T) {
				stores, region, rules := makeUnusedRuleSuffix(20, 20, distinctLastPeer)
				expected := [][]*metapb.Peer{region.GetPeers()[:1], region.GetPeers()[1:]}
				if cumulative {
					rules = append([]*Rule{
						rules[0],
						{Role: Voter, Count: 3},
						{Role: Voter, Count: 4},
					}, rules[2:]...)
					expected = [][]*metapb.Peer{region.GetPeers()[:1], region.GetPeers()[1:4], region.GetPeers()[4:]}
				}
				w := newFitWorker(stores, region, rules, false)
				w.run()
				require.Len(t, w.bestFit.RuleFits, len(rules))
				for i, peers := range expected {
					require.Equal(t, peers, w.bestFit.RuleFits[i].Peers)
					require.True(t, w.bestFit.RuleFits[i].IsSatisfied())
				}
				for _, rf := range w.bestFit.RuleFits[len(expected):] {
					require.Empty(t, rf.Peers)
				}
				require.Empty(t, w.bestFit.OrphanPeers)
				require.False(t, w.bestFit.IsSatisfied())
				for _, peer := range w.peers {
					require.False(t, peer.selected)
				}
			})
		}
	}
}

func TestEquivalentPeersRespectRemainingPeers(t *testing.T) {
	region := mockRegion(9, 0)
	stores := make([]*core.StoreInfo, 9)
	for i := range stores {
		pool, target := "x", "no"
		if i == 8 {
			pool = "y"
		}
		if i == 0 {
			target = "yes"
		}
		stores[i] = core.NewStoreInfoWithLabel(uint64(i+1), map[string]string{"zone": "z1", "pool": pool, "target": target})
	}
	for _, tc := range []struct {
		name      string
		second    *Rule
		secondIDs []uint64
		orphanIDs []uint64
	}{
		{
			name:      "peer-outside-current-candidates",
			second:    &Rule{Role: Voter, Count: 4},
			secondIDs: []uint64{6, 7, 8, 9},
		},
		{
			name: "remaining-candidates-do-not-match",
			second: &Rule{
				Role: Voter, Count: 5,
				LabelConstraints: []LabelConstraint{{Key: "pool", Op: In, Values: []string{"y"}}},
			},
			secondIDs: []uint64{9},
			orphanIDs: []uint64{6, 7, 8},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rules := []*Rule{
				{
					Role: Voter, Count: 4, LocationLabels: []string{"zone"},
					LabelConstraints: []LabelConstraint{{Key: "pool", Op: In, Values: []string{"x"}}},
				},
				tc.second,
				{Role: Voter, Count: 1, LabelConstraints: []LabelConstraint{{Key: "target", Op: In, Values: []string{"yes"}}}},
			}
			w := newFitWorker(stores, region, rules, false)
			w.run()
			require.Equal(t, region.GetPeers()[1:5], w.bestFit.RuleFits[0].Peers)
			require.Len(t, w.bestFit.RuleFits[1].Peers, len(tc.secondIDs))
			for i, id := range tc.secondIDs {
				require.Equal(t, region.GetPeer(id), w.bestFit.RuleFits[1].Peers[i])
			}
			require.Equal(t, region.GetPeers()[:1], w.bestFit.RuleFits[2].Peers)
			require.Len(t, w.bestFit.OrphanPeers, len(tc.orphanIDs))
			for i, id := range tc.orphanIDs {
				require.Equal(t, region.GetPeer(id), w.bestFit.OrphanPeers[i])
			}
			for _, peer := range w.peers {
				require.False(t, peer.selected)
			}
		})
	}
}

func TestEquivalentPeersConsumedAcrossMatchingRules(t *testing.T) {
	region := mockRegion(9, 0)
	stores := make([]*core.StoreInfo, 9)
	for i := range stores {
		pool, extra := "x", "same"
		if i == 7 {
			extra = "different"
		}
		if i == 8 {
			pool, extra = "y", "outside"
		}
		stores[i] = core.NewStoreInfoWithLabel(uint64(i+1), map[string]string{"zone": "z1", "pool": pool, "extra": extra})
	}
	constraints := []LabelConstraint{{Key: "pool", Op: In, Values: []string{"x"}}}
	rules := []*Rule{
		{Role: Voter, Count: 4, LocationLabels: []string{"zone"}, LabelConstraints: constraints},
		{Role: Voter, Count: 3, LabelConstraints: constraints},
		{Role: Voter, Count: 3, LabelConstraints: constraints},
		{Role: Voter, Count: 1, LabelConstraints: []LabelConstraint{{Key: "extra", Op: In, Values: []string{"same"}}}},
		{Role: Voter, Count: 1, LabelConstraints: []LabelConstraint{{Key: "pool", Op: In, Values: []string{"y"}}}},
	}
	// The two matching rules consume every x peer. The y peer remains available,
	// but cannot make the later extra-label distinction between x peers relevant.
	w := newFitWorker(stores, region, rules, false)
	w.run()
	require.Equal(t, region.GetPeers()[:4], w.bestFit.RuleFits[0].Peers)
	require.Equal(t, region.GetPeers()[4:7], w.bestFit.RuleFits[1].Peers)
	require.Equal(t, region.GetPeers()[7:8], w.bestFit.RuleFits[2].Peers)
	require.Empty(t, w.bestFit.RuleFits[3].Peers)
	require.Equal(t, region.GetPeers()[8:], w.bestFit.RuleFits[4].Peers)
	require.Empty(t, w.bestFit.OrphanPeers)
	require.False(t, w.bestFit.IsSatisfied())
	for _, peer := range w.peers {
		require.False(t, peer.selected)
	}
}

func TestEquivalentPeersRespectConsumingRule(t *testing.T) {
	t.Run("role", func(t *testing.T) {
		region := mockRegion(8, 0)
		stores := newMockStoresSet(8).GetStores()
		rules := []*Rule{
			{Role: Voter, Count: 7, LocationLabels: []string{"zone"}},
			{Role: Leader, Count: 1},
			{Role: Voter, Count: 1},
		}
		fit := fitRegion(stores, region, rules, false)
		require.Equal(t, region.GetPeers()[1:], fit.RuleFits[0].Peers)
		require.Equal(t, region.GetPeers()[:1], fit.RuleFits[1].Peers)
		require.Empty(t, fit.RuleFits[1].PeersWithDifferentRole)
		require.Empty(t, fit.RuleFits[2].Peers)
	})
	t.Run("isolation", func(t *testing.T) {
		region := mockRegion(8, 0)
		stores := make([]*core.StoreInfo, 8)
		for i := range stores {
			zone := "z1"
			if i == 0 {
				zone = "z2"
			}
			stores[i] = core.NewStoreInfoWithLabel(uint64(i+1), map[string]string{"zone": zone})
		}
		rules := []*Rule{
			{Role: Voter, Count: 1},
			{Role: Voter, Count: 7, LocationLabels: []string{"zone"}},
			{Role: Voter, Count: 1},
		}
		fit := fitRegion(stores, region, rules, false)
		require.Equal(t, region.GetPeers()[1:2], fit.RuleFits[0].Peers)
		expected := append([]*metapb.Peer{region.GetPeer(1)}, region.GetPeers()[2:]...)
		require.Equal(t, expected, fit.RuleFits[1].Peers)
		require.Equal(t, float64(6), fit.RuleFits[1].IsolationScore)
		require.Empty(t, fit.RuleFits[2].Peers)
	})
	t.Run("witness-eligibility-and-score", func(t *testing.T) {
		region := mockRegion(10, 0)
		stores := newMockStoresSet(10).GetStores()
		for i := range stores {
			if i != 1 && i != 2 {
				stores[i] = stores[i].Clone(core.SetWitnessCount(10))
			}
		}
		rules := []*Rule{
			{Role: Voter, Count: 8, LocationLabels: []string{"zone"}},
			{Role: Voter, Count: 2, IsWitness: true},
			{Role: Voter, Count: 1},
		}
		fit := fitRegion(stores, region, rules, true)
		expected := append([]*metapb.Peer{region.GetPeer(1)}, region.GetPeers()[3:]...)
		require.Equal(t, expected, fit.RuleFits[0].Peers)
		require.Equal(t, region.GetPeers()[1:3], fit.RuleFits[1].Peers)
		require.Zero(t, fit.RuleFits[1].WitnessScore)
		require.Empty(t, fit.RuleFits[2].Peers)
	})
}

func TestEquivalentPeersRespectLaterRules(t *testing.T) {
	region := mockRegion(8, 0)
	stores := make([]*core.StoreInfo, 8)
	for i := range stores {
		stores[i] = core.NewStoreInfoWithLabel(uint64(i+1), map[string]string{"zone": "z1", "disk": strconv.Itoa(i % 2)})
	}
	// The first rule needs no label distinction, but must leave peers for the
	// later rule. Peers in different disk labels cannot be considered equivalent.
	rules := []*Rule{
		{Role: Voter, Count: 4, LocationLabels: []string{"zone"}},
		{Role: Voter, Count: 4, LabelConstraints: []LabelConstraint{{Key: "disk", Op: In, Values: []string{"1"}}}},
	}
	w := newFitWorker(stores, region, rules, false)
	w.run()
	require.True(t, w.bestFit.IsSatisfied())
	require.Equal(t, []*metapb.Peer{region.GetPeer(1), region.GetPeer(3), region.GetPeer(5), region.GetPeer(7)}, w.bestFit.RuleFits[0].Peers)
	require.Equal(t, []*metapb.Peer{region.GetPeer(2), region.GetPeer(4), region.GetPeer(6), region.GetPeer(8)}, w.bestFit.RuleFits[1].Peers)
	for _, peer := range w.peers {
		require.False(t, peer.selected)
	}
}

func TestEquivalentPeersRespectLaterLeader(t *testing.T) {
	region := mockRegion(8, 0)
	stores := newMockStoresSet(8).GetStores()
	rules := []*Rule{
		{Role: Voter, Count: 7, LocationLabels: []string{"zone"}},
		{Role: Leader, Count: 1},
	}
	fit := fitRegion(stores, region, rules, false)
	require.True(t, fit.IsSatisfied())
	require.Equal(t, region.GetPeers()[1:], fit.RuleFits[0].Peers)
	require.Equal(t, region.GetPeers()[:1], fit.RuleFits[1].Peers)
}

func TestEquivalentPeersPreferHealthyPeers(t *testing.T) {
	region := mockRegion(8, 0)
	region = region.Clone(core.WithPendingPeers(region.GetPeers()[:1]))
	stores := newMockStoresSet(8).GetStores()
	fit := fitRegion(stores, region, []*Rule{{Role: Voter, Count: 4, LocationLabels: []string{"zone"}}}, false)
	require.Equal(t, region.GetPeers()[1:5], fit.RuleFits[0].Peers)
	require.Equal(t, []*metapb.Peer{region.GetPeer(6), region.GetPeer(7), region.GetPeer(8), region.GetPeer(1)}, fit.OrphanPeers)
}

func TestEquivalentPeersWitnessScore(t *testing.T) {
	peers := make([]*metapb.Peer, 10)
	stores := newMockStoresSet(10).GetStores()
	for i := range peers {
		peers[i] = &metapb.Peer{Id: uint64(i + 1), StoreId: uint64(i + 1), IsWitness: i > 0}
		stores[i] = stores[i].Clone(core.SetWitnessCount(i % 2))
	}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
	rules := []*Rule{{Role: Voter, Count: 2, IsWitness: true, LocationLabels: []string{"zone"}}}
	w := newFitWorker(stores, region, rules, true)
	w.run()
	require.Equal(t, []*metapb.Peer{peers[2], peers[4]}, w.bestFit.RuleFits[0].Peers)
	require.Zero(t, w.bestFit.RuleFits[0].WitnessScore)
	for _, peer := range w.peers {
		require.False(t, peer.selected)
	}
}

func TestRuleFitConfiguredIsolation(t *testing.T) {
	tests := []struct {
		name, level string
		paths       [][]string
		want        bool
	}{
		{"unset", "", [][]string{{"A", "a"}, {"A", "a"}}, true},
		{"host-separated", "host", [][]string{{"A", "a"}, {"A", "b"}}, true},
		{"zone-not-separated", "zone", [][]string{{"A", "a"}, {"A", "b"}}, false},
		{"host-collision", "host", [][]string{{"B", "d"}, {"B", "d"}}, false},
		{"case-insensitive", "host", [][]string{{"B", "d"}, {"b", "D"}}, false},
		{"host-local-to-zone", "host", [][]string{{"A", "a"}, {"B", "a"}}, true},
		{"missing-host", "host", [][]string{{"A", ""}, {"A", "b"}}, false},
		{"separated-above-missing-host", "host", [][]string{{"A", ""}, {"B", ""}}, true},
		{"single-peer", "host", [][]string{{"", ""}}, true},
		{"level-not-in-labels", "rack", [][]string{{"B", "d"}, {"B", "d"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := &RuleFit{Rule: &Rule{LocationLabels: []string{"zone", "host"}, IsolationLevel: tt.level}}
			for i, path := range tt.paths {
				rf.Stores = append(rf.Stores, core.NewStoreInfoWithLabel(uint64(i+1), map[string]string{"zone": path[0], "host": path[1]}))
			}
			require.Equal(t, tt.want, rf.IsIsolationSatisfied())
		})
	}
}

func TestConfiguredIsolationNonRegression(t *testing.T) {
	makeFit := func(hosts ...string) *RuleFit {
		fit := &RuleFit{Rule: &Rule{LocationLabels: []string{"host"}, IsolationLevel: "host"}}
		for i, host := range hosts {
			fit.Stores = append(fit.Stores, core.NewStoreInfoWithLabel(uint64(i+1), map[string]string{"host": host}))
		}
		return fit
	}
	before := makeFit("a", "a", "a", "b", "b", "b")
	// Equal pair count does not permit a larger concentration on one host.
	require.False(t, before.IsIsolationPreserved(makeFit("a", "a", "a", "a", "b", "c")))
	require.True(t, before.IsIsolationPreserved(makeFit("a", "a", "a", "b", "b", "c")))
	require.True(t, before.IsIsolationPreserved(makeFit("a", "a", "a", "b", "b", "b")))
	require.False(t, makeFit("a", "a", "b", "c", "d").IsIsolationPreserved(makeFit("a", "a", "b", "d", "d")))
	require.False(t, before.IsIsolationPreserved(makeFit("a", "b")), "replica-count changes need their repair contract")
	require.True(t, makeFit("a", "", "b").IsIsolationPreserved(makeFit("a", "c", "b")))
	require.False(t, makeFit("a", "c", "b").IsIsolationPreserved(makeFit("a", "", "b")))
	require.False(t, makeFit("a", "b").IsIsolationPreserved(makeFit("A", "a")))
	invalid := makeFit("a")
	invalid.Stores[0] = nil
	require.False(t, invalid.IsIsolationPreserved(makeFit("b")))
}

func TestIsolationLevelNotInLabelsIsUnset(t *testing.T) {
	// Rule validation accepts this combination. Peer moves keep only the
	// weighted score check instead of rejecting every target.
	rule := &Rule{Role: Voter, Count: 2, LocationLabels: []string{"host"}, IsolationLevel: "zone"}
	rf := &RuleFit{Rule: rule, IsolationScore: 1}
	for i, host := range []string{"a", "b"} {
		id := uint64(i + 1)
		rf.Peers = append(rf.Peers, &metapb.Peer{Id: id, StoreId: id})
		rf.Stores = append(rf.Stores, core.NewStoreInfoWithLabel(id, map[string]string{"host": host}))
	}
	fit := &RegionFit{RuleFits: []*RuleFit{rf}}
	separated := core.NewStoreInfoWithLabel(3, map[string]string{"host": "c"})
	require.True(t, fit.Replace(1, separated))
	require.True(t, fit.NewReplacementChecker(1)(separated))
	colocated := core.NewStoreInfoWithLabel(4, map[string]string{"host": "b"})
	require.False(t, fit.Replace(1, colocated), "the weighted score check still applies")
	require.True(t, rf.IsIsolationPreserved(rf))
}
