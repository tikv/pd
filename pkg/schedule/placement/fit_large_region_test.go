// Copyright 2026 TiKV Project Authors.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

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
