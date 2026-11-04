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
