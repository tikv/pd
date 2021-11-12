// Copyright 2017 TiKV Project Authors.
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
	"math"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testDistinctScoreSuite{})

type testDistinctScoreSuite struct{}

func (s *testDistinctScoreSuite) TestDistinctScore(c *C) {
	labels := []string{"zone", "rack", "host"}
	zones := []string{"z1", "z2", "z3"}
	racks := []string{"r1", "r2", "r3"}
	hosts := []string{"h1", "h2", "h3"}

	var stores []*StoreInfo
	for i, zone := range zones {
		for j, rack := range racks {
			for k, host := range hosts {
				storeID := uint64(i*len(racks)*len(hosts) + j*len(hosts) + k)
				storeLabels := map[string]string{
					"zone": zone,
					"rack": rack,
					"host": host,
				}
				store := NewStoreInfoWithLabel(storeID, 1, storeLabels)
				stores = append(stores, store)

				// Number of stores in different zones.
				numZones := i * len(racks) * len(hosts)
				// Number of stores in the same zone but in different racks.
				numRacks := j * len(hosts)
				// Number of stores in the same rack but in different hosts.
				numHosts := k
				score := (numZones*replicaBaseScore+numRacks)*replicaBaseScore + numHosts
				c.Assert(DistinctScore(labels, stores, store), Equals, float64(score))
			}
		}
	}
	store := NewStoreInfoWithLabel(100, 1, nil)
	c.Assert(DistinctScore(labels, stores, store), Equals, float64(0))
}

var _ = Suite(&testConcurrencySuite{})

type testConcurrencySuite struct{}

func (s *testConcurrencySuite) TestCloneStore(c *C) {
	meta := &metapb.Store{Id: 1, Address: "mock://tikv-1", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}}}
	store := NewStoreInfo(meta)
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			if time.Since(start) > time.Second {
				break
			}
			store.GetMeta().GetState()
		}
	}()
	go func() {
		defer wg.Done()
		for {
			if time.Since(start) > time.Second {
				break
			}
			store.Clone(
				UpStore(),
				SetLastHeartbeatTS(time.Now()),
			)
		}
	}()
	wg.Wait()
}

var _ = Suite(&testStoreSuite{})

type testStoreSuite struct{}

func (s *testStoreSuite) TestRegionScore(c *C) {
	stats := &pdpb.StoreStats{}
	stats.Capacity = 512 * (1 << 20)  // 512 MB
	stats.Available = 100 * (1 << 20) // 100 MB
	stats.UsedSize = 0

	store := NewStoreInfo(
		&metapb.Store{Id: 1},
		SetStoreStats(stats),
		SetRegionSize(1),
	)
	score := store.RegionScore("v1", 0.7, 0.9, 0)
	// Region score should never be NaN, or /store API would fail.
	c.Assert(math.IsNaN(score), Equals, false)
}

func (s *testStoreSuite) TestLowSpaceRatio(c *C) {
	store := NewStoreInfoWithLabel(1, 20, nil)
	store.rawStats.Capacity = initialMinSpace << 4
	store.rawStats.Available = store.rawStats.Capacity >> 3

	c.Assert(store.IsLowSpace(0.8), Equals, false)
	store.regionCount = 31
	c.Assert(store.IsLowSpace(0.8), Equals, true)
	store.rawStats.Available = store.rawStats.Capacity >> 2
	c.Assert(store.IsLowSpace(0.8), Equals, false)
}

func (s *testStoreSuite) TestLowSpaceScoreV2(c *C) {
	testdata := []struct {
		bigger *StoreInfo
		small  *StoreInfo
	}{{
		// store1 and store2 has same store available ratio and store1 less 50gb
		bigger: NewStoreInfoWithAvailable(1, 20*gb, 100*gb, 1.4),
		small:  NewStoreInfoWithAvailable(2, 200*gb, 1000*gb, 1.4),
	}, {
		// store1 and store2 has same available space and less than 50gb
		bigger: NewStoreInfoWithAvailable(1, 10*gb, 1000*gb, 1.4),
		small:  NewStoreInfoWithAvailable(2, 10*gb, 100*gb, 1.4),
	}, {
		// store1 and store2 has same available ratio less than 0.2
		bigger: NewStoreInfoWithAvailable(1, 10*gb, 1000*gb, 1.4),
		small:  NewStoreInfoWithAvailable(2, 1*gb, 100*gb, 1.4),
	}, {
		// store1 and store2 has same available ratio
		// but the store1 ratio less than store2 ((50-10)/50=0.8<(200-100)/200=0.5)
		bigger: NewStoreInfoWithAvailable(1, 10*gb, 100*gb, 1.4),
		small:  NewStoreInfoWithAvailable(2, 100*gb, 1000*gb, 1.4),
	}, {
		// store1 and store2 has same usedSize and capacity,
		// but the bigger's amp is bigger
		bigger: NewStoreInfoWithAvailable(1, 10*gb, 100*gb, 1.5),
		small:  NewStoreInfoWithAvailable(2, 10*gb, 100*gb, 1.4),
	}, {
		// store1 and store2 has same capacity and regionSize（40g),
		// but store1 has less available space size
		bigger: NewStoreInfoWithAvailable(1, 60*gb, 100*gb, 1),
		small:  NewStoreInfoWithAvailable(2, 80*gb, 100*gb, 2),
	}, {
		// store1 and store2 has same capacity and store2 (40g) has twice usedSize than store1 (20g)
		// but store1 has higher amp, so store1(60g) has more regionSize (40g)
		bigger: NewStoreInfoWithAvailable(1, 80*gb, 100*gb, 3),
		small:  NewStoreInfoWithAvailable(2, 60*gb, 100*gb, 1),
	}}
	for _, v := range testdata {
		score1 := v.bigger.regionScoreV2(0, 0.8)
		score2 := v.small.regionScoreV2(0, 0.8)
		c.Assert(score1, Greater, score2)
	}
}
