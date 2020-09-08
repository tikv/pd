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
				SetStoreState(metapb.StoreState_Up),
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
	score := store.RegionScore(0.7, 0.9, 0)
	// Region score should never be NaN, or /store API would fail.
	c.Assert(math.IsNaN(score), Equals, false)
}
