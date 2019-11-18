// Copyright 2017 PingCAP, Inc.
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
				nzones := i * len(racks) * len(hosts)
				// Number of stores in the same zone but in different racks.
				nracks := j * len(hosts)
				// Number of stores in the same rack but in different hosts.
				nhosts := k
				score := (nzones*replicaBaseScore+nracks)*replicaBaseScore + nhosts
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

const (
	MB = uint64(1024 * 1024)
	GB = 1024 * MB
	TB = 1024 * GB
)

var _ = Suite(&testMaxScoreSuite{})

type testMaxScoreSuite struct{}

func putStore(stores *StoresInfo, flexibleScore, capacity uint64, storeIDs ...uint64) {
	storeID := uint64(len(stores.GetStores()) + 1)
	if len(storeIDs) != 0 {
		storeID = storeIDs[0]
	}
	var results []*StoreInfo
	if storeID == 0 {
		results = stores.GetStoresAfterUpdateMaxScore(flexibleScore)
	} else {
		store := NewStoreInfo(
			&metapb.Store{
				Id: storeID,
			},
			SetStoreStats(&pdpb.StoreStats{
				Capacity: capacity,
			}),
		)
		results = stores.GetStoresAfterUpdateMaxScore(flexibleScore, store)
	}
	for _, result := range results {
		stores.SetStore(result)
	}
}

func checkAllStoresMaxScore(c *C, stores *StoresInfo, maxScore float64) {
	for _, store := range stores.GetStores() {
		c.Assert(store.GetMaxScore(), Equals, maxScore)
	}
}

func (s *testMaxScoreSuite) TestMaxScore(c *C) {
	stores := NewStoresInfo()
	flexibleScore := 4 * TB / bytesPerMB
	results := stores.GetStoresAfterUpdateMaxScore(flexibleScore)
	c.Assert(results, HasLen, 0)

	// 6 tikv stores of 1t
	num := 6
	oldCapacity := 1 * TB
	oldMaxScore := float64(5 * TB / bytesPerMB)
	for i := 1; i <= 6; i++ {
		putStore(stores, flexibleScore, oldCapacity)
		c.Assert(stores.GetStores(), HasLen, i)
		c.Assert(stores.GetStore(uint64(i)).GetMaxScore(), Equals, oldMaxScore)
	}
	// add larger store
	// store id         7	8	9	10	11		15
	// add new store	2t	3t	4t	5t	6t	...	10t
	// max score 		5t	5t	5t	5t	10t	...	10t
	var newMaxScore float64
	step := oldCapacity
	for capacity := oldCapacity + step; capacity <= uint64(2*oldMaxScore*bytesPerMB); capacity += step {
		putStore(stores, flexibleScore, capacity)
		for _, store := range stores.GetStores() {
			if float64(capacity/bytesPerMB) <= oldMaxScore {
				c.Assert(store.maxScore, Equals, oldMaxScore)
			} else {
				newMaxScore = stores.GetStores()[len(stores.GetStores())-1].maxScore
				c.Assert(store.maxScore, Equals, newMaxScore)
				c.Assert(store.maxScore, Greater, oldMaxScore)
			}
		}
	}
	// add smaller store
	for i := 1; i <= 6; i++ {
		putStore(stores, flexibleScore, oldCapacity)
		c.Assert(stores.GetStore(uint64(num)).GetMaxScore(), Equals, newMaxScore)
	}

	// update flexible score
	flexibleScore = 0
	putStore(stores, flexibleScore, 0, 0)
	maxCapacity := getMaxCapacity(stores.GetStores())
	checkAllStoresMaxScore(c, stores, float64(maxCapacity/bytesPerMB+flexibleScore))

	// update flexible score and new store
	flexibleScore = 4 * TB / bytesPerMB
	capacity := 30 * TB
	putStore(stores, flexibleScore, capacity)
	checkAllStoresMaxScore(c, stores, float64(capacity/bytesPerMB+flexibleScore))

	// update store with larger capacity than current store but smaller than max score
	flexibleScore = 4 * TB / bytesPerMB
	capacity = 32 * TB
	putStore(stores, flexibleScore, capacity)
	checkAllStoresMaxScore(c, stores, float64(30*TB/bytesPerMB+flexibleScore))

	// update with smaller
	capacity = 2 * TB
	putStore(stores, flexibleScore, capacity)
	checkAllStoresMaxScore(c, stores, float64(30*TB/bytesPerMB+flexibleScore))

	// update with larger than max score
	capacity = 64 * TB
	putStore(stores, flexibleScore, capacity)
	checkAllStoresMaxScore(c, stores, float64(capacity/bytesPerMB+flexibleScore))
}
