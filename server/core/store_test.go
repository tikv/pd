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

func putStore(stores *StoresInfo, capacity uint64, storeIDs ...uint64) {
	storeID := uint64(len(stores.GetStores()) + 1)
	if len(storeIDs) != 0 {
		storeID = storeIDs[0]
	}
	var results []*StoreInfo
	if storeID == 0 {
		results = stores.UpdateMaxScore()
	} else {
		store := NewStoreInfo(
			&metapb.Store{
				Id: storeID,
			},
			SetStoreStats(&pdpb.StoreStats{
				Capacity: capacity,
			}),
		)
		results = stores.UpdateMaxScore(store)
	}
	for _, result := range results {
		stores.SetStore(result)
	}
}

func (s *testMaxScoreSuite) TestMaxScore(c *C) {
	stores := NewStoresInfo()
	results := stores.UpdateMaxScore()
	c.Assert(results, HasLen, 0)

	// 6 tikv stores of 1t
	num := 6
	oldCapacity := 1 * TB
	oldMaxScore := float64(1 * TB / bytesPerMB)
	for i := 1; i <= num; i++ {
		putStore(stores, oldCapacity)
		c.Assert(stores.GetStores(), HasLen, i)
		c.Assert(stores.GetStore(uint64(i)).GetMaxScore(), Equals, oldMaxScore)
	}
	// Expansion
	newCapacity := 2 * TB
	newMaxScore := float64(2 * TB / bytesPerMB)
	for i := 1; i <= num; i++ {
		putStore(stores, newCapacity, uint64(i))
		c.Assert(stores.GetStore(uint64(i)).GetMaxScore(), Equals, newMaxScore)
	}
	// Reduction
	newCapacity = 1 * TB
	for i := 1; i <= num; i++ {
		putStore(stores, newCapacity, uint64(i))
		if i != num {
			c.Assert(stores.GetStore(uint64(i)).GetMaxScore(), Equals, newMaxScore)
		}
	}
	newMaxScore = float64(1 * TB / bytesPerMB)
	for i := 1; i <= num; i++ {
		c.Assert(stores.GetStore(uint64(i)).GetMaxScore(), Equals, newMaxScore)
	}
	// add larger store
	newCapacity = 2 * TB
	newMaxScore = float64(2 * TB / bytesPerMB)
	putStore(stores, newCapacity)
	for _, store := range stores.GetStores() {
		c.Assert(store.GetMaxScore(), Equals, newMaxScore)
	}
	// add smaller store
	newCapacity = 1 * TB / 2
	putStore(stores, newCapacity)
	for _, store := range stores.GetStores() {
		c.Assert(store.GetMaxScore(), Equals, newMaxScore)
	}
}
