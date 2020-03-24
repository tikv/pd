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
	_ uint64 = 1 << (10 * iota)
	_
	MB
	_
	TB
)

var _ = Suite(&testMaxScoreSuite{})

type testMaxScoreSuite struct{}

func (s *testMaxScoreSuite) TestMaxScore(c *C) {
	bc := NewBasicCluster()
	var flexibleScore uint64 = 4 * 1024 * 1024
	// add 3 tikv with capacity 2TB
	num := 3
	oldCapacity := 2 * TB
	oldMaxScore := float64(oldCapacity/MB + flexibleScore)
	for i := 1; i <= num; i++ {
		bc.PutStore(NewStoreInfo(
			&metapb.Store{Id: uint64(i)},
			SetStoreStats(&pdpb.StoreStats{Capacity: oldCapacity}),
		))
	}
	bc.UpdateStoresMaxScore(flexibleScore)
	checkMaxScore(c, bc, num, oldMaxScore)

	// add 1 tikv with capacity 2TB
	num = num + 1
	bc.UpdateStoresMaxScore(flexibleScore, NewStoreInfo(
		&metapb.Store{Id: uint64(num)},
		SetStoreStats(&pdpb.StoreStats{Capacity: oldCapacity}),
	))
	checkMaxScore(c, bc, num, oldMaxScore)

	// add 1 tikv with capacity 4TB
	num = num + 1
	newCapacity := 4 * TB
	newMaxScore := float64(newCapacity/MB + flexibleScore)
	bc.UpdateStoresMaxScore(flexibleScore, NewStoreInfo(
		&metapb.Store{Id: uint64(num)},
		SetStoreStats(&pdpb.StoreStats{Capacity: newCapacity}),
	))
	checkMaxScore(c, bc, num, newMaxScore)

	// add 1 tikv with capacity 1TB
	num = num + 1
	newCapacity = 1 * TB
	bc.UpdateStoresMaxScore(flexibleScore, NewStoreInfo(
		&metapb.Store{Id: uint64(num)},
		SetStoreStats(&pdpb.StoreStats{Capacity: newCapacity}),
	))
	checkMaxScore(c, bc, num, newMaxScore)
}

func checkMaxScore(c *C, bc *BasicCluster, num int, maxscore float64) {
	c.Assert(bc.GetStores(), HasLen, num)
	for i := 1; i <= num; i++ {
		c.Assert(bc.GetStore(uint64(i)).GetMaxScore(), Equals, maxscore)
	}
}
