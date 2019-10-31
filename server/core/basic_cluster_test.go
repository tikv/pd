// Copyright 2019 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testBasicClusterSuite{})

type testBasicClusterSuite struct{}

func (s *testBasicClusterSuite) TestUpdateMaxScore(c *C) {
	bc := NewBasicCluster()
	c.Assert(bc.GetStoreCount(), Equals, 0)
	// 6 tikv store of 1t
	num := 6
	originCapacity := uint64(1 * 1024 * 1024)
	for i := 0; i < num; i++ {
		bc.PutStore(NewStoreInfo(&metapb.Store{
			Id: uint64(i),
		}, SetStoreStats(&pdpb.StoreStats{
			Capacity: originCapacity,
		})))
	}
	c.Assert(bc.GetStoreCount(), Equals, num)
	maxScore := bc.GetStores()[0].CalculateMaxScore()
	for _, store := range bc.GetStores() {
		c.Assert(store.maxScore, Equals, maxScore)
	}
	// add new store	2t	3t	...	11t	12t	...	22t
	// max score: 		11t	11t	11t	11t	22t	...	22t
	step := float64(originCapacity)
	for capacity := step; capacity < 2*maxScore; capacity += step {
		bc.PutStore(NewStoreInfo(&metapb.Store{
			Id: uint64(num),
		}, SetStoreStats(&pdpb.StoreStats{
			Capacity: uint64(capacity),
		})))
		num += 1
		c.Assert(bc.GetStoreCount(), Equals, num)
		if capacity < maxScore {
			for _, store := range bc.GetStores() {
				c.Assert(store.maxScore, Equals, maxScore)
			}
		} else {
			for _, store := range bc.GetStores() {
				c.Assert(store.maxScore == maxScore || store.maxScore == bc.GetStores()[len(bc.GetStores())-1].maxScore, IsTrue)
			}
		}
	}
}
