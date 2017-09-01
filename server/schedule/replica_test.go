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

package schedule

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

func TestCore(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testReplicationSuite{})

type testReplicationSuite struct{}

func (s *testReplicationSuite) TestDistinctScore(c *C) {
	labels := []string{"zone", "rack", "host"}
	zones := []string{"z1", "z2", "z3"}
	racks := []string{"r1", "r2", "r3"}
	hosts := []string{"h1", "h2", "h3"}

	var stores []*core.StoreInfo
	for i, zone := range zones {
		for j, rack := range racks {
			for k, host := range hosts {
				storeID := uint64(i*len(racks)*len(hosts) + j*len(hosts) + k)
				storeLabels := map[string]string{
					"zone": zone,
					"rack": rack,
					"host": host,
				}
				store := s.newStoreInfo(storeID, storeLabels)
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
	store := s.newStoreInfo(100, nil)
	c.Assert(DistinctScore(labels, stores, store), Equals, float64(0))
}

func (s *testReplicationSuite) newStoreInfo(id uint64, labels map[string]string) *core.StoreInfo {
	var storeLabels []*metapb.StoreLabel
	for k, v := range labels {
		storeLabels = append(storeLabels, &metapb.StoreLabel{
			Key:   k,
			Value: v,
		})
	}
	return core.NewStoreInfo(&metapb.Store{
		Id:     id,
		Labels: storeLabels,
	})
}
