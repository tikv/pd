// Copyright 2016 PingCAP, Inc.
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

package server

import . "github.com/pingcap/check"

func newTestReplication(maxReplicas int, locationLabels ...string) *Replication {
	cfg := &ReplicationConfig{
		MaxReplicas:    uint64(maxReplicas),
		LocationLabels: locationLabels,
	}
	return newReplication(cfg)
}

var _ = Suite(&testReplicationSuite{})

type testReplicationSuite struct{}

func (s *testReplicationSuite) TestDistinctScore(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)
	rep := newTestReplication(3, "zone", "rack", "host")

	zones := []string{"z1", "z2", "z3"}
	racks := []string{"r1", "r2", "r3"}
	hosts := []string{"h1", "h2", "h3"}

	var stores []*storeInfo
	for i, zone := range zones {
		for j, rack := range racks {
			for k, host := range hosts {
				storeID := uint64(i*len(racks)*len(hosts) + j*len(hosts) + k)
				labels := map[string]string{
					"zone": zone,
					"rack": rack,
					"host": host,
				}
				tc.addLabelsStore(storeID, 1, labels)
				store := cluster.getStore(storeID)
				stores = append(stores, store)

				// Number of stores with different zones.
				nzones := i * len(racks) * len(hosts)
				// Number of stores with different racks.
				nracks := nzones + j*len(hosts)
				// Number of stores with different hosts.
				nhosts := nracks + k
				score := (nzones*replicaBaseScore+nracks)*replicaBaseScore + nhosts
				c.Assert(rep.GetDistinctScore(stores, store), Equals, float64(score))
			}
		}
	}

	tc.addLabelsStore(100, 1, map[string]string{})
	store := cluster.getStore(100)
	c.Assert(rep.GetDistinctScore(stores, store), Equals, float64(0))
}

func (s *testReplicationSuite) TestCompareStoreScore(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	tc.addRegionStore(1, 1)
	tc.addRegionStore(2, 1)
	tc.addRegionStore(3, 3)

	store1 := cluster.getStore(1)
	store2 := cluster.getStore(2)
	store3 := cluster.getStore(3)

	c.Assert(compareStoreScore(store1, 2, store2, 1), Equals, 1)
	c.Assert(compareStoreScore(store1, 1, store2, 1), Equals, 0)
	c.Assert(compareStoreScore(store1, 1, store2, 2), Equals, -1)

	c.Assert(compareStoreScore(store1, 2, store3, 1), Equals, 1)
	c.Assert(compareStoreScore(store1, 1, store3, 1), Equals, 1)
	c.Assert(compareStoreScore(store1, 1, store3, 2), Equals, -1)
}
