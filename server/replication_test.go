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

func (s *testReplicationSuite) TestReplicaScore(c *C) {
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

				tc.addLabelsStore(storeID, 1, 0.1, labels)

				store := cluster.getStore(storeID)
				// We have (j*len(hosts)) replicas with the same zone,
				// k replicas with the same rack, and the base score
				// for the same rack is (len(stores)+1).
				score := j*len(hosts) + k + k*(len(stores)+1)
				c.Assert(rep.GetReplicaScore(stores, store), Equals, score)

				stores = append(stores, store)
			}
		}
	}

	baseScore := len(stores) + 1
	zoneReplicas := len(racks) * len(hosts) // replicas with the same zone
	rackReplicas := len(zones) * len(hosts) // replicas with the same rack
	hostReplicas := len(zones) * len(racks) // replicas with the same host
	storeID := uint64(len(zones) * len(racks) * len(hosts))

	// Missing rack and host, we assume it has the same rack and host with
	// other stores with the same zone.
	tc.addLabelsStore(storeID, 1, 0.1, map[string]string{"zone": "z3"})
	score := (1 + baseScore + baseScore*baseScore) * zoneReplicas
	c.Assert(rep.GetReplicaScore(stores, cluster.getStore(storeID)), Equals, score)

	// Missing rack and host, but the zone is different with other stores.
	tc.addLabelsStore(storeID, 1, 0.1, map[string]string{"zone": "z4"})
	c.Assert(rep.GetReplicaScore(stores, cluster.getStore(storeID)), Equals, 0)

	// Missing zone and host, we assume it has the same zone with other stores
	// and the same host with other stores with the same rack.
	tc.addLabelsStore(storeID, 1, 0.1, map[string]string{"rack": "r3"})
	score = 1*len(stores) + (baseScore+baseScore*baseScore)*rackReplicas
	c.Assert(rep.GetReplicaScore(stores, cluster.getStore(storeID)), Equals, score)

	// Missing zone and host, we assume it has the same zone with other stores,
	// but different rack with other stores.
	tc.addLabelsStore(storeID, 1, 0.1, map[string]string{"rack": "r4"})
	score = 1 * len(stores)
	c.Assert(rep.GetReplicaScore(stores, cluster.getStore(storeID)), Equals, score)

	// Missing zone and rack, we assume it has the same zone and rack with other
	// stores with the same host.
	tc.addLabelsStore(storeID, 1, 0.1, map[string]string{"host": "h3"})
	score = (1+baseScore)*len(stores) + (baseScore*baseScore)*hostReplicas
	c.Assert(rep.GetReplicaScore(stores, cluster.getStore(storeID)), Equals, score)

	// Missing zone and rack, we assume it has the same zone and rack with other
	// stores, but different host with other stores.
	tc.addLabelsStore(storeID, 1, 0.1, map[string]string{"host": "h4"})
	score = (1 + baseScore) * len(stores)
	c.Assert(rep.GetReplicaScore(stores, cluster.getStore(storeID)), Equals, score)
}

func (s *testReplicationSuite) TestCompareStoreScore(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	tc.addRegionStore(1, 1, 0.1)
	tc.addRegionStore(2, 1, 0.1)
	tc.addRegionStore(3, 1, 0.2)

	store1 := cluster.getStore(1)
	store2 := cluster.getStore(2)
	store3 := cluster.getStore(3)

	c.Assert(compareStoreScore(store1, 1, store2, 2), Equals, 1)
	c.Assert(compareStoreScore(store1, 1, store2, 1), Equals, 0)
	c.Assert(compareStoreScore(store1, 2, store2, 1), Equals, -1)

	c.Assert(compareStoreScore(store1, 1, store3, 2), Equals, 1)
	c.Assert(compareStoreScore(store1, 1, store3, 1), Equals, 1)
	c.Assert(compareStoreScore(store1, 2, store3, 1), Equals, -1)
}
