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
