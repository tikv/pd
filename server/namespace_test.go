// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server/checker"
	"github.com/pingcap/pd/server/config"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
)

var _ = Suite(&testNamespaceSuite{})

type testNamespaceSuite struct {
	classifier     *mapClassifer
	tc             *testCluster
	opt            *config.ScheduleOption
	scheduleConfig *config.ScheduleConfig
}

func (s *testNamespaceSuite) SetUpTest(c *C) {
	var err error
	s.classifier = newMapClassifer()
	s.scheduleConfig, s.opt, err = newTestScheduleConfig()
	c.Assert(err, IsNil)
	s.tc = newTestCluster(s.opt)
}

func (s *testNamespaceSuite) TestReplica(c *C) {
	// store regionCount namespace
	//     1           0       ns1
	//     2          10       ns1
	//     3           0       ns2
	c.Assert(s.tc.addRegionStore(1, 0), IsNil)
	c.Assert(s.tc.addRegionStore(2, 10), IsNil)
	c.Assert(s.tc.addRegionStore(3, 0), IsNil)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")

	rc := checker.NewReplicaChecker(s.tc, s.classifier)

	// Replica should be added to the store with the same namespace.
	s.classifier.setRegion(1, "ns1")
	c.Assert(s.tc.addLeaderRegion(1, 1), IsNil)
	op := rc.Check(s.tc.GetRegion(1))
	testutil.CheckAddPeer(c, op, operator.OpReplica, 2)
	c.Assert(s.tc.addLeaderRegion(1, 3), IsNil)
	op = rc.Check(s.tc.GetRegion(1))
	testutil.CheckAddPeer(c, op, operator.OpReplica, 1)

	// Stop adding replica if no store in the same namespace.
	c.Assert(s.tc.addLeaderRegion(1, 1, 2), IsNil)
	op = rc.Check(s.tc.GetRegion(1))
	c.Assert(op, IsNil)
}

func (s *testNamespaceSuite) TestNamespaceChecker(c *C) {
	// store regionCount namespace
	//     1           0       ns1
	//     2          10       ns1
	//     3           0       ns2
	c.Assert(s.tc.addRegionStore(1, 0), IsNil)
	c.Assert(s.tc.addRegionStore(2, 10), IsNil)
	c.Assert(s.tc.addRegionStore(3, 0), IsNil)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")

	nc := checker.NewNamespaceChecker(s.tc, s.classifier)

	// Move the region if it was not in the right store.
	s.classifier.setRegion(1, "ns2")
	c.Assert(s.tc.addLeaderRegion(1, 1), IsNil)
	op := nc.Check(s.tc.GetRegion(1))
	testutil.CheckTransferPeer(c, op, operator.OpReplica, 1, 3)

	// Only move one region if the one was in the right store while the other was not.
	s.classifier.setRegion(2, "ns1")
	c.Assert(s.tc.addLeaderRegion(2, 1), IsNil)
	s.classifier.setRegion(3, "ns2")
	c.Assert(s.tc.addLeaderRegion(3, 2), IsNil)
	op = nc.Check(s.tc.GetRegion(2))
	c.Assert(op, IsNil)
	op = nc.Check(s.tc.GetRegion(3))
	testutil.CheckTransferPeer(c, op, operator.OpReplica, 2, 3)

	// Do NOT move the region if it was in the right store.
	s.classifier.setRegion(4, "ns2")
	c.Assert(s.tc.addLeaderRegion(4, 3), IsNil)
	op = nc.Check(s.tc.GetRegion(4))
	c.Assert(op, IsNil)

	// Move the peer with questions to the right store if the region has multiple peers.
	s.classifier.setRegion(5, "ns1")
	c.Assert(s.tc.addLeaderRegion(5, 1, 1, 3), IsNil)

	s.scheduleConfig.DisableNamespaceRelocation = true
	c.Assert(nc.Check(s.tc.GetRegion(5)), IsNil)
	s.scheduleConfig.DisableNamespaceRelocation = false

	op = nc.Check(s.tc.GetRegion(5))
	testutil.CheckTransferPeer(c, op, operator.OpReplica, 3, 2)
}

func (s *testNamespaceSuite) TestSchedulerBalanceRegion(c *C) {
	// store regionCount namespace
	//     1           0       ns1
	//     2         100       ns1
	//     3         200       ns2
	c.Assert(s.tc.addRegionStore(1, 0), IsNil)
	c.Assert(s.tc.addRegionStore(2, 100), IsNil)
	c.Assert(s.tc.addRegionStore(3, 200), IsNil)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")
	s.opt.SetMaxReplicas(1)

	oc := schedule.NewOperatorController(nil, nil)
	sched, _ := schedule.CreateScheduler("balance-region", oc)

	// Balance is limited within a namespace.
	c.Assert(s.tc.addLeaderRegion(1, 2), IsNil)
	s.classifier.setRegion(1, "ns1")
	op := scheduleByNamespace(s.tc, s.classifier, sched)
	testutil.CheckTransferPeer(c, op[0], operator.OpBalance, 2, 1)

	// If no more store in the namespace, balance stops.
	c.Assert(s.tc.addLeaderRegion(1, 3), IsNil)
	s.classifier.setRegion(1, "ns2")
	op = scheduleByNamespace(s.tc, s.classifier, sched)
	c.Assert(op, IsNil)

	// If region is not in the correct namespace, it will not be balanced. The
	// region should be in 'ns1', but its replica is located in 'ns2', neither
	// namespace will select it for balance.
	c.Assert(s.tc.addRegionStore(4, 0), IsNil)
	s.classifier.setStore(4, "ns2")
	c.Assert(s.tc.addLeaderRegion(1, 3), IsNil)
	s.classifier.setRegion(1, "ns1")
	op = scheduleByNamespace(s.tc, s.classifier, sched)
	c.Assert(op, IsNil)
}

func (s *testNamespaceSuite) TestSchedulerBalanceLeader(c *C) {
	// store regionCount namespace
	//     1         100       ns1
	//     2         200       ns1
	//     3           0       ns2
	//     4         300       ns2
	c.Assert(s.tc.addLeaderStore(1, 100), IsNil)
	c.Assert(s.tc.addLeaderStore(2, 200), IsNil)
	c.Assert(s.tc.addLeaderStore(3, 0), IsNil)
	c.Assert(s.tc.addLeaderStore(4, 300), IsNil)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")
	s.classifier.setStore(4, "ns2")

	oc := schedule.NewOperatorController(nil, nil)
	sched, _ := schedule.CreateScheduler("balance-leader", oc)

	// Balance is limited within a namespace.
	c.Assert(s.tc.addLeaderRegion(1, 2, 1), IsNil)
	s.classifier.setRegion(1, "ns1")
	op := scheduleByNamespace(s.tc, s.classifier, sched)
	testutil.CheckTransferLeader(c, op[0], operator.OpBalance, 2, 1)

	// If region is not in the correct namespace, it will not be balanced.
	c.Assert(s.tc.addLeaderRegion(1, 4, 1), IsNil)
	s.classifier.setRegion(1, "ns1")
	op = scheduleByNamespace(s.tc, s.classifier, sched)
	c.Assert(op, IsNil)
}

type mapClassifer struct {
	stores  map[uint64]string
	regions map[uint64]string
}

func newMapClassifer() *mapClassifer {
	return &mapClassifer{
		stores:  make(map[uint64]string),
		regions: make(map[uint64]string),
	}
}

func (c *mapClassifer) GetStoreNamespace(store *core.StoreInfo) string {
	if ns, ok := c.stores[store.GetID()]; ok {
		return ns
	}
	return namespace.DefaultNamespace
}

func (c *mapClassifer) GetRegionNamespace(region *core.RegionInfo) string {
	if ns, ok := c.regions[region.GetID()]; ok {
		return ns
	}
	return namespace.DefaultNamespace
}

func (c *mapClassifer) GetAllNamespaces() []string {
	all := make(map[string]struct{})
	for _, ns := range c.stores {
		all[ns] = struct{}{}
	}
	for _, ns := range c.regions {
		all[ns] = struct{}{}
	}

	nss := make([]string, 0, len(all))

	for ns := range all {
		nss = append(nss, ns)
	}
	return nss
}

func (c *mapClassifer) IsNamespaceExist(name string) bool {
	for _, ns := range c.stores {
		if ns == name {
			return true
		}
	}
	for _, ns := range c.regions {
		if ns == name {
			return true
		}
	}
	return false
}

func (c *mapClassifer) IsMetaExist() bool {
	return false
}

func (c *mapClassifer) IsTableIDExist(tableID int64) bool {
	return false
}

func (c *mapClassifer) IsStoreIDExist(storeID uint64) bool {
	return false
}

func (c *mapClassifer) AllowMerge(one *core.RegionInfo, other *core.RegionInfo) bool {
	return c.GetRegionNamespace(one) == c.GetRegionNamespace(other)
}

func (c *mapClassifer) ReloadNamespaces() error {
	return nil
}

func (c *mapClassifer) setStore(id uint64, namespace string) {
	c.stores[id] = namespace
}

func (c *mapClassifer) setRegion(id uint64, namespace string) {
	c.regions[id] = namespace
}
