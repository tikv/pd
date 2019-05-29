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

package table_ns_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/tests"
	"github.com/pingcap/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&tableNSTestSuite{})

type tableNSTestSuite struct{}

func (s *tableNSTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *tableNSTestSuite) TestTableNS(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := pdctl.InitCommand()

	store := metapb.Store{
		Id:    1,
		State: metapb.StoreState_Up,
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store.Id, store.State, store.Labels)
	classifier := svr.GetClassifier()
	defer cluster.Destroy()

	// table_ns create <namespace>
	c.Assert(svr.IsNamespaceExist("ts1"), IsFalse)
	args := []string{"-u", pdAddr, "table_ns", "create", "ts1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(svr.IsNamespaceExist("ts1"), IsTrue)

	// table_ns add <name> <table_id>
	args = []string{"-u", pdAddr, "table_ns", "add", "ts1", "1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsTableIDExist(1), IsTrue)

	// table_ns remove <name> <table_id>
	args = []string{"-u", pdAddr, "table_ns", "remove", "ts1", "1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsTableIDExist(1), IsFalse)

	// table_ns set_meta <namespace>
	args = []string{"-u", pdAddr, "table_ns", "set_meta", "ts1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsMetaExist(), IsTrue)

	// table_ns rm_meta <namespace>
	args = []string{"-u", pdAddr, "table_ns", "rm_meta", "ts1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsMetaExist(), IsFalse)

	// table_ns set_store <store_id> <namespace>
	args = []string{"-u", pdAddr, "table_ns", "set_store", "1", "ts1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsStoreIDExist(1), IsTrue)

	// table_ns rm_store <store_id> <namespace>
	args = []string{"-u", pdAddr, "table_ns", "rm_store", "1", "ts1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	c.Assert(classifier.IsStoreIDExist(1), IsFalse)
}
