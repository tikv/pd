// Copyright 2018 PingCAP, Inc.
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

package integration

import (
	"context"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

func (s *integrationTestSuite) bootstrapCluster(server *testServer, c *C) {
	bootstrapReq := &pdpb.BootstrapRequest{
		Header: &pdpb.RequestHeader{ClusterId: server.GetClusterID()},
		Store:  &metapb.Store{Id: 1, Address: "mock://1"},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{3, 1, false}}},
	}
	_, err := server.server.Bootstrap(context.Background(), bootstrapReq)
	c.Assert(err, IsNil)
}

func (s *integrationTestSuite) TestStoreRegister(c *C) {
	c.Parallel()
	cluster, err := newTestCluster(3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	s.bootstrapCluster(leaderServer, c)

	putStoreRequest := &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
		Store: &metapb.Store{
			Id:      1,
			Address: "mock-1",
			Version: "2.0.1",
		},
	}
	_, err = leaderServer.server.PutStore(context.Background(), putStoreRequest)
	c.Assert(err, IsNil)
	// FIX ME: read v0.0.0 in sometime
	cluster.WaitLeader()
	version := leaderServer.GetClusterVersion()
	// Restart all PDs.
	err = cluster.StopAll()
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer = cluster.GetServer(cluster.GetLeader())
	newVersion := leaderServer.GetClusterVersion()
	c.Assert(version, Equals, newVersion)

	// putNewStore with old version
	putStoreRequest = &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
		Store: &metapb.Store{
			Id:      4,
			Address: "mock-4",
			Version: "1.0.1",
		},
	}
	_, err = leaderServer.server.PutStore(context.Background(), putStoreRequest)
	c.Assert(err, NotNil)
}

func (s *integrationTestSuite) TestRollingUpgrade(c *C) {
	c.Parallel()
	cluster, err := newTestCluster(3)
	c.Assert(err, IsNil)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	s.bootstrapCluster(leaderServer, c)

	stores := []*pdpb.PutStoreRequest{
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      1,
				Address: "mock-1",
				Version: "2.0.1",
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      4,
				Address: "mock-4",
				Version: "2.0.1",
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      6,
				Address: "mock-6",
				Version: "2.0.1",
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      7,
				Address: "mock-7",
				Version: "2.0.1",
			},
		},
	}
	for _, store := range stores {
		_, err = leaderServer.server.PutStore(context.Background(), store)
		c.Assert(err, IsNil)
	}
	c.Assert(leaderServer.GetClusterVersion(), Equals, semver.Version{Major: 2, Minor: 0, Patch: 1})
	// rolling update
	for i, store := range stores {
		if i == 0 {
			store.Store.State = metapb.StoreState_Tombstone
		}
		store.Store.Version = "2.1.0"
		resp, err := leaderServer.server.PutStore(context.Background(), store)
		c.Assert(err, IsNil)
		if i != len(stores)-1 {
			c.Assert(leaderServer.GetClusterVersion(), Equals, semver.Version{Major: 2, Minor: 0, Patch: 1})
			c.Assert(resp.GetHeader().GetError(), IsNil)
		}
	}
	c.Assert(leaderServer.GetClusterVersion(), Equals, semver.Version{Major: 2, Minor: 1})
}
