// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compatibility_test

import (
	"context"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
)

func TestStoreRegister(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

	putStoreRequest := &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
		Store: &metapb.Store{
			Id:      1,
			Address: "mock-1",
			Version: "2.0.1",
		},
	}

	svr := &server.GrpcServer{Server: leaderServer.GetServer()}
	_, err = svr.PutStore(context.Background(), putStoreRequest)
	re.NoError(err)
	// FIX ME: read v0.0.0 in sometime
	re.NotEmpty(cluster.WaitLeader())
	version := leaderServer.GetClusterVersion()
	// Restart all PDs.
	err = cluster.StopAll()
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())

	leaderServer = cluster.GetLeaderServer()
	re.NotNil(leaderServer)
	newVersion := leaderServer.GetClusterVersion()
	re.Equal(version, newVersion)

	// putNewStore with old version
	putStoreRequest = &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
		Store: &metapb.Store{
			Id:      4,
			Address: "mock-4",
			Version: "1.0.1",
		},
	}
	putStoreResponse, err := svr.PutStore(context.Background(), putStoreRequest)
	re.NoError(err)
	re.Error(errors.New(putStoreResponse.GetHeader().GetError().String()))
}

func TestRollingUpgrade(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leaderServer := cluster.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())

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

	svr := &server.GrpcServer{Server: leaderServer.GetServer()}
	for _, store := range stores {
		_, err = svr.PutStore(context.Background(), store)
		re.NoError(err)
	}
	re.Equal(semver.Version{Major: 2, Minor: 0, Patch: 1}, leaderServer.GetClusterVersion())
	// rolling update
	for i, store := range stores {
		if i == 0 {
			store.Store.State = metapb.StoreState_Tombstone
			store.Store.NodeState = metapb.NodeState_Removed
		}
		store.Store.Version = "2.1.0"
		resp, err := svr.PutStore(context.Background(), store)
		re.NoError(err)
		if i != len(stores)-1 {
			re.Equal(semver.Version{Major: 2, Minor: 0, Patch: 1}, leaderServer.GetClusterVersion())
			re.Nil(resp.GetHeader().GetError())
		}
	}
	re.Equal(semver.Version{Major: 2, Minor: 1}, leaderServer.GetClusterVersion())
}
