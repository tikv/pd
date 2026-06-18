// Copyright 2026 TiKV Project Authors.
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

package server_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestLoadKeyspaceByID(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	leader := cluster.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	service := &server.KeyspaceServer{
		GrpcServer: &server.GrpcServer{Server: leader.GetServer()},
	}

	created, err := leader.GetKeyspaceManager().CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name: "grpc_load_by_id",
	})
	re.NoError(err)

	_, err = service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{
		Header: testutil.NewRequestHeader(leader.GetClusterID() + 1),
		Id:     created.GetId(),
	})
	re.Error(err)

	resp, err := service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{
		Header: testutil.NewRequestHeader(leader.GetClusterID()),
		Id:     created.GetId() + 1000,
	})
	re.NoError(err)
	re.Equal(pdpb.ErrorType_ENTRY_NOT_FOUND, resp.GetHeader().GetError().GetType())
	re.Nil(resp.GetKeyspace())

	resp, err = service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{
		Header: testutil.NewRequestHeader(leader.GetClusterID()),
		Id:     created.GetId(),
	})
	re.NoError(err)
	re.Equal(pdpb.ErrorType_ENTRY_NOT_FOUND, resp.GetHeader().GetError().GetType())
	re.Nil(resp.GetKeyspace())

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/skipKeyspaceRegionCheck", "return"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/skipKeyspaceRegionCheck"))
	}()
	resp, err = service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{
		Header: testutil.NewRequestHeader(leader.GetClusterID()),
		Id:     created.GetId(),
	})
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	re.Equal(created, resp.GetKeyspace())
}
