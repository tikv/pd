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

package keyspace

import (
	"context"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	keyspacepkg "github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	pdtests "github.com/tikv/pd/tests"
)

func (suite *keyspaceTestSuite) TestLoadKeyspaceByIDGRPC() {
	re := suite.Require()
	service := &server.KeyspaceServer{
		GrpcServer: &server.GrpcServer{Server: suite.server.GetServer()},
	}
	ctx := context.Background()

	created, err := suite.manager.CreateKeyspace(&keyspacepkg.CreateKeyspaceRequest{
		Name: "grpc_load_by_id",
	})
	re.NoError(err)

	_, err = service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{Header: testutil.NewRequestHeader(suite.server.GetClusterID() + 1), Keyspace: &keyspacepb.LoadKeyspaceByIDRequest_Id{Id: created.GetId()}})
	re.Error(err)

	resp, err := service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{Header: testutil.NewRequestHeader(suite.server.GetClusterID()), Keyspace: &keyspacepb.LoadKeyspaceByIDRequest_Id{Id: created.GetId() + 1000}})
	re.NoError(err)
	re.Equal(pdpb.ErrorType_ENTRY_NOT_FOUND, resp.GetHeader().GetError().GetType())
	re.Nil(resp.GetKeyspace())

	resp, err = service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{Header: testutil.NewRequestHeader(suite.server.GetClusterID()), Keyspace: &keyspacepb.LoadKeyspaceByIDRequest_Id{Id: created.GetId()}})
	re.NoError(err)
	re.Equal(pdpb.ErrorType_ENTRY_NOT_FOUND, resp.GetHeader().GetError().GetType())
	re.Nil(resp.GetKeyspace())

	regionBound := keyspacepkg.MakeRegionBound(created.GetId())
	regionID := uint64(created.GetId()) * 10
	pdtests.MustPutRegion(re, suite.cluster, regionID+1, 1, regionBound.RawLeftBound, regionBound.RawRightBound)
	pdtests.MustPutRegion(re, suite.cluster, regionID+2, 1, regionBound.RawRightBound, regionBound.TxnLeftBound)
	pdtests.MustPutRegion(re, suite.cluster, regionID+3, 1, regionBound.TxnLeftBound, regionBound.TxnRightBound)
	pdtests.MustPutRegion(re, suite.cluster, regionID+4, 1, regionBound.TxnRightBound, []byte{})
	resp, err = service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{Header: testutil.NewRequestHeader(suite.server.GetClusterID()), Keyspace: &keyspacepb.LoadKeyspaceByIDRequest_Id{Id: created.GetId()}})
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	re.Equal(created, resp.GetKeyspace())

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	skipRegionCheckKeyspace, err := suite.manager.CreateKeyspace(&keyspacepkg.CreateKeyspaceRequest{
		Name: "grpc_skip_region",
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	re.NoError(err)
	resp, err = service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{Header: testutil.NewRequestHeader(suite.server.GetClusterID()), Keyspace: &keyspacepb.LoadKeyspaceByIDRequest_Id{Id: skipRegionCheckKeyspace.GetId()}})
	re.NoError(err)
	re.Equal(pdpb.ErrorType_ENTRY_NOT_FOUND, resp.GetHeader().GetError().GetType())
	re.Nil(resp.GetKeyspace())

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/skipKeyspaceRegionCheck", "return"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/skipKeyspaceRegionCheck"))
	}()
	resp, err = service.LoadKeyspaceByID(ctx, &keyspacepb.LoadKeyspaceByIDRequest{Header: testutil.NewRequestHeader(suite.server.GetClusterID()), Keyspace: &keyspacepb.LoadKeyspaceByIDRequest_Id{Id: skipRegionCheckKeyspace.GetId()}})
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	re.Equal(skipRegionCheckKeyspace, resp.GetKeyspace())
}
