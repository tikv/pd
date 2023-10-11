// Copyright 2023 TiKV Project Authors.
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

package tso

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/client/testutil"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/storage/endpoint"
	tsopkg "github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
	"google.golang.org/grpc"
)

type tsoKeyspaceGroupManagerTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	// The PD cluster.
	cluster *tests.TestCluster
	// pdLeaderServer is the leader server of the PD cluster.
	pdLeaderServer *tests.TestServer
	// tsoServer is the TSO service provider.
	tsoServer        *tso.Server
	tsoServerCleanup func()
	tsoClientConn    *grpc.ClientConn

	tsoClient tsopb.TSOClient
}

func TestTSOKeyspaceGroupManager(t *testing.T) {
	suite.Run(t, &tsoKeyspaceGroupManagerTestSuite{})
}

func (suite *tsoKeyspaceGroupManagerTestSuite) SetupSuite() {
	re := suite.Require()

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	re.NoError(suite.pdLeaderServer.BootstrapCluster())
	backendEndpoints := suite.pdLeaderServer.GetAddr()
	suite.tsoServer, suite.tsoServerCleanup = mcs.StartSingleTSOTestServer(suite.ctx, re, backendEndpoints, tempurl.Alloc())
	suite.tsoClientConn, suite.tsoClient = tso.MustNewGrpcClient(re, suite.tsoServer.GetAddr())
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TearDownSuite() {
	suite.cancel()
	suite.tsoClientConn.Close()
	suite.tsoServerCleanup()
	suite.cluster.Destroy()
}

func (suite *tsoKeyspaceGroupManagerTestSuite) TestTSOKeyspaceGroupSplit() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	// Create the keyspace group 1 with keyspaces [111, 222, 333].
	handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:        1,
				UserKind:  endpoint.Standard.String(),
				Members:   []endpoint.KeyspaceGroupMember{{Address: suite.tsoServer.GetAddr()}},
				Keyspaces: []uint32{111, 222, 333},
			},
		},
	})
	kg1 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 1)
	re.Equal(uint32(1), kg1.ID)
	re.Equal([]uint32{111, 222, 333}, kg1.Keyspaces)
	re.False(kg1.IsSplitting())
	// Get a TSO from the keyspace group 1.
	var ts *pdpb.Timestamp
	testutil.Eventually(re, func() bool {
		resp, err := request(ctx, re, suite.tsoClient, 1, suite.pdLeaderServer.GetClusterID(), 222, 1)
		ts = resp.GetTimestamp()
		return err == nil && tsoutil.CompareTimestamp(ts, &pdpb.Timestamp{}) > 0
	})
	ts.Physical += time.Hour.Milliseconds()
	// Set the TSO of the keyspace group 1 to a large value.
	err := suite.tsoServer.GetHandler().ResetTS(tsoutil.GenerateTS(ts), false, true, 1)
	re.NoError(err)
	// Split the keyspace group 1 to 2.
	handlersutil.MustSplitKeyspaceGroup(re, suite.pdLeaderServer, 1, &handlers.SplitKeyspaceGroupByIDParams{
		NewID:     2,
		Keyspaces: []uint32{222, 333},
	})
	kg2 := handlersutil.MustLoadKeyspaceGroupByID(re, suite.pdLeaderServer, 2)
	re.Equal(uint32(2), kg2.ID)
	re.Equal([]uint32{222, 333}, kg2.Keyspaces)
	re.True(kg2.IsSplitTarget())
	// Check the split TSO from keyspace group 2.
	var splitTS *pdpb.Timestamp
	testutil.Eventually(re, func() bool {
		resp, err := request(ctx, re, suite.tsoClient, 1, suite.pdLeaderServer.GetClusterID(), 222, 2)
		splitTS = resp.GetTimestamp()
		return err == nil && tsoutil.CompareTimestamp(splitTS, &pdpb.Timestamp{}) > 0
	})
	re.Greater(tsoutil.CompareTimestamp(splitTS, ts), 0)
}

func request(
	ctx context.Context,
	re *require.Assertions,
	client tsopb.TSOClient, count uint32,
	clusterID uint64, keyspaceID, keyspaceGroupID uint32,
) (ts *tsopb.TsoResponse, err error) {
	req := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId:       clusterID,
			KeyspaceId:      keyspaceID,
			KeyspaceGroupId: keyspaceGroupID,
		},
		DcLocation: tsopkg.GlobalDCLocation,
		Count:      count,
	}
	tsoClient, err := client.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()
	re.NoError(tsoClient.Send(req))
	return tsoClient.Recv()
}
