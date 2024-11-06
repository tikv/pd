// Copyright 2020 TiKV Project Authors.
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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/stretchr/testify/suite"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	tsopkg "github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/tempurl"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"google.golang.org/grpc"
)

type tsoServerTestSuite struct {
	suite.Suite
	legacy bool

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

	pdClient  pdpb.PDClient
	tsoClient tsopb.TSOClient
}

func TestLegacyTSOServer(t *testing.T) {
	suite.Run(t, &tsoServerTestSuite{
		legacy: true,
	})
}

func TestMicroserviceTSOServer(t *testing.T) {
	suite.Run(t, &tsoServerTestSuite{
		legacy: false,
	})
}

func (suite *tsoServerTestSuite) SetupSuite() {
	re := suite.Require()

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	if suite.legacy {
		suite.cluster, err = tests.NewTestCluster(suite.ctx, serverCount)
	} else {
		suite.cluster, err = tests.NewTestAPICluster(suite.ctx, serverCount)
	}
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	suite.pdLeaderServer.BootstrapCluster()
	backendEndpoints := suite.pdLeaderServer.GetAddr()
	if suite.legacy {
		suite.pdClient = tu.MustNewGrpcClient(re, backendEndpoints)
	} else {
		suite.tsoServer, suite.tsoServerCleanup = tests.StartSingleTSOTestServer(suite.ctx, re, backendEndpoints, tempurl.Alloc())
		suite.tsoClientConn, suite.tsoClient = tso.MustNewGrpcClient(re, suite.tsoServer.GetAddr())
	}
}

func (suite *tsoServerTestSuite) TearDownSuite() {
	suite.cancel()
	if !suite.legacy {
		suite.tsoClientConn.Close()
		suite.tsoServerCleanup()
	}
	suite.cluster.Destroy()
}

func (suite *tsoServerTestSuite) resetTS(ts uint64, ignoreSmaller, skipUpperBoundCheck bool) {
	var err error
	if suite.legacy {
		err = suite.pdLeaderServer.GetServer().GetHandler().ResetTS(ts, ignoreSmaller, skipUpperBoundCheck, 0)
	} else {
		err = suite.tsoServer.ResetTS(ts, ignoreSmaller, skipUpperBoundCheck, 0)
	}
	// Only this error is acceptable.
	if err != nil {
		suite.Require().ErrorContains(err, "is smaller than now")
	}
}

func (suite *tsoServerTestSuite) request(ctx context.Context, count uint32) (err error) {
	re := suite.Require()
	clusterID := keypath.ClusterID()
	if suite.legacy {
		req := &pdpb.TsoRequest{
			Header:     &pdpb.RequestHeader{ClusterId: clusterID},
			DcLocation: tsopkg.GlobalDCLocation,
			Count:      count,
		}
		tsoClient, err := suite.pdClient.Tso(ctx)
		re.NoError(err)
		defer tsoClient.CloseSend()
		re.NoError(tsoClient.Send(req))
		_, err = tsoClient.Recv()
		return err
	}
	req := &tsopb.TsoRequest{
		Header:     &tsopb.RequestHeader{ClusterId: clusterID},
		DcLocation: tsopkg.GlobalDCLocation,
		Count:      count,
	}
	tsoClient, err := suite.tsoClient.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()
	re.NoError(tsoClient.Send(req))
	_, err = tsoClient.Recv()
	return err
}

func (suite *tsoServerTestSuite) TestConcurrentlyReset() {
	var wg sync.WaitGroup
	wg.Add(2)
	now := time.Now()
	for range 2 {
		go func() {
			defer wg.Done()
			for j := 0; j <= 50; j++ {
				// Get a copy of now then call base.add, because now is shared by all goroutines
				// and now.add() will add to itself which isn't atomic and multi-goroutine safe.
				base := now
				physical := base.Add(time.Duration(j)*time.Minute).UnixNano() / int64(time.Millisecond)
				ts := uint64(physical) << 18
				suite.resetTS(ts, false, false)
			}
		}()
	}
	wg.Wait()
}

func (suite *tsoServerTestSuite) TestZeroTSOCount() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	re.ErrorContains(suite.request(ctx, 0), "tso count should be positive")
}
