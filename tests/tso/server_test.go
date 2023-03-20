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
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/mcs"
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
	switch suite.legacy {
	case true:
		suite.cluster, err = tests.NewTestCluster(suite.ctx, serverCount)
		re.NoError(err)
		err = suite.cluster.RunInitialServers()
		re.NoError(err)
		leaderName := suite.cluster.WaitLeader()
		suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
		suite.pdClient = testutil.MustNewPDGrpcClient(re, suite.pdLeaderServer.GetAddr())
	case false:
		suite.cluster, err = tests.NewTestAPICluster(suite.ctx, serverCount)
		re.NoError(err)
		err = suite.cluster.RunInitialServers()
		re.NoError(err)
		leaderName := suite.cluster.WaitLeader()
		suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
		backendEndpoints := suite.pdLeaderServer.GetAddr()
		suite.tsoServer, suite.tsoServerCleanup = mcs.StartSingleTSOTestServer(suite.ctx, re, backendEndpoints)
		suite.tsoClient = testutil.MustNewTSOGrpcClient(re, suite.tsoServer.GetAddr())
	}
}

func (suite *tsoServerTestSuite) TearDownSuite() {
	suite.cancel()
	if !suite.legacy {
		suite.tsoServerCleanup()
	}
	suite.cluster.Destroy()
}

func (suite *tsoServerTestSuite) getClusterID() uint64 {
	switch suite.legacy {
	case true:
		return suite.pdLeaderServer.GetServer().ClusterID()
	case false:
		return suite.tsoServer.ClusterID()
	}
	panic("unreachable")
}

func (suite *tsoServerTestSuite) resetTS(ts uint64, ignoreSmaller, skipUpperBoundCheck bool) {
	var err error
	switch suite.legacy {
	case true:
		err = suite.pdLeaderServer.GetServer().GetHandler().ResetTS(ts, ignoreSmaller, skipUpperBoundCheck)
	case false:
		err = suite.tsoServer.GetHandler().ResetTS(ts, ignoreSmaller, skipUpperBoundCheck)
	}
	// Only this error is acceptable.
	if err != nil {
		suite.Require().ErrorContains(err, "is smaller than now")
	}
}

func (suite *tsoServerTestSuite) request(ctx context.Context, count uint32) (err error) {
	re := suite.Require()
	clusterID := suite.getClusterID()
	switch suite.legacy {
	case true:
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
	case false:
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
	}
	return err
}

func (suite *tsoServerTestSuite) TestConcurrentlyReset() {
	var wg sync.WaitGroup
	wg.Add(2)
	now := time.Now()
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i <= 100; i++ {
				physical := now.Add(time.Duration(2*i)*time.Minute).UnixNano() / int64(time.Millisecond)
				ts := uint64(physical << 18)
				suite.resetTS(ts, false, false)
			}
		}()
	}
	wg.Wait()
}

func (suite *tsoServerTestSuite) TestZeroTSOCount() {
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	suite.Require().NoError(suite.request(ctx, 0))
}
