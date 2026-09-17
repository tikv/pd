// Copyright 2021 TiKV Project Authors.
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"

	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/tests"
)

type tsoConsistencyTestSuite struct {
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
	conn      *grpc.ClientConn
	tsoClient tsopb.TSOClient
}

func TestLegacyTSOConsistencySuite(t *testing.T) {
	suite.Run(t, &tsoConsistencyTestSuite{
		legacy: true,
	})
}

func TestMicroserviceTSOConsistencySuite(t *testing.T) {
	suite.Run(t, &tsoConsistencyTestSuite{
		legacy: false,
	})
}

func (suite *tsoConsistencyTestSuite) SetupSuite() {
	re := suite.Require()

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	if suite.legacy {
		suite.cluster, err = tests.NewTestCluster(suite.ctx, serverCount)
	} else {
		suite.cluster, err = tests.NewTestClusterWithKeyspaceGroup(suite.ctx, serverCount)
	}
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	err = suite.pdLeaderServer.BootstrapCluster()
	re.NoError(err)
	backendEndpoints := suite.pdLeaderServer.GetAddr()
	if suite.legacy {
		suite.pdClient, suite.conn = testutil.MustNewGrpcClient(re, backendEndpoints)
	} else {
		suite.tsoServer, suite.tsoServerCleanup = tests.StartSingleTSOTestServer(suite.ctx, re, backendEndpoints, tempurl.Alloc())
		suite.tsoClientConn, suite.tsoClient = tso.MustNewGrpcClient(re, suite.tsoServer.GetAddr())
	}
}

func (suite *tsoConsistencyTestSuite) TearDownSuite() {
	suite.cancel()
	if !suite.legacy {
		suite.tsoClientConn.Close()
		suite.tsoServerCleanup()
	}
	if suite.conn != nil {
		suite.conn.Close()
	}
	suite.cluster.Destroy()
}

func (suite *tsoConsistencyTestSuite) request(ctx context.Context, count uint32) *pdpb.Timestamp {
	as := assert.New(suite.T())
	noError := func(err error) bool {
		if err == nil {
			return true
		}
		return as.Fail("Received unexpected error", "%+v", err)
	}
	clusterID := keypath.ClusterID()
	if suite.legacy {
		req := &pdpb.TsoRequest{
			Header: &pdpb.RequestHeader{ClusterId: clusterID},
			Count:  count,
		}
		tsoClient, err := suite.pdClient.Tso(ctx)
		if !noError(err) {
			return nil
		}
		defer func() {
			err := tsoClient.CloseSend()
			noError(err)
		}()
		if !noError(tsoClient.Send(req)) {
			return nil
		}
		resp, err := tsoClient.Recv()
		if !noError(err) {
			return nil
		}
		return checkAndReturnTimestampResponse(as, resp)
	}
	req := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{ClusterId: clusterID},
		Count:  count,
	}
	var resp *tsopb.TsoResponse
	if !as.Eventually(func() bool {
		tsoClient, err := suite.tsoClient.Tso(ctx)
		if err != nil {
			return false
		}
		if err := tsoClient.Send(req); err != nil {
			_ = tsoClient.CloseSend()
			return false
		}
		resp, err = tsoClient.Recv()
		closeErr := tsoClient.CloseSend()
		return err == nil && closeErr == nil && resp != nil
	}, 20*time.Second, 100*time.Millisecond) {
		return nil
	}
	return checkAndReturnTimestampResponse(as, resp)
}

func (suite *tsoConsistencyTestSuite) TestRequestTSOConcurrently() {
	re := suite.Require()
	lastTS := suite.requestTSOConcurrently(&pdpb.Timestamp{})
	// Test TSO after the leader change
	oldLeaderName := suite.pdLeaderServer.GetConfig().Name
	suite.pdLeaderServer.GetServer().GetMember().Resign()
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	leader := suite.cluster.GetServer(leaderName)
	suite.pdLeaderServer = leader
	if suite.legacy {
		// The PD leader is published before its embedded TSO allocator becomes
		// ready. Wait for that separate readiness condition before checking TSO
		// consistency. A direct gRPC client has no leader discovery, so reconnect
		// it only when another PD becomes the leader.
		testutil.Eventually(re, func() bool {
			return leader.GetServer().GetTSOAllocator().IsInitialize()
		})
		if leaderName != oldLeaderName {
			re.NoError(suite.conn.Close())
			suite.pdClient, suite.conn = testutil.MustNewGrpcClient(re, leader.GetAddr())
		}
	}
	suite.requestTSOConcurrently(lastTS)
}

func (suite *tsoConsistencyTestSuite) requestTSOConcurrently(lowerBound *pdpb.Timestamp) *pdpb.Timestamp {
	as := assert.New(suite.T())
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	var (
		wg    sync.WaitGroup
		maxTS atomic.Pointer[pdpb.Timestamp]
	)
	maxTS.Store(lowerBound)
	wg.Add(tsoRequestConcurrencyNumber)
	for range tsoRequestConcurrencyNumber {
		go func() {
			defer wg.Done()
			last := lowerBound
			var ts *pdpb.Timestamp
			for range tsoRequestRound {
				ts = suite.request(ctx, tsoCount)
				if !as.NotNil(ts) {
					return
				}
				// Check whether the TSO fallbacks
				if !as.Equal(1, tsoutil.CompareTimestamp(ts, last)) {
					return
				}
				last = ts
				for current := maxTS.Load(); tsoutil.CompareTimestamp(ts, current) > 0; current = maxTS.Load() {
					if maxTS.CompareAndSwap(current, ts) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	return maxTS.Load()
}

func (suite *tsoConsistencyTestSuite) TestFallbackTSOConsistency() {
	as := assert.New(suite.T())
	re := suite.Require()

	// Re-create the cluster to enable the failpoints.
	suite.TearDownSuite()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fallBackSync", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fallBackUpdate", `return(true)`))
	suite.SetupSuite()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fallBackSync"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fallBackUpdate"))
	defer func() {
		// Do not let the simulated future TSO affect the following test.
		suite.TearDownSuite()
		suite.SetupSuite()
	}()

	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for range tsoRequestConcurrencyNumber {
		go func() {
			defer wg.Done()
			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}
			var ts *pdpb.Timestamp
			for range tsoRequestRound {
				ts = suite.request(ctx, tsoCount)
				if !as.NotNil(ts) || !as.Equal(1, tsoutil.CompareTimestamp(ts, last)) {
					return
				}
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}
