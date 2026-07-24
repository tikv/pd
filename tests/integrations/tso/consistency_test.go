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
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestRunInitialServersClosesStartingServersBeforeRetry(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, serverCount)
	re.NoError(err)
	t.Cleanup(func() {
		cancel()
		cluster.Destroy()
	})

	oldServer := cluster.GetServer("pd1")
	oldClientURL, err := url.Parse(oldServer.GetConfig().ClientUrls)
	re.NoError(err)
	oldPeerURL, err := url.Parse(oldServer.GetConfig().PeerUrls)
	re.NoError(err)
	conflictURL, err := url.Parse(cluster.GetServer("pd3").GetConfig().ClientUrls)
	re.NoError(err)
	listener, err := net.Listen("tcp", conflictURL.Host)
	re.NoError(err)
	t.Cleanup(func() { re.NoError(listener.Close()) })

	re.NoError(cluster.RunInitialServers())
	re.Equal(tests.Destroy, oldServer.State())
	for _, addr := range []string{oldClientURL.Host, oldPeerURL.Host} {
		testutil.Eventually(re, func() bool {
			conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
			if conn != nil {
				re.NoError(conn.Close())
			}
			return err != nil
		}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(100*time.Millisecond))
	}
}

func TestRunFailureAfterEtcdStartClosesServer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	t.Cleanup(func() {
		cancel()
		cluster.Destroy()
	})

	const failpointName = "github.com/tikv/pd/server/failAfterStartEtcd"
	re.NoError(failpoint.Enable(failpointName, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(failpointName)) })

	testServer := cluster.GetServer("pd1")
	clientURL, err := url.Parse(testServer.GetConfig().ClientUrls)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.ErrorContains(err, "injected error after etcd startup")
	re.NoError(testServer.Destroy())

	conn, err := net.DialTimeout("tcp", clientURL.Host, time.Second)
	re.Error(err)
	if conn != nil {
		re.NoError(conn.Close())
	}
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
	re := suite.Require()
	clusterID := keypath.ClusterID()
	if suite.legacy {
		req := &pdpb.TsoRequest{
			Header: &pdpb.RequestHeader{ClusterId: clusterID},
			Count:  count,
		}
		tsoClient, err := suite.pdClient.Tso(ctx)
		re.NoError(err)
		defer func() {
			err := tsoClient.CloseSend()
			re.NoError(err)
		}()
		re.NoError(tsoClient.Send(req))
		resp, err := tsoClient.Recv()
		re.NoError(err)
		return checkAndReturnTimestampResponse(re, resp)
	}
	req := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{ClusterId: clusterID},
		Count:  count,
	}
	var resp *tsopb.TsoResponse
	testutil.Eventually(re, func() bool {
		tsoClient, err := suite.tsoClient.Tso(ctx)
		re.NoError(err)
		defer func() {
			err := tsoClient.CloseSend()
			re.NoError(err)
		}()
		re.NoError(tsoClient.Send(req))
		resp, err = tsoClient.Recv()
		return err == nil && resp != nil
	})
	return checkAndReturnTimestampResponse(re, resp)
}

func (suite *tsoConsistencyTestSuite) TestRequestTSOConcurrently() {
	suite.requestTSOConcurrently()
	// Test TSO after the leader change
	suite.pdLeaderServer.GetServer().GetMember().Resign()
	suite.cluster.WaitLeader()
	suite.requestTSOConcurrently()
}

func (suite *tsoConsistencyTestSuite) requestTSOConcurrently() {
	re := suite.Require()
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
				// Check whether the TSO fallbacks
				re.Equal(1, tsoutil.CompareTimestamp(ts, last))
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

func (suite *tsoConsistencyTestSuite) TestFallbackTSOConsistency() {
	re := suite.Require()

	// Re-create the cluster to enable the failpoints.
	suite.TearDownSuite()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fallBackSync", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fallBackUpdate", `return(true)`))
	suite.SetupSuite()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fallBackSync"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fallBackUpdate"))

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
				re.Equal(1, tsoutil.CompareTimestamp(ts, last))
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}
