// Copyright 2025 TiKV Project Authors.
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

package tso_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type tsoProxyTestSuite struct {
	suite.Suite
	serverCtx    context.Context
	serverCancel context.CancelFunc
	cluster      *tests.TestCluster
	leader       *tests.TestServer
	follower     *tests.TestServer

	pdClient     pdpb.PDClient
	conn         *grpc.ClientConn
	defaultReq   *pdpb.TsoRequest
	proxyClient  pdpb.PD_TsoClient
	clientCtx    context.Context
	clientCancel context.CancelFunc
}

func TestTSOProxyTestSuite(t *testing.T) {
	suite.Run(t, new(tsoProxyTestSuite))
}

func (s *tsoProxyTestSuite) SetupTest() {
	re := s.Require()

	var err error
	s.serverCtx, s.serverCancel = context.WithCancel(context.Background())
	s.cluster, err = tests.NewTestCluster(s.serverCtx, 2)
	re.NoError(err)

	re.NoError(s.cluster.RunInitialServers())
	re.NotEmpty(s.cluster.WaitLeader())

	for _, server := range s.cluster.GetServers() {
		if server.GetConfig().Name != s.cluster.GetLeader() {
			s.follower = server
		} else {
			s.leader = server
		}
	}

	s.pdClient, s.conn = testutil.MustNewGrpcClient(re, s.follower.GetAddr())
	clusterID := s.leader.GetClusterID()
	s.defaultReq = &pdpb.TsoRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Count:  1,
	}

	s.reCreateProxyClient()
}

func (s *tsoProxyTestSuite) reCreateProxyClient() {
	re := s.Require()
	if s.proxyClient != nil {
		err := s.proxyClient.CloseSend()
		if err != nil && err != io.EOF {
			re.NoError(err)
		}
	}
	if s.clientCancel != nil {
		s.clientCancel()
	}
	var err error
	s.proxyClient, s.clientCtx, s.clientCancel, err = s.createClient()
	re.NoError(err)
}

func (s *tsoProxyTestSuite) createClient() (pdpb.PD_TsoClient, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = grpcutil.BuildForwardContext(ctx, s.leader.GetAddr())
	tsoClient, err := s.pdClient.Tso(ctx)
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}
	return tsoClient, ctx, cancel, nil
}
func (s *tsoProxyTestSuite) TearDownTest() {
	re := s.Require()
	if s.proxyClient != nil {
		err := s.proxyClient.CloseSend()
		if err != nil && err != io.EOF {
			re.NoError(err)
		}
	}
	if s.conn != nil {
		s.conn.Close()
	}
	s.clientCancel()
	s.cluster.Destroy()
	s.serverCancel()
}

func (s *tsoProxyTestSuite) verifyProxyIsHealthy() {
	s.verifyProxyIsHealthyWith(s.proxyClient)
}

func (s *tsoProxyTestSuite) verifyProxyIsHealthyWith(client pdpb.PD_TsoClient) {
	re := s.Require()
	re.NoError(client.Send(s.defaultReq))
	resp, err := client.Recv()
	re.NoError(err)
	re.Equal(s.defaultReq.GetCount(), resp.GetCount())
	timestamp := resp.GetTimestamp()
	re.Positive(timestamp.GetPhysical())
	re.GreaterOrEqual(uint32(timestamp.GetLogical()), s.defaultReq.GetCount())
}

func (s *tsoProxyTestSuite) assertReceiveError(re *require.Assertions, errStr string) {
	re.NoError(s.proxyClient.Send(s.defaultReq))
	_, err := s.proxyClient.Recv()
	re.Error(err)
	re.Contains(err.Error(), errStr)
}

func (s *tsoProxyTestSuite) TestProxyPropagatesLeaderErrorQuickly() {
	re := s.Require()
	s.verifyProxyIsHealthy()

	// change leader
	re.NoError(s.cluster.ResignLeader())

	start := time.Now()
	s.assertReceiveError(re, "pd is not leader of cluster")

	// verify fails faster than timeout, otherwise the unavailable time will be too long.
	re.Less(time.Since(start), time.Second)
}

func (s *tsoProxyTestSuite) TestProxyClientIsCancelledQuicklyOnServerShutdown() {
	re := s.Require()
	// open a proxy stream
	s.verifyProxyIsHealthy()

	s.serverCancel()

	start := time.Now()
	s.assertReceiveError(re, "Canceled")

	// verify fails faster than timeout, otherwise the unavailable time will be too long.
	re.Less(time.Since(start), time.Second)
}

func (s *tsoProxyTestSuite) TestProxyCanNotCreateConnectionToLeader() {
	re := s.Require()

	// set idle timeout to zero
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/tsoutil/canNotCreateForwardStream", `return()`))

	// send request to trigger failpoint above
	s.assertReceiveError(re, "canNotCreateForwardStream")

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/tsoutil/canNotCreateForwardStream"))

	// verify stream can be recreated
	s.reCreateProxyClient()
	s.verifyProxyIsHealthy()
}

func (s *tsoProxyTestSuite) TestClientsContinueToWorkAfterFirstStreamIsClosed() {
	re := s.Require()
	s.verifyProxyIsHealthy()
	// open second stream
	proxyClient, _, cancel, err := s.createClient()
	defer cancel()
	re.NoError(err)
	defer func() {
		err := proxyClient.CloseSend()
		if err != nil && err != io.EOF {
			re.NoError(err)
		}
	}()

	// close the first stream
	err = s.proxyClient.CloseSend()
	if err != nil && err != io.EOF {
		re.NoError(err)
	}

	// verify other streams are still working
	s.verifyProxyIsHealthyWith(proxyClient)

	// restart new stream again
	s.reCreateProxyClient()
	s.verifyProxyIsHealthy()
}

func (s *tsoProxyTestSuite) TestIdleStreamToLeaderIsClosedAndRecreated() {
	re := s.Require()

	// set idle timeout to zero
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/tsoutil/tsoProxyStreamIdleTimeout", `return()`))

	// send request to trigger failpoint above
	s.assertReceiveError(re, "TSOProxyStreamIdleTimeout")

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/tsoutil/tsoProxyStreamIdleTimeout"))
	s.reCreateProxyClient()

	// now sever proxy is closed, let's send one more request to verify reset stream is recreated on demand
	s.verifyProxyIsHealthy()
}
