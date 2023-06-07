// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
	"google.golang.org/grpc"
)

type tsoProxyTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	apiCluster       *tests.TestCluster
	apiLeader        *tests.TestServer
	backendEndpoints string
	tsoCluster       *mcs.TestTSOCluster
	defaultReq       *pdpb.TsoRequest
}

func TestTSOProxyTestSuite(t *testing.T) {
	suite.Run(t, new(tsoProxyTestSuite))
}

func (suite *tsoProxyTestSuite) SetupSuite() {
	re := suite.Require()

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	// Create an API cluster with 1 server
	suite.apiCluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)
	err = suite.apiCluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.apiCluster.WaitLeader()
	suite.apiLeader = suite.apiCluster.GetServer(leaderName)
	suite.backendEndpoints = suite.apiLeader.GetAddr()
	suite.NoError(suite.apiLeader.BootstrapCluster())

	// Create a TSO cluster with 2 servers
	suite.tsoCluster, err = mcs.NewTestTSOCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	suite.tsoCluster.WaitForDefaultPrimaryServing(re)

	suite.defaultReq = &pdpb.TsoRequest{
		Header: &pdpb.RequestHeader{ClusterId: suite.apiLeader.GetClusterID()},
		Count:  1,
	}
}

func (suite *tsoProxyTestSuite) TearDownSuite() {
	suite.tsoCluster.Destroy()
	suite.apiCluster.Destroy()
	suite.cancel()
}

// TestTSOProxy tests the TSO Proxy.
func (suite *tsoProxyTestSuite) TestTSOProxy() {
	re := suite.Require()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcClientConns, streams, cancelFuns := createTSOStreams(re, ctx, suite.backendEndpoints, 100, true)

	err := tsoProxy(streams, suite.defaultReq)
	re.NoError(err)

	for _, stream := range streams {
		stream.CloseSend()
	}
	for _, conn := range grpcClientConns {
		conn.Close()
	}
	for _, cancelFun := range cancelFuns {
		cancelFun()
	}
}

// createTSOStreams creates multiple TSO client streams and each stream uses a different gRPC connection
// to simulate multiple clients.
func createTSOStreams(
	re *require.Assertions, ctx context.Context,
	backendEndpoints string, clientCount int, sameContext bool,
) ([]*grpc.ClientConn, []pdpb.PD_TsoClient, []context.CancelFunc) {
	grpcClientConns := make([]*grpc.ClientConn, 0, clientCount)
	streams := make([]pdpb.PD_TsoClient, 0, clientCount)
	cancelFuns := make([]context.CancelFunc, 0, clientCount)

	for i := 0; i < clientCount; i++ {
		conn, err := grpc.Dial(strings.TrimPrefix(backendEndpoints, "http://"), grpc.WithInsecure())
		re.NoError(err)
		grpcClientConns = append(grpcClientConns, conn)
		grpcPDClient := pdpb.NewPDClient(conn)
		var stream pdpb.PD_TsoClient
		if sameContext {
			stream, err = grpcPDClient.Tso(ctx)
			re.NoError(err)
		} else {
			cctx, cancel := context.WithCancel(ctx)
			cancelFuns = append(cancelFuns, cancel)
			stream, err = grpcPDClient.Tso(cctx)
			re.NoError(err)
		}
		streams = append(streams, stream)
	}

	return grpcClientConns, streams, cancelFuns
}

func tsoProxy(streams []pdpb.PD_TsoClient, tsoReq *pdpb.TsoRequest) error {
	for _, stream := range streams {
		if err := stream.Send(tsoReq); err != nil {
			return err
		}
		if _, err := stream.Recv(); err != nil {
			return err
		}
	}
	return nil
}

// BenchmarkTSOProxy10ClientsSameContext benchmarks TSO proxy performance with 10 clients and the same context.
func BenchmarkTSOProxy10ClientsSameContext(b *testing.B) {
	benchmarkTSOProxyNClients(10, true, b)
}

// BenchmarkTSOProxy10ClientsDiffContext benchmarks TSO proxy performance with 10 clients and different contexts.
func BenchmarkTSOProxy10ClientsDiffContext(b *testing.B) {
	benchmarkTSOProxyNClients(10, false, b)
}

// BenchmarkTSOProxy100ClientsSameContext benchmarks TSO proxy performance with 100 clients and the same context.
func BenchmarkTSOProxy100ClientsSameContext(b *testing.B) {
	benchmarkTSOProxyNClients(100, true, b)
}

// BenchmarkTSOProxy100ClientsDiffContext benchmarks TSO proxy performance with 100 clients and different contexts.
func BenchmarkTSOProxy100ClientsDiffContext(b *testing.B) {
	benchmarkTSOProxyNClients(100, false, b)
}

// BenchmarkTSOProxy1000ClientsSameContext benchmarks TSO proxy performance with 1000 clients and the same context.
func BenchmarkTSOProxy1000ClientsSameContext(b *testing.B) {
	benchmarkTSOProxyNClients(1000, true, b)
}

// BenchmarkTSOProxy1000ClientsDiffContext benchmarks TSO proxy performance with 1000 clients and different contexts.
func BenchmarkTSOProxy1000ClientsDiffContext(b *testing.B) {
	benchmarkTSOProxyNClients(1000, false, b)
}

// benchmarkTSOProxyNClients benchmarks TSO proxy performance.
func benchmarkTSOProxyNClients(clientCount int, sameContext bool, b *testing.B) {
	suite := new(tsoProxyTestSuite)
	suite.SetT(&testing.T{})
	suite.SetupSuite()
	re := suite.Require()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcClientConns, streams, cancelFuns := createTSOStreams(re, ctx, suite.backendEndpoints, clientCount, sameContext)

	// Benchmark TSO proxy
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := tsoProxy(streams, suite.defaultReq)
		re.NoError(err)
	}
	b.StopTimer()

	for _, stream := range streams {
		stream.CloseSend()
	}
	for _, conn := range grpcClientConns {
		conn.Close()
	}
	for _, cancelFun := range cancelFuns {
		cancelFun()
	}

	suite.TearDownSuite()
}
