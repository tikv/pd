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
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/client/tsoutil"
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
	grpcClientConns  []*grpc.ClientConn
	streams          []pdpb.PD_TsoClient
	cancelFuncs      []context.CancelFunc
}

func TestTSOProxyTestSuite(t *testing.T) {
	suite.Run(t, new(tsoProxyTestSuite))
}

func (s *tsoProxyTestSuite) SetupSuite() {
	re := s.Require()

	var err error
	s.ctx, s.cancel = context.WithCancel(context.Background())
	// Create an API cluster with 1 server
	s.apiCluster, err = tests.NewTestAPICluster(s.ctx, 1)
	re.NoError(err)
	err = s.apiCluster.RunInitialServers()
	re.NoError(err)
	leaderName := s.apiCluster.WaitLeader()
	s.apiLeader = s.apiCluster.GetServer(leaderName)
	s.backendEndpoints = s.apiLeader.GetAddr()
	s.NoError(s.apiLeader.BootstrapCluster())

	// Create a TSO cluster with 2 servers
	s.tsoCluster, err = mcs.NewTestTSOCluster(s.ctx, 2, s.backendEndpoints)
	re.NoError(err)
	s.tsoCluster.WaitForDefaultPrimaryServing(re)

	s.defaultReq = &pdpb.TsoRequest{
		Header: &pdpb.RequestHeader{ClusterId: s.apiLeader.GetClusterID()},
		Count:  1,
	}

	// Create some TSO client streams with the same context.
	s.grpcClientConns, s.streams, s.cancelFuncs = createTSOStreams(re, s.ctx, s.backendEndpoints, 100, true)
	// Create some TSO client streams with the different context.
	grpcClientConns, streams, cancelFuncs := createTSOStreams(re, s.ctx, s.backendEndpoints, 100, false)
	s.grpcClientConns = append(s.grpcClientConns, grpcClientConns...)
	s.streams = append(s.streams, streams...)
	s.cancelFuncs = append(s.cancelFuncs, cancelFuncs...)
}

func (s *tsoProxyTestSuite) TearDownSuite() {
	s.cleanupGRPCStreams(s.grpcClientConns, s.streams, s.cancelFuncs)
	s.tsoCluster.Destroy()
	s.apiCluster.Destroy()
	s.cancel()
}

// TestTSOProxyBasic tests the TSO Proxy's basic function to forward TSO requests to TSO microservice.
func (s *tsoProxyTestSuite) TestTSOProxyBasic() {
	s.verifyTSOProxy(s.streams, 800)
}

func (s *tsoProxyTestSuite) cleanupGRPCStreams(
	grpcClientConns []*grpc.ClientConn, streams []pdpb.PD_TsoClient, cancelFuncs []context.CancelFunc,
) {
	for _, stream := range streams {
		stream.CloseSend()
	}
	for _, conn := range grpcClientConns {
		conn.Close()
	}
	for _, cancelFun := range cancelFuncs {
		cancelFun()
	}
}

func (s *tsoProxyTestSuite) verifyTSOProxy(
	streams []pdpb.PD_TsoClient, requestsPerClient int,
) {
	re := s.Require()
	reqs := make([]*pdpb.TsoRequest, requestsPerClient)
	for i := 0; i < requestsPerClient; i++ {
		reqs[i] = &pdpb.TsoRequest{
			Header: &pdpb.RequestHeader{ClusterId: s.apiLeader.GetClusterID()},
			Count:  uint32(i) + 1, // Make sure the count is positive.
		}
	}

	wg := &sync.WaitGroup{}
	for _, stream := range streams {
		streamCopy := stream
		wg.Add(1)
		go func(streamCopy pdpb.PD_TsoClient) {
			defer wg.Done()
			lastPhysical, lastLogical := int64(0), int64(0)
			for i := 0; i < requestsPerClient; i++ {
				req := reqs[rand.Intn(requestsPerClient)]
				err := streamCopy.Send(req)
				re.NoError(err)
				resp, err := streamCopy.Recv()
				re.NoError(err)
				re.Equal(req.GetCount(), resp.GetCount())
				ts := resp.GetTimestamp()
				count := int64(resp.GetCount())
				physical, largestLogic, suffixBits := ts.GetPhysical(), ts.GetLogical(), ts.GetSuffixBits()
				firstLogical := tsoutil.AddLogical(largestLogic, -count+1, suffixBits)
				re.False(tsoutil.TSLessEqual(physical, firstLogical, lastPhysical, lastLogical))
			}
		}(streamCopy)
	}
	wg.Wait()
}

// createTSOStreams creates multiple TSO client streams and each stream uses a different gRPC connection
// to simulate multiple clients.
func createTSOStreams(
	re *require.Assertions, ctx context.Context,
	backendEndpoints string, clientCount int, sameContext bool,
) ([]*grpc.ClientConn, []pdpb.PD_TsoClient, []context.CancelFunc) {
	grpcClientConns := make([]*grpc.ClientConn, 0, clientCount)
	streams := make([]pdpb.PD_TsoClient, 0, clientCount)
	cancelFuncs := make([]context.CancelFunc, 0, clientCount)

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
			cancelFuncs = append(cancelFuncs, cancel)
			stream, err = grpcPDClient.Tso(cctx)
			re.NoError(err)
		}
		streams = append(streams, stream)
	}

	return grpcClientConns, streams, cancelFuncs
}

func tsoProxy(
	tsoReq *pdpb.TsoRequest, streams []pdpb.PD_TsoClient,
	concurrentClient bool, requestsPerClient int,
) error {
	if concurrentClient {
		wg := &sync.WaitGroup{}
		errsReturned := make([]error, len(streams))
		for index, stream := range streams {
			streamCopy := stream
			wg.Add(1)
			go func(index int, streamCopy pdpb.PD_TsoClient) {
				defer wg.Done()
				for i := 0; i < requestsPerClient; i++ {
					if err := streamCopy.Send(tsoReq); err != nil {
						errsReturned[index] = err
						return
					}
					if _, err := streamCopy.Recv(); err != nil {
						return
					}
				}
			}(index, streamCopy)
		}
		wg.Wait()
		for _, err := range errsReturned {
			if err != nil {
				return err
			}
		}
	} else {
		for _, stream := range streams {
			for i := 0; i < requestsPerClient; i++ {
				if err := stream.Send(tsoReq); err != nil {
					return err
				}
				if _, err := stream.Recv(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

var benmarkTSOProxyTable = []struct {
	concurrentClient  bool
	requestsPerClient int
}{
	{true, 2},
	{true, 10},
	{true, 100},
	{false, 2},
	{false, 10},
	{false, 100},
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

	grpcClientConns, streams, cancelFuncs := createTSOStreams(re, ctx, suite.backendEndpoints, clientCount, sameContext)

	// Benchmark TSO proxy
	b.ResetTimer()
	for _, t := range benmarkTSOProxyTable {
		var builder strings.Builder
		if t.concurrentClient {
			builder.WriteString("ConcurrentClients_")
		} else {
			builder.WriteString("SequentialClients_")
		}
		b.Run(fmt.Sprintf("%s_%dReqsPerClient", builder.String(), t.requestsPerClient), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := tsoProxy(suite.defaultReq, streams, t.concurrentClient, t.requestsPerClient)
				re.NoError(err)
			}
		})
	}
	b.StopTimer()

	suite.cleanupGRPCStreams(grpcClientConns, streams, cancelFuncs)

	suite.TearDownSuite()
}
