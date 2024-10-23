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
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/client/tsoutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type tsoProxyTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	apiCluster       *tests.TestCluster
	apiLeader        *tests.TestServer
	backendEndpoints string
	tsoCluster       *tests.TestTSOCluster
	defaultReq       *pdpb.TsoRequest
	streams          []pdpb.PD_TsoClient
	cleanupFuncs     []testutil.CleanupFunc
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
	re.NotEmpty(leaderName)
	s.apiLeader = s.apiCluster.GetServer(leaderName)
	s.backendEndpoints = s.apiLeader.GetAddr()
	re.NoError(s.apiLeader.BootstrapCluster())

	// Create a TSO cluster with 2 servers
	s.tsoCluster, err = tests.NewTestTSOCluster(s.ctx, 2, s.backendEndpoints)
	re.NoError(err)
	s.tsoCluster.WaitForDefaultPrimaryServing(re)

	s.defaultReq = &pdpb.TsoRequest{
		Header: &pdpb.RequestHeader{ClusterId: s.apiLeader.GetClusterID()},
		Count:  1,
	}

	// Create some TSO client streams with different context.
	s.streams, s.cleanupFuncs = createTSOStreams(s.ctx, re, s.backendEndpoints, 200)
}

func (s *tsoProxyTestSuite) TearDownSuite() {
	cleanupGRPCStreams(s.cleanupFuncs)
	s.tsoCluster.Destroy()
	s.apiCluster.Destroy()
	s.cancel()
}

// TestTSOProxyBasic tests the TSO Proxy's basic function to forward TSO requests to TSO microservice.
// It also verifies the correctness of the TSO Proxy's TSO response, such as the count of timestamps
// to retrieve in one TSO request and the monotonicity of the returned timestamps.
func (s *tsoProxyTestSuite) TestTSOProxyBasic() {
	s.verifyTSOProxy(s.ctx, s.streams, s.cleanupFuncs, 100, true)
}

// TestTSOProxyWithLargeCount tests while some grpc streams being cancelled and the others are still
// working, the TSO Proxy can still work correctly.
func (s *tsoProxyTestSuite) TestTSOProxyWorksWithCancellation() {
	re := s.Require()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		go func() {
			defer wg.Done()
			for range 3 {
				streams, cleanupFuncs := createTSOStreams(s.ctx, re, s.backendEndpoints, 10)
				for range 10 {
					s.verifyTSOProxy(s.ctx, streams, cleanupFuncs, 10, true)
				}
				cleanupGRPCStreams(cleanupFuncs)
			}
		}()
		for range 10 {
			s.verifyTSOProxy(s.ctx, s.streams, s.cleanupFuncs, 10, true)
		}
	}()
	wg.Wait()
}

// TestTSOProxyStress tests the TSO Proxy can work correctly under the stress. gPRC and TSO failures are allowed,
// but the TSO Proxy should not panic, blocked or deadlocked, and if it returns a timestamp, it should be a valid
// timestamp monotonic increasing. After the stress, the TSO Proxy should still work correctly.
func TestTSOProxyStress(_ *testing.T) {
	s := new(tsoProxyTestSuite)
	s.SetT(&testing.T{})
	s.SetupSuite()
	re := s.Require()

	const (
		totalRounds = 4
		clientsIncr = 500
		// The graceful period for TSO Proxy to recover from gPRC and TSO failures.
		recoverySLA = 5 * time.Second
	)
	streams := make([]pdpb.PD_TsoClient, 0)
	cleanupFuncs := make([]testutil.CleanupFunc, 0)

	// Start stress test for 90 seconds to avoid ci-test-job to timeout.
	ctxTimeout, cancel := context.WithTimeout(s.ctx, 90*time.Second)
	defer cancel()

	// Push load from many concurrent clients in multiple rounds and increase the #client each round.
	for i := range totalRounds {
		log.Info("start a new round of stress test",
			zap.Int("round-id", i), zap.Int("clients-count", len(streams)+clientsIncr))
		streamsTemp, cleanupFuncsTemp :=
			createTSOStreams(s.ctx, re, s.backendEndpoints, clientsIncr)
		streams = append(streams, streamsTemp...)
		cleanupFuncs = append(cleanupFuncs, cleanupFuncsTemp...)
		s.verifyTSOProxy(ctxTimeout, streams, cleanupFuncs, 50, false)
	}
	cleanupGRPCStreams(cleanupFuncs)
	log.Info("the stress test completed.")

	// Verify the TSO Proxy can still work correctly after the stress.
	testutil.Eventually(re, func() bool {
		err := s.verifyTSOProxy(s.ctx, s.streams, s.cleanupFuncs, 1, false)
		return err == nil
	}, testutil.WithWaitFor(recoverySLA), testutil.WithTickInterval(500*time.Millisecond))

	s.TearDownSuite()
}

// TestTSOProxyClientsWithSameContext tests the TSO Proxy can work correctly while the grpc streams
// are created with the same context.
func (s *tsoProxyTestSuite) TestTSOProxyClientsWithSameContext() {
	re := s.Require()
	const clientCount = 1000
	cleanupFuncs := make([]testutil.CleanupFunc, clientCount)
	streams := make([]pdpb.PD_TsoClient, clientCount)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	for i := range clientCount {
		conn, err := grpc.Dial(strings.TrimPrefix(s.backendEndpoints, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
		re.NoError(err)
		grpcPDClient := pdpb.NewPDClient(conn)
		stream, err := grpcPDClient.Tso(ctx)
		re.NoError(err)
		streams[i] = stream
		cleanupFunc := func() {
			stream.CloseSend()
			conn.Close()
		}
		cleanupFuncs[i] = cleanupFunc
	}

	s.verifyTSOProxy(ctx, streams, cleanupFuncs, 100, true)
	cleanupGRPCStreams(cleanupFuncs)
}

// TestTSOProxyRecvFromClientTimeout tests the TSO Proxy can properly close the grpc stream on the server side
// when the client does not send any request to the server for a long time.
func (s *tsoProxyTestSuite) TestTSOProxyRecvFromClientTimeout() {
	re := s.Require()

	// Enable the failpoint to make the TSO Proxy's grpc stream timeout on the server side to be 1 second.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/tsoProxyRecvFromClientTimeout", `return(1)`))
	streams, cleanupFuncs := createTSOStreams(s.ctx, re, s.backendEndpoints, 1)
	// Sleep 2 seconds to make the TSO Proxy's grpc stream timeout on the server side.
	time.Sleep(2 * time.Second)
	err := streams[0].Send(s.defaultReq)
	re.Error(err)
	cleanupGRPCStreams(cleanupFuncs)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/tsoProxyRecvFromClientTimeout"))

	// Verify the streams with no fault injection can work correctly.
	s.verifyTSOProxy(s.ctx, s.streams, s.cleanupFuncs, 1, true)
}

// TestTSOProxyFailToSendToClient tests the TSO Proxy can properly close the grpc stream on the server side
// when it fails to send the response to the client.
func (s *tsoProxyTestSuite) TestTSOProxyFailToSendToClient() {
	re := s.Require()

	// Enable the failpoint to make the TSO Proxy's grpc stream timeout on the server side to be 1 second.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/tsoProxyFailToSendToClient", `return(true)`))
	streams, cleanupFuncs := createTSOStreams(s.ctx, re, s.backendEndpoints, 1)
	err := streams[0].Send(s.defaultReq)
	re.NoError(err)
	_, err = streams[0].Recv()
	re.Error(err)
	cleanupGRPCStreams(cleanupFuncs)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/tsoProxyFailToSendToClient"))

	s.verifyTSOProxy(s.ctx, s.streams, s.cleanupFuncs, 1, true)
}

// TestTSOProxySendToTSOTimeout tests the TSO Proxy can properly close the grpc stream on the server side
// when it sends the request to the TSO service and encounters timeout.
func (s *tsoProxyTestSuite) TestTSOProxySendToTSOTimeout() {
	re := s.Require()

	// Enable the failpoint to make the TSO Proxy's grpc stream timeout on the server side to be 1 second.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/tsoProxySendToTSOTimeout", `return(true)`))
	streams, cleanupFuncs := createTSOStreams(s.ctx, re, s.backendEndpoints, 1)
	err := streams[0].Send(s.defaultReq)
	re.NoError(err)
	_, err = streams[0].Recv()
	re.Error(err)
	cleanupGRPCStreams(cleanupFuncs)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/tsoProxySendToTSOTimeout"))

	s.verifyTSOProxy(s.ctx, s.streams, s.cleanupFuncs, 1, true)
}

// TestTSOProxyRecvFromTSOTimeout tests the TSO Proxy can properly close the grpc stream on the server side
// when it receives the response from the TSO service and encounters timeout.
func (s *tsoProxyTestSuite) TestTSOProxyRecvFromTSOTimeout() {
	re := s.Require()

	// Enable the failpoint to make the TSO Proxy's grpc stream timeout on the server side to be 1 second.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/tsoProxyRecvFromTSOTimeout", `return(true)`))
	streams, cleanupFuncs := createTSOStreams(s.ctx, re, s.backendEndpoints, 1)
	err := streams[0].Send(s.defaultReq)
	re.NoError(err)
	_, err = streams[0].Recv()
	re.Error(err)
	cleanupGRPCStreams(cleanupFuncs)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/tsoProxyRecvFromTSOTimeout"))

	s.verifyTSOProxy(s.ctx, s.streams, s.cleanupFuncs, 1, true)
}

func cleanupGRPCStreams(cleanupFuncs []testutil.CleanupFunc) {
	for i := 0; i < len(cleanupFuncs); i++ {
		if cleanupFuncs[i] != nil {
			cleanupFuncs[i]()
			cleanupFuncs[i] = nil
		}
	}
}

func cleanupGRPCStream(
	streams []pdpb.PD_TsoClient, cleanupFuncs []testutil.CleanupFunc, index int,
) {
	if cleanupFuncs[index] != nil {
		cleanupFuncs[index]()
		cleanupFuncs[index] = nil
	}
	if streams[index] != nil {
		streams[index] = nil
	}
}

// verifyTSOProxy verifies the TSO Proxy can work correctly.
//
//  1. If mustReliable == true
//     no gPRC or TSO failures, the TSO Proxy should return a valid timestamp monotonic increasing.
//
//  2. If mustReliable == false
//     gPRC and TSO failures are allowed, but the TSO Proxy should not panic, blocked or deadlocked.
//     If it returns a timestamp, it should be a valid timestamp monotonic increasing.
func (s *tsoProxyTestSuite) verifyTSOProxy(
	ctx context.Context, streams []pdpb.PD_TsoClient,
	cleanupFuncs []testutil.CleanupFunc, requestsPerClient int, mustReliable bool,
) error {
	re := s.Require()
	reqs := s.generateRequests(requestsPerClient)

	var respErr atomic.Value

	wg := &sync.WaitGroup{}
	for i := range streams {
		if streams[i] == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lastPhysical, lastLogical := int64(0), int64(0)
			for range requestsPerClient {
				select {
				case <-ctx.Done():
					cleanupGRPCStream(streams, cleanupFuncs, i)
					return
				default:
				}

				req := reqs[rand.Intn(requestsPerClient)]
				err := streams[i].Send(req)
				if err != nil && !mustReliable {
					respErr.Store(err)
					cleanupGRPCStream(streams, cleanupFuncs, i)
					return
				}
				re.NoError(err)
				resp, err := streams[i].Recv()
				if err != nil && !mustReliable {
					respErr.Store(err)
					cleanupGRPCStream(streams, cleanupFuncs, i)
					return
				}
				re.NoError(err)
				re.Equal(req.GetCount(), resp.GetCount())
				ts := resp.GetTimestamp()
				count := int64(resp.GetCount())
				physical, largestLogic, suffixBits := ts.GetPhysical(), ts.GetLogical(), ts.GetSuffixBits()
				firstLogical := tsoutil.AddLogical(largestLogic, -count+1, suffixBits)
				re.False(tsoutil.TSLessEqual(physical, firstLogical, lastPhysical, lastLogical))
			}
		}(i)
	}
	wg.Wait()

	if val := respErr.Load(); val != nil {
		return val.(error)
	}
	return nil
}

func (s *tsoProxyTestSuite) generateRequests(requestsPerClient int) []*pdpb.TsoRequest {
	reqs := make([]*pdpb.TsoRequest, requestsPerClient)
	for i := range requestsPerClient {
		reqs[i] = &pdpb.TsoRequest{
			Header: &pdpb.RequestHeader{ClusterId: s.apiLeader.GetClusterID()},
			Count:  uint32(i) + 1, // Make sure the count is positive.
		}
	}
	return reqs
}

// createTSOStreams creates multiple TSO client streams, and each stream uses a different gRPC connection
// to simulate multiple clients.
func createTSOStreams(
	ctx context.Context, re *require.Assertions,
	backendEndpoints string, clientCount int,
) ([]pdpb.PD_TsoClient, []testutil.CleanupFunc) {
	cleanupFuncs := make([]testutil.CleanupFunc, clientCount)
	streams := make([]pdpb.PD_TsoClient, clientCount)

	for i := range clientCount {
		conn, err := grpc.Dial(strings.TrimPrefix(backendEndpoints, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
		re.NoError(err)
		grpcPDClient := pdpb.NewPDClient(conn)
		cctx, cancel := context.WithCancel(ctx)
		stream, err := grpcPDClient.Tso(cctx)
		re.NoError(err)
		streams[i] = stream
		cleanupFunc := func() {
			stream.CloseSend()
			cancel()
			conn.Close()
		}
		cleanupFuncs[i] = cleanupFunc
	}

	return streams, cleanupFuncs
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
				for range requestsPerClient {
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
			for range requestsPerClient {
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

// BenchmarkTSOProxy10Clients benchmarks TSO proxy performance with 10 clients.
func BenchmarkTSOProxy10Clients(b *testing.B) {
	benchmarkTSOProxyNClients(10, b)
}

// BenchmarkTSOProxy100Clients benchmarks TSO proxy performance with 100 clients.
func BenchmarkTSOProxy100Clients(b *testing.B) {
	benchmarkTSOProxyNClients(100, b)
}

// BenchmarkTSOProxy1000Clients benchmarks TSO proxy performance with 1000 clients.
func BenchmarkTSOProxy1000Clients(b *testing.B) {
	benchmarkTSOProxyNClients(1000, b)
}

// benchmarkTSOProxyNClients benchmarks TSO proxy performance.
func benchmarkTSOProxyNClients(clientCount int, b *testing.B) {
	suite := new(tsoProxyTestSuite)
	suite.SetT(&testing.T{})
	suite.SetupSuite()
	re := suite.Require()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streams, cleanupFuncs := createTSOStreams(ctx, re, suite.backendEndpoints, clientCount)

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

	cleanupGRPCStreams(cleanupFuncs)

	suite.TearDownSuite()
}
