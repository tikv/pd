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

package tsoutil

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/utils/testutil"
)

// mockStream is a mock stream used to test TSO request processing
type mockStream struct {
	sync.Mutex
	// delay is used to simulate a long-running processRequest call
	delay     time.Duration
	succCount int // Counter for successfully processed requests
}

// process simulates the process of handling TSO requests
func (m *mockStream) process(clusterID uint64, _ uint32, _ uint32, _ uint32) (tsoResp, error) {
	m.Lock()
	defer m.Unlock()

	m.succCount++
	// Simulate an error after every 10 successful requests
	if m.succCount == 10 {
		// Simulate stream error
		err := errors.New("simulated stream error")
		m.succCount = 0
		return nil, err
	}

	// If delay is set, sleep for a while to simulate processing time
	if m.delay > 0 {
		time.Sleep(m.delay)
	}

	// Simulate a successful response
	return &pdpb.TsoResponse{
		Header: &pdpb.ResponseHeader{
			ClusterId: clusterID,
		},
		Timestamp: &pdpb.Timestamp{
			Physical: time.Now().UnixMilli(), // Use current time as physical timestamp
			Logical:  1,                      // Logical timestamp is fixed at 1
		},
	}, nil
}

type mockProtoFactory struct {
	stream *mockStream
}

func (m *mockProtoFactory) createForwardStream(_ context.Context, _ *grpc.ClientConn) (stream, context.CancelFunc, error) {
	if m.stream == nil {
		return nil, nil, errors.New("no stream available")
	}
	return m.stream, func() {}, nil
}

type mockRequest struct {
	forwardedHost string
	clientConn    *grpc.ClientConn
	count         uint32
	doneCh        chan struct{}
	err           error
}

func (m *mockRequest) getForwardedHost() string {
	return m.forwardedHost
}

func (m *mockRequest) getClientConn() *grpc.ClientConn {
	return m.clientConn
}

func (m *mockRequest) getCount() uint32 {
	return m.count
}

func (m *mockRequest) process(forwardStream stream, count uint32) (tsoResp, error) {
	if m.err != nil {
		return nil, m.err
	}
	return forwardStream.process(0, count, 0, 0)
}

func (m *mockRequest) postProcess(countSum int64, _ int64, _ int64) (int64, error) {
	close(m.doneCh)
	return countSum, m.err
}

type tsoDispatcherTestSuite struct {
	suite.Suite
	dispatcher *TSODispatcher
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestTSODispatcherTestSuite(t *testing.T) {
	suite.Run(t, new(tsoDispatcherTestSuite))
}

func (suite *tsoDispatcherTestSuite) SetupTest() {
	tsoProxyHandleDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "test_tso_proxy_handle_duration_seconds",
		Help: "Histogram of TSO proxy handle duration",
	})
	suite.dispatcher = NewTSODispatcher(tsoProxyHandleDuration, nil)
}

func (suite *tsoDispatcherTestSuite) TearDownTest() {
	suite.dispatcher.Stop()
	runtime.GC() // Force garbage collection to clean up any lingering goroutines
}

func (suite *tsoDispatcherTestSuite) TestGoroutineLeakOnStreamError() {
	re := suite.Require()

	// Create a mock tso stream
	mockStream := &mockStream{
		delay: 10 * time.Millisecond,
	}
	protoFactory := &mockProtoFactory{
		stream: mockStream,
	}

	ctx := context.Background()
	forwardedHost := "test-host"
	var reqDispatchCount atomic.Int64
	var wg sync.WaitGroup
	// mock 11000 goroutines as tso proxy client
	for range 11000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dipatchReqDoneCh := make(chan struct{})
			for range 20 {
				req := &mockRequest{
					forwardedHost: forwardedHost,
					clientConn:    nil,
					count:         1,
					doneCh:        make(chan struct{}),
				}

				go func() {
					// if there has goroutine leak, it will block here
					suite.dispatcher.DispatchRequest(ctx, req, protoFactory)
					dipatchReqDoneCh <- struct{}{}
				}()

				select {
				case <-dipatchReqDoneCh:
					reqDispatchCount.Add(1)
				case <-time.After(20 * time.Second):
					// If the request takes too long, we assume there is a goroutine leak
				}
			}
		}()
	}

	wg.Wait()

	re.Equal(int64(11000*20), reqDispatchCount.Load(), "The number of dispatched requests should match the total requests sent")

	suite.dispatcher.Stop()
}
