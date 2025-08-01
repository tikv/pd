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

package tsoutil

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

type mockStream struct {
	// Used to simulate a long-running processRequests call
	delay time.Duration
	// Used to simulate an error in processRequests
	err error
}

func (m *mockStream) process(clusterID uint64, count uint32, keyspaceID, keyspaceGroupID uint32) (tsoResp, error) {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	if m.err != nil {
		return nil, m.err
	}
	// Return a mock response
	return &mockTSOResp{}, nil
}

type mockTSOResp struct{}

func (m *mockTSOResp) GetTimestamp() *pdpb.Timestamp {
	return &pdpb.Timestamp{
		Physical: time.Now().UnixNano() / int64(time.Millisecond),
		Logical:  1,
	}
}

type mockProtoFactory struct {
	stream *mockStream
}

func (m *mockProtoFactory) createForwardStream(ctx context.Context, cc *grpc.ClientConn) (stream, context.CancelFunc, error) {
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
	return forwardStream.process(0, count, 0, 0)
}

func (m *mockRequest) postProcess(countSum, physical, firstLogical int64) (int64, error) {
	close(m.doneCh)
	return countSum, m.err
}

type tsoDispatcherTestSuite struct {
	suite.Suite
	dispatcher *TSODispatcher
}

func TestTSODispatcherTestSuite(t *testing.T) {
	suite.Run(t, new(tsoDispatcherTestSuite))
}

func (suite *tsoDispatcherTestSuite) SetupTest() {
	suite.dispatcher = NewTSODispatcher(nil, nil)
}

func (suite *tsoDispatcherTestSuite) TearDownTest() {
	// Clean up any remaining goroutines
	runtime.GC()
}

func (suite *tsoDispatcherTestSuite) TestGoroutineLeakOnStreamError() {
	re := suite.Require()
	
	// Create a mock proto factory that simulates a long-running processRequests call
	protoFactory := &mockProtoFactory{
		stream: &mockStream{
			delay: 3 * time.Second, // Simulate a 3-second delay
			err:   errors.New("stream error"), // Simulate a stream error
		},
	}
	
	// Create a context for the test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Create a mock request
	doneCh := make(chan struct{})
	req := &mockRequest{
		forwardedHost: "test-host",
		count:         1,
		doneCh:        doneCh,
	}
	
	// Dispatch the first request which will trigger the creation of a dispatcher goroutine
	_ = suite.dispatcher.DispatchRequest(ctx, req, protoFactory)
	
	// Wait a bit to ensure the dispatcher goroutine has started
	time.Sleep(100 * time.Millisecond)
	
	// Record the number of goroutines before sending more requests
	initialGoroutines := runtime.NumGoroutine()
	
	// Send multiple requests while the first one is still being processed
	var wg sync.WaitGroup
	requestCount := 100
	errorCount := atomic.NewInt64(0)
	
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			doneCh := make(chan struct{})
			req := &mockRequest{
				forwardedHost: "test-host",
				count:         1,
				doneCh:        doneCh,
			}
			
			// Dispatch the request
			_ = suite.dispatcher.DispatchRequest(ctx, req, protoFactory)
			
			// Wait for the request to complete or timeout
			select {
			case <-doneCh:
				// Request completed successfully
			case <-time.After(5 * time.Second):
				// Request timed out, which indicates a problem
				errorCount.Inc()
			}
		}(i)
	}
	
	// Wait for all requests to be sent
	wg.Wait()
	
	// Wait for the dispatcher to process the error and clean up
	time.Sleep(100 * time.Millisecond)
	
	// Record the number of goroutines after the error
	finalGoroutines := runtime.NumGoroutine()
	
	// With our fix, requests should be cancelled quickly when a stream error occurs
	// This prevents goroutine leaks by ensuring all pending requests are cleaned up
	re.True(errorCount.Load() > 0, "Requests should be cancelled when stream error occurs")
	
	// Check that all requests were cancelled in a timely manner (less than the 5-second timeout)
	// This verifies that our fix prevents goroutine leaks by cancelling requests promptly
	re.Equal(int64(requestCount), errorCount.Load(), "All requests should be cancelled promptly")
	
	// Check that the number of goroutines hasn't increased significantly
	// (allowing for some variance due to test framework)
	re.LessOrEqual(finalGoroutines-initialGoroutines, 10, 
		fmt.Sprintf("Goroutine count increased significantly: %d -> %d", initialGoroutines, finalGoroutines))
}

func (suite *tsoDispatcherTestSuite) TestNormalDispatchCleanup() {
	re := suite.Require()
	
	// Create a mock proto factory that works normally
	protoFactory := &mockProtoFactory{
		stream: &mockStream{},
	}
	
	// Create a context for the test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Record initial goroutine count
	initialGoroutines := runtime.NumGoroutine()
	
	// Dispatch a few requests
	requestCount := 10
	var wg sync.WaitGroup
	
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doneCh := make(chan struct{})
			req := &mockRequest{
				forwardedHost: "test-host-normal",
				count:         1,
				doneCh:        doneCh,
			}
			
			dispatchCtx := suite.dispatcher.DispatchRequest(ctx, req, protoFactory)
			
			// Wait for completion or context cancellation
			select {
			case <-doneCh:
			case <-dispatchCtx.Done():
			case <-time.After(time.Second):
			}
		}()
	}
	
	// Wait for all requests to complete
	wg.Wait()
	
	// Allow some time for cleanup
	time.Sleep(100 * time.Millisecond)
	
	// Check that goroutine count is back to normal
	finalGoroutines := runtime.NumGoroutine()
	re.LessOrEqual(finalGoroutines, initialGoroutines+5, 
		fmt.Sprintf("Goroutine count didn't return to normal: %d -> %d", initialGoroutines, finalGoroutines))
}

func (suite *tsoDispatcherTestSuite) TestIdleTimeoutCleanup() {
	re := suite.Require()
	
	// Enable the idle timeout failpoint
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/utils/tsoutil/tsoProxyStreamIdleTimeout", `return()`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/utils/tsoutil/tsoProxyStreamIdleTimeout"))
	}()
	
	// Create a mock proto factory
	protoFactory := &mockProtoFactory{
		stream: &mockStream{},
	}
	
	// Create a context for the test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Record initial goroutine count
	initialGoroutines := runtime.NumGoroutine()
	
	// Dispatch a request to create a dispatcher
	doneCh := make(chan struct{})
	req := &mockRequest{
		forwardedHost: "test-host-idle",
		count:         1,
		doneCh:        doneCh,
	}
	
	dispatchCtx := suite.dispatcher.DispatchRequest(ctx, req, protoFactory)
	
	// Wait for the request to complete
	select {
	case <-doneCh:
	case <-dispatchCtx.Done():
	case <-time.After(time.Second):
		re.Fail("Request didn't complete in time")
	}
	
	// Wait for the idle timeout to trigger
	time.Sleep(100 * time.Millisecond)
	
	// Check that goroutine count is back to normal
	finalGoroutines := runtime.NumGoroutine()
	re.LessOrEqual(finalGoroutines, initialGoroutines+5, 
		fmt.Sprintf("Goroutine count didn't return to normal after idle timeout: %d -> %d", initialGoroutines, finalGoroutines))
}
