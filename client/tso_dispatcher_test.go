// Copyright 2024 TiKV Project Authors.
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

package pd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap/zapcore"
)

type mockTSOServiceProvider struct {
	option       *option
	createStream func(ctx context.Context) *tsoStream
	updateConnMu sync.Mutex
}

func newMockTSOServiceProvider(option *option, createStream func(ctx context.Context) *tsoStream) *mockTSOServiceProvider {
	return &mockTSOServiceProvider{
		option:       option,
		createStream: createStream,
	}
}

func (m *mockTSOServiceProvider) getOption() *option {
	return m.option
}

func (*mockTSOServiceProvider) getServiceDiscovery() ServiceDiscovery {
	return NewMockPDServiceDiscovery([]string{mockStreamURL}, nil)
}

func (m *mockTSOServiceProvider) updateConnectionCtxs(ctx context.Context, connectionCtxs *sync.Map) bool {
	// Avoid concurrent updating in the background updating goroutine and active updating in the dispatcher loop when
	// stream is missing.
	m.updateConnMu.Lock()
	defer m.updateConnMu.Unlock()

	_, ok := connectionCtxs.Load(mockStreamURL)
	if ok {
		return true
	}
	ctx, cancel := context.WithCancel(ctx)
	var stream *tsoStream
	if m.createStream == nil {
		stream = newTSOStream(ctx, mockStreamURL, newMockTSOStreamImpl(ctx, resultModeGenerated))
	} else {
		stream = m.createStream(ctx)
	}
	connectionCtxs.LoadOrStore(mockStreamURL, &tsoConnectionContext{ctx, cancel, mockStreamURL, stream})
	return true
}

type testTSODispatcherSuite struct {
	suite.Suite
	re *require.Assertions

	streamInner  *mockTSOStreamImpl
	stream       *tsoStream
	dispatcher   *tsoDispatcher
	dispatcherWg sync.WaitGroup
	option       *option

	reqPool *sync.Pool
}

func (s *testTSODispatcherSuite) SetupTest() {
	s.re = require.New(s.T())
	s.option = newOption()
	s.option.timeout = time.Hour
	// As the internal logic of the tsoDispatcher allows it to create streams multiple times, but our tests needs
	// single stable access to the inner stream, we do not allow it to create it more than once in these tests.
	creating := new(atomic.Bool)
	// To avoid data race on reading `stream` and `streamInner` fields.
	created := new(atomic.Bool)
	createStream := func(ctx context.Context) *tsoStream {
		if !creating.CompareAndSwap(false, true) {
			s.re.FailNow("testTSODispatcherSuite: trying to create stream more than once, which is unsupported in this tests")
		}
		s.streamInner = newMockTSOStreamImpl(ctx, resultModeGenerateOnSignal)
		s.stream = newTSOStream(ctx, mockStreamURL, s.streamInner)
		created.Store(true)
		return s.stream
	}
	s.dispatcher = newTSODispatcher(context.Background(), defaultMaxTSOBatchSize, newMockTSOServiceProvider(s.option, createStream))
	s.reqPool = &sync.Pool{
		New: func() any {
			return &tsoRequest{
				done:     make(chan error, 1),
				physical: 0,
				logical:  0,
			}
		},
	}

	s.dispatcherWg.Add(1)
	go s.dispatcher.handleDispatcher(&s.dispatcherWg)

	// Perform a request to ensure the stream must be created.

	{
		ctx := context.Background()
		req := s.sendReq(ctx)
		s.reqMustNotReady(req)
		// Wait until created
		for !created.Load() {
			time.Sleep(time.Millisecond)
		}
		s.streamInner.generateNext()
		s.reqMustReady(req)
	}
	s.re.True(created.Load())
	s.re.NotNil(s.stream)
}

func (s *testTSODispatcherSuite) TearDownTest() {
	s.dispatcher.close()
	s.streamInner.stop()
	s.dispatcherWg.Wait()
	s.stream.WaitForClosed()
	s.streamInner = nil
	s.stream = nil
	s.dispatcher = nil
	s.reqPool = nil
}

func (s *testTSODispatcherSuite) getReq(ctx context.Context) *tsoRequest {
	req := s.reqPool.Get().(*tsoRequest)
	req.clientCtx = context.Background()
	req.requestCtx = ctx
	req.physical = 0
	req.logical = 0
	req.start = time.Now()
	req.pool = s.reqPool
	return req
}

func (s *testTSODispatcherSuite) sendReq(ctx context.Context) *tsoRequest {
	req := s.getReq(ctx)
	s.dispatcher.push(req)
	return req
}

func (s *testTSODispatcherSuite) reqMustNotReady(req *tsoRequest) {
	_, _, err := req.waitTimeout(time.Millisecond * 50)
	s.re.Error(err)
	s.re.ErrorIs(err, context.DeadlineExceeded)
}

func (s *testTSODispatcherSuite) reqMustReady(req *tsoRequest) (physical int64, logical int64) {
	physical, logical, err := req.waitTimeout(time.Second)
	s.re.NoError(err)
	return physical, logical
}

func TestTSODispatcherTestSuite(t *testing.T) {
	suite.Run(t, new(testTSODispatcherSuite))
}

func (s *testTSODispatcherSuite) TestBasic() {
	ctx := context.Background()
	req := s.sendReq(ctx)
	s.reqMustNotReady(req)
	s.streamInner.generateNext()
	s.reqMustReady(req)
}

func (s *testTSODispatcherSuite) checkIdleTokenCount(expectedTotal int) {
	// When the tsoDispatcher is idle, the dispatcher loop will acquire a token and wait for requests. Therefore
	// there should be N-1 free tokens remaining.
	spinStart := time.Now()
	for time.Since(spinStart) < time.Second {
		if s.dispatcher.tokenCount != expectedTotal {
			continue
		}
		if len(s.dispatcher.tokenCh) == expectedTotal-1 {
			break
		}
	}
	s.re.Equal(expectedTotal, s.dispatcher.tokenCount)
	s.re.Len(s.dispatcher.tokenCh, expectedTotal-1)
}

func (s *testTSODispatcherSuite) testStaticConcurrencyImpl(concurrency int) {
	ctx := context.Background()
	s.option.setTSOClientRPCConcurrency(concurrency)

	// Make sure the state of the mock stream is clear. Unexpected batching may make the requests sent to the stream
	// less than expected, causing there are more `generateNext` signals or generated results.
	s.re.Empty(s.streamInner.resultCh)

	// The dispatcher may block on fetching requests, which is after checking concurrency option. Perform a request
	// to make sure the concurrency setting takes effect.
	req := s.sendReq(ctx)
	s.reqMustNotReady(req)
	s.streamInner.generateNext()
	s.reqMustReady(req)

	// For concurrent mode, the actual token count is twice the concurrency.
	// Note that the concurrency is a hint, and it's allowed to have more than `concurrency` requests running.
	tokenCount := concurrency
	if concurrency > 1 {
		tokenCount = concurrency * 2
	}
	s.checkIdleTokenCount(tokenCount)

	// As the failpoint `tsoDispatcherConcurrentModeNoDelay` is set, tsoDispatcher won't collect requests in blocking
	// way. And as `reqMustNotReady` delays for a while, requests shouldn't be batched as long as there are free tokens.
	// The first N requests (N=tokenCount) will each be a single batch, occupying a token. The last 3 are blocked,
	// and will be batched together once there is a free token.
	reqs := make([]*tsoRequest, 0, tokenCount+3)

	for range tokenCount + 3 {
		req := s.sendReq(ctx)
		s.reqMustNotReady(req)
		reqs = append(reqs, req)
	}

	// The dispatcher won't process more request batches if tokens are used up.
	// Note that `reqMustNotReady` contains a delay, which makes it nearly impossible that dispatcher is processing the
	// second batch but not finished yet.
	// Also note that in current implementation, the tsoStream tries to receive the next result before checking
	// the `tsoStream.pendingRequests` queue. Changing this behavior may need to update this test.
	for i := range tokenCount + 3 {
		expectedPending := tokenCount + 1 - i
		if expectedPending > tokenCount {
			expectedPending = tokenCount
		}
		if expectedPending < 0 {
			expectedPending = 0
		}

		// Spin for a while as the dispatcher loop may have not finished sending next batch to pendingRequests
		spinStart := time.Now()
		for time.Since(spinStart) < time.Second {
			if expectedPending == len(s.stream.pendingRequests) {
				break
			}
		}
		s.re.Len(s.stream.pendingRequests, expectedPending)

		req := reqs[i]
		// The last 3 requests should be in a single batch. Don't need to generate new results for the last 2.
		if i <= tokenCount {
			s.reqMustNotReady(req)
			s.streamInner.generateNext()
		}
		s.reqMustReady(req)
	}
}

func (s *testTSODispatcherSuite) TestConcurrentRPC() {
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/tsoDispatcherConcurrentModeNoDelay", "return"))
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/tsoDispatcherAlwaysCheckConcurrency", "return"))
	defer func() {
		s.re.NoError(failpoint.Disable("github.com/tikv/pd/client/tsoDispatcherConcurrentModeNoDelay"))
		s.re.NoError(failpoint.Disable("github.com/tikv/pd/client/tsoDispatcherAlwaysCheckConcurrency"))
	}()

	s.testStaticConcurrencyImpl(1)
	s.testStaticConcurrencyImpl(2)
	s.testStaticConcurrencyImpl(4)
	s.testStaticConcurrencyImpl(16)
}

func (s *testTSODispatcherSuite) TestBatchDelaying() {
	ctx := context.Background()
	s.option.setTSOClientRPCConcurrency(2)

	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/tsoDispatcherConcurrentModeNoDelay", "return"))
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/tsoStreamSimulateEstimatedRPCLatency", `return("12ms")`))
	defer func() {
		s.re.NoError(failpoint.Disable("github.com/tikv/pd/client/tsoDispatcherConcurrentModeNoDelay"))
		s.re.NoError(failpoint.Disable("github.com/tikv/pd/client/tsoStreamSimulateEstimatedRPCLatency"))
	}()

	// Make sure concurrency option takes effect.
	req := s.sendReq(ctx)
	s.streamInner.generateNext()
	s.reqMustReady(req)

	// Trigger the check.
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/tsoDispatcherConcurrentModeAssertDelayDuration", `return("6ms")`))
	defer func() {
		s.re.NoError(failpoint.Disable("github.com/tikv/pd/client/tsoDispatcherConcurrentModeAssertDelayDuration"))
	}()
	req = s.sendReq(ctx)
	s.streamInner.generateNext()
	s.reqMustReady(req)

	// Try other concurrency.
	s.option.setTSOClientRPCConcurrency(3)
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/tsoDispatcherConcurrentModeAssertDelayDuration", `return("4ms")`))
	req = s.sendReq(ctx)
	s.streamInner.generateNext()
	s.reqMustReady(req)

	s.option.setTSOClientRPCConcurrency(4)
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/tsoDispatcherConcurrentModeAssertDelayDuration", `return("3ms")`))
	req = s.sendReq(ctx)
	s.streamInner.generateNext()
	s.reqMustReady(req)
}

func BenchmarkTSODispatcherHandleRequests(b *testing.B) {
	log.SetLevel(zapcore.FatalLevel)

	ctx := context.Background()

	reqPool := &sync.Pool{
		New: func() any {
			return &tsoRequest{
				done:     make(chan error, 1),
				physical: 0,
				logical:  0,
			}
		},
	}
	getReq := func() *tsoRequest {
		req := reqPool.Get().(*tsoRequest)
		req.clientCtx = ctx
		req.requestCtx = ctx
		req.physical = 0
		req.logical = 0
		req.start = time.Now()
		req.pool = reqPool
		return req
	}

	dispatcher := newTSODispatcher(ctx, defaultMaxTSOBatchSize, newMockTSOServiceProvider(newOption(), nil))
	var wg sync.WaitGroup
	wg.Add(1)

	go dispatcher.handleDispatcher(&wg)
	defer func() {
		dispatcher.close()
		wg.Wait()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := getReq()
		dispatcher.push(req)
		_, _, err := req.Wait()
		if err != nil {
			panic(fmt.Sprintf("unexpected error from tsoReq: %+v", err))
		}
	}
	// Don't count the time cost in `defer`
	b.StopTimer()
}
