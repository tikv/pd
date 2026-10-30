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

package tso

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	cctx "github.com/tikv/pd/client/pkg/connectionctx"
	"github.com/tikv/pd/client/pkg/retry"
	sd "github.com/tikv/pd/client/servicediscovery"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type mockTSOServiceProvider struct {
	option       *opt.Option
	createStream func(ctx context.Context) *tsoStream
	conCtxMgr    *cctx.Manager[*tsoStream]
	svcDiscovery sd.ServiceDiscovery
}

func newMockTSOServiceProvider(option *opt.Option, createStream func(ctx context.Context) *tsoStream) *mockTSOServiceProvider {
	return &mockTSOServiceProvider{
		option:       option,
		createStream: createStream,
		conCtxMgr:    newConnectionCtxManager(),
	}
}

func (m *mockTSOServiceProvider) getOption() *opt.Option {
	return m.option
}

func (m *mockTSOServiceProvider) getServiceDiscovery() sd.ServiceDiscovery {
	if m.svcDiscovery != nil {
		return m.svcDiscovery
	}
	return sd.NewMockServiceDiscovery([]string{mockStreamURL}, nil)
}

func (m *mockTSOServiceProvider) getConnectionCtxMgr() *cctx.Manager[*tsoStream] {
	return m.conCtxMgr
}

func (m *mockTSOServiceProvider) updateConnectionCtxs(ctx context.Context) bool {
	if m.conCtxMgr.Exist(mockStreamURL) {
		return true
	}
	cctx, cancel := context.WithCancel(ctx)
	var stream *tsoStream
	if m.createStream == nil {
		stream = newTSOStream(cctx, mockStreamURL, newMockTSOStreamImpl(ctx, resultModeGenerated))
	} else {
		stream = m.createStream(ctx)
	}
	return m.conCtxMgr.Store(cctx, cancel, mockStreamURL, stream)
}

type testTSODispatcherSuite struct {
	suite.Suite
	re *require.Assertions

	streamInner  *mockTSOStreamImpl
	stream       *tsoStream
	dispatcher   *tsoDispatcher
	dispatcherWg sync.WaitGroup
	option       *opt.Option

	reqPool *sync.Pool
}

func (s *testTSODispatcherSuite) SetupTest() {
	s.re = require.New(s.T())
	s.option = opt.NewOption()
	s.option.Timeout = time.Hour
	// As the internal logic of the tsoDispatcher allows it to create streams multiple times, but our tests needs
	// single stable access to the inner stream, we do not allow it to create it more than once in these tests.
	// To avoid data race on reading `stream` and `streamInner` fields.
	created := new(atomic.Bool)
	createStream := func(ctx context.Context) *tsoStream {
		s.streamInner = newMockTSOStreamImpl(ctx, resultModeGenerateOnSignal)
		s.stream = newTSOStream(ctx, mockStreamURL, s.streamInner)
		created.Store(true)
		return s.stream
	}
	s.dispatcher = newTSODispatcher(context.Background(), newMockTSOServiceProvider(s.option, createStream))
	s.reqPool = &sync.Pool{
		New: func() any {
			return &Request{
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

func (s *testTSODispatcherSuite) getReq(ctx context.Context) *Request {
	req := s.reqPool.Get().(*Request)
	req.clientCtx = context.Background()
	req.requestCtx = ctx
	req.physical = 0
	req.logical = 0
	req.start = time.Now()
	req.pool = s.reqPool
	return req
}

func (s *testTSODispatcherSuite) sendReq(ctx context.Context) *Request {
	req := s.getReq(ctx)
	s.dispatcher.push(req)
	return req
}

func (s *testTSODispatcherSuite) reqMustNotReady(req *Request) {
	_, _, err := req.waitTimeout(time.Millisecond * 50)
	s.re.Error(err)
	s.re.ErrorIs(err, context.DeadlineExceeded)
}

func (s *testTSODispatcherSuite) reqMustReady(req *Request) (physical int64, logical int64) {
	physical, logical, err := req.waitTimeout(time.Second)
	s.re.NoError(err)
	return physical, logical
}

func (s *testTSODispatcherSuite) TestEvictStreamWithBatchOnIt() {
	conCtxMgr := s.dispatcher.provider.getConnectionCtxMgr()
	// Serve the next batch with a stream bound to the context of its connection, so that
	// canceling the connection fails the batch on it.
	oldCtx, oldCancel := context.WithCancel(context.Background())
	defer oldCancel()
	oldInner := newMockTSOStreamImpl(oldCtx, resultModeGenerateOnSignal)
	defer oldInner.stop()
	s.re.True(conCtxMgr.Store(oldCtx, oldCancel, mockStreamURL, newTSOStream(oldCtx, mockStreamURL, oldInner), true))
	req := s.sendReq(context.Background())
	s.reqMustNotReady(req)

	// Overwriting the stream evicts it, which fails the batch on it with an error that GetTS retries.
	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	newInner := newMockTSOStreamImpl(newCtx, resultModeGenerateOnSignal)
	// Keep the timestamps above those already returned by the stream created in SetupTest.
	newInner.resGenPhysical += 10000
	defer newInner.stop()
	s.re.True(conCtxMgr.CleanAllAndStore(newCtx, newCancel, "new-url", newTSOStream(newCtx, "new-url", newInner)))
	s.re.Error(oldCtx.Err())
	_, _, err := req.waitTimeout(time.Second)
	s.re.True(errs.ErrClientTSOStreamClosed.Equal(err), "%v", err)

	// A retried request is served by the new stream.
	req = s.sendReq(context.Background())
	s.reqMustNotReady(req)
	newInner.generateNext()
	s.reqMustReady(req)
}

func TestTSODispatcherTestSuite(t *testing.T) {
	suite.Run(t, new(testTSODispatcherSuite))
}

type countingServiceDiscovery struct {
	removeCount   atomic.Int32
	scheduleCount atomic.Int32
}

func (*countingServiceDiscovery) Init() error                                        { return nil }
func (*countingServiceDiscovery) Close()                                             {}
func (*countingServiceDiscovery) GetClusterID() uint64                               { return 0 }
func (*countingServiceDiscovery) GetKeyspaceID() uint32                              { return 0 }
func (*countingServiceDiscovery) SetKeyspaceID(uint32)                               {}
func (*countingServiceDiscovery) GetKeyspaceGroupID() uint32                         { return 0 }
func (*countingServiceDiscovery) GetServiceURLs() []string                           { return nil }
func (*countingServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn     { return nil }
func (*countingServiceDiscovery) GetClientConns() *sync.Map                          { return &sync.Map{} }
func (*countingServiceDiscovery) GetServingURL() string                              { return "" }
func (*countingServiceDiscovery) GetBackupURLs() []string                            { return nil }
func (*countingServiceDiscovery) GetServiceClient() sd.ServiceClient                 { return nil }
func (*countingServiceDiscovery) GetServiceClientByKind(sd.APIKind) sd.ServiceClient { return nil }
func (*countingServiceDiscovery) GetAllServiceClients() []sd.ServiceClient           { return nil }
func (*countingServiceDiscovery) GetOrCreateGRPCConn(string) (*grpc.ClientConn, error) {
	return nil, nil
}
func (s *countingServiceDiscovery) RemoveClientConn(string) { s.removeCount.Add(1) }
func (s *countingServiceDiscovery) ScheduleCheckMemberChanged() {
	s.scheduleCount.Add(1)
}
func (*countingServiceDiscovery) CheckMemberChanged() error                                      { return nil }
func (*countingServiceDiscovery) ExecAndAddLeaderSwitchedCallback(sd.LeaderSwitchedCallbackFunc) {}
func (*countingServiceDiscovery) AddLeaderSwitchedCallback(sd.LeaderSwitchedCallbackFunc)        {}
func (*countingServiceDiscovery) AddMembersChangedCallback(func())                               {}

func (s *testTSODispatcherSuite) TestBasic() {
	ctx := context.Background()
	req := s.sendReq(ctx)
	s.reqMustNotReady(req)
	s.streamInner.generateNext()
	s.reqMustReady(req)
	// close one context and check if the dispatcher can still work
	s.dispatcher.closeContext(mockStreamURL)
	s.streamInner.generateNext()
	req = s.sendReq(ctx)
	s.reqMustNotReady(req)
}

func (s *testTSODispatcherSuite) TestDispatchRequestCancellationAfterEnqueue() {
	for _, target := range []string{"request", "client", "tso-client"} {
		s.Run(target, func() {
			re := require.New(s.T())
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			clientCtx, cancelClient := context.WithCancel(context.Background())
			defer cancelClient()
			tsoCtx, cancelTSO := context.WithCancel(context.Background())
			defer cancelTSO()
			cancel := map[string]context.CancelFunc{
				"request": cancelRequest, "client": cancelClient, "tso-client": cancelTSO,
			}[target]
			canceledAfterEnqueue := false
			re.NoError(failpoint.EnableCall("github.com/tikv/pd/client/clients/tso/afterPushTSORequest", func() {
				cancel()
				canceledAfterEnqueue = true
			}))
			defer func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/client/clients/tso/afterPushTSORequest"))
			}()

			cli := &Cli{ctx: tsoCtx}
			cli.dispatcher.Store(s.dispatcher)
			req := s.getReq(requestCtx)
			req.clientCtx = clientCtx
			retryable, err := cli.DispatchRequest(req)
			re.True(canceledAfterEnqueue, "cancellation must happen after the request is enqueued")
			re.False(retryable)
			// An enqueue must not report an error that makes innerClient finish it again.
			re.NoError(err)
			re.Empty(req.done, "only the dispatcher may complete the enqueued request")

			s.streamInner.generateNext()
			select {
			case err = <-req.done:
				re.NoError(err)
			case <-time.After(time.Second):
				s.T().Fatal("dispatcher did not complete the enqueued request")
			}
			re.Positive(req.physical)
			re.Positive(req.logical)
		})
	}
}

func TestPushToClosedDispatcher(t *testing.T) {
	for _, target := range []string{"before-enqueue", "after-enqueue"} {
		t.Run(target, func(t *testing.T) {
			re := require.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			td := &tsoDispatcher{ctx: ctx, cancel: cancel, tsoRequestCh: make(chan *Request, 1)}
			t.Cleanup(td.close)
			if target == "before-enqueue" {
				td.close()
			} else {
				re.NoError(failpoint.EnableCall("github.com/tikv/pd/client/clients/tso/afterPushTSORequest", func() {
					td.close()
				}))
				t.Cleanup(func() {
					re.NoError(failpoint.Disable("github.com/tikv/pd/client/clients/tso/afterPushTSORequest"))
				})
			}
			req := &Request{requestCtx: context.Background(), clientCtx: context.Background(), done: make(chan error, 1)}
			td.push(req)
			select {
			case err := <-req.done:
				re.ErrorIs(err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("request sent to a closed dispatcher was not completed")
			}
			re.Empty(td.tsoRequestCh)
		})
	}
}

func TestPushCancellationWithFullQueue(t *testing.T) {
	for _, target := range []string{"request", "client", "dispatcher"} {
		t.Run(target, func(t *testing.T) {
			re := require.New(t)
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			clientCtx, cancelClient := context.WithCancel(context.Background())
			defer cancelClient()
			ctx, cancelDispatcher := context.WithCancel(context.Background())
			td := &tsoDispatcher{ctx: ctx, cancel: cancelDispatcher, tsoRequestCh: make(chan *Request, 1)}
			// No consumer can free this slot before cleanup.
			td.tsoRequestCh <- &Request{done: make(chan error, 1)}
			req := &Request{requestCtx: requestCtx, clientCtx: clientCtx, done: make(chan error, 1)}
			pushed := make(chan struct{})
			go func() {
				td.push(req)
				close(pushed)
			}()
			t.Cleanup(func() {
				td.close()
				<-pushed
				td.revokePendingRequests(errs.ErrClosing)
			})
			cancel := map[string]context.CancelFunc{
				"request": cancelRequest, "client": cancelClient, "dispatcher": cancelDispatcher,
			}[target]
			cancel()
			select {
			case <-pushed:
			case <-time.After(time.Second):
				t.Fatal("enqueue did not stop after cancellation")
			}
			select {
			case err := <-req.done:
				re.ErrorIs(err, context.Canceled)
			default:
				t.Fatal("canceled request was not completed")
			}
		})
	}
}

func (s *testTSODispatcherSuite) TestHandleProcessRequestErrorRemoveConnOnCalleeMismatch() {
	svcDiscovery := &countingServiceDiscovery{}
	td := newTSODispatcher(context.Background(), &mockTSOServiceProvider{
		option:       s.option,
		conCtxMgr:    cctx.NewManager[*tsoStream](),
		svcDiscovery: svcDiscovery,
	})
	defer td.close()
	bo := retry.InitialBackoffer(time.Millisecond, time.Millisecond, time.Millisecond)

	ok := td.handleProcessRequestError(
		context.Background(),
		bo,
		cctx.NewManager[*tsoStream](),
		mockStreamURL,
		fmt.Errorf("%s", errs.MismatchCalleeIDErr),
	)

	s.re.True(ok)
	s.re.Equal(int32(1), svcDiscovery.removeCount.Load())
	s.re.Zero(svcDiscovery.scheduleCount.Load())
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
	s.option.SetTSOClientRPCConcurrency(concurrency)

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
	reqs := make([]*Request, 0, tokenCount+3)

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
		expectedPending := max(min(tokenCount+1-i, tokenCount), 0)

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
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/clients/tso/tsoDispatcherConcurrentModeNoDelay", "return"))
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/clients/tso/tsoDispatcherAlwaysCheckConcurrency", "return"))
	defer func() {
		s.re.NoError(failpoint.Disable("github.com/tikv/pd/client/clients/tso/tsoDispatcherConcurrentModeNoDelay"))
		s.re.NoError(failpoint.Disable("github.com/tikv/pd/client/clients/tso/tsoDispatcherAlwaysCheckConcurrency"))
	}()

	s.testStaticConcurrencyImpl(1)
	s.testStaticConcurrencyImpl(2)
	s.testStaticConcurrencyImpl(4)
	s.testStaticConcurrencyImpl(16)
}

func (s *testTSODispatcherSuite) TestBatchDelaying() {
	ctx := context.Background()
	s.option.SetTSOClientRPCConcurrency(2)

	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/clients/tso/tsoDispatcherConcurrentModeNoDelay", "return"))
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/clients/tso/tsoStreamSimulateEstimatedRPCLatency", `return("12ms")`))
	defer func() {
		s.re.NoError(failpoint.Disable("github.com/tikv/pd/client/clients/tso/tsoDispatcherConcurrentModeNoDelay"))
		s.re.NoError(failpoint.Disable("github.com/tikv/pd/client/clients/tso/tsoStreamSimulateEstimatedRPCLatency"))
	}()

	// Make sure concurrency option takes effect.
	req := s.sendReq(ctx)
	s.streamInner.generateNext()
	s.reqMustReady(req)

	// Trigger the check.
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/clients/tso/tsoDispatcherConcurrentModeAssertDelayDuration", `return("6ms")`))
	defer func() {
		s.re.NoError(failpoint.Disable("github.com/tikv/pd/client/clients/tso/tsoDispatcherConcurrentModeAssertDelayDuration"))
	}()
	req = s.sendReq(ctx)
	s.streamInner.generateNext()
	s.reqMustReady(req)

	// Try other concurrency.
	s.option.SetTSOClientRPCConcurrency(3)
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/clients/tso/tsoDispatcherConcurrentModeAssertDelayDuration", `return("4ms")`))
	req = s.sendReq(ctx)
	s.streamInner.generateNext()
	s.reqMustReady(req)

	s.option.SetTSOClientRPCConcurrency(4)
	s.re.NoError(failpoint.Enable("github.com/tikv/pd/client/clients/tso/tsoDispatcherConcurrentModeAssertDelayDuration", `return("3ms")`))
	req = s.sendReq(ctx)
	s.streamInner.generateNext()
	s.reqMustReady(req)
}

func BenchmarkTSODispatcherHandleRequests(b *testing.B) {
	log.SetLevel(zapcore.FatalLevel)

	ctx := context.Background()

	reqPool := &sync.Pool{
		New: func() any {
			return &Request{
				done:     make(chan error, 1),
				physical: 0,
				logical:  0,
			}
		},
	}
	getReq := func() *Request {
		req := reqPool.Get().(*Request)
		req.clientCtx = ctx
		req.requestCtx = ctx
		req.physical = 0
		req.logical = 0
		req.start = time.Now()
		req.pool = reqPool
		return req
	}

	dispatcher := newTSODispatcher(ctx, newMockTSOServiceProvider(opt.NewOption(), nil))
	var wg sync.WaitGroup
	wg.Add(1)

	go dispatcher.handleDispatcher(&wg)
	defer func() {
		dispatcher.close()
		wg.Wait()
	}()

	b.ResetTimer()
	for b.Loop() {
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
