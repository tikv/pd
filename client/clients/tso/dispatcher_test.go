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

	"github.com/tikv/pd/client/constants"
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
	checkCount    atomic.Int32
	conn          *grpc.ClientConn
	onSchedule    func()
	servingURL    string
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
func (s *countingServiceDiscovery) GetServingURL() string                            { return s.servingURL }
func (*countingServiceDiscovery) GetBackupURLs() []string                            { return nil }
func (*countingServiceDiscovery) GetServiceClient() sd.ServiceClient                 { return nil }
func (*countingServiceDiscovery) GetServiceClientByKind(sd.APIKind) sd.ServiceClient { return nil }
func (*countingServiceDiscovery) GetAllServiceClients() []sd.ServiceClient           { return nil }
func (s *countingServiceDiscovery) GetOrCreateGRPCConn(string) (*grpc.ClientConn, error) {
	return s.conn, nil
}
func (s *countingServiceDiscovery) RemoveClientConn(string) { s.removeCount.Add(1) }
func (s *countingServiceDiscovery) ScheduleCheckMemberChanged() {
	s.scheduleCount.Add(1)
	if s.onSchedule != nil {
		s.onSchedule()
	}
}
func (s *countingServiceDiscovery) CheckMemberChanged() error                                    { s.checkCount.Add(1); return nil }
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
	s.re.Equal(int32(1), svcDiscovery.scheduleCount.Load())
}

type reconnectTestBuilder func(context.Context, context.CancelFunc, time.Duration) (*tsoStream, error)

func (b reconnectTestBuilder) makeBuilder(*grpc.ClientConn) tsoStreamBuilder { return b }
func (b reconnectTestBuilder) build(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) (*tsoStream, error) {
	return b(ctx, cancel, timeout)
}

func TestTSOReconnectSkipsRedundantDiscovery(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "new-stream"
		if existing {
			name = "existing-stream"
		}
		t.Run(name, func(t *testing.T) {
			svc := &countingServiceDiscovery{conn: new(grpc.ClientConn)}
			var builds int
			builder := reconnectTestBuilder(func(ctx context.Context, _ context.CancelFunc, _ time.Duration) (*tsoStream, error) {
				builds++
				return newTSOStream(ctx, mockStreamURL, newMockTSOStreamImpl(ctx, resultModeGenerated)), nil
			})
			client := NewClient(t.Context(), opt.NewOption(), svc, builder)
			client.leaderURL.Store(mockStreamURL)
			t.Cleanup(func() {
				client.cancel()
				client.conCtxMgr.GC(func(string) bool { return true })
			})
			if existing {
				ctx, cancel := context.WithCancel(client.ctx)
				stream, err := builder(ctx, cancel, time.Second)
				require.NoError(t, err)
				require.True(t, client.conCtxMgr.Store(ctx, cancel, mockStreamURL, stream))
				builds = 0
			}
			require.NoError(t, client.tryConnectToTSO(client.ctx))
			require.Zero(t, svc.scheduleCount.Load(), "successful reconnect must not queue another membership RPC")
			require.Equal(t, 1, client.conCtxMgr.Size())
			if existing {
				require.Zero(t, builds)
			} else {
				require.Equal(t, 1, builds)
			}
		})
	}
}

func TestTSOReconnectFailureRefreshesBeforeBackoff(t *testing.T) {
	svc := &countingServiceDiscovery{conn: new(grpc.ClientConn)}
	errConnect := context.DeadlineExceeded
	var builds int
	builder := reconnectTestBuilder(func(context.Context, context.CancelFunc, time.Duration) (*tsoStream, error) {
		builds++
		return nil, errConnect
	})
	client := NewClient(t.Context(), opt.NewOption(), svc, builder)
	defer client.cancel()
	client.leaderURL.Store(mockStreamURL)
	svc.onSchedule = client.cancel
	require.ErrorIs(t, client.tryConnectToTSO(client.ctx), errConnect)
	require.Equal(t, int32(1), svc.scheduleCount.Load())
	require.Equal(t, 1, builds)
}

func TestTSORecoveryRefreshesOnRequestErrors(t *testing.T) {
	for _, leaderChange := range []bool{false, true} {
		name, message := "transport-error", "connection unavailable"
		if leaderChange {
			name, message = "not-leader", errs.NotLeaderErr
		}
		t.Run(name, func(t *testing.T) {
			svc := &countingServiceDiscovery{}
			provider := &mockTSOServiceProvider{option: opt.NewOption(), conCtxMgr: cctx.NewManager[*tsoStream](), svcDiscovery: svc}
			dispatcher := newTSODispatcher(t.Context(), provider)
			defer dispatcher.close()
			bo := retry.InitialBackoffer(time.Millisecond, time.Millisecond, time.Second)
			require.True(t, dispatcher.handleProcessRequestError(t.Context(), bo, provider.conCtxMgr, mockStreamURL, fmt.Errorf("%s", message)))
			if leaderChange {
				require.Equal(t, int32(1), svc.checkCount.Load())
				require.Zero(t, svc.scheduleCount.Load())
			} else {
				require.Equal(t, int32(1), svc.scheduleCount.Load())
				require.Zero(t, svc.checkCount.Load())
			}
		})
	}
}

func TestTSORecoveryUsesKnownNewLeader(t *testing.T) {
	for _, tc := range []struct {
		name       string
		leaderURL  string
		forwarding bool
		proxy      bool
		checks     int32
		schedules  int32
	}{
		{name: "new-direct-leader", leaderURL: "new-leader", checks: 0, schedules: 1},
		{name: "unchanged-leader", leaderURL: mockStreamURL, checks: 1},
		{name: "unknown-leader", checks: 1},
		{name: "forwarded-stream", leaderURL: "new-leader", forwarding: true, checks: 1},
		{name: "proxy-stream", leaderURL: "new-leader", proxy: true, checks: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &countingServiceDiscovery{servingURL: tc.leaderURL}
			option := opt.NewOption()
			option.EnableForwarding = tc.forwarding
			option.SetEnableTSOFollowerProxy(tc.proxy)
			provider := &mockTSOServiceProvider{option: option, conCtxMgr: cctx.NewManager[*tsoStream](), svcDiscovery: svc}
			dispatcher := newTSODispatcher(t.Context(), provider)
			defer dispatcher.close()
			bo := retry.InitialBackoffer(time.Millisecond, time.Millisecond, time.Second)
			require.True(t, dispatcher.handleProcessRequestError(t.Context(), bo, provider.conCtxMgr, mockStreamURL, fmt.Errorf("%s", errs.NotLeaderErr)))
			require.Equal(t, tc.checks, svc.checkCount.Load(), "a known new direct leader must not wait for another membership RPC")
			require.Equal(t, tc.schedules, svc.scheduleCount.Load())
		})
	}
}

func TestTSOLeaderChangeCancelsObsoleteBuild(t *testing.T) {
	svc := &countingServiceDiscovery{conn: new(grpc.ClientConn)}
	started := make(chan context.Context, 1)
	var client *Cli
	builder := reconnectTestBuilder(func(ctx context.Context, _ context.CancelFunc, _ time.Duration) (*tsoStream, error) {
		if client.getLeaderURL() == "old-leader" {
			started <- ctx
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return newTSOStream(ctx, "new-leader", newMockTSOStreamImpl(ctx, resultModeGenerated)), nil
	})
	client = NewClient(t.Context(), opt.NewOption(), svc, builder)
	client.leaderURL.Store("old-leader")
	t.Cleanup(func() {
		client.cancel()
		client.conCtxMgr.GC(func(string) bool { return true })
	})
	finished := make(chan error, 1)
	go func() { finished <- client.tryConnectToTSO(client.ctx) }()
	var obsolete context.Context
	select {
	case obsolete = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the old stream build did not start")
	}
	client.leaderURL.Store("new-leader")
	client.notifyLeaderSwitched()
	require.ErrorIs(t, obsolete.Err(), context.Canceled, "a leader change must cancel the obsolete stream build")
	select {
	case err := <-finished:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the reconnect did not use the new leader")
	}
	require.True(t, client.conCtxMgr.Exist("new-leader"))
	require.Equal(t, int32(1), svc.scheduleCount.Load())
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

type recoveringTSOServiceProvider struct {
	*mockTSOServiceProvider
	update func(context.Context) bool
}

func (p *recoveringTSOServiceProvider) updateConnectionCtxs(ctx context.Context) bool {
	return p.update(ctx)
}

func startRecoveryDispatcher(t *testing.T, provider tsoServiceProvider) (*tsoDispatcher, *Request) {
	t.Helper()
	td := newTSODispatcher(t.Context(), provider)
	var wg sync.WaitGroup
	wg.Add(1)
	go td.handleDispatcher(&wg)
	t.Cleanup(func() {
		td.close()
		wg.Wait()
	})
	req := &Request{
		requestCtx: td.ctx,
		clientCtx:  td.ctx,
		done:       make(chan error, 1),
		start:      time.Now(),
		pool:       &sync.Pool{},
	}
	td.push(req)
	return td, req
}

func TestDispatcherUsesBackgroundStream(t *testing.T) {
	for _, duringRefresh := range []bool{true, false} {
		name := "after-refresh"
		if duringRefresh {
			name = "during-refresh"
		}
		t.Run(name, func(t *testing.T) {
			refreshStarted := make(chan struct{})
			refreshFinished := make(chan struct{})
			releaseRefresh := make(chan struct{})
			var once sync.Once
			provider := &recoveringTSOServiceProvider{
				mockTSOServiceProvider: newMockTSOServiceProvider(opt.NewOption(), nil),
				update: func(ctx context.Context) bool {
					once.Do(func() { close(refreshStarted) })
					select {
					case <-ctx.Done():
					case <-releaseRefresh:
					}
					select {
					case <-refreshFinished:
					default:
						close(refreshFinished)
					}
					return false
				},
			}
			td, req := startRecoveryDispatcher(t, provider)
			select {
			case <-refreshStarted:
			case <-time.After(time.Second):
				t.Fatal("dispatcher did not attempt stream recovery")
			}
			if !duringRefresh {
				close(releaseRefresh)
				select {
				case <-refreshFinished:
				case <-time.After(time.Second):
					t.Fatal("stream recovery did not return")
				}
			}

			// A concurrent background updater has established the new leader's stream.
			// The foreground attempt still reports failure, as it used the old leader.
			ctx, cancel := context.WithCancel(td.ctx)
			inner := newMockTSOStreamImpl(ctx, resultModeGenerated)
			stream := newTSOStream(ctx, mockStreamURL, inner)
			require.True(t, provider.conCtxMgr.CleanAllAndStore(ctx, cancel, mockStreamURL, stream))
			if duringRefresh {
				close(releaseRefresh)
			}

			// Readiness must resume this request before the 500ms polling fallback.
			physical, _, err := req.waitTimeout(constants.RetryInterval / 2)
			require.NoError(t, err)
			require.Positive(t, physical)
		})
	}
}

func TestDispatcherFindsStreamPublishedBeforeSubscription(t *testing.T) {
	provider := &recoveringTSOServiceProvider{
		mockTSOServiceProvider: newMockTSOServiceProvider(opt.NewOption(), nil),
		update:                 func(context.Context) bool { return false },
	}
	published := make(chan bool, 1)
	var once sync.Once
	const injection = "github.com/tikv/pd/client/clients/tso/beforeSubscribeTSOStreamUpdates"
	require.NoError(t, failpoint.EnableCall(injection, func() {
		once.Do(func() {
			// Publish after the first empty lookup but before the subscription. There
			// is no waiter yet, so only rechecking availability can observe this store.
			ctx, cancel := context.WithCancel(t.Context())
			stream := newTSOStream(ctx, mockStreamURL, newMockTSOStreamImpl(ctx, resultModeGenerated))
			stored := provider.conCtxMgr.CleanAllAndStore(ctx, cancel, mockStreamURL, stream)
			if !stored {
				cancel()
			}
			published <- stored
		})
	}))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(injection)) })
	_, req := startRecoveryDispatcher(t, provider)
	physical, _, err := req.waitTimeout(constants.RetryInterval / 2)
	require.NoError(t, err)
	require.True(t, <-published)
	require.Positive(t, physical)
}

func TestDispatcherStreamRecoveryPollingFallback(t *testing.T) {
	var attempts atomic.Int32
	mock := newMockTSOServiceProvider(opt.NewOption(), nil)
	provider := &recoveringTSOServiceProvider{
		mockTSOServiceProvider: mock,
		update: func(ctx context.Context) bool {
			if attempts.Add(1) == 1 {
				return false
			}
			return mock.updateConnectionCtxs(ctx)
		},
	}
	_, req := startRecoveryDispatcher(t, provider)
	physical, _, err := req.waitTimeout(2 * time.Second)
	require.NoError(t, err)
	require.Positive(t, physical)
	require.Equal(t, int32(2), attempts.Load())
}

func TestDispatcherStreamRecoveryShutdown(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	provider := &recoveringTSOServiceProvider{
		mockTSOServiceProvider: newMockTSOServiceProvider(opt.NewOption(), nil),
		update: func(context.Context) bool {
			once.Do(func() { close(started) })
			return false
		},
	}
	td, req := startRecoveryDispatcher(t, provider)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not attempt stream recovery")
	}
	td.close()
	_, _, err := req.waitTimeout(time.Second)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDispatcherStreamRecoveryTimeout(t *testing.T) {
	option := opt.NewOption()
	option.Timeout = 50 * time.Millisecond
	provider := &recoveringTSOServiceProvider{
		mockTSOServiceProvider: newMockTSOServiceProvider(option, nil),
		update:                 func(context.Context) bool { return false },
	}
	td, req := startRecoveryDispatcher(t, provider)
	_, _, err := req.waitTimeout(time.Second)
	require.Error(t, err)
	require.True(t, errs.ErrClientCreateTSOStream.Equal(err), "%v", err)
	require.NoError(t, td.ctx.Err())
}
