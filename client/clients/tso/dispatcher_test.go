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
	"errors"
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
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	cctx "github.com/tikv/pd/client/pkg/connectionctx"
	"github.com/tikv/pd/client/pkg/retry"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
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

func (m *mockTSOServiceProvider) scheduleUpdateTSOConnectionCtxs() {
	m.updateConnectionCtxs(context.Background())
}

func (m *mockTSOServiceProvider) updateConnectionCtxs(ctx context.Context) bool {
	if m.conCtxMgr.Exist(mockStreamURL) {
		return true
	}
	cctx, cancel := context.WithCancel(ctx)
	var stream *tsoStream
	if m.createStream == nil {
		stream = newTSOStream(cctx, mockStreamURL, newMockTSOStreamImpl(cctx, resultModeGenerated))
	} else {
		stream = m.createStream(cctx)
	}
	if stream == nil {
		cancel()
		return false
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
		if created.Load() {
			return nil
		}
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
	s.dispatcherWg.Wait()
	s.streamInner.stop()
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
	conns         map[string]*grpc.ClientConn
	backupURLs    []string
	serviceURLs   []string
	onSchedule    func()
	onGetConn     func(string)
	servingURL    string
}

func (*countingServiceDiscovery) Init() error                                        { return nil }
func (*countingServiceDiscovery) Close()                                             {}
func (*countingServiceDiscovery) GetClusterID() uint64                               { return 0 }
func (*countingServiceDiscovery) GetKeyspaceID() uint32                              { return 0 }
func (*countingServiceDiscovery) SetKeyspaceID(uint32)                               {}
func (*countingServiceDiscovery) GetKeyspaceGroupID() uint32                         { return 0 }
func (s *countingServiceDiscovery) GetServiceURLs() []string                         { return s.serviceURLs }
func (*countingServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn     { return nil }
func (*countingServiceDiscovery) GetClientConns() *sync.Map                          { return &sync.Map{} }
func (s *countingServiceDiscovery) GetServingURL() string                            { return s.servingURL }
func (s *countingServiceDiscovery) GetBackupURLs() []string                          { return s.backupURLs }
func (*countingServiceDiscovery) GetServiceClient() sd.ServiceClient                 { return nil }
func (*countingServiceDiscovery) GetServiceClientByKind(sd.APIKind) sd.ServiceClient { return nil }
func (*countingServiceDiscovery) GetAllServiceClients() []sd.ServiceClient           { return nil }
func (s *countingServiceDiscovery) GetOrCreateGRPCConn(url string) (*grpc.ClientConn, error) {
	if s.onGetConn != nil {
		s.onGetConn(url)
	}
	if s.conns != nil {
		return s.conns[url], nil
	}
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
		s.stream,
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

func newTSOHealthTestConn(t *testing.T, url string, servingStatus healthpb.HealthCheckResponse_ServingStatus) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(url,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(_ context.Context, _ string, _, reply any,
			_ *grpc.ClientConn, _ grpc.UnaryInvoker, _ ...grpc.CallOption,
		) error {
			reply.(*healthpb.HealthCheckResponse).Status = servingStatus
			return nil
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func TestTSOReconnectTimeoutFallsBackToFollower(t *testing.T) {
	for _, tt := range []struct {
		name              string
		obsoleteLookup    bool
		obsoleteLookups   int
		lookupFailed      bool
		supersededBuild   bool
		switchBack        bool
		successfulTimeout bool
	}{
		{name: "build-timeout"},
		{name: "successful-build-at-timeout", successfulTimeout: true},
		{name: "leader-switch-away-and-back", supersededBuild: true, switchBack: true},
		{name: "superseded-successful-build-before-timeouts", supersededBuild: true},
		{name: "obsolete-lookup-before-build-timeouts", obsoleteLookup: true},
		{name: "obsolete-failed-lookup-before-build-timeouts", obsoleteLookup: true, lookupFailed: true},
		{name: "repeated-obsolete-lookups-before-build-timeouts", obsoleteLookup: true, obsoleteLookups: 2 * constants.MaxRetryTimes},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const leaderURL, backupURL = "leader", "backup"

			svc := &countingServiceDiscovery{
				servingURL: leaderURL,
				backupURLs: []string{backupURL},
				conns: map[string]*grpc.ClientConn{
					leaderURL: newTSOHealthTestConn(t, leaderURL, healthpb.HealthCheckResponse_NOT_SERVING),
					backupURL: newTSOHealthTestConn(t, backupURL, healthpb.HealthCheckResponse_SERVING),
				},
			}
			var directBuilds int
			var client *Cli
			var superseded bool
			builder := reconnectTestBuilder(func(ctx context.Context, cancel context.CancelFunc, _ time.Duration) (*tsoStream, error) {
				if forwardedHost := grpcutil.GetForwardedHost(ctx, metadata.FromOutgoingContext); forwardedHost != "" {
					require.Equal(t, leaderURL, forwardedHost)
					return newTSOStream(ctx, backupURL, newMockTSOStreamImpl(ctx, resultModeGenerated)), nil
				}
				if tt.supersededBuild && !superseded {
					superseded = true
					stream := newTSOStream(ctx, client.getLeaderURL(), newMockTSOStreamImpl(ctx, resultModeGenerated))
					if tt.switchBack {
						client.leaderURL.Store("other-leader")
						client.notifyLeaderSwitched()
					}
					client.leaderURL.Store(leaderURL)
					client.notifyLeaderSwitched()
					return stream, nil
				}
				directBuilds++
				// Trigger the production build timeout immediately, then return the same
				// gRPC cancellation error as a connection that remains unavailable.
				done := make(chan struct{})
				go checkStreamTimeout(ctx, cancel, done, 0)
				<-ctx.Done()
				done <- struct{}{}
				if tt.successfulTimeout {
					return newTSOStream(ctx, leaderURL, newMockTSOStreamImpl(ctx, resultModeGenerated)), nil
				}
				return nil, status.FromContextError(ctx.Err()).Err()
			})
			option := opt.NewOption()
			option.EnableForwarding = true
			client = NewClient(t.Context(), option, svc, builder)
			client.leaderURL.Store(leaderURL)
			if tt.supersededBuild && !tt.switchBack {
				client.leaderURL.Store("old-leader")
				svc.conns["old-leader"] = svc.conns[leaderURL]
			}
			if tt.obsoleteLookup {
				const oldLeader = "old-leader"
				if !tt.lookupFailed {
					svc.conns[oldLeader] = svc.conns[leaderURL]
					svc.conns["other-old-leader"] = svc.conns[leaderURL]
				}
				client.leaderURL.Store(oldLeader)
				remaining := max(1, tt.obsoleteLookups)
				svc.onGetConn = func(url string) {
					if remaining == 0 {
						return
					}
					remaining--
					next := leaderURL
					if remaining > 0 {
						next = oldLeader
						if url == next {
							next = "other-old-leader"
						}
					}
					// Switch after the lookup reads its URL, before it registers a build.
					client.leaderURL.Store(next)
					client.notifyLeaderSwitched()
				}
			}
			t.Cleanup(func() {
				client.cancel()
				client.wg.Wait()
				client.conCtxMgr.ReleaseAll()
			})

			require.NoError(t, client.tryConnectToTSO(client.ctx))
			require.Equal(t, constants.MaxRetryTimes, directBuilds)
			require.False(t, client.conCtxMgr.Exist(leaderURL))
			require.True(t, client.conCtxMgr.Exist(backupURL))
			_, req := startRecoveryDispatcher(t, client)
			physical, _, err := req.waitTimeout(time.Second)
			require.NoError(t, err)
			require.Positive(t, physical)
		})
	}
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
			stream := &tsoStream{leaderURL: mockStreamURL}
			require.True(t, dispatcher.handleProcessRequestError(t.Context(), bo, provider.conCtxMgr, mockStreamURL, stream, fmt.Errorf("%s", message)))
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
		name          string
		serverURL     string
		forwardedHost string
		leaderURL     string
		forwarding    bool
		proxy         bool
		checks        int32
		schedules     int32
	}{
		{name: "new-direct-leader", serverURL: "old-leader", leaderURL: "http://new-leader", schedules: 1},
		{name: "unchanged-leader", serverURL: "leader", leaderURL: "http://leader", checks: 1},
		{name: "unchanged-tls-leader", serverURL: "leader", leaderURL: "https://leader", checks: 1},
		{name: "unknown-leader", serverURL: "leader", checks: 1},
		{name: "forwarding-enabled-direct-stream", serverURL: "old-leader", leaderURL: "http://new-leader", forwarding: true, schedules: 1},
		{name: "current-forwarded-target", serverURL: "backup", forwardedHost: "http://leader", leaderURL: "http://leader", forwarding: true, checks: 1},
		{name: "obsolete-forwarded-target", serverURL: "backup", forwardedHost: "http://old-leader", leaderURL: "http://new-leader", forwarding: true, schedules: 1},
		{name: "proxy-direct-stream", serverURL: "old-leader", leaderURL: "http://new-leader", proxy: true, schedules: 1},
		{name: "current-proxy-target", serverURL: "backup", forwardedHost: "http://leader", leaderURL: "http://leader", proxy: true, checks: 1},
		{name: "obsolete-proxy-target", serverURL: "backup", forwardedHost: "http://old-leader", leaderURL: "http://new-leader", proxy: true, schedules: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &countingServiceDiscovery{servingURL: tc.leaderURL}
			option := opt.NewOption()
			option.EnableForwarding = tc.forwarding
			option.SetEnableTSOFollowerProxy(tc.proxy)
			provider := &mockTSOServiceProvider{option: option, conCtxMgr: cctx.NewManager[*tsoStream](), svcDiscovery: svc}
			dispatcher := newTSODispatcher(t.Context(), provider)
			defer dispatcher.close()
			ctx, cancel := context.WithCancel(t.Context())
			if tc.forwardedHost != "" {
				ctx = grpcutil.BuildForwardContext(ctx, tc.forwardedHost)
			}
			stream := newTSOStream(ctx, tc.serverURL, newMockTSOStreamImpl(ctx, resultModeGenerated))
			t.Cleanup(func() {
				cancel()
				stream.WaitForClosed()
			})
			bo := retry.InitialBackoffer(time.Millisecond, time.Millisecond, time.Second)
			require.True(t, dispatcher.handleProcessRequestError(t.Context(), bo, provider.conCtxMgr, tc.serverURL, stream, fmt.Errorf("%s", errs.NotLeaderErr)))
			require.Equal(t, tc.checks, svc.checkCount.Load(), "a known new target leader must not wait for another membership RPC")
			require.Equal(t, tc.schedules, svc.scheduleCount.Load())
		})
	}
}

func TestDispatcherCanceledStreamRefreshesBeforeReconnect(t *testing.T) {
	for _, tc := range []struct {
		name          string
		streamURL     string
		forwardedHost string
		leaderURL     string
		forwarding    bool
		schedules     int32
	}{
		{name: "current-forwarded-target", streamURL: "backup", forwardedHost: "http://leader", leaderURL: "http://leader", forwarding: true, schedules: 1},
		{name: "obsolete-forwarded-target", streamURL: "backup", forwardedHost: "http://old-leader", leaderURL: "http://new-leader", forwarding: true},
		{name: "current-direct-leader", streamURL: "leader", leaderURL: "http://leader", schedules: 1},
		{name: "current-tls-leader", streamURL: "leader", leaderURL: "https://leader", schedules: 1},
		{name: "obsolete-direct-leader", streamURL: "old-leader", leaderURL: "http://new-leader"},
		{name: "forwarding-enabled-current-direct", streamURL: "leader", leaderURL: "http://leader", forwarding: true, schedules: 1},
		{name: "forwarding-enabled-obsolete-direct", streamURL: "old-leader", leaderURL: "http://new-leader", forwarding: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &countingServiceDiscovery{conn: new(grpc.ClientConn), servingURL: tc.leaderURL}
			var builds, scheduledBeforeBuild atomic.Int32
			builder := reconnectTestBuilder(func(ctx context.Context, _ context.CancelFunc, _ time.Duration) (*tsoStream, error) {
				// A real connection attempt may block. Observe discovery at entry so
				// scheduling it after the attempt cannot satisfy the regression check.
				builds.Add(1)
				scheduledBeforeBuild.Store(svc.scheduleCount.Load())
				return newTSOStream(ctx, tc.leaderURL, newMockTSOStreamImpl(ctx, resultModeGenerated)), nil
			})
			option := opt.NewOption()
			option.EnableForwarding = tc.forwarding
			client := NewClient(t.Context(), option, svc, builder)
			client.leaderURL.Store(tc.leaderURL)
			t.Cleanup(client.cancel)

			ctx, cancel := context.WithCancel(client.ctx)
			if tc.forwardedHost != "" {
				ctx = grpcutil.BuildForwardContext(ctx, tc.forwardedHost)
			}
			stream := newTSOStream(ctx, tc.streamURL, newMockTSOStreamImpl(ctx, resultModeGenerated))
			require.True(t, client.conCtxMgr.Store(ctx, cancel, tc.streamURL, stream))
			// The deadline watcher cancels this outer context when a request times out.
			cancel()
			stream.WaitForClosed()

			_, req := startRecoveryDispatcher(t, client)
			physical, _, err := req.waitTimeout(time.Second)
			require.NoError(t, err)
			require.Positive(t, physical)
			require.Equal(t, int32(1), builds.Load())
			require.Equal(t, tc.schedules, scheduledBeforeBuild.Load(), "refresh must be scheduled before reconnecting")
			require.Equal(t, tc.schedules, svc.scheduleCount.Load(), "successful reconnect must not add another refresh")
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
	update    func(context.Context) bool
	scheduled chan struct{}
}

func (p *recoveringTSOServiceProvider) scheduleUpdateTSOConnectionCtxs() {
	select {
	case p.scheduled <- struct{}{}:
	default:
	}
}

func (p *recoveringTSOServiceProvider) updateConnectionCtxs(ctx context.Context) bool {
	return p.update(ctx)
}

func startRecoveryDispatcher(t *testing.T, provider tsoServiceProvider) (*tsoDispatcher, *Request) {
	t.Helper()
	var wg sync.WaitGroup
	ctx := t.Context()
	dispatcherWg := &wg
	client, realClient := provider.(*Cli)
	if realClient {
		ctx = client.ctx
		dispatcherWg = &client.wg
		client.wg.Add(1)
		go func() {
			// Start the real updater when recovery is requested, so eager startup
			// cannot publish a stream before the test reaches its intended state.
			select {
			case <-ctx.Done():
				client.wg.Done()
			case <-client.updateConCtxsCh:
				client.connectionCtxsUpdater()
			}
		}()
	}
	td := newTSODispatcher(ctx, provider)
	if p, ok := provider.(*recoveringTSOServiceProvider); ok {
		p.scheduled = make(chan struct{}, 1)
		wg.Go(func() {
			for {
				select {
				case <-td.ctx.Done():
					return
				case <-p.scheduled:
					p.updateConnectionCtxs(td.ctx)
				}
			}
		})
	}
	if realClient {
		client.dispatcher.Store(td)
	}
	dispatcherWg.Add(1)
	go td.handleDispatcher(dispatcherWg)
	t.Cleanup(func() {
		if realClient {
			client.Close()
			return
		}
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

			// A concurrent publisher has established the new leader's stream while
			// the requested background refresh still reports failure.
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

func TestDispatcherUsesBackgroundStreamDuringReconnect(t *testing.T) {
	for _, tt := range []struct {
		name       string
		beforeWait bool
		forwarded  bool
	}{
		{name: "direct-before-retry-wait", beforeWait: true},
		{name: "direct-during-retry-wait"},
		{name: "forwarded-before-retry-wait", beforeWait: true, forwarded: true},
		{name: "forwarded-during-retry-wait", forwarded: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var builds atomic.Int32
			scheduled := make(chan struct{})
			published := make(chan error, 1)
			var once sync.Once
			var client *Cli
			publish := func() error {
				if !tt.forwarded {
					return client.tryConnectToTSO(client.ctx)
				}
				ctx, cancel := context.WithCancel(client.ctx)
				ctx = grpcutil.BuildForwardContext(ctx, "leader")
				stream := newTSOStream(ctx, "follower", newMockTSOStreamImpl(ctx, resultModeGenerated))
				if !client.conCtxMgr.CleanAllAndStore(ctx, cancel, "follower", stream) {
					cancel()
					return errors.New("forwarded stream was not stored")
				}
				return nil
			}
			svc := &countingServiceDiscovery{conn: new(grpc.ClientConn), servingURL: "leader"}
			svc.onSchedule = func() {
				once.Do(func() {
					if tt.beforeWait {
						// Publish before the failed recovery attempt reaches its wait.
						published <- publish()
					}
					close(scheduled)
				})
			}
			builder := reconnectTestBuilder(func(ctx context.Context, _ context.CancelFunc, _ time.Duration) (*tsoStream, error) {
				if builds.Add(1) == 1 {
					return nil, errors.New("temporary stream creation failure")
				}
				return newTSOStream(ctx, "leader", newMockTSOStreamImpl(ctx, resultModeGenerated)), nil
			})
			client = NewClient(t.Context(), opt.NewOption(), svc, builder)
			client.leaderURL.Store("leader")
			t.Cleanup(func() {
				client.cancel()
				client.conCtxMgr.ReleaseAll()
			})
			_, req := startRecoveryDispatcher(t, client)
			select {
			case <-scheduled:
			case <-time.After(time.Second):
				t.Fatal("stream recovery attempt did not fail")
			}
			if tt.beforeWait {
				require.NoError(t, <-published)
			} else {
				require.NoError(t, publish())
			}
			if tt.forwarded {
				require.True(t, client.conCtxMgr.Exist("follower"))
				require.False(t, client.conCtxMgr.Exist("leader"))
			}
			physical, _, err := req.waitTimeout(constants.RetryInterval / 2)
			require.NoError(t, err, "a ready background stream must wake the dispatcher before polling")
			require.Positive(t, physical)
			expectedBuilds := int32(2)
			if tt.forwarded {
				expectedBuilds = 1
			}
			require.Equal(t, expectedBuilds, builds.Load())
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

	t.Run("active-updater", func(t *testing.T) {
		started := make(chan struct{})
		var once sync.Once
		var client *Cli
		svc := &countingServiceDiscovery{}
		svc.onGetConn = func(string) {
			once.Do(func() { close(started) })
			<-client.ctx.Done()
		}
		client = NewClient(t.Context(), opt.NewOption(), svc, nil)
		client.leaderURL.Store("leader")
		_, req := startRecoveryDispatcher(t, client)
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("background recovery did not start")
		}
		closed := make(chan struct{})
		go func() {
			client.Close()
			close(closed)
		}()
		_, _, err := req.waitTimeout(time.Second)
		require.ErrorIs(t, err, context.Canceled)
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("Close did not join the active updater and dispatcher")
		}
	})
}

func TestDispatcherStreamRecoveryTimeout(t *testing.T) {
	t.Run("unavailable", func(t *testing.T) {
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
	})
	for _, tt := range []struct {
		name     string
		nilConn  bool
		switches int
	}{
		{name: "blocked-lookup"},
		{name: "superseded-lookups", switches: 2 * constants.MaxRetryTimes},
		{name: "superseded-nil-lookups", nilConn: true, switches: 2 * constants.MaxRetryTimes},
	} {
		t.Run(tt.name, func(t *testing.T) {
			option := opt.NewOption()
			option.Timeout = 50 * time.Millisecond
			conn := new(grpc.ClientConn)
			oldConn := conn
			if tt.nilConn {
				oldConn = nil
			}
			svc := &countingServiceDiscovery{conns: map[string]*grpc.ClientConn{
				"leader-a": oldConn, "leader-b": oldConn, "leader-ready": conn,
			}}
			var lookups atomic.Int32
			blocked := make(chan struct{})
			release := make(chan struct{})
			var client *Cli
			svc.onGetConn = func(url string) {
				count := lookups.Add(1)
				if count <= int32(tt.switches) {
					next := "leader-a"
					if url == next {
						next = "leader-b"
					}
					client.leaderURL.Store(next)
					client.notifyLeaderSwitched()
				} else if count == int32(tt.switches)+1 {
					close(blocked)
					select {
					case <-client.ctx.Done():
					case <-release:
					}
					client.leaderURL.Store("leader-ready")
					client.notifyLeaderSwitched()
				}
			}
			builder := reconnectTestBuilder(func(ctx context.Context, _ context.CancelFunc, _ time.Duration) (*tsoStream, error) {
				return newTSOStream(ctx, "leader-ready", newMockTSOStreamImpl(ctx, resultModeGenerated)), nil
			})
			client = NewClient(t.Context(), option, svc, builder)
			client.leaderURL.Store("leader-a")
			td, req := startRecoveryDispatcher(t, client)
			select {
			case <-blocked:
			case <-time.After(time.Second):
				t.Fatal("background recovery did not reach the blocked lookup")
			}
			_, _, err := req.waitTimeout(constants.RetryInterval / 2)
			require.Error(t, err)
			require.True(t, errs.ErrClientCreateTSOStream.Equal(err), "background recovery must not hide the batch timeout: %v", err)
			require.Equal(t, int32(tt.switches)+1, lookups.Load(), "the batch must wait without starting a second recovery")
			require.NoError(t, td.ctx.Err())
			require.NoError(t, client.ctx.Err())

			// Completing recovery after the first batch timed out must still produce
			// a usable stream for a later request.
			close(release)
			next := &Request{
				requestCtx: td.ctx,
				clientCtx:  td.ctx,
				done:       make(chan error, 1),
				start:      time.Now(),
				pool:       &sync.Pool{},
			}
			td.push(next)
			physical, _, err := next.waitTimeout(constants.RetryInterval / 2)
			require.NoError(t, err)
			require.Positive(t, physical)
			require.NoError(t, client.conCtxMgr.GetConnectionCtx("leader-ready").Ctx.Err())
		})
	}
}

func TestTSOConnectionUpdaterSwitchesProxyDuringLeaderChanges(t *testing.T) {
	const proxyURL, directURL = "http://proxy", "http://direct"
	conn := newTSOHealthTestConn(t, proxyURL, healthpb.HealthCheckResponse_SERVING)
	svc := &countingServiceDiscovery{
		serviceURLs: []string{proxyURL},
		servingURL:  proxyURL,
		conns: map[string]*grpc.ClientConn{
			"leader-a": conn, "leader-b": conn, proxyURL: conn, directURL: conn,
		},
	}
	option := opt.NewOption()
	var lookups atomic.Int32
	var builds atomic.Int64
	proxyStarted, directStarted := make(chan struct{}), make(chan struct{})
	var proxyOnce, directOnce sync.Once
	var client *Cli
	svc.onGetConn = func(url string) {
		switch url {
		case proxyURL:
			proxyOnce.Do(func() { close(proxyStarted) })
		case directURL:
			directOnce.Do(func() { close(directStarted) })
		default:
			count := lookups.Add(1)
			next := "leader-a"
			if url == next {
				next = "leader-b"
			}
			if count > 2*constants.MaxRetryTimes {
				next = proxyURL
				option.SetEnableTSOFollowerProxy(true)
			}
			client.leaderURL.Store(next)
			client.notifyLeaderSwitched()
		}
	}
	builder := reconnectTestBuilder(func(ctx context.Context, _ context.CancelFunc, _ time.Duration) (*tsoStream, error) {
		inner := newMockTSOStreamImpl(ctx, resultModeGenerated)
		inner.resGenPhysical += builds.Add(1)
		return newTSOStream(ctx, client.getLeaderURL(), inner), nil
	})
	client = NewClient(t.Context(), option, svc, builder)
	client.leaderURL.Store("leader-a")
	td, req := startRecoveryDispatcher(t, client)
	select {
	case <-proxyStarted:
	case <-time.After(time.Second):
		t.Fatal("leader changes prevented switching to proxy recovery")
	}
	physical, _, err := req.waitTimeout(constants.RetryInterval / 2)
	require.NoError(t, err)
	require.Positive(t, physical)
	require.Greater(t, lookups.Load(), int32(constants.MaxRetryTimes))

	client.conCtxMgr.ReleaseAll()
	client.leaderURL.Store(directURL)
	client.notifyLeaderSwitched()
	option.SetEnableTSOFollowerProxy(false)
	select {
	case <-directStarted:
	case <-time.After(time.Second):
		t.Fatal("updater did not return to direct recovery")
	}
	next := &Request{
		requestCtx: td.ctx,
		clientCtx:  td.ctx,
		done:       make(chan error, 1),
		start:      time.Now(),
		pool:       &sync.Pool{},
	}
	td.push(next)
	nextPhysical, _, err := next.waitTimeout(constants.RetryInterval / 2)
	require.NoError(t, err)
	require.Greater(t, nextPhysical, physical)
}

func TestTSOConnectionUpdaterRestoresDirectStreamAfterProxyDisabled(t *testing.T) {
	for _, forwarding := range []bool{false, true} {
		t.Run(fmt.Sprintf("forwarding-%t", forwarding), func(t *testing.T) {
			const leaderURL, followerURL = "leader", "follower"
			svc := &countingServiceDiscovery{
				serviceURLs: []string{leaderURL, followerURL},
				servingURL:  leaderURL,
				conns: map[string]*grpc.ClientConn{
					leaderURL:   newTSOHealthTestConn(t, leaderURL, healthpb.HealthCheckResponse_NOT_SERVING),
					followerURL: newTSOHealthTestConn(t, followerURL, healthpb.HealthCheckResponse_SERVING),
				},
			}
			var builds atomic.Int64
			var directBuilds atomic.Int32
			directStarted, releaseDirect := make(chan struct{}), make(chan struct{})
			builder := reconnectTestBuilder(func(ctx context.Context, _ context.CancelFunc, _ time.Duration) (*tsoStream, error) {
				url := followerURL
				if grpcutil.GetForwardedHost(ctx, metadata.FromOutgoingContext) == "" {
					url = leaderURL
					if directBuilds.Add(1) == 1 {
						close(directStarted)
					}
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-releaseDirect:
					}
				}
				inner := newMockTSOStreamImpl(ctx, resultModeGenerated)
				inner.resGenPhysical += builds.Add(1)
				return newTSOStream(ctx, url, inner), nil
			})
			option := opt.NewOption()
			option.EnableForwarding = forwarding
			option.SetEnableTSOFollowerProxy(true)
			client := NewClient(t.Context(), option, svc, builder)
			client.leaderURL.Store(leaderURL)
			t.Cleanup(func() {
				client.cancel()
				client.wg.Wait()
				client.conCtxMgr.ReleaseAll()
			})
			client.updateConnectionCtxs(client.ctx)
			require.False(t, client.conCtxMgr.Exist(leaderURL))
			follower := client.conCtxMgr.GetConnectionCtx(followerURL)
			require.NotNil(t, follower)
			require.Zero(t, directBuilds.Load())

			// Keep the proxy stream when the leader recovers and proxy mode is disabled.
			svc.conns[leaderURL] = newTSOHealthTestConn(t, leaderURL, healthpb.HealthCheckResponse_SERVING)
			option.SetEnableTSOFollowerProxy(false)
			client.scheduleUpdateTSOConnectionCtxs()
			td, req := startRecoveryDispatcher(t, client)
			select {
			case <-directStarted:
			case <-time.After(time.Second):
				t.Fatal("disabling follower proxy did not start direct leader recovery")
			}
			physical, _, err := req.waitTimeout(constants.RetryInterval / 2)
			require.NoError(t, err, "direct recovery must not block requests on the retained follower stream")
			require.Positive(t, physical)
			require.NoError(t, follower.Ctx.Err())

			close(releaseDirect)
			require.Eventually(t, func() bool {
				return client.conCtxMgr.Exist(leaderURL) && !client.conCtxMgr.Exist(followerURL)
			}, time.Second, 10*time.Millisecond, "disabling follower proxy must restore direct mode")
			require.Equal(t, int32(1), directBuilds.Load())
			require.ErrorIs(t, follower.Ctx.Err(), context.Canceled)
			next := &Request{
				requestCtx: td.ctx,
				clientCtx:  td.ctx,
				done:       make(chan error, 1),
				start:      time.Now(),
				pool:       &sync.Pool{},
			}
			td.push(next)
			nextPhysical, _, err := next.waitTimeout(constants.RetryInterval / 2)
			require.NoError(t, err)
			require.Greater(t, nextPhysical, physical)
		})
	}
}
