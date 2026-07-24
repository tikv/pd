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

package servicediscovery

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	"github.com/tikv/pd/client/pkg/utils/testutil"
	"github.com/tikv/pd/client/pkg/utils/tlsutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type testGRPCServer struct {
	pb.UnimplementedGreeterServer
	isLeader     bool
	leaderAddr   string
	leaderConn   *grpc.ClientConn
	handleCount  atomic.Int32
	forwardCount atomic.Int32
}

// SayHello implements helloworld.GreeterServer
func (s *testGRPCServer) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	if !s.isLeader {
		if !grpcutil.IsFollowerHandleEnabled(ctx, metadata.FromIncomingContext) {
			if addr := grpcutil.GetForwardedHost(ctx, metadata.FromIncomingContext); addr == s.leaderAddr {
				s.forwardCount.Add(1)
				return pb.NewGreeterClient(s.leaderConn).SayHello(ctx, in)
			}
			return nil, errors.New("not leader")
		}
	}
	s.handleCount.Add(1)
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (s *testGRPCServer) resetCount() {
	s.handleCount.Store(0)
	s.forwardCount.Store(0)
}

func (s *testGRPCServer) getHandleCount() int32 {
	return s.handleCount.Load()
}

func (s *testGRPCServer) getForwardCount() int32 {
	return s.forwardCount.Load()
}

type testServer struct {
	server     *testGRPCServer
	grpcServer *grpc.Server
	addr       string
}

func newTestServer(isLeader bool) *testServer {
	addr := testutil.Alloc()
	u, err := url.Parse(addr)
	if err != nil {
		return nil
	}
	grpcServer := grpc.NewServer()
	server := &testGRPCServer{
		isLeader: isLeader,
	}
	pb.RegisterGreeterServer(grpcServer, server)
	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hsrv)
	return &testServer{
		server:     server,
		grpcServer: grpcServer,
		addr:       u.Host,
	}
}

func (s *testServer) run() {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

type serviceClientTestSuite struct {
	suite.Suite
	ctx   context.Context
	clean context.CancelFunc

	leaderServer   *testServer
	followerServer *testServer

	leaderClient   ServiceClient
	followerClient ServiceClient
}

func TestServiceClientClientTestSuite(t *testing.T) {
	suite.Run(t, new(serviceClientTestSuite))
}

func TestServiceClientGetClientConnReturnsNilAfterClose(t *testing.T) {
	re := require.New(t)

	conn, err := grpc.Dial("localhost:0", grpc.WithTransportCredentials(insecure.NewCredentials())) //nolint:staticcheck
	re.NoError(err)

	client := &serviceClient{conn: conn}
	re.NotNil(client.GetClientConn())
	re.NoError(conn.Close())
	re.Nil(client.GetClientConn())
}

func (suite *serviceClientTestSuite) SetupSuite() {
	suite.ctx, suite.clean = context.WithCancel(context.Background())

	suite.leaderServer = newTestServer(true)
	suite.followerServer = newTestServer(false)
	go suite.leaderServer.run()
	go suite.followerServer.run()
	for range 10 {
		leaderConn, err1 := grpc.Dial(suite.leaderServer.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))     //nolint:staticcheck
		followerConn, err2 := grpc.Dial(suite.followerServer.addr, grpc.WithTransportCredentials(insecure.NewCredentials())) //nolint:staticcheck
		if err1 == nil && err2 == nil {
			suite.followerClient = newPDServiceClient(
				tlsutil.ModifyURLScheme(suite.followerServer.addr, nil),
				tlsutil.ModifyURLScheme(suite.leaderServer.addr, nil),
				followerConn, false)
			suite.leaderClient = newPDServiceClient(
				tlsutil.ModifyURLScheme(suite.leaderServer.addr, nil),
				tlsutil.ModifyURLScheme(suite.leaderServer.addr, nil),
				leaderConn, true)
			suite.followerServer.server.leaderConn = suite.leaderClient.GetClientConn()
			suite.followerServer.server.leaderAddr = suite.leaderClient.GetURL()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	suite.Require().NotNil(suite.leaderClient)
}

func (suite *serviceClientTestSuite) TearDownTest() {
	suite.leaderServer.server.resetCount()
	suite.followerServer.server.resetCount()
}

func (suite *serviceClientTestSuite) TearDownSuite() {
	suite.leaderServer.grpcServer.GracefulStop()
	suite.followerServer.grpcServer.GracefulStop()
	suite.leaderClient.GetClientConn().Close()
	suite.followerClient.GetClientConn().Close()
	suite.clean()
}

func (suite *serviceClientTestSuite) TestServiceClient() {
	re := suite.Require()
	leaderAddress := tlsutil.ModifyURLScheme(suite.leaderServer.addr, nil)
	followerAddress := tlsutil.ModifyURLScheme(suite.followerServer.addr, nil)

	follower := suite.followerClient
	leader := suite.leaderClient

	re.Equal(follower.GetURL(), followerAddress)
	re.Equal(leader.GetURL(), leaderAddress)

	re.True(follower.Available())
	re.True(leader.Available())

	re.False(follower.IsConnectedToLeader())
	re.True(leader.IsConnectedToLeader())

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/unreachableNetwork1", "return(true)"))
	follower.(*serviceClient).checkNetworkAvailable(suite.ctx)
	leader.(*serviceClient).checkNetworkAvailable(suite.ctx)
	re.False(follower.Available())
	re.False(leader.Available())
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/unreachableNetwork1"))

	follower.(*serviceClient).checkNetworkAvailable(suite.ctx)
	leader.(*serviceClient).checkNetworkAvailable(suite.ctx)
	re.True(follower.Available())
	re.True(leader.Available())

	followerConn := follower.GetClientConn()
	leaderConn := leader.GetClientConn()
	re.NotNil(followerConn)
	re.NotNil(leaderConn)

	_, err := pb.NewGreeterClient(followerConn).SayHello(suite.ctx, &pb.HelloRequest{Name: "pd"})
	re.ErrorContains(err, errs.NotLeaderErr)
	resp, err := pb.NewGreeterClient(leaderConn).SayHello(suite.ctx, &pb.HelloRequest{Name: "pd"})
	re.NoError(err)
	re.Equal("Hello pd", resp.GetMessage())

	re.False(follower.NeedRetry(nil, nil))
	re.False(leader.NeedRetry(nil, nil))

	ctx1 := context.WithoutCancel(suite.ctx)
	ctx1 = follower.BuildGRPCTargetContext(ctx1, false)
	re.True(grpcutil.IsFollowerHandleEnabled(ctx1, metadata.FromOutgoingContext))
	re.Empty(grpcutil.GetForwardedHost(ctx1, metadata.FromOutgoingContext))
	ctx2 := context.WithoutCancel(suite.ctx)
	ctx2 = follower.BuildGRPCTargetContext(ctx2, true)
	re.False(grpcutil.IsFollowerHandleEnabled(ctx2, metadata.FromOutgoingContext))
	re.Equal(grpcutil.GetForwardedHost(ctx2, metadata.FromOutgoingContext), leaderAddress)

	ctx3 := context.WithoutCancel(suite.ctx)
	ctx3 = leader.BuildGRPCTargetContext(ctx3, false)
	re.False(grpcutil.IsFollowerHandleEnabled(ctx3, metadata.FromOutgoingContext))
	re.Empty(grpcutil.GetForwardedHost(ctx3, metadata.FromOutgoingContext))
	ctx4 := context.WithoutCancel(suite.ctx)
	ctx4 = leader.BuildGRPCTargetContext(ctx4, true)
	re.False(grpcutil.IsFollowerHandleEnabled(ctx4, metadata.FromOutgoingContext))
	re.Empty(grpcutil.GetForwardedHost(ctx4, metadata.FromOutgoingContext))

	followerAPIClient := newPDServiceAPIClient(follower, regionAPIErrorFn)
	leaderAPIClient := newPDServiceAPIClient(leader, regionAPIErrorFn)

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/servicediscovery/fastCheckAvailable", "return(true)"))

	re.True(followerAPIClient.Available())
	re.True(leaderAPIClient.Available())
	pdErr1 := &pdpb.Error{
		Type: pdpb.ErrorType_UNKNOWN,
	}
	pdErr2 := &pdpb.Error{
		Type: pdpb.ErrorType_REGION_NOT_FOUND,
	}
	err = errors.New("error")
	re.True(followerAPIClient.NeedRetry(pdErr1, nil))
	re.False(leaderAPIClient.NeedRetry(pdErr1, nil))
	re.True(followerAPIClient.Available())
	re.True(leaderAPIClient.Available())

	re.True(followerAPIClient.NeedRetry(pdErr2, nil))
	re.False(leaderAPIClient.NeedRetry(pdErr2, nil))
	re.False(followerAPIClient.Available())
	re.True(leaderAPIClient.Available())
	followerAPIClient.(*serviceAPIClient).markAsAvailable()
	leaderAPIClient.(*serviceAPIClient).markAsAvailable()
	re.False(followerAPIClient.Available())
	time.Sleep(time.Millisecond * 100)
	followerAPIClient.(*serviceAPIClient).markAsAvailable()
	re.True(followerAPIClient.Available())

	re.True(followerAPIClient.NeedRetry(nil, err))
	re.False(leaderAPIClient.NeedRetry(nil, err))
	re.True(followerAPIClient.Available())
	re.True(leaderAPIClient.Available())

	re.NoError(failpoint.Disable("github.com/tikv/pd/client/servicediscovery/fastCheckAvailable"))
}

func (suite *serviceClientTestSuite) TestServiceClientBalancer() {
	re := suite.Require()
	follower := suite.followerClient
	leader := suite.leaderClient
	b := &serviceBalancer{}
	b.set([]ServiceClient{leader, follower})
	re.Equal(2, b.totalNode)

	for range 10 {
		client := b.get()
		ctx := client.BuildGRPCTargetContext(suite.ctx, false)
		conn := client.GetClientConn()
		re.NotNil(conn)
		resp, err := pb.NewGreeterClient(conn).SayHello(ctx, &pb.HelloRequest{Name: "pd"})
		re.NoError(err)
		re.Equal("Hello pd", resp.GetMessage())
	}
	re.Equal(int32(5), suite.leaderServer.server.getHandleCount())
	re.Equal(int32(5), suite.followerServer.server.getHandleCount())
	suite.followerServer.server.resetCount()
	suite.leaderServer.server.resetCount()

	for range 10 {
		client := b.get()
		ctx := client.BuildGRPCTargetContext(suite.ctx, true)
		conn := client.GetClientConn()
		re.NotNil(conn)
		resp, err := pb.NewGreeterClient(conn).SayHello(ctx, &pb.HelloRequest{Name: "pd"})
		re.NoError(err)
		re.Equal("Hello pd", resp.GetMessage())
	}
	re.Equal(int32(10), suite.leaderServer.server.getHandleCount())
	re.Equal(int32(0), suite.followerServer.server.getHandleCount())
	re.Equal(int32(5), suite.followerServer.server.getForwardCount())
}

func TestServiceClientScheme(t *testing.T) {
	re := require.New(t)
	cli := newPDServiceClient(tlsutil.ModifyURLScheme("127.0.0.1:2379", nil), tlsutil.ModifyURLScheme("127.0.0.1:2379", nil), nil, false)
	re.Equal("http://127.0.0.1:2379", cli.GetURL())
	cli = newPDServiceClient(tlsutil.ModifyURLScheme("https://127.0.0.1:2379", nil), tlsutil.ModifyURLScheme("127.0.0.1:2379", nil), nil, false)
	re.Equal("http://127.0.0.1:2379", cli.GetURL())
	cli = newPDServiceClient(tlsutil.ModifyURLScheme("http://127.0.0.1:2379", nil), tlsutil.ModifyURLScheme("127.0.0.1:2379", nil), nil, false)
	re.Equal("http://127.0.0.1:2379", cli.GetURL())
	cli = newPDServiceClient(tlsutil.ModifyURLScheme("127.0.0.1:2379", &tls.Config{}), tlsutil.ModifyURLScheme("127.0.0.1:2379", &tls.Config{}), nil, false)
	re.Equal("https://127.0.0.1:2379", cli.GetURL())
	cli = newPDServiceClient(tlsutil.ModifyURLScheme("https://127.0.0.1:2379", &tls.Config{}), tlsutil.ModifyURLScheme("127.0.0.1:2379", &tls.Config{}), nil, false)
	re.Equal("https://127.0.0.1:2379", cli.GetURL())
	cli = newPDServiceClient(tlsutil.ModifyURLScheme("http://127.0.0.1:2379", &tls.Config{}), tlsutil.ModifyURLScheme("127.0.0.1:2379", &tls.Config{}), nil, false)
	re.Equal("https://127.0.0.1:2379", cli.GetURL())
}

func TestSchemeFunction(t *testing.T) {
	re := require.New(t)
	tlsCfg := &tls.Config{}

	endpoints1 := []string{
		"http://tc-pd:2379",
		"tc-pd:2379",
		"https://tc-pd:2379",
	}
	endpoints2 := []string{
		"127.0.0.1:2379",
		"http://127.0.0.1:2379",
		"https://127.0.0.1:2379",
	}
	urls := tlsutil.AddrsToURLs(endpoints1, tlsCfg)
	for _, u := range urls {
		re.Equal("https://tc-pd:2379", u)
	}
	urls = tlsutil.AddrsToURLs(endpoints2, tlsCfg)
	for _, u := range urls {
		re.Equal("https://127.0.0.1:2379", u)
	}
	urls = tlsutil.AddrsToURLs(endpoints1, nil)
	for _, u := range urls {
		re.Equal("http://tc-pd:2379", u)
	}
	urls = tlsutil.AddrsToURLs(endpoints2, nil)
	for _, u := range urls {
		re.Equal("http://127.0.0.1:2379", u)
	}

	re.Equal("https://127.0.0.1:2379", tlsutil.ModifyURLScheme("https://127.0.0.1:2379", tlsCfg))
	re.Equal("https://127.0.0.1:2379", tlsutil.ModifyURLScheme("http://127.0.0.1:2379", tlsCfg))
	re.Equal("https://127.0.0.1:2379", tlsutil.ModifyURLScheme("127.0.0.1:2379", tlsCfg))
	re.Equal("https://tc-pd:2379", tlsutil.ModifyURLScheme("tc-pd:2379", tlsCfg))
	re.Equal("http://127.0.0.1:2379", tlsutil.ModifyURLScheme("https://127.0.0.1:2379", nil))
	re.Equal("http://127.0.0.1:2379", tlsutil.ModifyURLScheme("http://127.0.0.1:2379", nil))
	re.Equal("http://127.0.0.1:2379", tlsutil.ModifyURLScheme("127.0.0.1:2379", nil))
	re.Equal("http://tc-pd:2379", tlsutil.ModifyURLScheme("tc-pd:2379", nil))

	urls = []string{
		"http://127.0.0.1:2379",
		"https://127.0.0.1:2379",
	}
	re.Equal("https://127.0.0.1:2379", tlsutil.PickMatchedURL(urls, tlsCfg))
	urls = []string{
		"http://127.0.0.1:2379",
	}
	re.Equal("https://127.0.0.1:2379", tlsutil.PickMatchedURL(urls, tlsCfg))
	urls = []string{
		"http://127.0.0.1:2379",
		"https://127.0.0.1:2379",
	}
	re.Equal("http://127.0.0.1:2379", tlsutil.PickMatchedURL(urls, nil))
	urls = []string{
		"https://127.0.0.1:2379",
	}
	re.Equal("http://127.0.0.1:2379", tlsutil.PickMatchedURL(urls, nil))
}

func TestUpdateURLs(t *testing.T) {
	re := require.New(t)
	members := []*pdpb.Member{
		{Name: "pd4", ClientUrls: []string{"tmp://pd4"}},
		{Name: "pd1", ClientUrls: []string{"tmp://pd1"}},
		{Name: "pd3", ClientUrls: []string{"tmp://pd3"}},
		{Name: "pd2", ClientUrls: []string{"tmp://pd2"}},
	}
	getURLs := func(ms []*pdpb.Member) (urls []string) {
		for _, m := range ms {
			urls = append(urls, m.GetClientUrls()[0])
		}
		return
	}
	cli := &serviceDiscovery{callbacks: newServiceCallbacks(), option: opt.NewOption()}
	cli.urls.Store([]string{})
	cli.updateURLs(members[1:])
	re.Equal(getURLs([]*pdpb.Member{members[1], members[3], members[2]}), cli.GetServiceURLs())
	cli.updateURLs(members[1:])
	re.Equal(getURLs([]*pdpb.Member{members[1], members[3], members[2]}), cli.GetServiceURLs())
	cli.updateURLs(members)
	re.Equal(getURLs([]*pdpb.Member{members[1], members[3], members[2], members[0]}), cli.GetServiceURLs())
	cli.updateURLs(members[1:])
	re.Equal(getURLs([]*pdpb.Member{members[1], members[3], members[2]}), cli.GetServiceURLs())
	cli.updateURLs(members[2:])
	re.Equal(getURLs([]*pdpb.Member{members[3], members[2]}), cli.GetServiceURLs())
	cli.updateURLs(members[3:])
	re.Equal(getURLs([]*pdpb.Member{members[3]}), cli.GetServiceURLs())
}

func TestGRPCDialOption(t *testing.T) {
	re := require.New(t)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), 500*time.Millisecond)
	defer cancel()
	cli := &serviceDiscovery{
		callbacks:         newServiceCallbacks(),
		checkMembershipCh: make(chan struct{}, 1),
		ctx:               ctx,
		cancel:            cancel,
		tlsCfg:            nil,
		option:            opt.NewOption(),
	}
	cli.urls.Store([]string{"tmp://test.url:5255"})
	cli.option.GRPCDialOptions = []grpc.DialOption{grpc.WithBlock()} //nolint:staticcheck
	err := cli.updateMember()
	re.Error(err)
	re.Greater(time.Since(start), 500*time.Millisecond)
}

type memberTestPDServer struct {
	pdpb.UnimplementedPDServer
	getMembers func() (*pdpb.GetMembersResponse, error)
}

func (s *memberTestPDServer) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	return s.getMembers()
}

func TestUpdateMemberWithResultPreservesErrorContract(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	testServer := &memberTestPDServer{}
	pdpb.RegisterPDServer(server, testServer)
	go func() {
		require.NoError(t, server.Serve(listener))
	}()
	t.Cleanup(server.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	const memberURL = "http://pd.test:2379"
	testCases := []struct {
		name        string
		response    func() (*pdpb.GetMembersResponse, error)
		fingerprint string
		transport   bool
	}{
		{
			name: "rpc unavailable",
			response: func() (*pdpb.GetMembersResponse, error) {
				return nil, status.Error(codes.Unavailable, "dial tcp 192.0.2.1:2379: connection refused")
			},
			fingerprint: "rpc/Unavailable",
			transport:   true,
		},
		{
			name: "response header error",
			response: func() (*pdpb.GetMembersResponse, error) {
				return &pdpb.GetMembersResponse{
					Header: &pdpb.ResponseHeader{ClusterId: 1, Error: &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: "not ready"}},
				}, nil
			},
			fingerprint: "response/UNKNOWN",
		},
		{
			name: "cluster id mismatch",
			response: func() (*pdpb.GetMembersResponse, error) {
				return validMemberTestResponse(memberURL, 2), nil
			},
			fingerprint: "cluster-id",
		},
		{
			name: "missing leader",
			response: func() (*pdpb.GetMembersResponse, error) {
				response := validMemberTestResponse(memberURL, 1)
				response.Leader = nil
				return response, nil
			},
			fingerprint: "leader",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testServer.getMembers = testCase.response
			client := newMemberTestServiceDiscovery(ctx, cancel, memberURL, conn)

			result, structuredErr := client.updateMemberWithResult()
			compatibilityErr := client.updateMember()

			require.Error(t, structuredErr)
			require.Equal(t, structuredErr.Error(), compatibilityErr.Error())
			require.Equal(t, []string{memberURL}, result.attemptedURLs)
			require.Equal(t, testCase.transport, result.transportFailures == 1)

			summary, ok := client.memberFailures.summary(time.Now())
			require.True(t, ok)
			require.Equal(t, []string{testCase.fingerprint}, summary.errorClasses)
		})
	}
}

func validMemberTestResponse(memberURL string, clusterID uint64) *pdpb.GetMembersResponse {
	member := &pdpb.Member{MemberId: 1, ClientUrls: []string{memberURL}}
	return &pdpb.GetMembersResponse{
		Header:  &pdpb.ResponseHeader{ClusterId: clusterID},
		Members: []*pdpb.Member{member},
		Leader:  member,
	}
}

func newMemberTestServiceDiscovery(
	ctx context.Context,
	cancel context.CancelFunc,
	memberURL string,
	conn *grpc.ClientConn,
) *serviceDiscovery {
	client := &serviceDiscovery{
		ctx:       ctx,
		cancel:    cancel,
		callbacks: newServiceCallbacks(),
		option:    opt.NewOption(),
		clusterID: 1,
	}
	client.urls.Store([]string{memberURL})
	client.clientConns.Store(memberURL, conn)
	return client
}

func TestUpdateMemberLoopSuppressesScheduledRefreshDuringTransportFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var getMembersCalls atomic.Int32
	conn, err := grpc.NewClient(
		"passthrough:///unreachable.test:2379",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return nil, errors.New("transport unavailable")
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(
			ctx context.Context,
			method string,
			req, reply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			opts ...grpc.CallOption,
		) error {
			if method == "/pdpb.PD/GetMembers" {
				getMembersCalls.Add(1)
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
	)
	require.NoError(t, err)

	var wg sync.WaitGroup
	client := newMemberTestServiceDiscovery(ctx, cancel, "http://unreachable.test:2379", conn)
	client.wg = &wg
	client.checkMembershipCh = make(chan struct{}, 1)
	wg.Add(1)
	go client.updateMemberLoop()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		require.NoError(t, conn.Close())
	})

	client.ScheduleCheckMemberChanged()
	require.Eventually(t, func() bool { return getMembersCalls.Load() >= 12 }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	callsAfterInitialBatch := getMembersCalls.Load()
	require.GreaterOrEqual(t, callsAfterInitialBatch, int32(12))

	for range 100 {
		client.ScheduleCheckMemberChanged()
	}
	require.Never(t, func() bool {
		return getMembersCalls.Load() != callsAfterInitialBatch
	}, 300*time.Millisecond, 10*time.Millisecond)

	// A synchronous check is an explicit request and must bypass background suppression.
	require.Error(t, client.CheckMemberChanged())
	require.Equal(t, callsAfterInitialBatch+1, getMembersCalls.Load())
}
