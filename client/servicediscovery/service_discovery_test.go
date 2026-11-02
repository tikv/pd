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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	"github.com/tikv/pd/client/pkg/utils/testutil"
	"github.com/tikv/pd/client/pkg/utils/tlsutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
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

func TestExecWithBackoffLimitsCalls(t *testing.T) {
	re := require.New(t)
	for _, maxRetryTimes := range []int{0, 1, 3} {
		calls := 0
		err := execWithBackoff(context.Background(), time.Millisecond, maxRetryTimes, func() error {
			calls++
			return errors.New("mock error")
		})
		re.Error(err)
		re.Equal(max(maxRetryTimes, 1), calls)
	}
}

type memberProbeTestServer struct {
	pdpb.UnimplementedPDServer
	getMembers func(context.Context) (*pdpb.GetMembersResponse, error)
}

func (s *memberProbeTestServer) GetMembers(ctx context.Context, _ *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	return s.getMembers(ctx)
}

func startMemberProbeTestServer(t *testing.T, getMembers func(context.Context) (*pdpb.GetMembersResponse, error)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pdpb.RegisterPDServer(server, &memberProbeTestServer{getMembers: getMembers})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		err := <-done
		require.True(t, err == nil || errors.Is(err, grpc.ErrServerStopped), "%v", err)
	})
	return "http://" + listener.Addr().String()
}

func startMemberProbeTestLeader(t *testing.T) string {
	t.Helper()
	var response atomic.Pointer[pdpb.GetMembersResponse]
	url := startMemberProbeTestServer(t, func(context.Context) (*pdpb.GetMembersResponse, error) {
		return response.Load(), nil
	})
	leader := &pdpb.Member{MemberId: 1, ClientUrls: []string{url}}
	response.Store(&pdpb.GetMembersResponse{
		Header: &pdpb.ResponseHeader{ClusterId: 42}, Members: []*pdpb.Member{leader}, Leader: leader,
	})
	return url
}

func newMemberProbeTestDiscovery(t *testing.T, urls []string, leaderURL string) *serviceDiscovery {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cli := NewDefaultServiceDiscovery(ctx, cancel, urls, nil).(*serviceDiscovery)
	cli.clusterID = 42
	if leaderURL != "" {
		cli.leader.Store(newPDServiceClient(leaderURL, leaderURL, nil, true))
	}
	t.Cleanup(func() {
		cancel()
		cli.Close()
	})
	return cli
}

func TestUpdateMemberBypassesUnresponsiveEndpoints(t *testing.T) {
	blocked := func(ctx context.Context) (*pdpb.GetMembersResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	oldLeader := startMemberProbeTestServer(t, blocked)
	follower := startMemberProbeTestServer(t, blocked)
	leaderURL := startMemberProbeTestLeader(t)
	cli := newMemberProbeTestDiscovery(t, []string{oldLeader, follower, leaderURL}, oldLeader)
	start := time.Now()
	err := cli.updateMember()
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, leaderURL, cli.getLeaderURL())
	t.Logf("membership_recovery_ms=%.3f", float64(elapsed)/float64(time.Millisecond))
	require.Less(t, elapsed, UpdateMemberTimeout/4, "healthy endpoint was blocked behind an unresponsive endpoint")
}

func TestUpdateMemberFastResponseUsesSingleProbe(t *testing.T) {
	var followerCalls atomic.Int32
	follower := startMemberProbeTestServer(t, func(context.Context) (*pdpb.GetMembersResponse, error) {
		followerCalls.Add(1)
		return nil, status.Error(codes.Unavailable, "unavailable")
	})
	leaderURL := startMemberProbeTestLeader(t)
	cli := newMemberProbeTestDiscovery(t, []string{leaderURL, follower}, leaderURL)
	_, err := cli.getMembers(cli.ctx, leaderURL, UpdateMemberTimeout)
	require.NoError(t, err)
	require.NoError(t, cli.updateMember())
	require.Zero(t, followerCalls.Load())
}

func TestUpdateMemberRejectsInvalidResponses(t *testing.T) {
	for _, foreign := range []bool{true, false} {
		name := "missing-leader"
		if foreign {
			name = "foreign-cluster"
		}
		t.Run(name, func(t *testing.T) {
			leaderURL := startMemberProbeTestLeader(t)
			invalid := startMemberProbeTestServer(t, func(context.Context) (*pdpb.GetMembersResponse, error) {
				if foreign {
					return &pdpb.GetMembersResponse{Header: &pdpb.ResponseHeader{ClusterId: 99}, Leader: &pdpb.Member{ClientUrls: []string{"http://foreign"}}}, nil
				}
				return &pdpb.GetMembersResponse{Header: &pdpb.ResponseHeader{ClusterId: 42}}, nil
			})
			cli := newMemberProbeTestDiscovery(t, []string{invalid, leaderURL}, "")
			require.NoError(t, cli.updateMember())
			require.Equal(t, leaderURL, cli.getLeaderURL())
		})
	}
}

func TestUpdateMemberConcurrentRefreshAndClose(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	var startOnce, cancelOnce sync.Once
	blocked := startMemberProbeTestServer(t, func(ctx context.Context) (*pdpb.GetMembersResponse, error) {
		startOnce.Do(func() { close(started) })
		<-ctx.Done()
		cancelOnce.Do(func() { close(canceled) })
		return nil, ctx.Err()
	})
	leaderURL := startMemberProbeTestLeader(t)
	cli := newMemberProbeTestDiscovery(t, []string{blocked, leaderURL}, blocked)
	finished := make(chan error, 2)
	for range cap(finished) {
		go func() { finished <- cli.updateMember() }()
	}
	for range cap(finished) {
		select {
		case err := <-finished:
			require.NoError(t, err)
		case <-time.After(UpdateMemberTimeout / 2):
			t.Fatal("concurrent refresh did not recover")
		}
	}
	select {
	case <-started:
	default:
		t.Fatal("the slow shared RPC was not reached")
	}
	select {
	case <-canceled:
		t.Fatal("a successful refresh canceled another waiter's shared RPC")
	default:
	}
	cli.Close()
	require.NoError(t, cli.ctx.Err(), "closing discovery must not cancel its owner's context")
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel its outstanding membership probes")
	}
}

func TestUpdateMemberCloseCancelsBlockingDial(t *testing.T) {
	cli := newMemberProbeTestDiscovery(t, []string{"http://127.0.0.1:1"}, "")
	started := make(chan struct{})
	var once sync.Once
	cli.option.GRPCDialOptions = []grpc.DialOption{
		grpc.WithBlock(), //nolint:staticcheck
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return nil, ctx.Err()
		}),
	}
	finished := make(chan error, 1)
	go func() { finished <- cli.updateMember() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking dial was not reached")
	}
	closed := make(chan struct{})
	go func() {
		cli.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(UpdateMemberTimeout / 4):
		t.Fatal("Close waited for the connection timeout")
	}
	select {
	case err := <-finished:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("membership refresh did not exit after Close")
	}
	require.NoError(t, cli.ctx.Err())
}

func TestUpdateMemberBoundsProbeConcurrency(t *testing.T) {
	started := make(chan struct{}, 4)
	var active, maximum atomic.Int32
	blocked := func(ctx context.Context) (*pdpb.GetMembersResponse, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, n) {
				break
			}
		}
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	urls := make([]string, cap(started))
	for i := range urls {
		urls[i] = startMemberProbeTestServer(t, blocked)
	}
	cli := newMemberProbeTestDiscovery(t, urls, "")
	finished := make(chan error, 1)
	go func() { finished <- cli.updateMember() }()
	for range 3 {
		select {
		case <-started:
		case <-time.After(UpdateMemberTimeout / 2):
			t.Fatal("hedged probes did not start")
		}
	}
	cli.cancel()
	require.Error(t, <-finished)
	require.LessOrEqual(t, maximum.Load(), int32(3))
}
