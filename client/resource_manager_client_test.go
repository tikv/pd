// Copyright 2026 TiKV Project Authors.
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
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
	sd "github.com/tikv/pd/client/servicediscovery"
)

type testServiceDiscovery struct {
	servingURL  string
	keyspaceID  uint32
	clientConns sync.Map
}

func newTestServiceDiscovery(servingURL string, conn *grpc.ClientConn) *testServiceDiscovery {
	t := &testServiceDiscovery{
		servingURL: servingURL,
	}
	t.clientConns.Store(servingURL, conn)
	return t
}

func (*testServiceDiscovery) Init() error                { return nil }
func (*testServiceDiscovery) Close()                     {}
func (*testServiceDiscovery) GetClusterID() uint64       { return 0 }
func (t *testServiceDiscovery) GetKeyspaceID() uint32    { return t.keyspaceID }
func (t *testServiceDiscovery) SetKeyspaceID(id uint32)  { t.keyspaceID = id }
func (*testServiceDiscovery) GetKeyspaceGroupID() uint32 { return 0 }
func (t *testServiceDiscovery) GetServiceURLs() []string { return []string{t.servingURL} }
func (t *testServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn {
	conn, _ := t.clientConns.Load(t.servingURL)
	if conn == nil {
		return nil
	}
	return conn.(*grpc.ClientConn)
}
func (t *testServiceDiscovery) GetClientConns() *sync.Map                        { return &t.clientConns }
func (t *testServiceDiscovery) GetServingURL() string                            { return t.servingURL }
func (*testServiceDiscovery) GetBackupURLs() []string                            { return nil }
func (*testServiceDiscovery) GetServiceClient() sd.ServiceClient                 { return nil }
func (*testServiceDiscovery) GetServiceClientByKind(sd.APIKind) sd.ServiceClient { return nil }
func (*testServiceDiscovery) GetAllServiceClients() []sd.ServiceClient           { return nil }
func (t *testServiceDiscovery) GetOrCreateGRPCConn(url string) (*grpc.ClientConn, error) {
	conn, ok := t.clientConns.Load(url)
	if !ok {
		return nil, errors.New("unexpected URL")
	}
	return conn.(*grpc.ClientConn), nil
}
func (*testServiceDiscovery) ScheduleCheckMemberChanged() {}
func (*testServiceDiscovery) CheckMemberChanged() error   { return nil }
func (t *testServiceDiscovery) ExecAndAddLeaderSwitchedCallback(cb sd.LeaderSwitchedCallbackFunc) {
	_ = cb(t.servingURL)
}
func (*testServiceDiscovery) AddLeaderSwitchedCallback(sd.LeaderSwitchedCallbackFunc) {}
func (*testServiceDiscovery) AddMembersChangedCallback(func())                        {}

var _ sd.ServiceDiscovery = (*testServiceDiscovery)(nil)

type fakeMetaStorageClient struct {
	value []byte
}

func (*fakeMetaStorageClient) Watch(context.Context, []byte, ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeMetaStorageClient) Get(context.Context, []byte, ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	if len(f.value) == 0 {
		return &meta_storagepb.GetResponse{
			Header: &meta_storagepb.ResponseHeader{Revision: 1},
		}, nil
	}
	return &meta_storagepb.GetResponse{
		Header: &meta_storagepb.ResponseHeader{Revision: 1},
		Kvs: []*meta_storagepb.KeyValue{
			{Value: f.value},
		},
		Count: 1,
	}, nil
}

func (*fakeMetaStorageClient) Put(context.Context, []byte, []byte, ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	return nil, errors.New("not implemented")
}

var _ metastorage.Client = (*fakeMetaStorageClient)(nil)

type testRMServer struct {
	rmpb.UnimplementedResourceManagerServer
	id string

	listCount   atomic.Int32
	getCount    atomic.Int32
	addCount    atomic.Int32
	modifyCount atomic.Int32
	deleteCount atomic.Int32
	tokenCount  atomic.Int32
}

func (s *testRMServer) ListResourceGroups(context.Context, *rmpb.ListResourceGroupsRequest) (*rmpb.ListResourceGroupsResponse, error) {
	s.listCount.Add(1)
	return &rmpb.ListResourceGroupsResponse{
		Groups: []*rmpb.ResourceGroup{
			{Name: s.id},
		},
	}, nil
}

func (s *testRMServer) GetResourceGroup(context.Context, *rmpb.GetResourceGroupRequest) (*rmpb.GetResourceGroupResponse, error) {
	s.getCount.Add(1)
	return &rmpb.GetResourceGroupResponse{
		Group: &rmpb.ResourceGroup{
			Name: s.id,
		},
	}, nil
}

func (s *testRMServer) AddResourceGroup(context.Context, *rmpb.PutResourceGroupRequest) (*rmpb.PutResourceGroupResponse, error) {
	s.addCount.Add(1)
	return &rmpb.PutResourceGroupResponse{Body: "add@" + s.id}, nil
}

func (s *testRMServer) ModifyResourceGroup(context.Context, *rmpb.PutResourceGroupRequest) (*rmpb.PutResourceGroupResponse, error) {
	s.modifyCount.Add(1)
	return &rmpb.PutResourceGroupResponse{Body: "modify@" + s.id}, nil
}

func (s *testRMServer) DeleteResourceGroup(context.Context, *rmpb.DeleteResourceGroupRequest) (*rmpb.DeleteResourceGroupResponse, error) {
	s.deleteCount.Add(1)
	return &rmpb.DeleteResourceGroupResponse{Body: "delete@" + s.id}, nil
}

func (s *testRMServer) AcquireTokenBuckets(stream rmpb.ResourceManager_AcquireTokenBucketsServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		s.tokenCount.Add(1)
		resp := &rmpb.TokenBucketsResponse{
			Responses: make([]*rmpb.TokenBucketResponse, 0, len(req.GetRequests())),
		}
		for _, tokenReq := range req.GetRequests() {
			resp.Responses = append(resp.Responses, &rmpb.TokenBucketResponse{
				ResourceGroupName: tokenReq.GetResourceGroupName(),
				KeyspaceId: &rmpb.KeyspaceIDValue{
					Value: constants.NullKeyspaceID,
				},
			})
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func startTestRMServer(t *testing.T, id string) (string, *testRMServer, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	rmServer := &testRMServer{id: id}
	rmpb.RegisterResourceManagerServer(server, rmServer)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()

	addr := "http://" + listener.Addr().String()
	cleanup := func() {
		server.Stop()
		_ = listener.Close()
		<-done
	}
	return addr, rmServer, cleanup
}

func waitResourceManagerDiscoveryReady(t *testing.T, discovery *sd.ResourceManagerDiscovery) {
	t.Helper()
	require.Eventually(t, func() bool {
		return discovery.GetConn() != nil
	}, 3*time.Second, 10*time.Millisecond)
}

func newTestResourceManagerDiscovery(t *testing.T, ctx context.Context, serviceURL string) *sd.ResourceManagerDiscovery {
	t.Helper()
	participant := &rmpb.Participant{ListenUrls: []string{serviceURL}}
	value, err := proto.Marshal(participant)
	require.NoError(t, err)

	opt := opt.NewOption()
	opt.InitMetrics = false

	discovery := sd.NewResourceManagerDiscovery(
		ctx,
		1,
		&fakeMetaStorageClient{value: value},
		nil,
		opt,
		func(string) error { return nil },
	)
	discovery.Init()
	waitResourceManagerDiscoveryReady(t, discovery)
	return discovery
}

func newInnerClientForRMRouteTest(t *testing.T, ctx context.Context, pdAddr string) *innerClient {
	t.Helper()
	conn, err := grpcutil.GetClientConn(ctx, pdAddr, nil)
	require.NoError(t, err)

	serviceDiscovery := newTestServiceDiscovery(pdAddr, conn)
	opt := opt.NewOption()
	opt.InitMetrics = false

	inner := &innerClient{
		keyspaceID:              constants.NullKeyspaceID,
		serviceDiscovery:        serviceDiscovery,
		updateTokenConnectionCh: make(chan struct{}, 1),
		ctx:                     ctx,
		option:                  opt,
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})
	return inner
}

func TestResourceManagerMetadataRPCsAlwaysUsePD(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	pdAddr, pdServer, pdCleanup := startTestRMServer(t, "pd")
	t.Cleanup(pdCleanup)
	rmAddr, rmServer, rmCleanup := startTestRMServer(t, "rm")
	t.Cleanup(rmCleanup)

	inner := newInnerClientForRMRouteTest(t, ctx, pdAddr)
	inner.resourceManagerDiscovery = newTestResourceManagerDiscovery(t, ctx, rmAddr)
	t.Cleanup(inner.resourceManagerDiscovery.Close)

	cli := &client{inner: inner}

	_, err := cli.ListResourceGroups(ctx)
	require.NoError(t, err)
	_, err = cli.GetResourceGroup(ctx, "test-group")
	require.NoError(t, err)
	_, err = cli.AddResourceGroup(ctx, &rmpb.ResourceGroup{Name: "test-group"})
	require.NoError(t, err)
	_, err = cli.ModifyResourceGroup(ctx, &rmpb.ResourceGroup{Name: "test-group"})
	require.NoError(t, err)
	_, err = cli.DeleteResourceGroup(ctx, "test-group")
	require.NoError(t, err)

	require.EqualValues(t, 1, pdServer.listCount.Load())
	require.EqualValues(t, 1, pdServer.getCount.Load())
	require.EqualValues(t, 1, pdServer.addCount.Load())
	require.EqualValues(t, 1, pdServer.modifyCount.Load())
	require.EqualValues(t, 1, pdServer.deleteCount.Load())

	require.EqualValues(t, 0, rmServer.listCount.Load())
	require.EqualValues(t, 0, rmServer.getCount.Load())
	require.EqualValues(t, 0, rmServer.addCount.Load())
	require.EqualValues(t, 0, rmServer.modifyCount.Load())
	require.EqualValues(t, 0, rmServer.deleteCount.Load())
}

func TestTryResourceManagerConnectUsesRMForTokenAndFallbackToPD(t *testing.T) {
	t.Run("prefer-rm-when-available", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		pdAddr, pdServer, pdCleanup := startTestRMServer(t, "pd")
		t.Cleanup(pdCleanup)
		rmAddr, rmServer, rmCleanup := startTestRMServer(t, "rm")
		t.Cleanup(rmCleanup)

		inner := newInnerClientForRMRouteTest(t, ctx, pdAddr)
		inner.resourceManagerDiscovery = newTestResourceManagerDiscovery(t, ctx, rmAddr)
		t.Cleanup(inner.resourceManagerDiscovery.Close)

		connection := &resourceManagerConnectionContext{}
		err := inner.tryResourceManagerConnect(ctx, connection)
		require.NoError(t, err)
		require.NotNil(t, connection.stream)
		t.Cleanup(connection.reset)

		err = connection.stream.Send(&rmpb.TokenBucketsRequest{
			Requests: []*rmpb.TokenBucketRequest{{ResourceGroupName: "test-group"}},
		})
		require.NoError(t, err)
		_, err = connection.stream.Recv()
		require.NoError(t, err)

		require.EqualValues(t, 1, rmServer.tokenCount.Load())
		require.EqualValues(t, 0, pdServer.tokenCount.Load())
	})

	t.Run("fallback-to-pd-when-rm-unavailable", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		pdAddr, pdServer, pdCleanup := startTestRMServer(t, "pd")
		t.Cleanup(pdCleanup)

		inner := newInnerClientForRMRouteTest(t, ctx, pdAddr)

		connection := &resourceManagerConnectionContext{}
		err := inner.tryResourceManagerConnect(ctx, connection)
		require.NoError(t, err)
		require.NotNil(t, connection.stream)
		t.Cleanup(connection.reset)

		err = connection.stream.Send(&rmpb.TokenBucketsRequest{
			Requests: []*rmpb.TokenBucketRequest{{ResourceGroupName: "test-group"}},
		})
		require.NoError(t, err)
		_, err = connection.stream.Recv()
		require.NoError(t, err)

		require.EqualValues(t, 1, pdServer.tokenCount.Load())
	})
}
