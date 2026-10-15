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

package servicediscovery

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/kvproto/pkg/routerpb"
	"github.com/pingcap/kvproto/pkg/tsopb"

	"github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/opt"
)

// startOtherServiceServer starts a gRPC server which provides none of the PD
// client facing services, simulating an address reused by another service.
func startOtherServiceServer(t *testing.T) string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)
	return "http://" + lis.Addr().String()
}

func TestServiceDiscoveriesRecreateConnOnMissingService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pdDiscovery := &serviceDiscovery{ctx: ctx, option: opt.NewOption()}
	tsoDiscovery := &tsoServiceDiscovery{ctx: ctx, option: opt.NewOption()}
	routerDiscovery := &routerServiceDiscovery{ctx: ctx, option: opt.NewOption()}
	defer func() {
		for _, d := range []ServiceDiscovery{pdDiscovery, tsoDiscovery, routerDiscovery} {
			d.GetClientConns().Range(func(_, cc any) bool {
				_ = cc.(*grpc.ClientConn).Close()
				return true
			})
		}
	}()

	for _, tc := range []struct {
		name      string
		discovery ServiceDiscovery
		invoke    func(context.Context, *grpc.ClientConn) error
	}{
		{
			name:      "pd",
			discovery: pdDiscovery,
			invoke: func(ctx context.Context, cc *grpc.ClientConn) error {
				_, err := pdpb.NewPDClient(cc).GetMembers(ctx, &pdpb.GetMembersRequest{})
				return err
			},
		},
		{
			name:      "tso",
			discovery: tsoDiscovery,
			invoke: func(ctx context.Context, cc *grpc.ClientConn) error {
				_, err := tsopb.NewTSOClient(cc).FindGroupByKeyspaceID(ctx, &tsopb.FindGroupByKeyspaceIDRequest{})
				return err
			},
		},
		{
			name:      "router",
			discovery: routerDiscovery,
			invoke: func(ctx context.Context, cc *grpc.ClientConn) error {
				_, err := routerpb.NewRouterClient(cc).GetRegion(ctx, &pdpb.GetRegionRequest{})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			url := startOtherServiceServer(t)
			cc, err := tc.discovery.GetOrCreateGRPCConn(url)
			re.NoError(err)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			re.Equal(codes.Unimplemented, status.Code(tc.invoke(ctx, cc)))
			re.Equal(connectivity.Shutdown, cc.GetState())

			newCC, err := tc.discovery.GetOrCreateGRPCConn(url)
			re.NoError(err)
			re.NotSame(cc, newCC)
			re.NotEqual(connectivity.Shutdown, newCC.GetState())
		})
	}
}

func TestResourceManagerDiscoveryRecreatesConnOnMissingService(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	url := startOtherServiceServer(t)

	var notifications int
	discovery := NewResourceManagerDiscovery(ctx, 1, nil, nil, opt.NewOption(), func(string) error {
		notifications++
		return nil
	})
	defer discovery.Close()
	discovery.resetConn(url)
	re.Equal(1, notifications)
	cc := discovery.GetConn()
	re.NotNil(cc)

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	_, err := rmpb.NewResourceManagerClient(cc).ListResourceGroups(callCtx, &rmpb.ListResourceGroupsRequest{})
	re.Equal(codes.Unimplemented, status.Code(err))
	re.Equal(connectivity.Shutdown, cc.GetState())

	// The service URL is unchanged, but the closed connection must be recreated.
	discovery.resetConn(url)
	newCC := discovery.GetConn()
	re.NotNil(newCC)
	re.NotSame(cc, newCC)
	re.NotEqual(connectivity.Shutdown, newCC.GetState())
	re.Equal(url, discovery.GetServiceURL())
	// The token stream on the closed connection must switch to the new one.
	re.Equal(2, notifications)
}

// fixedRevisionMetaStorageClient always returns the same revision, i.e., the
// resource manager primary is unchanged.
type fixedRevisionMetaStorageClient struct {
	countingMetaStorageClient
}

func (c *fixedRevisionMetaStorageClient) Get(context.Context, []byte, ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	return &meta_storagepb.GetResponse{
		Header: &meta_storagepb.ResponseHeader{Revision: 1},
		Kvs:    []*meta_storagepb.KeyValue{{Value: c.value}},
		Count:  1,
	}, nil
}

func TestResourceManagerDiscoveryUpdateRecreatesClosedConn(t *testing.T) {
	re := require.New(t)
	oldRetryInterval := serviceURLRetryInterval
	serviceURLRetryInterval = 10 * time.Millisecond
	defer func() {
		serviceURLRetryInterval = oldRetryInterval
	}()
	url := startOtherServiceServer(t)
	value, err := proto.Marshal(&rmpb.Participant{ListenUrls: []string{url}})
	re.NoError(err)
	metaCli := &fixedRevisionMetaStorageClient{countingMetaStorageClient{value: value}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	discovery := NewResourceManagerDiscovery(ctx, 1, metaCli, nil, opt.NewOption(), func(string) error { return nil })
	discovery.Init()
	defer discovery.Close()
	re.Eventually(func() bool {
		return discovery.GetConn() != nil
	}, 5*time.Second, 10*time.Millisecond)
	cc := discovery.GetConn()
	re.NoError(cc.Close())

	discovery.ScheduleUpdateServiceURL()
	re.Eventually(func() bool {
		newCC := discovery.GetConn()
		return newCC != nil && newCC != cc && newCC.GetState() != connectivity.Shutdown
	}, 5*time.Second, 10*time.Millisecond)
}

// staticRegistryMetaStorageClient returns the same router service registry.
type staticRegistryMetaStorageClient struct {
	countingMetaStorageClient
}

func (c *staticRegistryMetaStorageClient) Get(context.Context, []byte, ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	return &meta_storagepb.GetResponse{
		Header: &meta_storagepb.ResponseHeader{Revision: 1},
		Kvs:    []*meta_storagepb.KeyValue{{Value: c.value}},
		Count:  1,
	}, nil
}

func TestRouterServiceDiscoveryRecreatesClosedNodeConn(t *testing.T) {
	re := require.New(t)
	url := startOtherServiceServer(t)
	value, err := json.Marshal(&metastorage.ServiceRegistryEntry{ServiceAddr: url})
	re.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	discovery := &routerServiceDiscovery{
		ServiceDiscovery:  NewMockServiceDiscovery(nil, nil),
		ctx:               ctx,
		cancel:            cancel,
		metaCli:           &staticRegistryMetaStorageClient{countingMetaStorageClient{value: value}},
		option:            opt.NewOption(),
		checkMembershipCh: make(chan struct{}, 1),
		balancer:          newServiceBalancer(emptyErrorFn),
		callbacks:         newServiceCallbacks(),
	}
	defer discovery.Close()
	re.NoError(discovery.updateMember())
	node, ok := discovery.nodes.Load(url)
	re.True(ok)
	cc := node.(*serviceClient).GetClientConn()
	re.NotNil(cc)
	re.NoError(cc.Close())

	// The router service URLs are unchanged, but the closed connection must be recreated.
	re.NoError(discovery.updateMember())
	node, ok = discovery.nodes.Load(url)
	re.True(ok)
	newCC := node.(*serviceClient).GetClientConn()
	re.NotNil(newCC)
	re.NotSame(cc, newCC)
	conns := 0
	discovery.GetClientConns().Range(func(_, value any) bool {
		conns++
		re.Same(newCC, value)
		return true
	})
	re.Equal(1, conns)
}

func TestServiceDiscoverySwitchLeaderRecreatesClosedConn(t *testing.T) {
	re := require.New(t)
	url := startOtherServiceServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	discovery := &serviceDiscovery{ctx: ctx, option: opt.NewOption(), callbacks: newServiceCallbacks()}
	defer discovery.clientConns.Range(func(_, cc any) bool {
		_ = cc.(*grpc.ClientConn).Close()
		return true
	})
	discovery.leader.Store(&serviceClient{})

	changed, err := discovery.switchLeader(url)
	re.NoError(err)
	re.True(changed)
	cc := discovery.GetServiceClient().GetClientConn()
	re.NotNil(cc)

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	_, err = pdpb.NewPDClient(cc).GetMembers(callCtx, &pdpb.GetMembersRequest{})
	re.Equal(codes.Unimplemented, status.Code(err))
	re.Nil(discovery.GetServiceClient().GetClientConn())

	// The leader is unchanged, but the closed connection must be recreated.
	changed, err = discovery.switchLeader(url)
	re.NoError(err)
	re.True(changed)
	newCC := discovery.GetServiceClient().GetClientConn()
	re.NotNil(newCC)
	re.NotSame(cc, newCC)
}
