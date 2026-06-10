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

package server

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/metadata"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestStopCluster(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hbStreams := hbstream.NewHeartbeatStreams(ctx, constant.SchedulingServiceName, core.NewBasicCluster())
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	cluster := &Cluster{hbStreams: hbStreams, storage: storage}

	s := &Server{}
	s.cluster.Store(cluster)

	s.stopCluster()

	re.Nil(s.GetCluster())
	re.Nil(cluster.GetHeartbeatStreams())
	re.Nil(cluster.GetStorage())
}

func TestGetClusterBeforeStoredDoesNotPanic(t *testing.T) {
	re := require.New(t)
	s := &Server{}

	re.NotPanics(func() {
		re.Nil(s.GetCluster())
		re.Nil(s.GetBasicCluster())
	})
}

// TestMultipleLeaderTermsNoHbStreamGoroutineLeak is a regression test for the
// production memory leak where each leader→follower transition left behind a
// leaked hbstream.run() goroutine. The leaked goroutines held a strong GC root
// to their term's BasicCluster via the storeInformer field, preventing the
// full region tree (~400 MB/term) from being collected. Four leaked goroutines
// were confirmed in the follower heap profile (IDs 11227, 1829530, 3823982,
// 9489109) with a total inuse heap of 1641 MB.
//
// The fix: stopCluster() calls cleanupRuntimeResources() which calls
// hbStreams.Close(). Close() cancels hbStreamCtx and waits (wg.Wait) for
// run() to exit before returning. The test exploits the LIFO ordering of
// deferred calls — goleak fires before cancel() — so any goroutine still
// blocking on ctx.Done() (instead of having been stopped by Close()) is
// detected as a leak.
func TestMultipleLeaderTermsNoHbStreamGoroutineLeak(t *testing.T) {
	// Simulate the server-level root context shared across all leadership terms.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // executes LAST (registered first)
	// goleak executes FIRST (registered second, LIFO), before cancel().
	// Goroutines that have not yet exited via Close() are still blocked on
	// ctx.Done() at this point and will be detected as leaks.
	defer goleak.VerifyNone(t, testutil.LeakOptions...)

	s := &Server{}

	// Simulate 4 leader→follower transitions on the same server instance,
	// matching the 4 leaked goroutines observed in the production profile.
	for range 4 {
		hbStreams := hbstream.NewHeartbeatStreams(ctx, constant.SchedulingServiceName, core.NewBasicCluster())
		s.cluster.Store(&Cluster{
			hbStreams: hbStreams,
			storage:   endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil),
		})
		s.stopCluster()
	}

	// After all terms, GetCluster() must return nil so incoming heartbeat
	// requests are rejected rather than processed by a stale cluster.
	require.Nil(t, s.GetCluster())
}

func TestRegionHeartbeatReturnsNotBootstrappedWhenHeartbeatStreamsMissing(t *testing.T) {
	re := require.New(t)
	cluster := &Cluster{BasicCluster: core.NewBasicCluster()}
	cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, Address: "store-1"}))
	service := &Service{Server: &Server{service: &Service{}}}
	service.service = service
	service.cluster.Store(cluster)

	stream := &mockRegionHeartbeatStream{
		recvs: []*schedulingpb.RegionHeartbeatRequest{{
			Leader: &metapb.Peer{StoreId: 1},
			Region: &metapb.Region{Id: 1},
		}},
	}

	err := service.RegionHeartbeat(stream)
	re.NoError(err)
	re.Len(stream.sent, 1)
	re.Equal(schedulingpb.ErrorType_NOT_BOOTSTRAPPED, stream.sent[0].GetHeader().GetError().GetType())
}

func TestStoreHeartbeatReturnsNotBootstrappedWhenMetaWatcherMissing(t *testing.T) {
	re := require.New(t)
	service := &Service{Server: &Server{service: &Service{}}}
	service.service = service
	service.cluster.Store(&Cluster{BasicCluster: core.NewBasicCluster()})

	resp, err := service.StoreHeartbeat(context.Background(), &schedulingpb.StoreHeartbeatRequest{
		Stats: &pdpb.StoreStats{StoreId: 1},
	})
	re.NoError(err)
	re.Equal(schedulingpb.ErrorType_NOT_BOOTSTRAPPED, resp.GetHeader().GetError().GetType())
}

func TestRegionHeartbeatDuringCleanupDoesNotPanic(t *testing.T) {
	re := require.New(t)

	basicCluster := core.NewBasicCluster()
	basicCluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, Address: "store-1"}))

	s := &Server{}
	s.cluster.Store(&Cluster{BasicCluster: basicCluster})

	stream := &mockRegionHeartbeatStream{
		recvs: []*schedulingpb.RegionHeartbeatRequest{
			{
				Region: &metapb.Region{
					Id:    1,
					Peers: []*metapb.Peer{{Id: 1, StoreId: 1}},
				},
				Leader: &metapb.Peer{Id: 1, StoreId: 1},
			},
		},
	}

	re.NotPanics(func() {
		_ = (&Service{Server: s}).RegionHeartbeat(stream)
	})
	re.Nil(s.GetCluster().GetHeartbeatStreams())
}

func TestStoreHeartbeatDuringCleanupDoesNotPanic(t *testing.T) {
	re := require.New(t)

	basicCluster := core.NewBasicCluster()
	s := &Server{}
	s.cluster.Store(&Cluster{BasicCluster: basicCluster})

	re.NotPanics(func() {
		_, _ = (&Service{Server: s}).StoreHeartbeat(context.Background(), &schedulingpb.StoreHeartbeatRequest{
			Stats: &pdpb.StoreStats{StoreId: 1},
		})
	})
	re.Nil(s.GetCluster().GetMetaWatcher())
}

func TestClusterResourceGettersHandleNilReceiver(t *testing.T) {
	re := require.New(t)

	var cluster *Cluster
	re.Nil(cluster.GetHeartbeatStreams())
	re.Nil(cluster.GetMetaWatcher())
	re.Nil(cluster.GetStorage())
}

type mockRegionHeartbeatStream struct {
	schedulingpb.Scheduling_RegionHeartbeatServer
	recvs []*schedulingpb.RegionHeartbeatRequest
	sent  []*schedulingpb.RegionHeartbeatResponse
}

func (m *mockRegionHeartbeatStream) Send(resp *schedulingpb.RegionHeartbeatResponse) error {
	m.sent = append(m.sent, resp)
	return nil
}

func (m *mockRegionHeartbeatStream) Recv() (*schedulingpb.RegionHeartbeatRequest, error) {
	if len(m.recvs) == 0 {
		return nil, io.EOF
	}
	req := m.recvs[0]
	m.recvs = m.recvs[1:]
	return req, nil
}

func (*mockRegionHeartbeatStream) SetHeader(metadata.MD) error  { return nil }
func (*mockRegionHeartbeatStream) SendHeader(metadata.MD) error { return nil }
func (*mockRegionHeartbeatStream) SetTrailer(metadata.MD)       {}
func (*mockRegionHeartbeatStream) Context() context.Context     { return context.Background() }
func (*mockRegionHeartbeatStream) SendMsg(any) error            { return nil }
func (*mockRegionHeartbeatStream) RecvMsg(any) error            { return nil }
