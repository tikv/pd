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

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestCleanupClusterResources(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hbStreams := hbstream.NewHeartbeatStreams(ctx, constant.SchedulingServiceName, core.NewBasicCluster())
	basicCluster := core.NewBasicCluster()
	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	cluster := &Cluster{}

	s := &Server{
		basicCluster: basicCluster,
		hbStreams:    hbStreams,
		storage:      storage,
	}
	s.cluster.Store(cluster)

	s.cleanupClusterResources()
	s.cleanupClusterResources()

	re.Nil(s.GetCluster())
	re.Nil(s.basicCluster)
	re.Nil(s.hbStreams)
	re.Nil(s.storage)
}

func TestRegionHeartbeatReturnsNotBootstrappedWhenHeartbeatStreamsMissing(t *testing.T) {
	re := require.New(t)
	cluster := &Cluster{BasicCluster: core.NewBasicCluster()}
	cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, Address: "store-1"}))
	service := &Service{Server: &Server{service: &Service{}, hbStreams: nil}}
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
	service := &Service{Server: &Server{service: &Service{}, metaWatcher: nil}}
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
	s.hbStreams = nil // Simulate an in-flight heartbeat observing a stale cluster after cleanup has already cleared hbStreams.

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
	re.Nil(s.hbStreams)
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
