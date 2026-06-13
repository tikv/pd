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
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

type captureHeartbeatStream struct {
	ch chan core.RegionHeartbeatResponse
}

func (s *captureHeartbeatStream) Send(resp core.RegionHeartbeatResponse) error {
	s.ch <- resp
	return nil
}

type splitScatterPDClient struct {
	pdpb.PDClient
	next uint64
}

func (c *splitScatterPDClient) AllocID(_ context.Context, _ *pdpb.AllocIDRequest, _ ...grpc.CallOption) (*pdpb.AllocIDResponse, error) {
	c.next++
	return &pdpb.AllocIDResponse{
		Header: &pdpb.ResponseHeader{},
		Id:     c.next,
	}, nil
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func waitHeartbeatStreamBound(t *testing.T, hbStreams *hbstream.HeartbeatStreams, region *core.RegionInfo, stream *captureHeartbeatStream) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(time.Second)
	for {
		hbStreams.SendMsg(region, &hbstream.Operation{})
		select {
		case <-stream.ch:
			for {
				select {
				case <-stream.ch:
				default:
					return
				}
			}
		case <-ticker.C:
		case <-timeout:
			require.FailNow(t, "expected heartbeat stream to be bound")
		}
	}
}

func TestAskBatchSplitRejectsAffinityAutoSplit(t *testing.T) {
	re := require.New(t)

	svc, cluster, stream := newTestSchedulingServiceForSplit(t)

	region := newAffinitySplitTestRegion()
	cluster.PutRegion(region)
	err := cluster.GetAffinityManager().CreateAffinityGroups([]affinity.GroupKeyRanges{
		{
			GroupID: "g1",
			KeyRanges: []keyutil.KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("z")},
			},
		},
	})
	re.NoError(err)
	_, err = cluster.GetAffinityManager().UpdateAffinityGroupPeers("g1", 1, []uint64{1, 2, 3})
	re.NoError(err)
	hbStreams := cluster.GetCoordinator().GetHeartbeatStreams()
	hbStreams.BindStream(1, stream)
	waitHeartbeatStreamBound(t, hbStreams, region, stream)

	resp, err := svc.AskBatchSplit(context.Background(), &schedulingpb.AskBatchSplitRequest{
		Region:     region.GetMeta(),
		SplitCount: 1,
		Reason:     pdpb.SplitReason_LOAD,
	})
	re.NoError(err)
	re.Equal(schedulingpb.ErrorType_UNKNOWN, resp.GetHeader().GetError().GetType())
	re.Contains(resp.GetHeader().GetError().GetMessage(), "cannot split affinity region")
	re.Empty(resp.GetIds())

	select {
	case msg := <-stream.ch:
		heartbeatResp, ok := msg.(*schedulingpb.RegionHeartbeatResponse)
		re.True(ok)
		re.Equal(region.GetID(), heartbeatResp.GetRegionId())
		re.NotNil(heartbeatResp.GetChangeSplit())
		re.False(heartbeatResp.GetChangeSplit().GetAutoSplitEnabled())
	case <-time.After(time.Second):
		re.Fail("expected ChangeSplit heartbeat response")
	}
}

func TestAskBatchSplitRecordsSplitScatterInSchedulingService(t *testing.T) {
	re := require.New(t)

	svc, cluster, _ := newTestSchedulingServiceForSplit(t)
	re.True(cluster.SwitchAPIServerLeader(&splitScatterPDClient{next: 1000}))

	region := newAffinitySplitTestRegion()
	cluster.PutRegion(region)
	resp, err := svc.AskBatchSplit(context.Background(), &schedulingpb.AskBatchSplitRequest{
		Region:     region.GetMeta(),
		SplitCount: 1,
		Reason:     pdpb.SplitReason_LOAD,
	})
	re.NoError(err)
	re.Empty(resp.GetHeader().GetError())
	re.Len(resp.GetIds(), 1)

	re.Equal(float64(2), splitScatterPendingMetricValue(t))
}

func newTestSchedulingServiceForSplit(
	t *testing.T,
) (*Service, *Cluster, *captureHeartbeatStream) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	oldClusterID := keypath.ClusterID()
	keypath.SetClusterID(1)
	t.Cleanup(func() {
		keypath.SetClusterID(oldClusterID)
	})
	cfg, err := GenerateConfig(&config.Config{
		Name:                "test-scheduling",
		ListenAddr:          "http://127.0.0.1:0",
		AdvertiseListenAddr: "http://127.0.0.1:0",
		BackendEndpoints:    "http://127.0.0.1:2379",
	})
	re.NoError(err)
	cfg.Schedule.AffinityScheduleLimit = 4
	cfg.Schedule.MaxAffinityMergeRegionSize = 10
	cfg.Schedule.SplitScatterScheduleLimit = 4

	basicCluster := core.NewBasicCluster()
	for i := uint64(1); i <= 3; i++ {
		basicCluster.PutStore(core.NewStoreInfo(&metapb.Store{
			Id:      i,
			Address: fmt.Sprintf("mock://tikv-%d", i),
			State:   metapb.StoreState_Up,
		}))
	}

	storage := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	hbStreams := hbstream.NewHeartbeatStreams(ctx, constant.SchedulingServiceName, basicCluster)
	persistConfig := config.NewPersistConfig(
		cfg,
		cache.NewStringTTL(ctx, sc.DefaultGCInterval, sc.DefaultTTL),
	)
	cluster, err := NewCluster(
		ctx,
		persistConfig,
		storage,
		basicCluster,
		hbStreams,
		make(chan struct{}),
	)
	re.NoError(err)

	server := &Server{cluster: cluster}
	stream := &captureHeartbeatStream{ch: make(chan core.RegionHeartbeatResponse, 16)}
	t.Cleanup(hbStreams.Close)
	return &Service{Server: server}, cluster, stream
}

func newAffinitySplitTestRegion() *core.RegionInfo {
	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1},
		{Id: 22, StoreId: 2},
		{Id: 33, StoreId: 3},
	}
	return core.NewRegionInfo(&metapb.Region{
		Id:          100,
		StartKey:    []byte("b"),
		EndKey:      []byte("y"),
		Peers:       peers,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}, peers[0],
		core.SetApproximateSize(5),
		core.SetApproximateKeys(int64(5*sc.RegionSizeToKeysRatio)),
	)
}

func splitScatterPendingMetricValue(t *testing.T) float64 {
	t.Helper()
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, metricFamily := range metricFamilies {
		if metricFamily.GetName() != "pd_checker_split_scatter_pending" {
			continue
		}
		var value float64
		for _, metric := range metricFamily.GetMetric() {
			value += metric.GetGauge().GetValue()
		}
		return value
	}
	require.FailNow(t, "split scatter pending metric not found")
	return 0
}
