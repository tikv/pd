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
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
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

func TestAskBatchSplitRejectsOversizedCountBeforeAllocation(t *testing.T) {
	for _, count := range []uint32{131073, 1000000, 1 << 30, math.MaxUint32} {
		t.Run(strconv.FormatUint(uint64(count), 10), func(t *testing.T) {
			svc, cluster, _ := newTestSchedulingServiceForSplit(t)
			client := &splitScatterPDClient{next: 1000, allocErr: errors.New("unexpected split ID allocation")}
			cluster.SwitchPDLeader(client)
			region := newAffinitySplitTestRegion()
			cluster.PutRegion(region)
			request := &schedulingpb.AskBatchSplitRequest{
				Region: region.GetMeta(), SplitCount: count,
			}
			resp, err := svc.AskBatchSplit(t.Context(), request)
			require.Zero(t, client.calls, "oversized requests must not reach the PD allocator")
			require.NoError(t, err)
			require.Contains(t, resp.GetHeader().GetError().GetMessage(), "exceeds maximum")
			require.Empty(t, resp.GetIds())
			require.Equal(t, uint64(1000), client.next)

			client.allocErr = nil
			request.SplitCount = 1
			resp, err = svc.AskBatchSplit(t.Context(), request)
			require.NoError(t, err)
			require.Nil(t, resp.GetHeader().GetError())
			require.Len(t, resp.GetIds(), 1)
		})
	}
}

func TestAskBatchSplitLargeCountCompatibility(t *testing.T) {
	svc, cluster, _ := newTestSchedulingServiceForSplit(t)
	client := &splitScatterPDClient{next: 1000}
	cluster.SwitchPDLeader(client)
	region := newAffinitySplitTestRegion()
	cluster.PutRegion(region)
	staleRegion := region.Clone(core.SetRegionVersion(0))
	// Large counts must reach normal region validation without consuming IDs.
	for _, count := range []uint32{1000000, 1 << 30, math.MaxUint32} {
		resp, err := svc.AskBatchSplit(t.Context(), &schedulingpb.AskBatchSplitRequest{
			Region: staleRegion.GetMeta(), SplitCount: count,
		})
		require.NoError(t, err)
		require.Contains(t, resp.GetHeader().GetError().GetMessage(), "invalid region epoch")
		require.Empty(t, resp.GetIds())
	}
	require.Equal(t, uint64(1000), client.next)
}

func TestBatchSplitIDCount(t *testing.T) {
	for _, test := range []struct {
		splits uint32
		peers  int
		count  uint32
		batch  bool
	}{
		{0, 3, 0, true},
		{1, 3, 4, true},
		{838861, 3, 3355444, true},
		{1000000, 3, 4000000, true},
		{1073741823, 3, 4294967292, true},
		{1 << 30, 3, 0, false},
		{math.MaxUint32, 3, 0, false},
		{math.MaxUint32, 0, math.MaxUint32, true},
		{math.MaxUint32, math.MaxInt, 0, false},
	} {
		t.Run(fmt.Sprintf("%d-peers-%d", test.splits, test.peers), func(t *testing.T) {
			count, batch := batchSplitIDCount(test.splits, test.peers)
			require.Equal(t, test.count, count)
			require.Equal(t, test.batch, batch)
		})
	}
}

func TestAskBatchSplitCountCompatibility(t *testing.T) {
	for _, nonBatch := range []bool{false, true} {
		for _, count := range []uint32{0, 1, 10, 10000, 100000, 131072} {
			t.Run(fmt.Sprintf("non-batch-%t-count-%d", nonBatch, count), func(t *testing.T) {
				svc, cluster, _ := newTestSchedulingServiceForSplit(t)
				cluster.SwitchPDLeader(&splitScatterPDClient{next: 1000, nonBatch: nonBatch})
				region := newAffinitySplitTestRegion()
				cluster.PutRegion(region)
				resp, err := svc.AskBatchSplit(t.Context(), &schedulingpb.AskBatchSplitRequest{
					Region: region.GetMeta(), SplitCount: count,
				})
				require.NoError(t, err)
				require.Nil(t, resp.GetHeader().GetError())
				require.Len(t, resp.GetIds(), int(count))
				var previous uint64
				for _, split := range resp.GetIds() {
					require.Greater(t, split.GetNewRegionId(), previous)
					previous = split.GetNewRegionId()
					require.Len(t, split.GetNewPeerIds(), len(region.GetPeers()))
					for _, peer := range split.GetNewPeerIds() {
						require.Greater(t, peer, previous)
						previous = peer
					}
				}
			})
		}
	}
}

type captureHeartbeatStream struct {
	ch chan core.RegionHeartbeatResponse
}

func (s *captureHeartbeatStream) Send(resp core.RegionHeartbeatResponse) error {
	s.ch <- resp
	return nil
}

type splitScatterPDClient struct {
	pdpb.PDClient
	next     uint64
	nonBatch bool
	allocErr error
	calls    int
}

func (c *splitScatterPDClient) AllocID(_ context.Context, req *pdpb.AllocIDRequest, _ ...grpc.CallOption) (*pdpb.AllocIDResponse, error) {
	c.calls++
	if c.allocErr != nil {
		return nil, c.allocErr
	}
	count := req.GetCount()
	if count == 0 || c.nonBatch {
		count = 1
	}
	c.next += uint64(count)
	return &pdpb.AllocIDResponse{
		Header: &pdpb.ResponseHeader{},
		Id:     c.next - 1,
		Count:  count,
	}, nil
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
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
	cluster.SwitchPDLeader(&splitScatterPDClient{next: 1000})

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
		http.DefaultClient,
		"",
	)
	re.NoError(err)

	server := &Server{}
	server.cluster.Store(cluster)
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
