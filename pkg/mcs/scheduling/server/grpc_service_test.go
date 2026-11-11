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
	for _, count := range []uint32{131073, math.MaxUint32} {
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

			request.Region = region.Clone(core.SetRegionVersion(0)).GetMeta()
			resp, err = svc.AskBatchSplit(t.Context(), request)
			require.NoError(t, err)
			require.Contains(t, resp.GetHeader().GetError().GetMessage(), "invalid region epoch")
			require.Empty(t, resp.GetIds())
			require.Zero(t, client.calls, "region validation must precede count validation and allocation")

			client.allocErr = nil
			request.Region = region.GetMeta()
			request.SplitCount = 1
			resp, err = svc.AskBatchSplit(t.Context(), request)
			require.NoError(t, err)
			require.Nil(t, resp.GetHeader().GetError())
			require.Len(t, resp.GetIds(), 1)
		})
	}
}

func TestAskBatchSplitRejectsIDCountOverflowBeforeAllocation(t *testing.T) {
	svc, cluster, _ := newTestSchedulingServiceForSplit(t)
	client := &splitScatterPDClient{next: 1000, allocErr: errors.New("unexpected split ID allocation")}
	cluster.SwitchPDLeader(client)
	region := newAffinitySplitTestRegion()
	cluster.PutRegion(region)
	peers := make([]*metapb.Peer, 32767)
	for i := range peers {
		peers[i] = &metapb.Peer{Id: uint64(i + 1), StoreId: uint64(i + 1)}
	}
	request := &schedulingpb.AskBatchSplitRequest{
		Region:     region.Clone(core.SetPeers(peers)).GetMeta(),
		SplitCount: core.MaxBatchSplitCount,
	}
	resp, err := svc.AskBatchSplit(t.Context(), request)
	require.Zero(t, client.calls, "overflowing ID counts must not reach the PD allocator")
	require.NoError(t, err)
	require.Contains(t, resp.GetHeader().GetError().GetMessage(), "split ID count exceeds maximum")
	require.Empty(t, resp.GetIds())
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
		{131072, 32766, 4294836224, true},
		{131072, 32767, 0, false},
		{131072, math.MaxInt, 0, false},
	} {
		t.Run(fmt.Sprintf("%d-peers-%d", test.splits, test.peers), func(t *testing.T) {
			count, batch := batchSplitIDCount(test.splits, test.peers)
			require.Equal(t, test.count, count)
			require.Equal(t, test.batch, batch)
		})
	}
}

func TestAskBatchSplitLegacyAllocation(t *testing.T) {
	for _, count := range []uint32{0, 1, 131072} {
		t.Run(strconv.FormatUint(uint64(count), 10), func(t *testing.T) {
			svc, cluster, _ := newTestSchedulingServiceForSplit(t)
			cluster.SwitchPDLeader(&splitScatterPDClient{next: 1000, nonBatch: true})
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

type captureHeartbeatStream struct {
	ch chan core.RegionHeartbeatResponse
}

func (s *captureHeartbeatStream) Send(resp core.RegionHeartbeatResponse) error {
	s.ch <- resp
	return nil
}

type splitScatterPDClient struct {
	pdpb.PDClient
	next        uint64
	nonBatch    bool
	allocErr    error
	headerErr   *pdpb.Error
	headerErrAt int
	onAlloc     func(context.Context, int)
	calls       int
}

func (c *splitScatterPDClient) AllocID(ctx context.Context, req *pdpb.AllocIDRequest, _ ...grpc.CallOption) (*pdpb.AllocIDResponse, error) {
	c.calls++
	if c.onAlloc != nil {
		c.onAlloc(ctx, c.calls)
	}
	if c.headerErr != nil && c.calls == c.headerErrAt {
		return &pdpb.AllocIDResponse{Header: &pdpb.ResponseHeader{Error: c.headerErr}}, nil
	}
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

func TestAskBatchSplitRejectsPeerWorkBeforeAllocation(t *testing.T) {
	for _, test := range []struct {
		name    string
		peers   int
		count   uint32
		newer   bool
		message string
	}{
		{"peer product", 32766, core.MaxBatchSplitCount, true, "split response exceeds maximum"},
		{"same configuration mismatch", 4, 1, false, "peers do not match"},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc, cluster, _ := newTestSchedulingServiceForSplit(t)
			client := &splitScatterPDClient{next: 1000, allocErr: errors.New("allocator reached")}
			cluster.SwitchPDLeader(client)
			region := newAffinitySplitTestRegion()
			cluster.PutRegion(region)
			peers := make([]*metapb.Peer, test.peers)
			for i := range peers {
				peers[i] = &metapb.Peer{Id: uint64(i + 1), StoreId: uint64(i + 1)}
			}
			meta := region.Clone(core.SetPeers(peers)).GetMeta()
			if test.newer {
				meta.RegionEpoch.ConfVer++
			}
			request := &schedulingpb.AskBatchSplitRequest{Region: meta, SplitCount: test.count}
			require.Less(t, request.Size(), 1<<20)
			resp, err := svc.AskBatchSplit(t.Context(), request)
			require.Zero(t, client.calls)
			require.NoError(t, err)
			require.Contains(t, resp.GetHeader().GetError().GetMessage(), test.message)
			require.Empty(t, resp.GetIds())
		})
	}
}

func TestAskBatchSplitAllocationHeaderError(t *testing.T) {
	for _, call := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(call), func(t *testing.T) {
			svc, cluster, _ := newTestSchedulingServiceForSplit(t)
			client := &splitScatterPDClient{next: 1000, nonBatch: true, headerErr: &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: "id exhausted"}, headerErrAt: call}
			cluster.SwitchPDLeader(client)
			region := newAffinitySplitTestRegion()
			cluster.PutRegion(region)
			resp, err := svc.AskBatchSplit(t.Context(), &schedulingpb.AskBatchSplitRequest{Region: region.GetMeta(), SplitCount: 10})
			require.ErrorContains(t, err, "id exhausted")
			require.Nil(t, resp)
			require.Equal(t, call, client.calls)
		})
	}
}

func TestAskBatchSplitAllocationCancellation(t *testing.T) {
	for _, call := range []int{0, 1, 2, 3} {
		t.Run(strconv.Itoa(call), func(t *testing.T) {
			svc, cluster, _ := newTestSchedulingServiceForSplit(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if call == 0 {
				cancel()
			}
			client := &splitScatterPDClient{next: 1000, nonBatch: true, onAlloc: func(ctx context.Context, n int) {
				if n == call {
					cancel()
					<-ctx.Done()
				}
			}}
			cluster.SwitchPDLeader(client)
			region := newAffinitySplitTestRegion()
			cluster.PutRegion(region)
			resp, err := svc.AskBatchSplit(ctx, &schedulingpb.AskBatchSplitRequest{Region: region.GetMeta(), SplitCount: 10})
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, resp)
			require.Equal(t, call, client.calls)
		})
	}
}
