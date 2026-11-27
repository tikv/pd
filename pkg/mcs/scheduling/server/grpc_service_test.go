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
	"io"
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/log"

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

func TestAskBatchSplitRejectsInvalidRequestBeforeAllocation(t *testing.T) {
	for _, test := range []struct {
		name    string
		count   uint32
		peers   int
		newer   bool
		message string
	}{
		{"count limit", core.MaxBatchSplitCount + 1, 0, false, "exceeds maximum"},
		{"count overflow", math.MaxUint32, 0, false, "exceeds maximum"},
		{"peer product", core.MaxBatchSplitCount, 32766, true, "split response exceeds maximum"},
		{"ID count overflow", core.MaxBatchSplitCount, 32767, true, "split response exceeds maximum"},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc, cluster, _ := newTestSchedulingServiceForSplit(t, false)
			client := &splitScatterPDClient{next: 1000, allocErr: errors.New("unexpected split ID allocation")}
			cluster.SwitchPDLeader(client)
			region := newAffinitySplitTestRegion()
			cluster.PutRegion(region)
			meta := region.GetMeta()
			if test.peers != 0 {
				peers := make([]*metapb.Peer, test.peers)
				for i := range peers {
					peers[i] = &metapb.Peer{Id: uint64(i + 1), StoreId: uint64(i + 1)}
				}
				meta = region.Clone(core.SetPeers(peers)).GetMeta()
				if test.newer {
					meta.RegionEpoch.ConfVer++
				}
			}
			request := &schedulingpb.AskBatchSplitRequest{Region: meta, SplitCount: test.count}
			require.Less(t, request.Size(), 1<<20)
			resp, err := svc.AskBatchSplit(t.Context(), request)
			require.Zero(t, client.calls, "invalid requests must not reach the PD allocator")
			require.NoError(t, err)
			require.Contains(t, resp.GetHeader().GetError().GetMessage(), test.message)
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

func TestAskBatchSplitLegacyAllocation(t *testing.T) {
	for _, count := range []uint32{0, 1, 3} {
		t.Run(strconv.FormatUint(uint64(count), 10), func(t *testing.T) {
			svc, cluster, _ := newTestSchedulingServiceForSplit(t, false)
			client := &splitScatterPDClient{next: 1000, nonBatch: true}
			cluster.SwitchPDLeader(client)
			region := newAffinitySplitTestRegion()
			cluster.PutRegion(region)
			resp, err := svc.AskBatchSplit(t.Context(), &schedulingpb.AskBatchSplitRequest{
				Region: region.GetMeta(), SplitCount: count,
			})
			require.NoError(t, err)
			require.Nil(t, resp.GetHeader().GetError())
			require.Len(t, resp.GetIds(), int(count))
			require.Equal(t, 1+int(count)*(1+len(region.GetPeers())), client.calls)
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
	if err := ctx.Err(); err != nil {
		return nil, err
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
	resp := &pdpb.AllocIDResponse{
		Header: &pdpb.ResponseHeader{},
		Id:     c.next - 1,
	}
	if count > 1 {
		resp.Count = count
	}
	return resp, nil
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

	svc, cluster, stream := newTestSchedulingServiceForSplit(t, true)

	region := putAffinitySplitTestGroup(t, cluster)
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

	// The group keeps its target after expiration, so the split is still rejected.
	cluster.GetAffinityManager().ExpireAffinityGroup("g1")
	resp, err = svc.AskBatchSplit(context.Background(), &schedulingpb.AskBatchSplitRequest{
		Region: region.GetMeta(), SplitCount: 1, Reason: pdpb.SplitReason_SIZE,
	})
	re.NoError(err)
	re.Contains(resp.GetHeader().GetError().GetMessage(), "cannot split affinity region")
}

func TestRegionHeartbeatRecoversAffinitySplitAfterSchedulingResumes(t *testing.T) {
	re := require.New(t)
	svc, cluster, _ := newTestSchedulingServiceForSplit(t, false)
	region := putAffinitySplitTestGroup(t, cluster)
	streams := cluster.GetCoordinator().GetHeartbeatStreams()
	resp, err := svc.AskBatchSplit(t.Context(), &schedulingpb.AskBatchSplitRequest{
		Region: region.GetMeta(), SplitCount: 1, Reason: pdpb.SplitReason_SIZE,
	})
	re.NoError(err)
	re.Contains(resp.GetHeader().GetError().GetMessage(), "cannot split affinity region")
	denial, ok := streams.PopMsg().(*pdpb.RegionHeartbeatResponse)
	re.True(ok)
	re.NotNil(denial.GetChangeSplit())
	re.False(denial.GetChangeSplit().GetAutoSplitEnabled())

	cluster.persistConfig.SetHaltScheduling(true, "test")
	re.NoError(cluster.GetAffinityManager().DeleteAffinityGroups([]string{"g1"}, true))
	re.NoError(cluster.HandleRegionHeartbeat(region.Clone()))
	re.Zero(streams.MsgLength(), "PD would drop a recovery during a halt")

	cluster.persistConfig.SetHaltScheduling(false, "test")
	re.NoError(cluster.HandleRegionHeartbeat(region.Clone()))
	recovery, ok := streams.PopMsg().(*pdpb.RegionHeartbeatResponse)
	re.True(ok)
	re.Equal(region.GetID(), recovery.GetRegionId())
	re.NotNil(recovery.GetChangeSplit())
	re.True(recovery.GetChangeSplit().GetAutoSplitEnabled())
	re.NoError(cluster.HandleRegionHeartbeat(region.Clone()))
	re.Zero(streams.MsgLength(), "recovery is sent once")
}

func TestAskBatchSplitRecordsSplitScatterInSchedulingService(t *testing.T) {
	re := require.New(t)

	svc, cluster, _ := newTestSchedulingServiceForSplit(t, true)
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

// TestSplitNotificationsAfterClusterStop covers requests that retained the cluster
// before primary cleanup cleared its heartbeat streams.
func TestSplitNotificationsAfterClusterStop(t *testing.T) {
	re := require.New(t)
	svc, cluster, _ := newTestSchedulingServiceForSplit(t, true)
	region := putAffinitySplitTestGroup(t, cluster)
	request := &schedulingpb.AskBatchSplitRequest{Region: region.GetMeta(), SplitCount: 1, Reason: pdpb.SplitReason_LOAD}
	_, err := svc.AskBatchSplit(t.Context(), request)
	re.NoError(err)
	cluster.stopCluster()
	resp, err := svc.AskBatchSplit(t.Context(), request)
	re.NoError(err)
	re.Contains(resp.GetHeader().GetError().GetMessage(), "cannot split affinity region")
	cfg := cluster.persistConfig.GetScheduleConfig().Clone()
	cfg.AffinityScheduleLimit = 0
	cluster.persistConfig.SetScheduleConfig(cfg)
	re.NoError(cluster.HandleRegionHeartbeat(region))
}

func newTestSchedulingServiceForSplit(
	t *testing.T,
	runHeartbeatStreams bool,
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
	var hbStreams *hbstream.HeartbeatStreams
	if runHeartbeatStreams {
		hbStreams = hbstream.NewHeartbeatStreams(ctx, constant.SchedulingServiceName, basicCluster)
	} else {
		hbStreams = hbstream.NewTestHeartbeatStreams(ctx, basicCluster, false)
	}
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
		StartKey:    testutil.TxnKeyspaceKey([]byte("b")),
		EndKey:      testutil.TxnKeyspaceKey([]byte("y")),
		Peers:       peers,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}, peers[0],
		core.SetApproximateSize(5),
		core.SetApproximateKeys(int64(5*sc.RegionSizeToKeysRatio)),
	)
}

// putAffinitySplitTestGroup puts the test Region into affinity group g1, which
// denies its automatic splits.
func putAffinitySplitTestGroup(t *testing.T, cluster *Cluster) *core.RegionInfo {
	t.Helper()
	re := require.New(t)
	region := newAffinitySplitTestRegion()
	cluster.PutRegion(region)
	manager := cluster.GetAffinityManager()
	re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{
		GroupID: "g1", KeyRanges: []keyutil.KeyRange{testutil.TxnKeyspaceRange([]byte("a"), []byte("z"))},
	}}))
	_, err := manager.UpdateAffinityGroupPeers("g1", 1, []uint64{1, 2, 3})
	re.NoError(err)
	return region
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

func TestAskBatchSplitAllocationHeaderError(t *testing.T) {
	for _, call := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(call), func(t *testing.T) {
			svc, cluster, _ := newTestSchedulingServiceForSplit(t, false)
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
			svc, cluster, _ := newTestSchedulingServiceForSplit(t, false)
			cluster.checkMembershipCh = make(chan struct{}, 1)
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
			require.Empty(t, cluster.checkMembershipCh)
		})
	}
}

func TestAllocIDRefreshesMembershipOnRPCError(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"transport failure", errors.New("connection closed")},
		{"RPC timeout", context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, cluster, _ := newTestSchedulingServiceForSplit(t, false)
			cluster.checkMembershipCh = make(chan struct{}, 1)
			client := &splitScatterPDClient{allocErr: test.err}
			if errors.Is(test.err, context.DeadlineExceeded) {
				client.onAlloc = func(ctx context.Context, _ int) {
					<-ctx.Done()
				}
			}
			cluster.SwitchPDLeader(client)
			_, _, err := cluster.AllocID(1)
			require.ErrorIs(t, err, test.err)
			require.NoError(t, cluster.ctx.Err(), "the parent context must remain active")
			require.Equal(t, 1, client.calls)
			require.Len(t, cluster.checkMembershipCh, 1)
		})
	}
}

func TestAskBatchSplitConstructionCancellation(t *testing.T) {
	for _, test := range []struct {
		name        string
		nonBatch    bool
		count       uint32
		cancelAfter int
	}{
		{"during batch construction", false, 1000, 1},
		{"before batch bookkeeping", false, 3, 3},
		{"before legacy bookkeeping", true, 3, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc, cluster, _ := newTestSchedulingServiceForSplit(t, false)
			client := &splitScatterPDClient{next: 1000, nonBatch: test.nonBatch}
			cluster.SwitchPDLeader(client)
			region := newAffinitySplitTestRegion()
			cluster.PutRegion(region)
			pendingBefore := splitScatterPendingMetricValue(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			constructed := 0
			core := zapcore.NewCore(zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()), zapcore.AddSync(io.Discard), zap.InfoLevel)
			logger := zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
				if entry.Message == "alloc ids for region split" {
					constructed++
					if constructed == test.cancelAfter {
						cancel()
					}
				}
				return nil
			}))
			restore := log.ReplaceGlobals(logger, &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.InfoLevel)})
			defer restore()

			resp, err := svc.AskBatchSplit(ctx, &schedulingpb.AskBatchSplitRequest{
				Region: region.GetMeta(), SplitCount: test.count, Reason: pdpb.SplitReason_LOAD,
			})
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, resp)
			require.Equal(t, test.cancelAfter, constructed)
			require.Empty(t, cluster.GetCoordinator().GetCheckerController().GetPendingProcessedRegions())
			require.Equal(t, pendingBefore, splitScatterPendingMetricValue(t))
		})
	}
}
