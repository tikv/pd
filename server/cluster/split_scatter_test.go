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

package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
)

const (
	// CPU usage is only populated to mimic load-split region heartbeat data.
	// Current split-scatter dispatch does not rank pending regions by CPU.
	splitScatterNoCPUUsage       uint64 = 0
	splitScatterReportedCPUUsage uint64 = 1
)

func TestResetPreparedAndResetRegionCacheForwardsToScheduling(t *testing.T) {
	re := require.New(t)
	cluster, _ := newSplitScatterTestCluster(t)
	cluster.SetServiceIndependent(constant.SchedulingServiceName)

	forwarded := 0
	cluster.SetResetSchedulingCacheFunc(func(context.Context) error {
		forwarded++
		return nil
	})
	re.NoError(cluster.ResetPreparedAndResetRegionCache(context.Background()))

	re.Equal(1, forwarded)
	re.False(cluster.GetCoordinator().GetPrepareChecker().IsPrepared())
	re.Zero(cluster.GetTotalRegionCount())
}

func TestHandleAskBatchSplitSchedulesSplitScatterInPatrol(t *testing.T) {
	re := require.New(t)
	cluster, cancelPatrol := newSplitScatterTestCluster(t)

	request := &pdpb.AskBatchSplitRequest{
		Region:     cluster.GetRegion(100).GetMeta(),
		SplitCount: 2,
		Reason:     pdpb.SplitReason_LOAD,
	}
	resp, err := cluster.HandleAskBatchSplit(request)
	re.NoError(err)
	re.Len(resp.GetIds(), 2)

	splitRegionIDs := []uint64{
		resp.GetIds()[0].GetNewRegionId(),
		resp.GetIds()[1].GetNewRegionId(),
	}
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), newSplitScatterRegion(100, []byte(""), []byte("m"), splitScatterNoCPUUsage).Clone(core.WithIncVersion())))
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), newSplitScatterRegion(splitRegionIDs[0], []byte("m"), []byte("t"), splitScatterReportedCPUUsage)))
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), newSplitScatterRegion(splitRegionIDs[1], []byte("t"), []byte(""), splitScatterReportedCPUUsage)))

	dispatchSplitScatterInPatrol(t, cluster, cancelPatrol, func() bool {
		return cluster.GetOperatorController().GetOperator(splitRegionIDs[0]) != nil &&
			cluster.GetOperatorController().GetOperator(splitRegionIDs[1]) != nil
	})

	group := ""
	for _, regionID := range splitRegionIDs {
		op := cluster.GetOperatorController().GetOperator(regionID)
		re.NotNil(op)
		re.Equal(scatter.InternalScatterOperatorDesc, op.Desc())
		opGroup, ok := op.GetAdditionalInfo("group")
		re.True(ok)
		if group == "" {
			group = opGroup
			re.True(strings.HasPrefix(group, "split-scatter-100-"))
			continue
		}
		re.Equal(group, opGroup)
	}
	re.NotEmpty(group)
}

func TestHandleAskBatchSplitSeedsIndexBaselineForFirstSplitRegion(t *testing.T) {
	re := require.New(t)
	cluster, cancelPatrol := newSplitScatterTestCluster(t)

	re.NoError(cluster.putRegion(newSplitScatterRegion(90, newSplitScatterIndexKey("a"), newSplitScatterIndexKey("j"), splitScatterNoCPUUsage)))
	re.NoError(cluster.putRegion(newSplitScatterRegion(91, newSplitScatterIndexKey("j"), newSplitScatterIndexKey("t"), splitScatterNoCPUUsage)))
	re.NoError(cluster.putRegion(newSplitScatterRegion(100, newSplitScatterIndexKey("t"), newSplitScatterIndexKey("z"), splitScatterNoCPUUsage)))

	request := &pdpb.AskBatchSplitRequest{
		Region:     cluster.GetRegion(100).GetMeta(),
		SplitCount: 1,
		Reason:     pdpb.SplitReason_LOAD,
	}
	resp, err := cluster.HandleAskBatchSplit(request)
	re.NoError(err)
	re.Len(resp.GetIds(), 1)

	splitRegionID := resp.GetIds()[0].GetNewRegionId()
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), newSplitScatterRegion(100, newSplitScatterIndexKey("w"), newSplitScatterIndexKey("z"), splitScatterNoCPUUsage).Clone(core.WithIncVersion())))
	re.NoError(cluster.processRegionHeartbeat(
		core.ContextTODO(),
		newSplitScatterRegion(splitRegionID, newSplitScatterIndexKey("t"), newSplitScatterIndexKey("w"), splitScatterReportedCPUUsage),
	))

	dispatchSplitScatterInPatrol(t, cluster, cancelPatrol, func() bool {
		return cluster.GetOperatorController().GetOperator(splitRegionID) != nil
	})

	op := cluster.GetOperatorController().GetOperator(splitRegionID)
	re.NotNil(op)
	re.Equal(scatter.InternalScatterOperatorDesc, op.Desc())
	opGroup, ok := op.GetAdditionalInfo("group")
	re.True(ok)
	re.Equal("split-scatter-index-42-7", opGroup)
	batchGroup, ok := op.GetAdditionalInfo("batch-group")
	re.True(ok)
	re.Equal(fmt.Sprintf("split-scatter-100-%d", splitRegionID), batchGroup)
}

func TestHandleAskBatchSplitSkipsSplitScatterForSizeReason(t *testing.T) {
	re := require.New(t)
	cluster, cancelPatrol := newSplitScatterTestCluster(t)

	request := &pdpb.AskBatchSplitRequest{
		Region:     cluster.GetRegion(100).GetMeta(),
		SplitCount: 1,
		Reason:     pdpb.SplitReason_SIZE,
	}
	resp, err := cluster.HandleAskBatchSplit(request)
	re.NoError(err)
	re.Len(resp.GetIds(), 1)

	splitRegionID := resp.GetIds()[0].GetNewRegionId()
	re.NoError(cluster.processRegionHeartbeat(
		core.ContextTODO(),
		newSplitScatterRegion(splitRegionID, []byte("m"), []byte(""), splitScatterReportedCPUUsage),
	))

	dispatchSplitScatterInPatrol(t, cluster, cancelPatrol, func() bool {
		// PatrolRegions uses a ticker; wait for at least one tick interval
		// so the dispatch phase is guaranteed to have executed.
		time.Sleep(100 * time.Millisecond)
		return true
	})

	re.Nil(cluster.GetOperatorController().GetOperator(splitRegionID))
	re.Nil(cluster.GetOperatorController().GetOperator(100))
}

func newSplitScatterTestCluster(t *testing.T) (*RaftCluster, context.CancelFunc) {
	t.Helper()
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	cluster.regionLabeler, err = labeler.NewRegionLabeler(ctx, cluster.storage, time.Second*5)
	re.NoError(err)
	hbStreams := hbstream.NewTestHeartbeatStreams(ctx, cluster.BasicCluster, false)
	cluster.initCoordinator(ctx, cluster, hbStreams)
	cluster.GetCoordinator().GetPrepareChecker().SetPrepared()
	t.Cleanup(func() {
		hbStreams.Close()
	})

	now := time.Now()
	for _, store := range newTestStores(4, "6.0.0") {
		re.NoError(cluster.setStore(store.Clone(core.SetLastHeartbeatTS(now))))
	}

	re.NoError(cluster.putRegion(newSplitScatterRegion(100, []byte(""), []byte("m"), splitScatterNoCPUUsage)))
	return cluster, cancel
}

func dispatchSplitScatterInPatrol(t *testing.T, cluster *RaftCluster, cancelPatrol context.CancelFunc, wait func() bool) {
	t.Helper()
	checkerController := cluster.GetCoordinator().GetCheckerController()
	done := make(chan struct{})
	go func() {
		defer close(done)
		checkerController.PatrolRegions()
	}()
	testutil.Eventually(require.New(t), wait)
	cancelPatrol()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("patrol regions did not exit after cancel")
	}
}

func newSplitScatterRegion(regionID uint64, start, end []byte, cpuUsage uint64) *core.RegionInfo {
	return newSplitScatterRegionWithStores(regionID, start, end, cpuUsage, 1, 2, 3)
}

func newSplitScatterRegionWithStores(regionID uint64, start, end []byte, cpuUsage uint64, stores ...uint64) *core.RegionInfo {
	peers := []*metapb.Peer{}
	for i, storeID := range stores {
		peers = append(peers, &metapb.Peer{
			Id:      regionID*10 + uint64(i) + 1,
			StoreId: storeID,
		})
	}
	region := &metapb.Region{
		Id:       regionID,
		StartKey: start,
		EndKey:   end,
		Peers:    peers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	return core.NewRegionInfo(
		region,
		peers[0],
		core.SetCPUUsage(cpuUsage),
	)
}

func newSplitScatterIndexKey(suffix string) []byte {
	key := codec.GenerateIndexKey(42, 7)
	key = append(key, suffix...)
	return codec.EncodeBytes(key)
}
