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

package checker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/scatter"
)

const (
	splitScatterObservedRegionID uint64 = 101
	splitScatterTestTableID      int64  = 42
	splitScatterTestIndexID      int64  = 7
)

func (c *Controller) collectTopPendingSplitScatter(limit int) []splitScatterPendingItem {
	return c.splitScatter.collectTopPendingSplitScatter(limit)
}

func (c *Controller) dispatchSplitScatterRegions() {
	c.splitScatter.dispatchSplitScatterRegions()
}

func TestRecordSplitScatterBatchCollectsPendingRegions(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101, 102, 103})
	re.Equal(4, splitScatterPendingCount(controller))

	sourceGroup := splitScatterPendingGroup(t, controller, 100)
	for _, regionID := range []uint64{101, 102, 103} {
		re.Equal(sourceGroup, splitScatterPendingGroup(t, controller, regionID))
	}

	putSplitScatterRegion(tc, 101, "m", "n", 0)
	putSplitScatterRegion(tc, 102, "n", "o", 120)
	putSplitScatterRegion(tc, 103, "o", "", 999)

	re.ElementsMatch([]uint64{101, 102, 103}, pendingRegionIDs(controller.collectTopPendingSplitScatter(3)))
}

func TestCheckSplitScatterRegionsCreatesScatterOperator(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101, 102})
	putSplitScatterRegion(tc, 101, "m", "t", 120)
	putSplitScatterRegion(tc, 102, "t", "", 80)

	group := splitScatterPendingGroup(t, controller, 101)

	controller.dispatchSplitScatterRegions()

	var op *operator.Operator
	for _, regionID := range []uint64{100, 101, 102} {
		op = oc.GetOperator(regionID)
		if op != nil {
			break
		}
	}
	re.NotNil(op)
	re.Equal(scatter.InternalScatterOperatorDesc, op.Desc())
	opGroup, ok := op.GetAdditionalInfo("group")
	re.True(ok)
	re.Equal(group, opGroup)
	re.Equal(1, splitScatterPendingCount(controller))
	re.Equal(group, splitScatterPendingGroup(t, controller, 100))
	re.Empty(pendingRegionIDs(controller.collectTopPendingSplitScatter(4)))
}

func TestDispatchSplitScatterKeepsPendingUntilSplitHeartbeat(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101})

	controller.dispatchSplitScatterRegions()

	re.Equal(2, splitScatterPendingCount(controller))
	re.Nil(oc.GetOperator(101))

	putSplitScatterRegion(tc, 101, "m", "", 120)

	re.Equal([]uint64{101}, pendingRegionIDs(controller.collectTopPendingSplitScatter(2)))

	controller.dispatchSplitScatterRegions()

	op := oc.GetOperator(101)
	re.NotNil(op)
	re.Equal(scatter.InternalScatterOperatorDesc, op.Desc())
}

func TestCollectTopPendingRemovesExpiredPending(t *testing.T) {
	re := require.New(t)
	controller, _, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101})
	expireSplitScatterPendingAt(t, controller, 100, time.Now().Add(-time.Second))
	expireSplitScatterPendingAt(t, controller, 101, time.Now().Add(-time.Second))

	re.Empty(controller.collectTopPendingSplitScatter(2))
	re.Equal(0, splitScatterPendingCount(controller))
}

func TestCollectTopPendingDefersSourceUntilVersionAdvances(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101})
	putSplitScatterRegion(tc, 101, "m", "", 120)

	re.ElementsMatch([]uint64{101}, pendingRegionIDs(controller.collectTopPendingSplitScatter(2)))

	source := tc.GetRegion(100)
	re.NotNil(source)
	tc.PutRegion(source.Clone(core.WithIncVersion()))

	re.ElementsMatch([]uint64{100, 101}, pendingRegionIDs(controller.collectTopPendingSplitScatter(2)))
}

func TestCollectTopPendingResolvesRangeHint(t *testing.T) {
	testCases := []struct {
		name      string
		startKey  []byte
		endKey    []byte
		wantRange splitScatterRangeHint
	}{
		{
			name:      "index region",
			startKey:  newSplitScatterIndexKey("a"),
			endKey:    newSplitScatterIndexKey("m"),
			wantRange: splitScatterPrefixRange(splitScatterIndexKeyPrefix()),
		},
		{
			name:      "record region",
			startKey:  newSplitScatterRecordKey(42, "a"),
			endKey:    newSplitScatterRecordKey(42, "m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(42)),
		},
		{
			name:      "bare table boundary",
			startKey:  newSplitScatterTableBoundaryKey(42),
			endKey:    newSplitScatterIndexKey("m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(42)),
		},
		{
			name:      "cross entity falls back to table",
			startKey:  newSplitScatterIndexKey("a"),
			endKey:    newSplitScatterRecordKey(42, "m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(42)),
		},
		{
			name:      "cross table uses start table",
			startKey:  newSplitScatterRecordKey(42, "a"),
			endKey:    newSplitScatterRecordKey(43, "m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(42)),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			controller, tc, _, cleanup := newTestSplitScatterController(t)
			defer cleanup()

			controller.RecordSplitScatterBatch(100, []uint64{101})
			putSplitScatterRegionWithKeys(tc, testCase.startKey, testCase.endKey, 120)

			re.Equal(makeSplitScatterGroup(100, 101), splitScatterPendingGroup(t, controller, 101))
			re.Equal([]uint64{101}, pendingRegionIDs(controller.collectTopPendingSplitScatter(1)))
			rangeHint := resolveSplitScatterRangeHint(tc.GetRegion(101))
			re.Equal(testCase.wantRange.startKey, rangeHint.startKey)
			re.Equal(testCase.wantRange.endKey, rangeHint.endKey)
		})
	}
}

func TestDispatchSplitScatterBacksOffWhenRegionIsNotFullyReplicated(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101})
	putSplitScatterRegionWithStores(tc, 101, "m", "", 120, 1, 2)

	controller.dispatchSplitScatterRegions()

	re.Equal(2, splitScatterPendingCount(controller))
	pending := splitScatterObservedPending(t, controller)
	re.True(pending.retryAt.After(time.Now()))
	re.Empty(pendingRegionIDs(controller.collectTopPendingSplitScatter(2)))
}

func TestDispatchSplitScatterBacksOffWhenScatterInternalFails(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101})
	putSplitScatterRegionWithoutLeader(tc, 101, "m", "", 120)

	controller.dispatchSplitScatterRegions()

	re.Equal(2, splitScatterPendingCount(controller))
	pending := splitScatterObservedPending(t, controller)
	re.True(pending.retryAt.After(time.Now()))
	re.Empty(pendingRegionIDs(controller.collectTopPendingSplitScatter(2)))
}

func TestDispatchSplitScatterIgnoresStalePendingSnapshot(t *testing.T) {
	re := require.New(t)
	controller, _, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101})
	stalePending := splitScatterObservedPending(t, controller)

	controller.RecordSplitScatterBatch(100, []uint64{102, 101})
	currentPending := splitScatterObservedPending(t, controller)
	re.NotEqual(stalePending.group, currentPending.group)
	re.Equal(time.Time{}, currentPending.retryAt)

	controller.splitScatter.delayPendingSplitScatter(stalePending)

	currentPending = splitScatterObservedPending(t, controller)
	re.Equal(time.Time{}, currentPending.retryAt)

	controller.splitScatter.deletePendingSplitScatter(stalePending)

	currentPending = splitScatterObservedPending(t, controller)
	re.Equal(makeSplitScatterGroup(100, 102), currentPending.group)
	re.Equal(time.Time{}, currentPending.retryAt)
}

func newTestSplitScatterController(t *testing.T) (*Controller, *mockcluster.Cluster, *operator.Controller, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	for storeID := uint64(1); storeID <= 4; storeID++ {
		tc.AddRegionStore(storeID, 0)
	}
	putSplitScatterRegion(tc, 100, "", "m", 0)

	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	controller := NewController(ctx, tc, tc.GetCheckerConfig(), oc)

	cleanup := func() {
		stream.Close()
		cancel()
	}
	return controller, tc, oc, cleanup
}

func putSplitScatterRegion(tc *mockcluster.Cluster, regionID uint64, startKey, endKey string, cpu uint64) {
	tc.AddLeaderRegionWithRange(regionID, startKey, endKey, 1, 2, 3)
	region := tc.GetRegion(regionID).Clone(core.SetCPUUsage(cpu))
	tc.PutRegion(region)
}

func putSplitScatterRegionWithKeys(tc *mockcluster.Cluster, startKey, endKey []byte, cpu uint64) {
	region := tc.AddLeaderRegion(splitScatterObservedRegionID, 1, 2, 3).Clone(
		core.WithStartKey(startKey),
		core.WithEndKey(endKey),
		core.SetCPUUsage(cpu),
	)
	tc.PutRegion(region)
}

func putSplitScatterRegionWithStores(tc *mockcluster.Cluster, regionID uint64, startKey, endKey string, cpu uint64, stores ...uint64) {
	peers := make([]*metapb.Peer, 0, len(stores))
	for i, storeID := range stores {
		peers = append(peers, &metapb.Peer{
			Id:      regionID*10 + uint64(i) + 1,
			StoreId: storeID,
		})
	}
	region := &metapb.Region{
		Id:       regionID,
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
		Peers:    peers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	tc.PutRegion(core.NewRegionInfo(
		region,
		peers[0],
		core.SetCPUUsage(cpu),
	))
}

func putSplitScatterRegionWithoutLeader(tc *mockcluster.Cluster, regionID uint64, startKey, endKey string, cpu uint64) {
	peers := []*metapb.Peer{
		{Id: regionID*10 + 1, StoreId: 1},
		{Id: regionID*10 + 2, StoreId: 2},
		{Id: regionID*10 + 3, StoreId: 3},
	}
	region := &metapb.Region{
		Id:       regionID,
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
		Peers:    peers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	tc.PutRegion(core.NewRegionInfo(
		region,
		nil,
		core.SetCPUUsage(cpu),
	))
}

func splitScatterPendingCount(controller *Controller) int {
	controller.splitScatter.pendingMu.RLock()
	defer controller.splitScatter.pendingMu.RUnlock()
	return len(controller.splitScatter.pending)
}

func splitScatterPendingGroup(t *testing.T, controller *Controller, regionID uint64) string {
	t.Helper()
	controller.splitScatter.pendingMu.RLock()
	defer controller.splitScatter.pendingMu.RUnlock()
	pending, ok := controller.splitScatter.pending[regionID]
	require.True(t, ok)
	return pending.group
}

func splitScatterObservedPending(t *testing.T, controller *Controller) splitScatterPendingItem {
	t.Helper()
	controller.splitScatter.pendingMu.RLock()
	defer controller.splitScatter.pendingMu.RUnlock()
	pending, ok := controller.splitScatter.pending[splitScatterObservedRegionID]
	require.True(t, ok)
	return pending
}

func expireSplitScatterPendingAt(t *testing.T, controller *Controller, regionID uint64, expireAt time.Time) {
	t.Helper()
	controller.splitScatter.pendingMu.Lock()
	defer controller.splitScatter.pendingMu.Unlock()
	pending, ok := controller.splitScatter.pending[regionID]
	require.True(t, ok)
	pending.expireAt = expireAt
	controller.splitScatter.pending[regionID] = pending
}

func pendingRegionIDs(regions []splitScatterPendingItem) []uint64 {
	ids := make([]uint64, 0, len(regions))
	for _, region := range regions {
		ids = append(ids, region.regionID)
	}
	return ids
}

func splitScatterIndexKeyPrefix() []byte {
	key := []byte{'t'}
	key = codec.EncodeInt(key, splitScatterTestTableID)
	key = append(key, '_', 'i')
	key = codec.EncodeInt(key, splitScatterTestIndexID)
	return key
}

func newSplitScatterIndexKey(suffix string) []byte {
	key := append([]byte(nil), splitScatterIndexKeyPrefix()...)
	key = append(key, suffix...)
	return codec.EncodeBytes(key)
}

func splitScatterRecordKeyPrefix(tableID int64) []byte {
	key := []byte{'t'}
	key = codec.EncodeInt(key, tableID)
	key = append(key, '_', 'r')
	return key
}

func newSplitScatterRecordKey(tableID int64, suffix string) []byte {
	key := append([]byte(nil), splitScatterRecordKeyPrefix(tableID)...)
	key = append(key, suffix...)
	return codec.EncodeBytes(key)
}

func newSplitScatterTableBoundaryKey(tableID int64) []byte {
	return codec.EncodeBytes(codec.GenerateTableKey(tableID))
}
