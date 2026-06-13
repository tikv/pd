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

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/scatter"
)

const (
	splitScatterObservedRegionID       uint64 = 101
	splitScatterTestTableID            int64  = 42
	splitScatterTestIndexID            int64  = 7
	splitScatterTestKeyspaceID         uint32 = 4242
	splitScatterTestMaxValidKeyspaceID        = uint32(0xFFFFFF)
	splitScatterTestNextGenKeyspaceID  uint32 = splitScatterTestMaxValidKeyspaceID - 1
	splitScatterTestSourceWaitVersion         = uint64(0)
	// CPU usage is only populated to mimic load-split region heartbeat data.
	// Current split-scatter dispatch does not rank pending regions by CPU.
	splitScatterNoCPUUsage       uint64 = 0
	splitScatterReportedCPUUsage uint64 = 1
)

func (c *Controller) collectTopPendingSplitScatter(limit int) []splitScatterPendingItem {
	return c.splitScatter.collectTopPendingSplitScatter(limit)
}

func (c *Controller) dispatchSplitScatterRegions() {
	c.splitScatter.dispatchSplitScatterRegions()
}

func TestSplitScatterControllerCleanupResetsPendingGauge(t *testing.T) {
	re := require.New(t)
	splitScatterPendingGauge.Set(7)

	controller, _, _, cleanup := newTestSplitScatterController(t)
	cleanup()

	re.Equal(0, splitScatterPendingCount(controller))
	re.Equal(float64(0), promtestutil.ToFloat64(splitScatterPendingGauge))
}

func TestRecordSplitScatterBatchCollectsPendingRegions(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101, 102, 103})
	re.Equal(4, splitScatterPendingCount(controller))
	re.Equal(float64(4), promtestutil.ToFloat64(splitScatterPendingGauge))

	sourceGroup := splitScatterPendingGroup(t, controller, 100)
	for _, regionID := range []uint64{101, 102, 103} {
		re.Equal(sourceGroup, splitScatterPendingGroup(t, controller, regionID))
	}

	putSplitScatterRegion(tc, 101, "m", "n", splitScatterReportedCPUUsage)
	putSplitScatterRegion(tc, 102, "n", "o", splitScatterReportedCPUUsage)
	putSplitScatterRegion(tc, 103, "o", "", splitScatterReportedCPUUsage)
	advanceSplitScatterSourceVersion(t, tc)

	re.ElementsMatch([]uint64{100, 101, 102, 103}, pendingRegionIDs(controller.collectTopPendingSplitScatter(4)))
}

func TestCheckSplitScatterRegionsCreatesScatterOperator(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101, 102})
	putSplitScatterRegion(tc, 101, "m", "t", splitScatterReportedCPUUsage)
	putSplitScatterRegion(tc, 102, "t", "", splitScatterReportedCPUUsage)
	advanceSplitScatterSourceVersion(t, tc)

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
	opGroup := op.GetAdditionalInfo("group")
	re.Equal(group, opGroup)
	batchGroup := op.GetAdditionalInfo("batch-group")
	re.Equal(group, batchGroup)
}

func TestDispatchSplitScatterKeepsPendingUntilSplitHeartbeat(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})

	controller.dispatchSplitScatterRegions()

	re.Equal(2, splitScatterPendingCount(controller))
	re.Nil(oc.GetOperator(101))

	putSplitScatterRegion(tc, 101, "m", "", splitScatterReportedCPUUsage)

	retrySplitScatterPendingAt(t, controller, 101, time.Now().Add(-time.Second))
	re.Empty(controller.collectTopPendingSplitScatter(2))
	advanceSplitScatterSourceVersion(t, tc)
	setSplitScatterNextDispatchAt(t, controller, time.Now().Add(-time.Second))
	re.ElementsMatch([]uint64{100, 101}, pendingRegionIDs(controller.collectTopPendingSplitScatter(2)))

	controller.dispatchSplitScatterRegions()

	op := oc.GetOperator(101)
	re.NotNil(op)
	re.Equal(scatter.InternalScatterOperatorDesc, op.Desc())
}

func TestDispatchSplitScatterUsesRequestWaitVersionWhenCacheLags(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	source := tc.GetRegion(100)
	re.NotNil(source)
	tc.PutRegion(source.Clone(core.SetRegionVersion(4)))

	controller.RecordSplitScatterBatch(100, 6, []uint64{101})
	putSplitScatterRegion(tc, 101, "m", "", splitScatterReportedCPUUsage)
	advanceSplitScatterRegionVersion(t, tc, 100)

	controller.dispatchSplitScatterRegions()

	re.Empty(oc.GetOperators())
	re.Equal(2, splitScatterPendingCount(controller))

	advanceSplitScatterRegionVersion(t, tc, 100)
	setSplitScatterNextDispatchAt(t, controller, time.Now().Add(-time.Second))
	controller.dispatchSplitScatterRegions()

	re.NotNil(oc.GetOperator(101))
}

func TestDispatchSplitScatterRespectsScheduleLimit(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101, 102})
	putSplitScatterRegion(tc, 101, "m", "t", splitScatterReportedCPUUsage)
	putSplitScatterRegion(tc, 102, "t", "", splitScatterReportedCPUUsage)
	advanceSplitScatterSourceVersion(t, tc)

	tc.SetSplitScatterScheduleLimit(1)
	controller.dispatchSplitScatterRegions()

	re.Len(oc.GetOperators(), 1)
	re.Equal(uint64(1), oc.OperatorCount(operator.OpSplitScatter))

	controller.dispatchSplitScatterRegions()

	re.Len(oc.GetOperators(), 1)
}

func TestRecordSplitScatterBatchSkipsWhenDisabled(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	tc.SetSplitScatterScheduleLimit(0)

	droppedBefore := promtestutil.ToFloat64(splitScatterPendingDroppedCounter)
	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101, 102})

	re.Equal(0, splitScatterPendingCount(controller))
	re.Equal(float64(0), promtestutil.ToFloat64(splitScatterPendingGauge))
	re.Equal(float64(0), promtestutil.ToFloat64(splitScatterPendingDroppedCounter)-droppedBefore)
}

func TestDispatchSplitScatterClearsPendingWhenDisabled(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101, 102})
	re.Equal(3, splitScatterPendingCount(controller))

	tc.SetSplitScatterScheduleLimit(0)
	setSplitScatterNextDispatchAt(t, controller, time.Now().Add(splitScatterRetryBackoff))
	disabledBefore := promtestutil.ToFloat64(splitScatterDispatchDisabledCounter)
	controller.dispatchSplitScatterRegions()

	re.Empty(oc.GetOperators())
	re.Equal(0, splitScatterPendingCount(controller))
	re.Equal(float64(0), promtestutil.ToFloat64(splitScatterPendingGauge))
	re.Equal(float64(1), promtestutil.ToFloat64(splitScatterDispatchDisabledCounter)-disabledBefore)
}

func TestDispatchSplitScatterCleansExpiredPendingBeforeEarlyReturn(t *testing.T) {
	testCases := []struct {
		name             string
		setupEarlyReturn func(*mockcluster.Cluster, *operator.Controller)
		counter          prometheus.Counter
	}{
		{
			name: "disabled",
			setupEarlyReturn: func(tc *mockcluster.Cluster, _ *operator.Controller) {
				tc.SetSplitScatterScheduleLimit(0)
			},
			counter: splitScatterDispatchDisabledCounter,
		},
		{
			name: "schedule limit",
			setupEarlyReturn: func(tc *mockcluster.Cluster, oc *operator.Controller) {
				tc.SetSplitScatterScheduleLimit(1)
				region := tc.GetRegion(100)
				op := operator.NewTestOperator(
					region.GetID(),
					region.GetRegionEpoch(),
					operator.OpSplitScatter|operator.OpRegion,
					operator.TransferLeader{FromStore: 1, ToStore: 2},
				)
				require.True(t, oc.AddOperator(op))
			},
			counter: splitScatterDispatchScheduleLimitCounter,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			controller, tc, oc, cleanup := newTestSplitScatterController(t)
			defer cleanup()

			controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
			expireSplitScatterPendingAt(t, controller, 100, time.Now().Add(-time.Second))
			expireSplitScatterPendingAt(t, controller, 101, time.Now().Add(-time.Second))
			testCase.setupEarlyReturn(tc, oc)

			expiredBefore := splitScatterPendingExpiredCount("false")
			counterBefore := promtestutil.ToFloat64(testCase.counter)
			controller.dispatchSplitScatterRegions()

			re.Equal(0, splitScatterPendingCount(controller))
			re.Equal(float64(0), promtestutil.ToFloat64(splitScatterPendingGauge))
			re.Equal(float64(2), splitScatterPendingExpiredCount("false")-expiredBefore)
			re.Equal(float64(0), promtestutil.ToFloat64(testCase.counter)-counterBefore)
		})
	}
}

func TestCollectTopPendingDelaysMissingRegions(t *testing.T) {
	re := require.New(t)
	controller, _, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})

	missingBefore := promtestutil.ToFloat64(splitScatterDispatchRegionMissingCounter)
	re.Empty(controller.collectTopPendingSplitScatter(2))

	re.Equal(float64(1), promtestutil.ToFloat64(splitScatterDispatchRegionMissingCounter)-missingBefore)
	pending := splitScatterPending(t, controller, 101)
	re.True(pending.retryAt.After(time.Now()))

	re.Empty(controller.collectTopPendingSplitScatter(2))
	re.Equal(float64(1), promtestutil.ToFloat64(splitScatterDispatchRegionMissingCounter)-missingBefore)
}

func TestDispatchSplitScatterBacksOffWhenNoCandidates(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})

	controller.dispatchSplitScatterRegions()

	re.True(splitScatterNextDispatchAt(t, controller).After(time.Now()))
	putSplitScatterRegion(tc, 101, "m", "", splitScatterReportedCPUUsage)
	advanceSplitScatterSourceVersion(t, tc)

	controller.dispatchSplitScatterRegions()

	re.Empty(oc.GetOperators())

	retrySplitScatterPendingAt(t, controller, 101, time.Now().Add(-time.Second))
	setSplitScatterNextDispatchAt(t, controller, time.Now().Add(-time.Second))
	controller.dispatchSplitScatterRegions()

	re.NotNil(oc.GetOperator(101))
}

func TestDispatchSplitScatterRespectsScheduleDeny(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
	putSplitScatterRegion(tc, 101, "m", "", splitScatterReportedCPUUsage)
	advanceSplitScatterSourceVersion(t, tc)

	re.NoError(tc.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID:       "split-scatter-schedule-deny",
		Labels:   []labeler.RegionLabel{{Key: "schedule", Value: "deny"}},
		RuleType: labeler.KeyRange,
		Data:     []any{map[string]any{"start_key": "", "end_key": ""}},
	}))

	counterBefore := promtestutil.ToFloat64(splitScatterDispatchScheduleDisabledCounter)
	controller.dispatchSplitScatterRegions()

	re.Empty(oc.GetOperators())
	re.Equal(2, splitScatterPendingCount(controller))
	re.Equal(float64(2), promtestutil.ToFloat64(splitScatterDispatchScheduleDisabledCounter)-counterBefore)
	for _, regionID := range []uint64{100, 101} {
		pending := splitScatterPending(t, controller, regionID)
		re.True(pending.retryAt.After(time.Now()))
	}
}

func TestCollectTopPendingRemovesExpiredPending(t *testing.T) {
	re := require.New(t)
	controller, _, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
	expireSplitScatterPendingAt(t, controller, 100, time.Now().Add(-time.Second))
	expireSplitScatterPendingAt(t, controller, 101, time.Now().Add(-time.Second))

	expiredBefore := splitScatterPendingExpiredCount("false")
	re.Empty(controller.collectTopPendingSplitScatter(2))
	re.Equal(0, splitScatterPendingCount(controller))
	re.Equal(float64(0), promtestutil.ToFloat64(splitScatterPendingGauge))
	re.Equal(float64(2), splitScatterPendingExpiredCount("false")-expiredBefore)
}

func TestCollectTopPendingMarksAttemptedBeforeExpiration(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
	putSplitScatterRegion(tc, 101, "m", "", splitScatterReportedCPUUsage)
	advanceSplitScatterSourceVersion(t, tc)

	attemptedBefore := splitScatterPendingExpiredCount("true")
	unattemptedBefore := splitScatterPendingExpiredCount("false")
	re.Len(controller.collectTopPendingSplitScatter(1), 1)
	expireSplitScatterPendingAt(t, controller, 100, time.Now().Add(-time.Second))
	expireSplitScatterPendingAt(t, controller, 101, time.Now().Add(-time.Second))

	re.Empty(controller.collectTopPendingSplitScatter(2))
	re.Equal(0, splitScatterPendingCount(controller))
	re.Equal(float64(0), promtestutil.ToFloat64(splitScatterPendingGauge))
	re.Equal(float64(1), splitScatterPendingExpiredCount("true")-attemptedBefore)
	re.Equal(float64(1), splitScatterPendingExpiredCount("false")-unattemptedBefore)
}

func TestRecordSplitScatterBatchRespectsPendingLimit(t *testing.T) {
	re := require.New(t)
	controller, _, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	fillSplitScatterPending(controller, time.Time{})

	droppedBefore := promtestutil.ToFloat64(splitScatterPendingDroppedCounter)
	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})

	re.Equal(splitScatterPendingLimit, splitScatterPendingCount(controller))
	re.Equal(float64(2), promtestutil.ToFloat64(splitScatterPendingDroppedCounter)-droppedBefore)
	controller.splitScatter.pendingMu.RLock()
	_, sourceExists := controller.splitScatter.pending[100]
	_, childExists := controller.splitScatter.pending[101]
	controller.splitScatter.pendingMu.RUnlock()
	re.False(sourceExists)
	re.False(childExists)
}

func TestRecordSplitScatterBatchClearsExpiredPendingBeforeLimitCheck(t *testing.T) {
	re := require.New(t)
	controller, _, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	fillSplitScatterPending(controller, time.Now().Add(-time.Second))

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})

	re.Equal(2, splitScatterPendingCount(controller))
	re.Equal(makeSplitScatterGroup(100, 101), splitScatterPendingGroup(t, controller, 101))
}

func TestCollectTopPendingSortsBeforeLimit(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(200, splitScatterTestSourceWaitVersion, []uint64{201})
	putSplitScatterRegion(tc, 200, "n", "o", splitScatterNoCPUUsage)
	putSplitScatterRegion(tc, 201, "o", "p", splitScatterReportedCPUUsage)

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
	putSplitScatterRegion(tc, 101, "m", "n", splitScatterReportedCPUUsage)
	advanceSplitScatterSourceVersion(t, tc)

	pending := controller.collectTopPendingSplitScatter(1)
	re.Len(pending, 1)
	re.Equal(uint64(100), pending[0].regionID)
}

func TestCollectTopPendingPrioritizesNearExpiration(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
	controller.RecordSplitScatterBatch(200, splitScatterTestSourceWaitVersion, []uint64{201})
	putSplitScatterRegion(tc, 100, "m", "n", splitScatterNoCPUUsage)
	putSplitScatterRegion(tc, 101, "n", "o", splitScatterReportedCPUUsage)
	putSplitScatterRegion(tc, 200, "o", "p", splitScatterNoCPUUsage)
	putSplitScatterRegion(tc, 201, "p", "q", splitScatterReportedCPUUsage)
	advanceSplitScatterSourceVersion(t, tc)
	advanceSplitScatterRegionVersion(t, tc, 200)

	expireSplitScatterPendingAt(t, controller, 100, time.Now().Add(2*time.Minute))
	expireSplitScatterPendingAt(t, controller, 101, time.Now().Add(2*time.Minute))
	expireSplitScatterPendingAt(t, controller, 200, time.Now().Add(time.Minute))
	expireSplitScatterPendingAt(t, controller, 201, time.Now().Add(time.Minute))

	pending := controller.collectTopPendingSplitScatter(1)
	re.Len(pending, 1)
	re.Equal(uint64(200), pending[0].regionID)
}

func TestCollectTopPendingResolvesRangeHint(t *testing.T) {
	testCases := []struct {
		name      string
		startKey  []byte
		endKey    []byte
		wantRange splitScatterRangeHint
		wantGroup string
		keyspaces []uint32
	}{
		{
			name:      "index region",
			startKey:  newSplitScatterIndexKey("a"),
			endKey:    newSplitScatterIndexKey("m"),
			wantRange: splitScatterPrefixRange(splitScatterIndexKeyPrefix()),
			wantGroup: makeSplitScatterIndexGroup(splitScatterTestTableID, splitScatterTestIndexID),
		},
		{
			name:      "record region",
			startKey:  newSplitScatterRecordKey(splitScatterTestTableID, "a"),
			endKey:    newSplitScatterRecordKey(splitScatterTestTableID, "m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(splitScatterTestTableID)),
			wantGroup: makeSplitScatterTableGroup(splitScatterTestTableID),
		},
		{
			name:      "bare table boundary",
			startKey:  newSplitScatterTableBoundaryKey(splitScatterTestTableID),
			endKey:    newSplitScatterIndexKey("m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(splitScatterTestTableID)),
			wantGroup: makeSplitScatterTableGroup(splitScatterTestTableID),
		},
		{
			name:      "cross entity falls back to table",
			startKey:  newSplitScatterIndexKey("a"),
			endKey:    newSplitScatterRecordKey(splitScatterTestTableID, "m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(splitScatterTestTableID)),
			wantGroup: makeSplitScatterTableGroup(splitScatterTestTableID),
		},
		{
			name:      "cross table uses start table",
			startKey:  newSplitScatterRecordKey(splitScatterTestTableID, "a"),
			endKey:    newSplitScatterRecordKey(splitScatterTestTableID+1, "m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(splitScatterTestTableID)),
			wantGroup: makeSplitScatterTableGroup(splitScatterTestTableID),
		},
		{
			name:      "nextgen keyspace index region",
			startKey:  newSplitScatterKeyspaceIndexKey(splitScatterTestNextGenKeyspaceID, "a"),
			endKey:    newSplitScatterKeyspaceIndexKey(splitScatterTestNextGenKeyspaceID, "m"),
			wantRange: splitScatterKeyspacePrefixRange(splitScatterTestNextGenKeyspaceID, splitScatterIndexKeyPrefix()),
			wantGroup: makeSplitScatterKeyspaceIndexGroup(splitScatterTestNextGenKeyspaceID, splitScatterTestTableID, splitScatterTestIndexID),
			keyspaces: []uint32{splitScatterTestNextGenKeyspaceID},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			controller, tc, _, cleanup := newTestSplitScatterController(t)
			defer cleanup()

			controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
			putSplitScatterRegionWithKeys(tc, testCase.startKey, testCase.endKey, splitScatterReportedCPUUsage)
			putSplitScatterRegion(tc, 100, "z", "", splitScatterNoCPUUsage)
			advanceSplitScatterSourceVersion(t, tc)

			re.Equal(makeSplitScatterGroup(100, 101), splitScatterPendingGroup(t, controller, 101))
			re.ElementsMatch([]uint64{100, 101}, pendingRegionIDs(controller.collectTopPendingSplitScatter(2)))
			rangeHint := resolveSplitScatterRangeHintWithKeyspaceValidator(
				tc.GetRegion(101),
				splitScatterKeyspaceValidatorFor(testCase.keyspaces...),
			)
			re.Equal(testCase.wantRange.startKey, rangeHint.startKey)
			re.Equal(testCase.wantRange.endKey, rangeHint.endKey)
			re.Equal(testCase.wantGroup, rangeHint.scatterGroup)
		})
	}
}

func TestResolveSplitScatterRangeHintIgnoresRawLikeKeyspaceKeys(t *testing.T) {
	re := require.New(t)
	region := core.NewRegionInfo(&metapb.Region{
		Id:       1,
		StartKey: newSplitScatterRawKeyspaceRecordKey(splitScatterTestKeyspaceID, splitScatterTestTableID, "a"),
		EndKey:   newSplitScatterRawKeyspaceRecordKey(splitScatterTestKeyspaceID, splitScatterTestTableID, "m"),
	}, nil)

	rangeHint := resolveSplitScatterRangeHintWithKeyspaceValidator(
		region,
		splitScatterKeyspaceValidatorFor(splitScatterTestKeyspaceID),
	)
	re.Equal(splitScatterRangeHint{}, rangeHint)
}

func TestResolveSplitScatterRangeHintRequiresKnownTxnKeyspaceBounds(t *testing.T) {
	testCases := []struct {
		name       string
		keyspaceID uint32
	}{
		{name: "normal keyspace", keyspaceID: splitScatterTestKeyspaceID},
		{name: "max valid keyspace", keyspaceID: splitScatterTestMaxValidKeyspaceID},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			controller, tc, _, cleanup := newTestSplitScatterController(t)
			defer cleanup()

			startKey := newSplitScatterKeyspaceRecordKey(testCase.keyspaceID, "a")
			endKey := newSplitScatterKeyspaceRecordKey(testCase.keyspaceID, "m")
			region := core.NewRegionInfo(&metapb.Region{
				Id:       1,
				StartKey: startKey,
				EndKey:   endKey,
			}, nil)
			re.Equal(splitScatterRangeHint{}, resolveSplitScatterRangeHintWithKeyspaceValidator(region, nil))

			regionBound := keyspace.MakeRegionBound(testCase.keyspaceID)
			putSplitScatterRegionWithKeysByID(tc, 90, regionBound.TxnLeftBound, startKey, splitScatterNoCPUUsage)
			putSplitScatterRegionWithKeysByID(tc, 91, regionBound.TxnRightBound, nil, splitScatterNoCPUUsage)

			rangeHint := resolveSplitScatterRangeHintWithKeyspaceValidator(
				region,
				controller.splitScatter.hasSplitScatterTxnKeyspaceBounds,
			)
			wantRange := splitScatterKeyspacePrefixRange(testCase.keyspaceID, codec.GenerateTableKey(splitScatterTestTableID))
			wantRange.scatterGroup = makeSplitScatterKeyspaceTableGroup(testCase.keyspaceID, splitScatterTestTableID)
			re.Equal(wantRange, rangeHint)
		})
	}
}

func TestDispatchSplitScatterUsesRangeScatterGroup(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
	putSplitScatterRegionWithKeysByID(tc, 90, newSplitScatterIndexKey("m"), newSplitScatterIndexKey("z"), splitScatterNoCPUUsage)
	putSplitScatterRegionWithKeysByID(tc, 101, newSplitScatterIndexKey("a"), newSplitScatterIndexKey("m"), splitScatterReportedCPUUsage)
	advanceSplitScatterRegionVersion(t, tc, 100)

	batchGroup := splitScatterPendingGroup(t, controller, 101)

	controller.dispatchSplitScatterRegions()

	expectedScatterGroup := makeSplitScatterIndexGroup(splitScatterTestTableID, splitScatterTestIndexID)
	op := oc.GetOperator(101)
	re.NotNil(op)
	opGroup := op.GetAdditionalInfo("group")
	re.Equal(expectedScatterGroup, opGroup)
	opBatchGroup := op.GetAdditionalInfo("batch-group")
	re.Equal(batchGroup, opBatchGroup)
}

func TestDispatchSplitScatterUsesKeyspaceRangeScatterGroup(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
	startKey := newSplitScatterKeyspaceIndexKey(splitScatterTestKeyspaceID, "a")
	endKey := newSplitScatterKeyspaceIndexKey(splitScatterTestKeyspaceID, "m")
	regionBound := keyspace.MakeRegionBound(splitScatterTestKeyspaceID)
	putSplitScatterRegionWithKeysByID(tc, 90, regionBound.TxnLeftBound, startKey, splitScatterNoCPUUsage)
	putSplitScatterRegionWithKeysByID(tc, 91, regionBound.TxnRightBound, nil, splitScatterNoCPUUsage)
	putSplitScatterRegionWithKeysByID(tc, 101, startKey, endKey, splitScatterReportedCPUUsage)
	advanceSplitScatterRegionVersion(t, tc, 100)

	batchGroup := splitScatterPendingGroup(t, controller, 101)

	controller.dispatchSplitScatterRegions()

	expectedScatterGroup := makeSplitScatterKeyspaceIndexGroup(
		splitScatterTestKeyspaceID,
		splitScatterTestTableID,
		splitScatterTestIndexID,
	)
	op := oc.GetOperator(101)
	re.NotNil(op)
	opGroup := op.GetAdditionalInfo("group")
	re.Equal(expectedScatterGroup, opGroup)
	opBatchGroup := op.GetAdditionalInfo("batch-group")
	re.Equal(batchGroup, opBatchGroup)
}

func TestDispatchSplitScatterKeepsStableGroupWhenRegionSplitsAgain(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	stableGroup := makeSplitScatterIndexGroup(splitScatterTestTableID, splitScatterTestIndexID)
	sourceID := uint64(100)
	childIDs := []uint64{101, 201, 301}
	splitKeys := []string{"m", "t", "x"}
	previousBatchGroup := ""

	putSplitScatterRegionWithKeysByID(tc, sourceID, newSplitScatterIndexKey("a"), newSplitScatterIndexKey("z"), splitScatterNoCPUUsage)
	for i, childID := range childIDs {
		controller.RecordSplitScatterBatch(sourceID, splitScatterTestSourceWaitVersion, []uint64{childID})
		batchGroup := splitScatterPendingGroup(t, controller, childID)
		re.NotEqual(previousBatchGroup, batchGroup)

		tc.PutRegion(tc.GetRegion(sourceID).Clone(
			core.WithEndKey(newSplitScatterIndexKey(splitKeys[i])),
			core.WithIncVersion(),
		))
		putSplitScatterRegionWithKeysByID(tc, childID, newSplitScatterIndexKey(splitKeys[i]), newSplitScatterIndexKey("z"), splitScatterReportedCPUUsage)

		controller.dispatchSplitScatterRegions()

		requireInternalScatterOpsUseGroups(t, oc, stableGroup, batchGroup)
		removeInternalScatterOps(oc)
		previousBatchGroup = batchGroup
	}
}

func TestDispatchSplitScatterBacksOff(t *testing.T) {
	testCases := []struct {
		name      string
		putRegion func(*mockcluster.Cluster)
	}{
		{
			name: "region is not fully replicated",
			putRegion: func(tc *mockcluster.Cluster) {
				putSplitScatterRegionWithStores(tc, 101, "m", "", splitScatterReportedCPUUsage, 1, 2)
			},
		},
		{
			name: "scatter internal fails",
			putRegion: func(tc *mockcluster.Cluster) {
				putSplitScatterRegionWithoutLeader(tc, 101, "m", "", splitScatterReportedCPUUsage)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			controller, tc, _, cleanup := newTestSplitScatterController(t)
			defer cleanup()

			controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
			testCase.putRegion(tc)
			advanceSplitScatterSourceVersion(t, tc)

			controller.dispatchSplitScatterRegions()

			re.Equal(1, splitScatterPendingCount(controller))
			pending := splitScatterObservedPending(t, controller)
			re.True(pending.retryAt.After(time.Now()))
			re.Empty(pendingRegionIDs(controller.collectTopPendingSplitScatter(2)))
		})
	}
}

func TestDispatchSplitScatterIgnoresStalePendingSnapshot(t *testing.T) {
	re := require.New(t)
	controller, _, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
	stalePending := splitScatterObservedPending(t, controller)

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{102, 101})
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

func TestDispatchSplitScatterIgnoresStalePendingWithSameGroup(t *testing.T) {
	re := require.New(t)
	controller, _, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, splitScatterTestSourceWaitVersion, []uint64{101})
	stalePending := splitScatterObservedPending(t, controller)

	controller.splitScatter.pendingMu.Lock()
	currentPending := controller.splitScatter.pending[splitScatterObservedRegionID]
	currentPending.expireAt = currentPending.expireAt.Add(time.Minute)
	controller.splitScatter.pending[splitScatterObservedRegionID] = currentPending
	controller.splitScatter.pendingMu.Unlock()

	controller.splitScatter.delayPendingSplitScatter(stalePending)

	currentPending = splitScatterObservedPending(t, controller)
	re.Equal(time.Time{}, currentPending.retryAt)

	controller.splitScatter.deletePendingSplitScatter(stalePending)

	currentPending = splitScatterObservedPending(t, controller)
	re.Equal(stalePending.group, currentPending.group)
	re.NotEqual(stalePending.expireAt, currentPending.expireAt)
}

func newTestSplitScatterController(t *testing.T) (*Controller, *mockcluster.Cluster, *operator.Controller, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	for storeID := uint64(1); storeID <= 4; storeID++ {
		tc.AddRegionStore(storeID, 0)
	}
	putSplitScatterRegion(tc, 100, "", "m", splitScatterNoCPUUsage)

	tc.SetSplitScatterScheduleLimit(4)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	controller := NewController(ctx, tc, tc.GetCheckerConfig(), oc)

	cleanup := func() {
		controller.splitScatter.clearPendingSplitScatter()
		stream.Close()
		cancel()
	}
	return controller, tc, oc, cleanup
}

func putSplitScatterRegion(tc *mockcluster.Cluster, regionID uint64, startKey, endKey string, cpuUsage uint64) {
	tc.AddLeaderRegionWithRange(regionID, startKey, endKey, 1, 2, 3)
	region := tc.GetRegion(regionID).Clone(core.SetCPUUsage(cpuUsage))
	tc.PutRegion(region)
}

func putSplitScatterRegionWithKeys(tc *mockcluster.Cluster, startKey, endKey []byte, cpuUsage uint64) {
	putSplitScatterRegionWithKeysByID(tc, splitScatterObservedRegionID, startKey, endKey, cpuUsage)
}

func putSplitScatterRegionWithKeysByID(tc *mockcluster.Cluster, regionID uint64, startKey, endKey []byte, cpuUsage uint64) {
	peers := []*metapb.Peer{
		{Id: regionID*10 + 1, StoreId: 1},
		{Id: regionID*10 + 2, StoreId: 2},
		{Id: regionID*10 + 3, StoreId: 3},
	}
	region := core.NewRegionInfo(
		&metapb.Region{
			Id:       regionID,
			StartKey: startKey,
			EndKey:   endKey,
			Peers:    peers,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
		},
		peers[0],
		core.SetCPUUsage(cpuUsage),
	)
	tc.PutRegion(region)
}

func newSplitScatterRegionInfo(
	regionID uint64,
	startKey, endKey string,
	peers []*metapb.Peer,
	leader *metapb.Peer,
	cpuUsage uint64,
) *core.RegionInfo {
	return core.NewRegionInfo(
		&metapb.Region{
			Id:       regionID,
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
			Peers:    peers,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
		},
		leader,
		core.SetCPUUsage(cpuUsage),
	)
}

func putSplitScatterRegionWithStores(tc *mockcluster.Cluster, regionID uint64, startKey, endKey string, cpuUsage uint64, stores ...uint64) {
	peers := make([]*metapb.Peer, 0, len(stores))
	for i, storeID := range stores {
		peers = append(peers, &metapb.Peer{
			Id:      regionID*10 + uint64(i) + 1,
			StoreId: storeID,
		})
	}
	tc.PutRegion(newSplitScatterRegionInfo(regionID, startKey, endKey, peers, peers[0], cpuUsage))
}

func putSplitScatterRegionWithoutLeader(tc *mockcluster.Cluster, regionID uint64, startKey, endKey string, cpuUsage uint64) {
	peers := []*metapb.Peer{
		{Id: regionID*10 + 1, StoreId: 1},
		{Id: regionID*10 + 2, StoreId: 2},
		{Id: regionID*10 + 3, StoreId: 3},
	}
	tc.PutRegion(newSplitScatterRegionInfo(regionID, startKey, endKey, peers, nil, cpuUsage))
}

func fillSplitScatterPending(controller *Controller, expireAt time.Time) {
	controller.splitScatter.pendingMu.Lock()
	defer controller.splitScatter.pendingMu.Unlock()
	for regionID := uint64(1000); regionID < 1000+splitScatterPendingLimit; regionID++ {
		controller.splitScatter.pending[regionID] = splitScatterPendingItem{
			regionID: regionID,
			group:    "old",
			expireAt: expireAt,
		}
	}
}

func advanceSplitScatterSourceVersion(t *testing.T, tc *mockcluster.Cluster) {
	advanceSplitScatterRegionVersion(t, tc, 100)
}

func advanceSplitScatterRegionVersion(t *testing.T, tc *mockcluster.Cluster, regionID uint64) {
	t.Helper()
	region := tc.GetRegion(regionID)
	require.NotNil(t, region)
	tc.PutRegion(region.Clone(core.WithIncVersion()))
}

func splitScatterPendingCount(controller *Controller) int {
	controller.splitScatter.pendingMu.RLock()
	defer controller.splitScatter.pendingMu.RUnlock()
	return len(controller.splitScatter.pending)
}

func splitScatterPendingExpiredCount(attempted string) float64 {
	return promtestutil.ToFloat64(splitScatterPendingExpiredCounter.WithLabelValues(attempted))
}

func splitScatterPendingGroup(t *testing.T, controller *Controller, regionID uint64) string {
	t.Helper()
	return splitScatterPending(t, controller, regionID).group
}

func splitScatterKeyspaceValidatorFor(keyspaces ...uint32) splitScatterKeyspaceValidator {
	return func(keyspaceID uint32) bool {
		for _, validKeyspaceID := range keyspaces {
			if validKeyspaceID == keyspaceID {
				return true
			}
		}
		return false
	}
}

func splitScatterPending(t *testing.T, controller *Controller, regionID uint64) splitScatterPendingItem {
	t.Helper()
	controller.splitScatter.pendingMu.RLock()
	defer controller.splitScatter.pendingMu.RUnlock()
	pending, ok := controller.splitScatter.pending[regionID]
	require.True(t, ok)
	return pending
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

func retrySplitScatterPendingAt(t *testing.T, controller *Controller, regionID uint64, retryAt time.Time) {
	t.Helper()
	controller.splitScatter.pendingMu.Lock()
	defer controller.splitScatter.pendingMu.Unlock()
	pending, ok := controller.splitScatter.pending[regionID]
	require.True(t, ok)
	pending.retryAt = retryAt
	controller.splitScatter.pending[regionID] = pending
}

func setSplitScatterNextDispatchAt(t *testing.T, controller *Controller, nextDispatchAt time.Time) {
	t.Helper()
	controller.splitScatter.pendingMu.Lock()
	defer controller.splitScatter.pendingMu.Unlock()
	controller.splitScatter.nextDispatchAt = nextDispatchAt
}

func splitScatterNextDispatchAt(t *testing.T, controller *Controller) time.Time {
	t.Helper()
	controller.splitScatter.pendingMu.RLock()
	defer controller.splitScatter.pendingMu.RUnlock()
	return controller.splitScatter.nextDispatchAt
}

func requireInternalScatterOpsUseGroups(t *testing.T, oc *operator.Controller, scatterGroup, batchGroup string) {
	t.Helper()
	re := require.New(t)
	ops := oc.GetOperators()
	re.NotEmpty(ops)
	for _, op := range ops {
		re.Equal(scatter.InternalScatterOperatorDesc, op.Desc())
		opGroup := op.GetAdditionalInfo("group")
		re.Equal(scatterGroup, opGroup)
		opBatchGroup := op.GetAdditionalInfo("batch-group")
		re.Equal(batchGroup, opBatchGroup)
	}
}

func removeInternalScatterOps(oc *operator.Controller) {
	for _, op := range oc.GetOperators() {
		oc.RemoveOperator(op)
	}
}

func pendingRegionIDs(regions []splitScatterPendingItem) []uint64 {
	ids := make([]uint64, 0, len(regions))
	for _, region := range regions {
		ids = append(ids, region.regionID)
	}
	return ids
}

func splitScatterIndexKeyPrefix() []byte {
	return codec.GenerateIndexKey(splitScatterTestTableID, splitScatterTestIndexID)
}

func splitScatterKeyspacePrefixRange(keyspaceID uint32, rawPrefix []byte) splitScatterRangeHint {
	startKey := newSplitScatterKeyspaceKey(keyspaceID, keyspace.TxnKeyspaceModePrefix, rawPrefix)
	endRawPrefix := splitScatterNextPrefix(rawPrefix)
	if len(endRawPrefix) == 0 {
		return splitScatterRangeHint{startKey: startKey}
	}
	return splitScatterRangeHint{
		startKey: startKey,
		endKey:   newSplitScatterKeyspaceKey(keyspaceID, keyspace.TxnKeyspaceModePrefix, endRawPrefix),
	}
}

func newSplitScatterIndexKey(suffix string) []byte {
	key := append([]byte(nil), splitScatterIndexKeyPrefix()...)
	key = append(key, suffix...)
	return codec.EncodeBytes(key)
}

func newSplitScatterKeyspaceIndexKey(keyspaceID uint32, suffix string) []byte {
	key := append([]byte(nil), splitScatterIndexKeyPrefix()...)
	key = append(key, suffix...)
	return newSplitScatterKeyspaceKey(keyspaceID, keyspace.TxnKeyspaceModePrefix, key)
}

func newSplitScatterRecordKey(tableID int64, suffix string) []byte {
	key := append([]byte(nil), codec.GenerateRecordKeyPrefix(tableID)...)
	key = append(key, suffix...)
	return codec.EncodeBytes(key)
}

func newSplitScatterKeyspaceRecordKey(keyspaceID uint32, suffix string) []byte {
	key := append([]byte(nil), codec.GenerateRecordKeyPrefix(splitScatterTestTableID)...)
	key = append(key, suffix...)
	return newSplitScatterKeyspaceKey(keyspaceID, keyspace.TxnKeyspaceModePrefix, key)
}

func newSplitScatterRawKeyspaceRecordKey(keyspaceID uint32, tableID int64, suffix string) []byte {
	key := append([]byte(nil), codec.GenerateRecordKeyPrefix(tableID)...)
	key = append(key, suffix...)
	return newSplitScatterKeyspaceKey(keyspaceID, keyspace.RawKeyspaceModePrefix, key)
}

func newSplitScatterTableBoundaryKey(tableID int64) []byte {
	return codec.EncodeBytes(codec.GenerateTableKey(tableID))
}

func newSplitScatterKeyspaceKey(keyspaceID uint32, mode byte, rawKey []byte) []byte {
	key := keyspace.MakeKeyspacePrefix(mode, keyspaceID)
	return codec.EncodeBytes(append(key, rawKey...))
}
