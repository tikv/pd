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

package controller

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/constants"
)

func TestRUTimeline(t *testing.T) {
	re := require.New(t)
	const start int64 = 1800000000
	tl := newRUTimeline(time.Unix(start, 0))
	tl.recordLocked(time.Unix(start, 999999999), 10, 20)
	tl.recordLocked(time.Unix(start+1, 0), -3, 5)
	first := tl.snapshotLocked(time.Unix(start+2, 0))
	re.Equal(start, first.StartUnixSec)
	re.Equal(start+2, first.EndUnixSec)
	re.Equal([]*rmpb.RUConsumptionBucket{{Rru: 10, Wru: 20, Offset: 0}, {Rru: -3, Wru: 5, Offset: 1}}, first.Buckets)

	// Snapshots are sparse: idle seconds are implicit zeros, and the returned
	// slices own their memory.
	again := tl.snapshotLocked(time.Unix(start+3, 0))
	re.Equal(start+3, again.EndUnixSec)
	re.Len(again.Buckets, 2)
	again.Buckets[0].Rru = 999
	re.Equal(float64(10), first.Buckets[0].Rru)
	re.Equal(float64(10), tl.snapshotLocked(time.Unix(start+3, 0)).Buckets[0].Rru)

	// Only the retained window is replayed, and only non-zero seconds are
	// listed; end_unix_sec still advances over the idle ones.
	tl.recordLocked(time.Unix(start+91, 0), 7, 8)
	latest := tl.snapshotLocked(time.Unix(start+93, 0))
	re.Equal(start+3, latest.StartUnixSec)
	re.Equal(start+93, latest.EndUnixSec)
	re.Equal([]*rmpb.RUConsumptionBucket{{Rru: 7, Wru: 8, Offset: 88}}, latest.Buckets)
}

func TestRUTimelineIdleWatermark(t *testing.T) {
	re := require.New(t)
	const start int64 = 1800000000
	tl := newRUTimeline(time.Unix(start, 0))
	// No record at all: the watermark still advances, so an idle group keeps
	// publishing zeros instead of letting its series disappear.
	idle := tl.snapshotLocked(time.Unix(start+5, 0))
	re.Equal(start, idle.StartUnixSec)
	re.Equal(start+5, idle.EndUnixSec)
	re.Empty(idle.Buckets)
	tl.ack(idle.EndUnixSec, tl.gen)
	next := tl.snapshotLocked(time.Unix(start+7, 0))
	re.Equal(start+5, next.StartUnixSec)
	re.Equal(start+7, next.EndUnixSec)
	re.Empty(next.Buckets)
}

func TestRUTimelineClockRollback(t *testing.T) {
	re := require.New(t)
	const start int64 = 1800000000
	tl := newRUTimeline(time.Unix(start, 0))
	tl.recordLocked(time.Unix(start+10, 0), 1, 1)
	_ = tl.snapshotLocked(time.Unix(start+12, 0))
	tl.recordLocked(time.Unix(start+20, 0), 2, 2)
	// The clock rolls back below the latest observed second: nothing is
	// reported until it passes that second again.
	rolledBack := tl.snapshotLocked(time.Unix(start+15, 0))
	re.Equal(start+15, rolledBack.StartUnixSec)
	re.Equal(start+15, rolledBack.EndUnixSec)
	re.Empty(rolledBack.Buckets)
	resumed := tl.snapshotLocked(time.Unix(start+22, 0))
	re.Equal(start+21, resumed.StartUnixSec)
	re.Equal(start+22, resumed.EndUnixSec)
	re.Empty(resumed.Buckets)
}

func TestRUTimelineLargeClockRollback(t *testing.T) {
	re := require.New(t)
	const start int64 = 1800000000
	const day = 86400
	tl := newRUTimeline(time.Unix(start, 0))
	// The clock jumps a day ahead and a report is sent from there.
	tl.recordLocked(time.Unix(start+day, 0), 1, 0)
	jumped := tl.snapshotLocked(time.Unix(start+day+1, 0))
	// After the clock is corrected, the timeline starts over instead of
	// waiting a day, and the late acknowledgement of the jumped report does
	// not skip the new seconds.
	gen := tl.gen
	tl.recordLocked(time.Unix(start+10, 0), 2, 0)
	tl.ack(jumped.EndUnixSec, gen)
	resumed := tl.snapshotLocked(time.Unix(start+12, 0))
	re.Equal(start+10, resumed.StartUnixSec)
	re.Equal(start+12, resumed.EndUnixSec)
	re.Equal([]*rmpb.RUConsumptionBucket{{Rru: 2, Offset: 0}}, resumed.Buckets)

	// A snapshot can start the timeline over too, and returns the new
	// generation.
	gen = tl.gen
	tl.last = time.Now().Unix() + day
	_, snapshotGen := tl.snapshot()
	re.Equal(gen+1, snapshotGen)
}

func TestRUTimelineAck(t *testing.T) {
	re := require.New(t)
	const start int64 = 1800000000
	tl := newRUTimeline(time.Unix(start, 0))
	tl.recordLocked(time.Unix(start, 0), 1, 2)
	sent := tl.snapshotLocked(time.Unix(start+2, 0))
	// An unacknowledged report is resent from the same second.
	re.Equal(start, tl.snapshotLocked(time.Unix(start+3, 0)).StartUnixSec)

	tl.ack(sent.EndUnixSec, tl.gen)
	next := tl.snapshotLocked(time.Unix(start+4, 0))
	re.Equal(start+2, next.StartUnixSec)
	re.Equal(start+4, next.EndUnixSec)
	re.Empty(next.Buckets)
	// A stale acknowledgement never rewinds the timeline.
	tl.ack(start+1, tl.gen)
	re.Equal(start+2, tl.snapshotLocked(time.Unix(start+4, 0)).StartUnixSec)
	// After a clock rollback within the retained seconds, acknowledged seconds
	// are not reported again, and reporting resumes after the latest second
	// observed before the rollback.
	sent = tl.snapshotLocked(time.Unix(start+600, 0))
	tl.ack(sent.EndUnixSec, tl.gen)
	rolledBack := tl.snapshotLocked(time.Unix(start+540, 0))
	re.Equal(start+540, rolledBack.StartUnixSec)
	re.Equal(start+540, rolledBack.EndUnixSec)
	re.Empty(rolledBack.Buckets)
	re.Empty(tl.snapshotLocked(time.Unix(start+580, 0)).Buckets)
	resumed := tl.snapshotLocked(time.Unix(start+603, 0))
	re.Equal(start+601, resumed.StartUnixSec)
	re.Equal(start+603, resumed.EndUnixSec)
	re.Empty(resumed.Buckets)
}

func TestRUTimelineConcurrentRecording(t *testing.T) {
	re := require.New(t)
	tl := newRUTimeline(time.Now())
	// Concurrent recording across second boundaries is never mistaken for a
	// clock rollback, and every recorded unit is reported exactly once.
	end := time.Now().Truncate(time.Second).Add(2 * time.Second)
	var recorded atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for time.Now().Before(end) {
				tl.record(1, 0)
				recorded.Add(1)
			}
		})
	}
	var reported float64
	drain := func() {
		s, gen := tl.snapshot()
		for _, b := range s.Buckets {
			reported += b.Rru
		}
		tl.ack(s.EndUnixSec, gen)
	}
	for time.Now().Before(end) {
		drain()
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
	// Wait for the second of the last record to close.
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second + 10*time.Millisecond)))
	drain()
	re.Equal(float64(recorded.Load()), reported)
}

func TestTrimRUTimelines(t *testing.T) {
	re := require.New(t)
	request := func(name string, start int64, offsets []uint32) *rmpb.TokenBucketRequest {
		buckets := make([]*rmpb.RUConsumptionBucket, len(offsets))
		for i, off := range offsets {
			buckets[i] = &rmpb.RUConsumptionBucket{Rru: float64(off) + 1, Offset: off}
		}
		return &rmpb.TokenBucketRequest{ResourceGroupName: name, ConsumptionSinceLastRequest: &rmpb.Consumption{
			RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, EndUnixSec: start + 90, Buckets: buckets},
		}}
	}
	requests := []*rmpb.TokenBucketRequest{
		request("a", 100, []uint32{0, 1, 2, 3, 4}),
		request("b", 200, []uint32{0}),
		request("idle", 300, nil),
	}
	// Keep only the newest two seconds of "a": offsets 3 and 4 become 0 and 1,
	// values 4 and 5, and the start moves to 103. "b" keeps its only second
	// and the idle group keeps its zero-bucket watermark.
	expected := []*rmpb.TokenBucketRequest{
		request("a", 103, []uint32{0, 1}),
		request("b", 200, []uint32{0}),
		request("idle", 300, nil),
	}
	expected[0].ConsumptionSinceLastRequest.RuBySecond.Buckets[0].Rru = 4
	expected[0].ConsumptionSinceLastRequest.RuBySecond.Buckets[1].Rru = 5
	expected[0].ConsumptionSinceLastRequest.RuBySecond.EndUnixSec = 190
	budget := (&rmpb.TokenBucketsRequest{Requests: expected}).Size()
	trimRUTimelines(requests, budget)
	re.Equal(expected, requests)
	re.Equal(int64(190), requests[0].ConsumptionSinceLastRequest.RuBySecond.EndUnixSec)

	// A budget for exactly one bucket keeps the newest second, relabels it to
	// offset 0 and advances the start to 104.
	oneRequests := []*rmpb.TokenBucketRequest{
		request("a", 100, []uint32{0, 1, 2, 3, 4}),
		request("b", 200, []uint32{0}),
		request("idle", 300, nil),
	}
	oneExpected := []*rmpb.TokenBucketRequest{
		request("a", 104, []uint32{0}),
		request("b", 200, []uint32{0}),
		request("idle", 300, nil),
	}
	oneExpected[0].ConsumptionSinceLastRequest.RuBySecond.Buckets[0].Rru = 5
	oneExpected[0].ConsumptionSinceLastRequest.RuBySecond.EndUnixSec = 190
	trimRUTimelines(oneRequests, (&rmpb.TokenBucketsRequest{Requests: oneExpected}).Size())
	re.Equal(oneExpected, oneRequests)

	// A budget that fits only the coverage fields keeps the watermark only:
	// the active groups end their interval where it ended, while the idle
	// group is unchanged and still confirms its coverage.
	watermark := []*rmpb.TokenBucketRequest{
		{ResourceGroupName: "a", ConsumptionSinceLastRequest: &rmpb.Consumption{RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: 190, EndUnixSec: 190}}},
		{ResourceGroupName: "b", ConsumptionSinceLastRequest: &rmpb.Consumption{RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: 290, EndUnixSec: 290}}},
		{ResourceGroupName: "idle", ConsumptionSinceLastRequest: &rmpb.Consumption{RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: 300, EndUnixSec: 390, Buckets: []*rmpb.RUConsumptionBucket{}}}},
	}
	trimRUTimelines(requests, (&rmpb.TokenBucketsRequest{Requests: watermark}).Size())
	re.Equal(watermark, requests)
	re.Equal(int64(190), requests[0].ConsumptionSinceLastRequest.RuBySecond.EndUnixSec)
	re.Equal(int64(290), requests[1].ConsumptionSinceLastRequest.RuBySecond.EndUnixSec)
	re.Equal(int64(300), requests[2].ConsumptionSinceLastRequest.RuBySecond.StartUnixSec)
}

func timelineTotals(gc *groupCostController) (rru, wru float64) {
	gc.ruTimeline.mu.Lock()
	defer gc.ruTimeline.mu.Unlock()
	for _, b := range gc.ruTimeline.buckets {
		rru += b.rru
		wru += b.wru
	}
	return
}

func TestRUTimelineRequestAndResponse(t *testing.T) {
	re := require.New(t)
	for _, wait := range []bool{false, true} {
		gc := createTestGroupCostController(re)
		gc.burstable.Store(true)
		req := &TestRequestInfo{isWrite: true, writeBytes: 100, numReplicas: 1, storeID: 1}
		delta, _, _, _, err := gc.onRequestWaitImpl(context.Background(), req)
		re.NoError(err)
		_, wru := timelineTotals(gc)
		re.Equal(delta.WRU, wru)
		// Failed writes refund at settlement; the full request must not be added again.
		resp := &TestResponseInfo{succeed: false}
		if wait {
			_, _, err = gc.onResponseWaitImpl(context.Background(), req, resp)
		} else {
			_, err = gc.onResponseImpl(req, resp)
		}
		re.NoError(err)
		_, wru = timelineTotals(gc)
		re.InDelta(float64(gc.mainCfg.WritePerBatchBaseCost)*defaultAvgBatchProportion, wru, 1e-12)
	}
}

func TestRUTimelineFailedWait(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	gc.burstable.Store(false)
	gc.isThrottled.Store(true)
	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{newTokens: 0, newFillRate: 0, newBurst: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A rejected request consumes nothing.
	req := &TestRequestInfo{isWrite: true, writeBytes: 1 << 30, numReplicas: 1, storeID: 1}
	_, _, _, _, err := gc.onRequestWaitImpl(ctx, req)
	re.Error(err)
	rru, wru := timelineTotals(gc)
	re.Zero(rru)
	re.Zero(wru)
	// A completed response is recorded even when its token wait fails.
	resp := &TestResponseInfo{readBytes: 1 << 30, succeed: true}
	_, _, err = gc.onResponseWaitImpl(ctx, &TestRequestInfo{storeID: 1}, resp)
	re.Error(err)
	rru, _ = timelineTotals(gc)
	re.Positive(rru)
}

func TestRUTimelineUntimedConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	// TiFlash aggregates count toward consumption but stay out of the timeline.
	gc.addRUConsumption(&rmpb.Consumption{RRU: 100})
	re.Equal(float64(100), gc.mu.consumption.RRU)
	rru, wru := timelineTotals(gc)
	re.Zero(rru)
	re.Zero(wru)
}

func TestTombstoneDoesNotReportRUTimeline(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	provider := newMockResourceGroupProvider()
	c, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	settings := &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}
	for _, name := range []string{defaultResourceGroupName, "test-group"} {
		provider.On("GetResourceGroup", mock.Anything, name, mock.Anything).Return(&rmpb.ResourceGroup{Name: name, Mode: rmpb.GroupMode_RUMode, RUSettings: settings}, nil)
	}
	defaultGC, err := c.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
	re.NoError(err)
	_, err = c.tryGetResourceGroupController(ctx, "test-group", false)
	re.NoError(err)
	c.tombstoneGroupCostController("test-group")
	tombstone, _ := c.loadGroupController("test-group")

	// A tombstone requests tokens as the default group, but its RU is not
	// part of any group's peak: it has no timeline of its own.
	re.Nil(tombstone.ruTimeline)
	seconds, _ := tombstone.ruTimeline.snapshot()
	re.Nil(seconds)
	_, _, _, _, err = c.OnRequestWait(ctx, "test-group", NewTestRequestInfo(true, 1, 1, AccessUnknown))
	re.NoError(err)
	rru, wru := timelineTotals(defaultGC)
	re.Zero(rru)
	re.Zero(wru)
}

func TestTokenRequestAcknowledgesRUTimeline(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	for _, tc := range []struct {
		rpcErr error
		// trimmed drops the timeline field, as when even the coverage
		// metadata exceeds the RPC budget.
		trimmed bool
		// restarted starts the timeline over while the report is in flight.
		restarted bool
		acked     bool
	}{
		{nil, false, false, true},
		{errors.New("rpc failed"), false, false, false},
		{nil, true, false, false},
		{nil, false, true, false},
	} {
		provider := newMockResourceGroupProvider()
		provider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).Return([]*rmpb.TokenBucketResponse(nil), tc.rpcErr)
		c, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID)
		re.NoError(err)
		gc := createTestGroupCostController(re)
		start := time.Now().Add(-3 * time.Second)
		gc.ruTimeline = newRUTimeline(start)
		gc.ruTimeline.recordLocked(start.Add(time.Second), 1, 0)
		gc.run.requestInProgress = true
		req := gc.collectRequestAndConsumption(periodicReport)
		report := ruTimelineReport{timeline: gc.ruTimeline}
		req.ConsumptionSinceLastRequest.RuBySecond, report.gen = gc.ruTimeline.snapshot()
		seconds := req.ConsumptionSinceLastRequest.RuBySecond
		re.Equal(start.Unix(), seconds.StartUnixSec)
		re.GreaterOrEqual(seconds.EndUnixSec, start.Unix()+3)
		re.Equal([]*rmpb.RUConsumptionBucket{{Rru: 1, Offset: 1}}, seconds.Buckets)
		if tc.trimmed {
			trimRUTimelines([]*rmpb.TokenBucketRequest{req}, 1)
			re.Nil(req.GetConsumptionSinceLastRequest().GetRuBySecond())
		}
		if tc.restarted {
			gc.ruTimeline.gen++
		}
		c.sendTokenBucketRequests(ctx, []*rmpb.TokenBucketRequest{req}, []ruTimelineReport{report}, FromPeriodReport, notifyMsg{})
		<-c.tokenResponseChan
		gc.ruTimeline.mu.Lock()
		from := gc.ruTimeline.from
		gc.ruTimeline.mu.Unlock()
		if tc.acked {
			re.Equal(seconds.EndUnixSec, from)
		} else {
			re.Equal(start.Unix(), from, "an unacknowledged report must be resent")
		}
	}
}

func TestTrimRUTimelinesWireBudget(t *testing.T) {
	re := require.New(t)
	const groups, seconds = 16000, 90
	name := "0123456789abcdef0123456789abcdef"
	requests := make([]*rmpb.TokenBucketRequest, groups)
	for g := range requests {
		buckets := make([]*rmpb.RUConsumptionBucket, seconds)
		for i := range buckets {
			buckets[i] = &rmpb.RUConsumptionBucket{Rru: 1.5, Wru: 2.5, Offset: uint32(i)}
		}
		requests[g] = &rmpb.TokenBucketRequest{
			ResourceGroupName: name,
			Request: &rmpb.TokenBucketRequest_RuItems{RuItems: &rmpb.TokenBucketRequest_RequestRU{
				RequestRU: []*rmpb.RequestUnitItem{
					{Type: rmpb.RequestUnitType_RU, Value: 100}, {Type: rmpb.RequestUnitType_RU, Value: 101},
					{Type: rmpb.RequestUnitType_RU, Value: 102},
				},
			}},
			ConsumptionSinceLastRequest: &rmpb.Consumption{
				RRU: 1, WRU: 2, ReadBytes: 3, WriteBytes: 4, TotalCpuTimeMs: 5, SqlLayerCpuTimeMs: 6,
				RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: 1800000000, EndUnixSec: 1800000090, Buckets: buckets},
			},
		}
	}
	full := &rmpb.TokenBucketsRequest{Requests: requests, TargetRequestPeriodMs: 1000, ClientUniqueId: 1}
	re.Greater(full.Size(), 4<<20, "the unbatched timeline must exceed the gRPC limit")
	trimRUTimelines(requests, ruTimelineRequestBudget)
	re.LessOrEqual(full.Size(), 4<<20)
	re.LessOrEqual(full.Size(), ruTimelineRequestBudget)
}

func TestTrimRUTimelinesSingleGroupKeepsAllSeconds(t *testing.T) {
	re := require.New(t)
	buckets := make([]*rmpb.RUConsumptionBucket, 90)
	for i := range buckets {
		buckets[i] = &rmpb.RUConsumptionBucket{Rru: 1, Offset: uint32(i)}
	}
	requests := []*rmpb.TokenBucketRequest{{ResourceGroupName: "g", ConsumptionSinceLastRequest: &rmpb.Consumption{
		RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: 100, EndUnixSec: 190, Buckets: buckets},
	}}}
	trimRUTimelines(requests, ruTimelineRequestBudget)
	re.Len(requests[0].ConsumptionSinceLastRequest.RuBySecond.Buckets, 90)
}

func TestTrimRUTimelinesFastPathDoesNotSave(t *testing.T) {
	re := require.New(t)
	requests := make([]*rmpb.TokenBucketRequest, 1000)
	for i := range requests {
		requests[i] = &rmpb.TokenBucketRequest{ConsumptionSinceLastRequest: &rmpb.Consumption{
			RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: 100, EndUnixSec: 102,
				Buckets: []*rmpb.RUConsumptionBucket{{Rru: 1, Offset: 0}}},
		}}
	}
	// A batch within the budget is sent as is, without saving the timelines.
	re.Zero(testing.AllocsPerRun(10, func() { trimRUTimelines(requests, ruTimelineRequestBudget) }))
}

func TestTrimRUTimelinesDropsTimelineField(t *testing.T) {
	const groups = 29000
	name := "0123456789abcdef0123456789abcdef"
	requests := make([]*rmpb.TokenBucketRequest, groups)
	for g := range requests {
		requests[g] = &rmpb.TokenBucketRequest{
			ResourceGroupName: name,
			Request: &rmpb.TokenBucketRequest_RuItems{RuItems: &rmpb.TokenBucketRequest_RequestRU{
				RequestRU: []*rmpb.RequestUnitItem{
					{Type: rmpb.RequestUnitType_RU, Value: 100}, {Type: rmpb.RequestUnitType_RU, Value: 101},
					{Type: rmpb.RequestUnitType_RU, Value: 102},
				},
			}},
			ConsumptionSinceLastRequest: &rmpb.Consumption{
				RRU: 1.5, WRU: 2.5, ReadBytes: 1024, WriteBytes: 2048, TotalCpuTimeMs: 10, SqlLayerCpuTimeMs: 5,
				RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: 1800000000, EndUnixSec: 1800000090,
					Buckets: []*rmpb.RUConsumptionBucket{{Rru: 1, Offset: 0}}},
			},
		}
	}
	full := &rmpb.TokenBucketsRequest{Requests: requests, TargetRequestPeriodMs: 1000, ClientUniqueId: 1}
	trimRUTimelines(requests, ruTimelineRequestBudget)
	re := require.New(t)
	re.LessOrEqual(full.Size(), ruTimelineRequestBudget)
	for _, req := range requests {
		re.Nil(req.GetConsumptionSinceLastRequest().GetRuBySecond())
	}
}
