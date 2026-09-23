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
	var tl ruTimeline
	tl.record(time.Unix(start, 999999999), 10, 20)
	tl.record(time.Unix(start+1, 0), -3, 5)
	first := tl.snapshot(time.Unix(start+2, 0))
	re.Equal(start, first.StartUnixSec)
	re.Equal([]*rmpb.RUConsumptionBucket{{Rru: 10, Wru: 20}, {Rru: -3, Wru: 5}}, first.Buckets)

	// Snapshots replay idle seconds as zero and own their memory.
	again := tl.snapshot(time.Unix(start+3, 0))
	re.Equal(&rmpb.RUConsumptionBucket{}, again.Buckets[2])
	again.Buckets[0].Rru = 999
	re.Equal(float64(10), first.Buckets[0].Rru)
	re.Equal(float64(10), tl.snapshot(time.Unix(start+3, 0)).Buckets[0].Rru)

	// Only the retained window is replayed, and a reused slot never replays
	// the second it held before.
	tl.record(time.Unix(start+181, 0), 7, 8)
	latest := tl.snapshot(time.Unix(start+183, 0))
	re.Len(latest.Buckets, ruTimelineSeconds)
	re.Equal(start+3, latest.StartUnixSec)
	re.Equal(&rmpb.RUConsumptionBucket{Rru: 7, Wru: 8}, latest.Buckets[ruTimelineSeconds-2])
	re.Equal(&rmpb.RUConsumptionBucket{}, latest.Buckets[ruTimelineSeconds-1])

	// A clock rollback quarantines a full window.
	tl.record(time.Unix(start+180, 0), 100, 100)
	re.Nil(tl.snapshot(time.Unix(start+184, 0)))
	re.Empty(tl.snapshot(time.Unix(start+360, 0)).Buckets)
}

func TestRUTimelineAck(t *testing.T) {
	re := require.New(t)
	const start int64 = 1800000000
	var tl ruTimeline
	tl.record(time.Unix(start, 0), 1, 2)
	sent := tl.snapshot(time.Unix(start+2, 0))
	// An unacknowledged report is resent from the same second.
	re.Equal(start, tl.snapshot(time.Unix(start+3, 0)).StartUnixSec)

	tl.ack(sent.StartUnixSec + int64(len(sent.Buckets)))
	next := tl.snapshot(time.Unix(start+4, 0))
	re.Equal(start+2, next.StartUnixSec)
	re.Len(next.Buckets, 2)
	// A stale acknowledgement never rewinds the timeline.
	tl.ack(start + 1)
	re.Equal(start+2, tl.snapshot(time.Unix(start+4, 0)).StartUnixSec)
	// After a clock rollback, acknowledged seconds are not reported again,
	// even once the quarantine is over.
	sent = tl.snapshot(time.Unix(start+600, 0))
	tl.ack(sent.StartUnixSec + int64(len(sent.Buckets)))
	re.Nil(tl.snapshot(time.Unix(start+300, 0)))
	re.Nil(tl.snapshot(time.Unix(start+480, 0)))
	re.Equal(start+600, tl.snapshot(time.Unix(start+601, 0)).StartUnixSec)
}

func TestTrimRUTimelines(t *testing.T) {
	re := require.New(t)
	request := func(start int64, n int) *rmpb.TokenBucketRequest {
		buckets := make([]*rmpb.RUConsumptionBucket, n)
		for i := range buckets {
			buckets[i] = &rmpb.RUConsumptionBucket{Rru: float64(start) + float64(i)}
		}
		return &rmpb.TokenBucketRequest{ConsumptionSinceLastRequest: &rmpb.Consumption{
			RuBySecond: &rmpb.RUConsumptionBySecond{StartUnixSec: start, Buckets: buckets},
		}}
	}
	requests := []*rmpb.TokenBucketRequest{request(100, 5), request(200, 1), {}}
	trimRUTimelines(requests, 6)
	re.Len(requests[0].ConsumptionSinceLastRequest.RuBySecond.Buckets, 5, "within budget")

	// Over budget, each request keeps its newest budget/len(requests) seconds.
	trimRUTimelines(requests, 3)
	kept := requests[0].ConsumptionSinceLastRequest.RuBySecond
	re.Equal(int64(104), kept.StartUnixSec)
	re.Equal([]*rmpb.RUConsumptionBucket{{Rru: 104}}, kept.Buckets)
	re.Len(requests[1].ConsumptionSinceLastRequest.RuBySecond.Buckets, 1)
}

func timelineTotals(gc *groupCostController) (rru, wru float64) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	for _, b := range gc.mu.ruTimeline.buckets {
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
	gc.addRUConsumption(&rmpb.Consumption{RRU: 100})
	re.Equal(float64(100), gc.mu.consumption.RRU)
	re.Nil(gc.mu.ruTimeline.snapshot(time.Now()))
}

func TestTombstoneSharesDefaultRUTimeline(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	// A tombstone reports as the default group; a second default timeline
	// from the same client would conflict with the real one.
	_, _, _, _, err = c.OnRequestWait(ctx, "test-group", NewTestRequestInfo(true, 1, 1, AccessUnknown))
	re.NoError(err)
	_, wru := timelineTotals(defaultGC)
	re.Positive(wru)
	_, wru = timelineTotals(tombstone)
	re.Zero(wru)

	// Its reports carry the shared timeline, including its quarantine.
	tombstone.addRUConsumption(&rmpb.Consumption{RRU: 1})
	tombstone.run.requestInProgress = true
	re.Nil(tombstone.collectRequestAndConsumption(periodicReport).ConsumptionSinceLastRequest.RuBySecond)
	defaultGC.run.requestInProgress = true
	re.Nil(defaultGC.collectRequestAndConsumption(periodicReport).ConsumptionSinceLastRequest.RuBySecond)
}

func TestTokenRequestAcknowledgesRUTimeline(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, rpcErr := range []error{nil, errors.New("rpc failed")} {
		provider := newMockResourceGroupProvider()
		provider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).Return([]*rmpb.TokenBucketResponse(nil), rpcErr)
		c, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID)
		re.NoError(err)
		gc := createTestGroupCostController(re)
		gc.run.requestInProgress = true
		req := gc.collectRequestAndConsumption(periodicReport)
		c.sendTokenBucketRequests(ctx, []*rmpb.TokenBucketRequest{req}, []*groupCostController{gc}, FromPeriodReport, notifyMsg{})
		<-c.tokenResponseChan
		gc.mu.Lock()
		acked := gc.mu.ruTimeline.acked
		gc.mu.Unlock()
		if rpcErr != nil {
			re.Zero(acked, "a failed report must be resent")
		} else {
			seconds := req.ConsumptionSinceLastRequest.RuBySecond
			re.Equal(seconds.StartUnixSec+int64(len(seconds.Buckets)), acked)
		}
	}
}

func TestTokenRequestCarriesRUTimeline(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	gc.run.requestInProgress = true
	report := gc.collectRequestAndConsumption(periodicReport)
	re.NotNil(report)
	re.NotNil(report.ConsumptionSinceLastRequest.RuBySecond)
}
