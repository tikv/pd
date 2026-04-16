// Copyright 2025 TiKV Project Authors.
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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/errs"
)

func createTestGroupCostController(re *require.Assertions) *groupCostController {
	group := &rmpb.ResourceGroup{
		Name:     "test",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 1,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 1000,
				},
			},
		},
		BackgroundSettings: &rmpb.BackgroundSettings{
			JobTypes: []string{"lightning", "br"},
		},
	}
	ch1 := make(chan notifyMsg)
	ch2 := make(chan *groupCostController)
	gc, err := newGroupCostController(group, DefaultRUConfig(), ch1, ch2)
	re.NoError(err)
	return gc
}

func TestGroupControlBurstable(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	args := tokenBucketReconfigureArgs{
		newFillRate: 1000,
		newBurst:    -1,
	}
	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), args)
	gc.updateAvgRequestResourcePerSec()
	re.True(gc.burstable.Load())
}

func TestRequestAndResponseConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	testCases := []struct {
		req  *TestRequestInfo
		resp *TestResponseInfo
	}{
		// Write request
		{
			req: &TestRequestInfo{
				isWrite:     true,
				writeBytes:  100,
				numReplicas: 3,
				accessType:  AccessUnknown,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				succeed:   true,
			},
		},
		// Write request local AZ
		{
			req: &TestRequestInfo{
				isWrite:     true,
				writeBytes:  100,
				numReplicas: 3,
				accessType:  AccessLocalZone,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				succeed:   true,
			},
		},
		// Write request cross AZ
		{
			req: &TestRequestInfo{
				isWrite:     true,
				writeBytes:  100,
				numReplicas: 3,
				accessType:  AccessCrossZone,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				succeed:   true,
			},
		},
		// Read request
		{
			req: &TestRequestInfo{
				isWrite:     false,
				writeBytes:  0,
				numReplicas: 3,
				accessType:  AccessLocalZone,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				kvCPU:     100 * time.Millisecond,
				succeed:   true,
			},
		},
		// Read request cross AZ
		{
			req: &TestRequestInfo{
				isWrite:     false,
				writeBytes:  0,
				numReplicas: 3,
				accessType:  AccessCrossZone,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				kvCPU:     100 * time.Millisecond,
				succeed:   true,
			},
		},
	}
	kvCalculator := gc.getKVCalculator()
	for idx, testCase := range testCases {
		caseNum := fmt.Sprintf("case %d", idx)
		consumption, _, _, priority, err := gc.onRequestWaitImpl(context.TODO(), testCase.req)
		re.NoError(err, caseNum)
		re.Equal(priority, gc.meta.Priority)
		expectedConsumption := &rmpb.Consumption{}
		if testCase.req.IsWrite() {
			kvCalculator.calculateWriteCost(expectedConsumption, testCase.req)
			re.Equal(expectedConsumption.WRU, consumption.WRU)
			if testCase.req.AccessLocationType() != AccessUnknown {
				re.Positive(expectedConsumption.WriteCrossAzTrafficBytes, caseNum)
			}
		}
		consumption, err = gc.onResponseImpl(testCase.req, testCase.resp)
		re.NoError(err, caseNum)
		kvCalculator.calculateReadCost(expectedConsumption, testCase.resp)
		kvCalculator.calculateCPUCost(expectedConsumption, testCase.resp)
		calculateCrossAZTraffic(expectedConsumption, testCase.req, testCase.resp)
		re.Equal(expectedConsumption.RRU, consumption.RRU, caseNum)
		re.Equal(expectedConsumption.TotalCpuTimeMs, consumption.TotalCpuTimeMs, caseNum)
		if testCase.req.IsWrite() && testCase.req.AccessLocationType() != AccessUnknown {
			re.Positive(expectedConsumption.WriteCrossAzTrafficBytes, caseNum)
		} else if !testCase.req.IsWrite() && testCase.req.AccessLocationType() == AccessCrossZone {
			re.Positive(expectedConsumption.ReadCrossAzTrafficBytes, caseNum)
		} else {
			re.Equal(expectedConsumption.ReadCrossAzTrafficBytes, uint64(0), caseNum)
			re.Equal(expectedConsumption.WriteCrossAzTrafficBytes, uint64(0), caseNum)
		}
	}
}

func TestOnResponseWaitConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	req := &TestRequestInfo{
		isWrite: false,
	}
	resp := &TestResponseInfo{
		readBytes: 2000 * 64 * 1024, // 2000RU
		succeed:   true,
	}

	consumption, waitTIme, err := gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	re.Zero(waitTIme)
	verify := func() {
		expectedConsumption := &rmpb.Consumption{}
		kvCalculator := gc.getKVCalculator()
		kvCalculator.calculateReadCost(expectedConsumption, resp)
		re.Equal(expectedConsumption.RRU, consumption.RRU)
	}
	verify()

	// modify the counter, then on response should has wait time.
	gc.modifyTokenCounter(gc.run.requestUnitTokens, &rmpb.TokenBucket{
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   1000,
			BurstLimit: 1000,
		},
	},
		int64(5*time.Second/time.Millisecond),
	)

	consumption, waitTIme, err = gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	re.NotZero(waitTIme)
	verify()
}

func TestPagingSizeBytesPreCharge(t *testing.T) {
	re := require.New(t)
	cfg := DefaultRUConfig()
	kvCalc := newKVCalculator(cfg)

	// Phase 1: BeforeKVRequest with pagingSizeBytes should pre-charge
	// baseCost + pagingSizeBytes * ReadBytesCost.
	pagingSizeBytes := uint64(4 * 1024 * 1024) // 4 MB
	req := &TestRequestInfo{
		isWrite:         false,
		pagingSizeBytes: pagingSizeBytes,
	}
	phase1 := &rmpb.Consumption{}
	kvCalc.BeforeKVRequest(phase1, req)

	baseCost := float64(cfg.ReadBaseCost) + float64(cfg.ReadPerBatchBaseCost)*0.7
	bytesCost := float64(cfg.ReadBytesCost) * float64(pagingSizeBytes)
	re.InDelta(baseCost+bytesCost, phase1.RRU, 1e-6,
		"Phase 1 should pre-charge baseCost + bytes RU")

	// Phase 2: AfterKVRequest should subtract the pre-charged bytes RU.
	resp := &TestResponseInfo{
		readBytes: 2 * 1024 * 1024, // actual read 2 MB
		kvCPU:     10 * time.Millisecond,
		succeed:   true,
	}
	phase2 := &rmpb.Consumption{}
	kvCalc.AfterKVRequest(phase2, req, resp)

	actualReadCost := float64(cfg.ReadBytesCost) * float64(resp.readBytes)
	cpuCost := float64(cfg.CPUMsCost) * 10.0
	expectedPhase2RRU := actualReadCost + cpuCost - bytesCost
	re.InDelta(expectedPhase2RRU, phase2.RRU, 1e-6,
		"Phase 2 should be actualCost - preCharged bytesCost")

	// Net total should equal baseCost + actualCost (no double-counting).
	totalRRU := phase1.RRU + phase2.RRU
	expectedTotal := baseCost + actualReadCost + cpuCost
	re.InDelta(expectedTotal, totalRRU, 1e-6,
		"Total RRU across Phase 1+2 should equal baseCost + actualCost")

	// Without pagingSizeBytes, Phase 1 should only charge baseCost.
	reqNoPaging := &TestRequestInfo{isWrite: false}
	noPaging := &rmpb.Consumption{}
	kvCalc.BeforeKVRequest(noPaging, reqNoPaging)
	re.InDelta(baseCost, noPaging.RRU, 1e-6,
		"Without paging, Phase 1 should only charge baseCost")
}

func TestPagingPreChargeTokenRefund(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	// Give the limiter a known amount of tokens with no fill rate for precise measurement.
	initialTokens := float64(100000)
	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   initialTokens,
		newFillRate: 0,
		newBurst:    0,
	})

	pagingSizeBytes := uint64(4 * 1024 * 1024) // 4 MB pre-charge
	actualReadBytes := uint64(1 * 1024 * 1024)  // 1 MB actual

	req := &TestRequestInfo{
		isWrite:         false,
		pagingSizeBytes: pagingSizeBytes,
	}
	resp := &TestResponseInfo{
		readBytes: actualReadBytes,
		succeed:   true,
	}

	// Pre-charge reserves tokens from the limiter.
	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	tokensAfterPreCharge := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	// Response settlement should refund excess tokens.
	_, _, err = gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	tokensAfterSettlement := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	// The limiter should have more tokens after settlement than after pre-charge,
	// because the refund (pre-charge - actual) exceeds the actual read cost.
	cfg := DefaultRUConfig()
	preChargeCost := float64(cfg.ReadBytesCost) * float64(pagingSizeBytes)
	actualCost := float64(cfg.ReadBytesCost) * float64(actualReadBytes)
	expectedRefund := preChargeCost - actualCost
	re.Positive(expectedRefund, "sanity: pre-charge should exceed actual cost")

	re.InDelta(tokensAfterPreCharge+expectedRefund, tokensAfterSettlement, 1.0,
		"limiter should be refunded the excess pre-charged tokens")

	// Verify net consumption is correct: baseCost + actualReadCost.
	gc.mu.Lock()
	netRRU := gc.mu.consumption.RRU
	gc.mu.Unlock()
	baseCost := float64(cfg.ReadBaseCost) + float64(cfg.ReadPerBatchBaseCost)*defaultAvgBatchProportion
	expectedNetRRU := baseCost + actualCost
	re.InDelta(expectedNetRRU, netRRU, 1e-6,
		"net consumption should equal baseCost + actualReadCost")
}

func TestPagingPreChargeNoRefundWhenActualExceedsEstimate(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	initialTokens := float64(100000)
	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   initialTokens,
		newFillRate: 0,
		newBurst:    0,
	})

	pagingSizeBytes := uint64(1 * 1024 * 1024) // 1 MB pre-charge
	actualReadBytes := uint64(4 * 1024 * 1024)  // 4 MB actual (exceeds estimate)

	req := &TestRequestInfo{
		isWrite:         false,
		pagingSizeBytes: pagingSizeBytes,
	}
	resp := &TestResponseInfo{
		readBytes: actualReadBytes,
		succeed:   true,
	}

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	tokensAfterPreCharge := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	_, _, err = gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	tokensAfterSettlement := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	// Actual exceeds pre-charge, so settlement should consume more tokens (not refund).
	re.Less(tokensAfterSettlement, tokensAfterPreCharge,
		"when actual exceeds pre-charge, settlement should consume tokens")
}

func TestOnResponseImplPagingRefund(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	initialTokens := float64(100000)
	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   initialTokens,
		newFillRate: 0,
		newBurst:    0,
	})

	pagingSizeBytes := uint64(4 * 1024 * 1024) // 4 MB pre-charge
	actualReadBytes := uint64(512 * 1024)       // 512 KB actual

	req := &TestRequestInfo{
		isWrite:         false,
		pagingSizeBytes: pagingSizeBytes,
	}
	resp := &TestResponseInfo{
		readBytes: actualReadBytes,
		succeed:   true,
	}

	// Pre-charge
	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	tokensAfterPreCharge := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	// Settlement via onResponseImpl (non-waiting path)
	_, err = gc.onResponseImpl(req, resp)
	re.NoError(err)
	tokensAfterSettlement := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	// Should have refunded tokens.
	re.Greater(tokensAfterSettlement, tokensAfterPreCharge,
		"onResponseImpl should refund excess pre-charged tokens")
}

func TestPagingPreChargeZeroDelta(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	initialTokens := float64(100000)
	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   initialTokens,
		newFillRate: 0,
		newBurst:    0,
	})

	// Set actual read bytes equal to pagingSizeBytes so the read-bytes delta is zero.
	// The only settlement cost should be CPU time (which we set to zero here).
	pagingSizeBytes := uint64(2 * 1024 * 1024)
	req := &TestRequestInfo{
		isWrite:         false,
		pagingSizeBytes: pagingSizeBytes,
	}
	resp := &TestResponseInfo{
		readBytes: pagingSizeBytes, // exact match
		succeed:   true,
	}

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	tokensAfterPreCharge := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	_, _, err = gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	tokensAfterSettlement := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	// With zero CPU cost and exact byte match, settlement delta is zero.
	// No tokens should be consumed or refunded in the settlement step.
	re.InDelta(tokensAfterPreCharge, tokensAfterSettlement, 1e-6,
		"exact byte match should produce zero settlement delta")
}

func TestResourceGroupThrottledError(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	req := &TestRequestInfo{
		isWrite:    true,
		writeBytes: 10000000,
	}
	// The group is throttled
	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.Error(err)
	re.True(errs.ErrClientResourceGroupThrottled.Equal(err))
}

func TestAcquireTokensSignalAwareWait(t *testing.T) {
	re := require.New(t)

	// Build controller with a buffered lowRUNotifyChan so the test can
	// observe the notify() call inside reserveN as a synchronization point.
	group := &rmpb.ResourceGroup{
		Name: "test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000},
			},
		},
	}
	notifyCh := make(chan notifyMsg, 1)
	cfg := DefaultRUConfig()
	cfg.WaitRetryInterval = 5 * time.Second
	cfg.WaitRetryTimes = 3
	gc, err := newGroupCostController(group, cfg, notifyCh, make(chan *groupCostController, 1))
	re.NoError(err)

	// Set fillRate=0 so reservation always fails with InfDuration,
	// which is the exact scenario described in issue #10251.
	counter := gc.run.requestUnitTokens
	counter.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   1000,
		newFillRate: 0,
		newBurst:    0,
	})

	delta := &rmpb.Consumption{RRU: 5000}
	type acquireResult struct {
		err          error
		waitDuration time.Duration
	}
	resultCh := make(chan acquireResult, 1)
	go func() {
		var waitDuration time.Duration
		_, err := gc.acquireTokens(context.Background(), delta, &waitDuration, false)
		resultCh <- acquireResult{err, waitDuration}
	}()

	// Wait for notify — Reserve has failed and the retry path is entered.
	select {
	case <-notifyCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for low-RU notification")
	}

	// Now reconfigure with enough tokens and a real fillRate.
	// This closes the reconfiguredCh (waking the select) and provides
	// tokens so the next Reserve() succeeds.
	counter.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   100000,
		newFillRate: 100000,
		newBurst:    0,
	})

	select {
	case r := <-resultCh:
		re.NoError(r.err)
		re.Less(r.waitDuration, cfg.WaitRetryInterval)
	case <-time.After(cfg.WaitRetryInterval):
		t.Fatal("acquireTokens was not woken up promptly by Reconfigure signal")
	}
}

func TestAcquireTokensFallbackToTimer(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	// Short retry interval so the test runs fast.
	gc.mainCfg.WaitRetryInterval = 50 * time.Millisecond
	gc.mainCfg.WaitRetryTimes = 3
	gc.mainCfg.LTBMaxWaitDuration = 100 * time.Millisecond

	// Set fillRate=0 and never reconfigure — no signal will arrive.
	counter := gc.run.requestUnitTokens
	counter.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   1000,
		newFillRate: 0,
		newBurst:    0,
	})

	delta := &rmpb.Consumption{RRU: 5000}
	ctx := context.Background()
	var waitDuration time.Duration
	_, err := gc.acquireTokens(ctx, delta, &waitDuration, false)

	// Without a Reconfigure signal, all retries should exhaust and return an error.
	re.Error(err)
	re.True(errs.ErrClientResourceGroupThrottled.Equal(err))
	// waitDuration should be roughly retryTimes * retryInterval.
	re.GreaterOrEqual(waitDuration, gc.mainCfg.WaitRetryInterval*time.Duration(gc.mainCfg.WaitRetryTimes))
}
