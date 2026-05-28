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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/resource_group/controller/metrics"
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

func TestPredictedReadBytesPreCharge(t *testing.T) {
	re := require.New(t)
	cfg := DefaultRUConfig()
	kvCalc := newKVCalculator(cfg)

	// BeforeKVRequest with a PredictedReadBytes hint should pre-charge
	// baseCost + predictedReadBytes * ReadBytesCost, regardless of
	// whether PagingSizeBytes is set. The two are decoupled.
	pagingSizeBytes := uint64(4 * 1024 * 1024) // protocol-level cap only
	predictedReadBytes := uint64(256 * 1024)   // learned EMA estimate
	req := &TestRequestInfo{
		isWrite:            false,
		pagingSizeBytes:    pagingSizeBytes,
		predictedReadBytes: predictedReadBytes,
	}
	precharge := &rmpb.Consumption{}
	kvCalc.BeforeKVRequest(precharge, req)

	baseCost := float64(cfg.ReadBaseCost) + float64(cfg.ReadPerBatchBaseCost)*defaultAvgBatchProportion
	hintCost := float64(cfg.ReadBytesCost) * float64(predictedReadBytes)
	re.InDelta(baseCost+hintCost, precharge.RRU, 1e-6,
		"BeforeKVRequest should pre-charge based on PredictedReadBytes")

	// AfterKVRequest should subtract the same hint basis, preserving
	// precharge + settle == baseCost + actualCost.
	actualReadBytes := uint64(300 * 1024) // close to the prediction
	resp := &TestResponseInfo{
		readBytes: actualReadBytes,
		kvCPU:     10 * time.Millisecond,
		succeed:   true,
	}
	settle := &rmpb.Consumption{}
	kvCalc.AfterKVRequest(settle, req, resp)

	actualReadCost := float64(cfg.ReadBytesCost) * float64(actualReadBytes)
	cpuCost := float64(cfg.CPUMsCost) * 10.0
	re.InDelta(actualReadCost+cpuCost-hintCost, settle.RRU, 1e-6,
		"AfterKVRequest should settle using the same hint basis as BeforeKVRequest")

	totalRRU := precharge.RRU + settle.RRU
	re.InDelta(baseCost+actualReadCost+cpuCost, totalRRU, 1e-6,
		"Total RRU across precharge+settle should equal baseCost + actualCost")
}

func TestNoPreChargeWithoutPredictedReadBytes(t *testing.T) {
	re := require.New(t)
	cfg := DefaultRUConfig()
	kvCalc := newKVCalculator(cfg)
	baseCost := float64(cfg.ReadBaseCost) + float64(cfg.ReadPerBatchBaseCost)*defaultAvgBatchProportion

	// With PagingSizeBytes but no PredictedReadBytes hint, BeforeKVRequest
	// must NOT pre-charge: the two quantities are decoupled. AfterKVRequest
	// bills the actual read bytes only.
	pagingSizeBytes := uint64(4 * 1024 * 1024)
	reqPagingOnly := &TestRequestInfo{
		isWrite:         false,
		pagingSizeBytes: pagingSizeBytes,
	}
	precharge := &rmpb.Consumption{}
	kvCalc.BeforeKVRequest(precharge, reqPagingOnly)
	re.InDelta(baseCost, precharge.RRU, 1e-6,
		"PagingSizeBytes alone must not trigger pre-charge")

	actualReadBytes := uint64(2 * 1024 * 1024)
	resp := &TestResponseInfo{
		readBytes: actualReadBytes,
		kvCPU:     10 * time.Millisecond,
		succeed:   true,
	}
	settle := &rmpb.Consumption{}
	kvCalc.AfterKVRequest(settle, reqPagingOnly, resp)

	actualReadCost := float64(cfg.ReadBytesCost) * float64(actualReadBytes)
	cpuCost := float64(cfg.CPUMsCost) * 10.0
	re.InDelta(actualReadCost+cpuCost, settle.RRU, 1e-6,
		"AfterKVRequest should bill actual read cost only when nothing was pre-charged")

	// Bare request without any hint or paging also pre-charges nothing.
	reqNone := &TestRequestInfo{isWrite: false}
	prechargeNone := &rmpb.Consumption{}
	kvCalc.BeforeKVRequest(prechargeNone, reqNone)
	re.InDelta(baseCost, prechargeNone.RRU, 1e-6,
		"Without hint or paging, BeforeKVRequest should only charge baseCost")
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

	predictedReadBytes := uint64(4 * 1024 * 1024) // 4 MB pre-charge
	actualReadBytes := uint64(1 * 1024 * 1024)    // 1 MB actual

	req := &TestRequestInfo{
		isWrite:            false,
		predictedReadBytes: predictedReadBytes,
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
	preChargeCost := float64(cfg.ReadBytesCost) * float64(predictedReadBytes)
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

	predictedReadBytes := uint64(1 * 1024 * 1024) // 1 MB pre-charge
	actualReadBytes := uint64(4 * 1024 * 1024)    // 4 MB actual (exceeds estimate)

	req := &TestRequestInfo{
		isWrite:            false,
		predictedReadBytes: predictedReadBytes,
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

	predictedReadBytes := uint64(4 * 1024 * 1024) // 4 MB pre-charge
	actualReadBytes := uint64(512 * 1024)         // 512 KB actual

	req := &TestRequestInfo{
		isWrite:            false,
		predictedReadBytes: predictedReadBytes,
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

func TestNonprechargeMetricsRecordedWhenHintZero(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	nonprechargeCounter := metrics.PagingNonprechargeCounter.WithLabelValues(gc.name)
	nonprechargeBytes := metrics.PagingNonprechargeActualBytes.WithLabelValues(gc.name)
	prechargeCounter := metrics.PagingPrechargeCounter.WithLabelValues(gc.name)
	actualBytes := metrics.PagingActualBytesCounter.WithLabelValues(gc.name)

	countBefore := testutil.ToFloat64(nonprechargeCounter)
	bytesBefore := testutil.ToFloat64(nonprechargeBytes)
	prechargeBefore := testutil.ToFloat64(prechargeCounter)
	actualBytesBefore := testutil.ToFloat64(actualBytes)

	// Hint=0 read: implements predictedReadBytesProvider but returns 0 ->
	// BeforeKVRequest skips pre-charge, AfterKVRequest must record
	// nonprecharge bytes.
	const readBytesAmount = uint64(256 * 1024)
	coldReq := &TestRequestInfo{isWrite: false}
	coldResp := &TestResponseInfo{readBytes: readBytesAmount, succeed: true}

	_, err := gc.onResponseImpl(coldReq, coldResp)
	re.NoError(err)
	re.InDelta(countBefore+1, testutil.ToFloat64(nonprechargeCounter), 1e-6,
		"hint=0 read should increment nonprecharge counter")
	re.InDelta(bytesBefore+float64(readBytesAmount),
		testutil.ToFloat64(nonprechargeBytes), 1e-6,
		"nonprecharge bytes must equal AfterKVRequest actual read bytes")
	re.InDelta(prechargeBefore, testutil.ToFloat64(prechargeCounter), 1e-6,
		"precharge counter must stay unchanged when hint=0")
	re.InDelta(actualBytesBefore, testutil.ToFloat64(actualBytes), 1e-6,
		"warm-path actual-bytes counter must stay unchanged when hint=0")

	// Sanity: a hint>0 read should hit the warm path, not the cold path.
	warmReq := &TestRequestInfo{isWrite: false, predictedReadBytes: 1 * 1024 * 1024}
	warmResp := &TestResponseInfo{readBytes: readBytesAmount, succeed: true}
	_, _, _, _, err = gc.onRequestWaitImpl(context.TODO(), warmReq)
	re.NoError(err)
	_, err = gc.onResponseImpl(warmReq, warmResp)
	re.NoError(err)
	re.InDelta(countBefore+1, testutil.ToFloat64(nonprechargeCounter), 1e-6,
		"warm request must not touch nonprecharge counter")
	re.InDelta(prechargeBefore+1, testutil.ToFloat64(prechargeCounter), 1e-6,
		"warm request should increment precharge counter")

	// Writes skip both buckets entirely.
	writeReq := &TestRequestInfo{isWrite: true, writeBytes: 1024}
	writeResp := &TestResponseInfo{readBytes: 0, succeed: true}
	_, err = gc.onResponseImpl(writeReq, writeResp)
	re.NoError(err)
	re.InDelta(countBefore+1, testutil.ToFloat64(nonprechargeCounter), 1e-6,
		"write must not touch nonprecharge counter")
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

	// Set actual read bytes equal to the predicted bytes so the read-bytes
	// delta is zero. The only settlement cost should be CPU time (which
	// we set to zero here).
	predictedReadBytes := uint64(2 * 1024 * 1024)
	req := &TestRequestInfo{
		isWrite:            false,
		predictedReadBytes: predictedReadBytes,
	}
	resp := &TestResponseInfo{
		readBytes: predictedReadBytes, // exact match
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
