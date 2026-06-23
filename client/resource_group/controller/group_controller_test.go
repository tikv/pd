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

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/resource_group/controller/metrics"
)

func counterValue(re *require.Assertions, c interface{ Write(*dto.Metric) error }) float64 {
	var m dto.Metric
	re.NoError(c.Write(&m))
	return m.GetCounter().GetValue()
}

func histogramSampleCount(re *require.Assertions, h prometheus.Observer) uint64 {
	metric, ok := h.(prometheus.Metric)
	re.True(ok)
	if !ok {
		return 0
	}
	var m dto.Metric
	re.NoError(metric.Write(&m))
	histogram := m.GetHistogram()
	re.NotNil(histogram)
	if histogram == nil {
		return 0
	}
	return histogram.GetSampleCount()
}

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
	// baseCost + predictedReadBytes * ReadBytesCost.
	predictedReadBytes := uint64(256 * 1024) // learned EMA estimate
	req := &TestRequestInfo{
		isWrite:            false,
		isCop:              true,
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

func TestPredictedReadBytesRequiresCopRead(t *testing.T) {
	re := require.New(t)
	cfg := DefaultRUConfig()
	kvCalc := newKVCalculator(cfg)

	predictedReadBytes := uint64(256 * 1024)
	req := &TestRequestInfo{
		isWrite:            false,
		isCop:              false,
		predictedReadBytes: predictedReadBytes,
	}
	precharge := &rmpb.Consumption{}
	kvCalc.BeforeKVRequest(precharge, req)

	baseCost := float64(cfg.ReadBaseCost) + float64(cfg.ReadPerBatchBaseCost)*defaultAvgBatchProportion
	re.InDelta(baseCost, precharge.RRU, 1e-6,
		"non-cop reads must not consume the paging predicted-read hint")

	actualReadBytes := uint64(300 * 1024)
	resp := &TestResponseInfo{
		readBytes: actualReadBytes,
		kvCPU:     10 * time.Millisecond,
		succeed:   true,
	}
	settle := &rmpb.Consumption{}
	kvCalc.AfterKVRequest(settle, req, resp)

	actualReadCost := float64(cfg.ReadBytesCost) * float64(actualReadBytes)
	cpuCost := float64(cfg.CPUMsCost) * 10.0
	re.InDelta(actualReadCost+cpuCost, settle.RRU, 1e-6,
		"non-cop reads must not settle against the paging predicted-read hint")
}

func TestPagingPreChargeTokenRefund(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

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
		isCop:              true,
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

	// Refund (pre-charge - actual) should exceed the actual read cost,
	// so the limiter ends settlement with more tokens than after pre-charge.
	cfg := DefaultRUConfig()
	preChargeCost := float64(cfg.ReadBytesCost) * float64(predictedReadBytes)
	actualCost := float64(cfg.ReadBytesCost) * float64(actualReadBytes)
	expectedRefund := preChargeCost - actualCost
	re.Positive(expectedRefund, "sanity: pre-charge should exceed actual cost")
	re.InDelta(tokensAfterPreCharge+expectedRefund, tokensAfterSettlement, 1.0,
		"limiter should be refunded the excess pre-charged tokens")

	gc.mu.Lock()
	netRRU := gc.mu.consumption.RRU
	gc.mu.Unlock()
	baseCost := float64(cfg.ReadBaseCost) + float64(cfg.ReadPerBatchBaseCost)*defaultAvgBatchProportion
	re.InDelta(baseCost+actualCost, netRRU, 1e-6,
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
		isCop:              true,
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

	re.Less(tokensAfterSettlement, tokensAfterPreCharge,
		"when actual exceeds pre-charge, settlement should consume tokens")
}

func TestOnResponseWaitPrechargedPositiveSettlementDebitsImmediately(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	gc.mainCfg.WaitRetryTimes = 1
	gc.mainCfg.WaitRetryInterval = time.Millisecond
	gc.mainCfg.LTBMaxWaitDuration = time.Millisecond

	limiter := gc.run.requestUnitTokens.limiter
	limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   0,
		newFillRate: 0,
		newBurst:    0,
	})
	limiter.RemoveTokens(time.Now(), limiter.AvailableTokens(time.Now()))
	gc.isThrottled.Store(true)

	req := &TestRequestInfo{
		isWrite:            false,
		predictedReadBytes: 16 * 1024 * 1024,
		isCop:              true,
	}
	resp := &TestResponseInfo{
		readBytes: 20 * 1024 * 1024,
		succeed:   true,
	}
	settlementDelta := &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.AfterKVRequest(settlementDelta, req, resp)
	}
	re.Greater(getRUValueFromConsumption(settlementDelta), float64(0))
	re.GreaterOrEqual(settlementDelta.ReadBytes+settlementDelta.WriteBytes, float64(bigRequestThreshold))
	re.True(gc.isThrottled.Load())

	tokensBefore := limiter.AvailableTokens(time.Now())
	_, waitDuration, err := gc.onResponseWaitImpl(context.Background(), req, resp)
	re.NoError(err)
	re.Zero(waitDuration)

	tokensAfter := limiter.AvailableTokens(time.Now())
	re.Less(tokensAfter, tokensBefore,
		"precharged positive settlement should be debited immediately")
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
		isCop:              true,
		predictedReadBytes: predictedReadBytes,
	}
	resp := &TestResponseInfo{
		readBytes: actualReadBytes,
		succeed:   true,
	}

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	tokensAfterPreCharge := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	_, err = gc.onResponseImpl(req, resp)
	re.NoError(err)
	tokensAfterSettlement := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	re.Greater(tokensAfterSettlement, tokensAfterPreCharge,
		"onResponseImpl should refund excess pre-charged tokens")
}

func TestOnResponseNegativePagingSettlementDoesNotTriggerReport(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   100000,
		newFillRate: 0,
		newBurst:    0,
	})

	predictedReadBytes := uint64(4 * 1024 * 1024)
	actualReadBytes := uint64(512 * 1024)
	req := &TestRequestInfo{
		isWrite:            false,
		predictedReadBytes: predictedReadBytes,
		isCop:              true,
	}
	resp := &TestResponseInfo{
		readBytes: actualReadBytes,
		succeed:   true,
	}

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	gc.updateRunState()
	prechargeReport := gc.collectRequestAndConsumption(periodicReport)
	re.NotNil(prechargeReport)
	gc.handleTokenBucketResponse(&rmpb.TokenBucketResponse{})

	settlementDelta, _, err := gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	gc.run.lastRequestTime = time.Now().Add(-defaultTargetPeriod)
	gc.updateRunState()
	settlementReport := gc.collectRequestAndConsumption(periodicReport)

	cfg := DefaultRUConfig()
	expectedRRU := float64(cfg.ReadBytesCost) * (float64(actualReadBytes) - float64(predictedReadBytes))
	re.InDelta(expectedRRU, settlementDelta.RRU, 1e-6)
	re.Nil(settlementReport, "negative paging settlement alone should not trigger an immediate report")
}

func TestPagingPreChargeRefundOnFailedRead(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	initialTokens := float64(100000)
	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   initialTokens,
		newFillRate: 0,
		newBurst:    0,
	})

	predictedReadBytes := uint64(4 * 1024 * 1024) // 4 MB pre-charge

	req := &TestRequestInfo{
		isWrite:            false,
		isCop:              true,
		predictedReadBytes: predictedReadBytes,
	}
	// Failed response: no bytes read, no CPU consumed, succeed=false.
	resp := &TestResponseInfo{
		readBytes: 0,
		kvCPU:     0,
		succeed:   false,
	}

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	tokensAfterPreCharge := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	_, _, err = gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	tokensAfterSettlement := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	// On a failed read, AfterKVRequest still runs: paging settlement subtracts
	// ReadBytesCost*predicted and calculateReadCost adds 0, yielding a negative
	// delta that flows through RefundTokens. ReadBaseCost is not refunded,
	// matching existing behavior for non-paging read failures.
	cfg := DefaultRUConfig()
	expectedRefund := float64(cfg.ReadBytesCost) * float64(predictedReadBytes)
	re.InDelta(tokensAfterPreCharge+expectedRefund, tokensAfterSettlement, 1.0,
		"failed read with paging hint should refund ReadBytesCost*predicted")
}

func TestNonCopPredictedReadBytesResponseIgnoresPagingAccounting(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	predictedReadBytes := uint64(4 * 1024 * 1024)
	actualReadBytes := uint64(512 * 1024)
	req := &TestRequestInfo{
		isWrite:            false,
		predictedReadBytes: predictedReadBytes,
		isCop:              false,
	}
	resp := &TestResponseInfo{
		readBytes: actualReadBytes,
		kvCPU:     10 * time.Millisecond,
		succeed:   true,
	}

	prechargeBefore := counterValue(re, gc.metrics.prechargeCounter)
	actualBefore := counterValue(re, gc.metrics.actualBytesCounter)
	nonprechargeBefore := counterValue(re, gc.metrics.nonprechargeCounter)
	delta, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	cfg := DefaultRUConfig()
	baseCost := float64(cfg.ReadBaseCost) + float64(cfg.ReadPerBatchBaseCost)*defaultAvgBatchProportion
	re.InDelta(baseCost, delta.RRU, 1e-6,
		"non-cop read hints must not add predicted read bytes to pre-charge")

	settlement, err := gc.onResponseImpl(req, resp)
	re.NoError(err)
	actualReadCost := float64(cfg.ReadBytesCost) * float64(actualReadBytes)
	cpuCost := float64(cfg.CPUMsCost) * 10.0
	re.InDelta(actualReadCost+cpuCost, settlement.RRU, 1e-6,
		"non-cop response settlement must bill actual read bytes and CPU without subtracting the ignored hint")
	re.InDelta(prechargeBefore, counterValue(re, gc.metrics.prechargeCounter), 1e-9)
	re.InDelta(actualBefore, counterValue(re, gc.metrics.actualBytesCounter), 1e-9)
	re.InDelta(nonprechargeBefore, counterValue(re, gc.metrics.nonprechargeCounter), 1e-9)
}

func TestDeletePagingLabelsResetsSeries(t *testing.T) {
	re := require.New(t)
	// Use a name no other test reuses so we measure only our own series.
	name := "test-paging-cleanup-rg"

	gmc := initMetrics(name, name)
	// Push a sample through every paging counter / histogram so each label
	// series actually exists in the underlying Vec.
	gmc.observePagingRequest(100, 1.0)
	gmc.observePagingResponse(100, 80, 2.0, 0.5)
	gmc.observePagingRequest(0, 0)
	gmc.observePagingResponse(0, 200, 0, 0)

	// Sanity: cached counters are non-zero before cleanup.
	re.Positive(counterValue(re, gmc.prechargeCounter))
	re.Positive(counterValue(re, gmc.actualBytesCounter))
	re.Positive(counterValue(re, gmc.nonprechargeCounter))
	re.Positive(histogramSampleCount(re, gmc.predictionResidualBytes))
	re.Positive(histogramSampleCount(re, gmc.settlementRUDelta))

	gmc.deletePagingLabels(name)

	// After cleanup, refetching each Vec with the same label must yield a
	// fresh zero-valued series. Covers every counter declared in
	// initMetrics so a forgotten DeleteLabelValues in deletePagingLabels
	// surfaces here.
	for _, vec := range []*prometheus.CounterVec{
		metrics.PagingPrechargeCounter,
		metrics.PagingNonprechargeCounter,
		metrics.PagingPrechargeBytesCounter,
		metrics.PagingActualBytesCounter,
		metrics.PagingPrechargeRU,
		metrics.PagingSettlementRU,
	} {
		re.Zero(counterValue(re, vec.WithLabelValues(name)),
			"paging counter series for %q should be cleared by deletePagingLabels", name)
	}
	for _, vec := range []*prometheus.HistogramVec{
		metrics.PagingPredictionResidualBytes,
		metrics.PagingSettlementRUDelta,
	} {
		re.Zero(histogramSampleCount(re, vec.WithLabelValues(name)),
			"paging histogram series for %q should be cleared by deletePagingLabels", name)
	}
}

func TestPagingNonprechargeGatedByIsCop(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	// Reads without a paging hint reach onResponseImpl through the same RC
	// interceptor regardless of cmd type (CmdGet, CmdBatchGet, CmdCop, ...).
	// Only IsCop()==true requests should land in paging_nonprecharge_*; the
	// rest must be ignored so the metric keeps its "paging cold-start"
	// semantics.
	resp := &TestResponseInfo{readBytes: 1024, succeed: true}
	nonCop := &TestRequestInfo{isWrite: false, isCop: false}
	cop := &TestRequestInfo{isWrite: false, isCop: true}

	counterBefore := counterValue(re, gc.metrics.nonprechargeCounter)
	residualSamplesBefore := histogramSampleCount(re, gc.metrics.predictionResidualBytes)

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), nonCop)
	re.NoError(err)

	_, err = gc.onResponseImpl(nonCop, resp)
	re.NoError(err)
	re.InDelta(counterBefore, counterValue(re, gc.metrics.nonprechargeCounter), 1e-9,
		"non-cop reads must not increment paging_nonprecharge_*")
	re.Equal(residualSamplesBefore, histogramSampleCount(re, gc.metrics.predictionResidualBytes),
		"non-cop reads must not be included in paging prediction residuals")

	_, _, _, _, err = gc.onRequestWaitImpl(context.TODO(), cop)
	re.NoError(err)
	re.InDelta(counterBefore+1, counterValue(re, gc.metrics.nonprechargeCounter), 1e-9,
		"cop reads without a hint must increment paging_nonprecharge_total on the request path")
	re.Equal(residualSamplesBefore, histogramSampleCount(re, gc.metrics.predictionResidualBytes),
		"cop reads without a hint must not record prediction residuals before response")

	_, err = gc.onResponseImpl(cop, resp)
	re.NoError(err)
	re.InDelta(counterBefore+1, counterValue(re, gc.metrics.nonprechargeCounter), 1e-9,
		"cop read responses must not increment paging_nonprecharge_total again")
	re.Equal(residualSamplesBefore, histogramSampleCount(re, gc.metrics.predictionResidualBytes),
		"cop read responses without a hint must not be included in precharge prediction residuals")
}

func TestPagingNonprechargeCounterObservedOnRequest(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	req := &TestRequestInfo{isWrite: false, isCop: true}

	counterBefore := counterValue(re, gc.metrics.nonprechargeCounter)
	residualSamplesBefore := histogramSampleCount(re, gc.metrics.predictionResidualBytes)

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)

	re.InDelta(counterBefore+1, counterValue(re, gc.metrics.nonprechargeCounter), 1e-9,
		"cop reads without a prediction should be counted at the same request boundary as precharged reads")
	re.Equal(residualSamplesBefore, histogramSampleCount(re, gc.metrics.predictionResidualBytes),
		"prediction residuals are response-only and must not be recorded on the request path")
}

func TestNoPreChargeWithoutPredictedReadBytes(t *testing.T) {
	re := require.New(t)
	cfg := DefaultRUConfig()
	kvCalc := newKVCalculator(cfg)
	baseCost := float64(cfg.ReadBaseCost) + float64(cfg.ReadPerBatchBaseCost)*defaultAvgBatchProportion

	// Without a PredictedReadBytes hint, BeforeKVRequest must not
	// pre-charge; AfterKVRequest bills actual read bytes only.
	req := &TestRequestInfo{isWrite: false}
	precharge := &rmpb.Consumption{}
	kvCalc.BeforeKVRequest(precharge, req)
	re.InDelta(baseCost, precharge.RRU, 1e-6,
		"Without a hint, BeforeKVRequest should only charge baseCost")

	actualReadBytes := uint64(2 * 1024 * 1024)
	resp := &TestResponseInfo{
		readBytes: actualReadBytes,
		kvCPU:     10 * time.Millisecond,
		succeed:   true,
	}
	settle := &rmpb.Consumption{}
	kvCalc.AfterKVRequest(settle, req, resp)

	actualReadCost := float64(cfg.ReadBytesCost) * float64(actualReadBytes)
	cpuCost := float64(cfg.CPUMsCost) * 10.0
	re.InDelta(actualReadCost+cpuCost, settle.RRU, 1e-6,
		"AfterKVRequest should bill actual read cost only when nothing was pre-charged")
}

func TestHandleTokenBucketUpdateEventCanceledByInitCounterNotify(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	counter := gc.run.requestUnitTokens

	counter.notify.mu.Lock()
	counter.notify.setupNotificationTimer = time.NewTimer(time.Hour)
	counter.notify.setupNotificationCh = counter.notify.setupNotificationTimer.C
	counter.notify.setupNotificationThreshold = 1
	counter.notify.cancelCh = make(chan struct{})
	counter.notify.mu.Unlock()
	defer initCounterNotify(counter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		gc.handleTokenBucketUpdateEvent(ctx)
		close(done)
	}()

	require.Never(t, func() bool {
		return isClosed(done)
	}, 50*time.Millisecond, 5*time.Millisecond)

	initCounterNotify(counter)
	require.Eventually(t, func() bool {
		return isClosed(done)
	}, time.Second, 10*time.Millisecond)
}

func TestHandleTokenBucketUpdateEventCleansNotifyOnTimer(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	counter := gc.run.requestUnitTokens
	threshold := 42.0

	counter.notify.mu.Lock()
	counter.notify.setupNotificationTimer = time.NewTimer(10 * time.Millisecond)
	counter.notify.setupNotificationCh = counter.notify.setupNotificationTimer.C
	counter.notify.setupNotificationThreshold = threshold
	counter.notify.cancelCh = make(chan struct{})
	counter.notify.mu.Unlock()
	defer initCounterNotify(counter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		gc.handleTokenBucketUpdateEvent(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		return isClosed(done)
	}, time.Second, 10*time.Millisecond)

	counter.notify.mu.Lock()
	re.Nil(counter.notify.setupNotificationTimer)
	re.Nil(counter.notify.setupNotificationCh)
	re.Nil(counter.notify.cancelCh)
	counter.notify.mu.Unlock()

	counter.limiter.mu.Lock()
	re.Equal(threshold, counter.limiter.notifyThreshold)
	counter.limiter.mu.Unlock()
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
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

func TestPagingPrechargeNotObservedOnThrottle(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	// 4 GiB predicted ~= 65536 RU at default ReadCostPerByte (1/64KiB),
	// exceeding the LTBMaxWaitDuration budget at the default FillRate so
	// acquireTokens returns ErrClientResourceGroupThrottled.
	req := &TestRequestInfo{
		isWrite:            false,
		isCop:              true,
		predictedReadBytes: 4 * 1024 * 1024 * 1024,
	}

	before := counterValue(re, gc.metrics.prechargeCounter)
	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.Error(err)
	re.True(errs.ErrClientResourceGroupThrottled.Equal(err))
	after := counterValue(re, gc.metrics.prechargeCounter)

	// Throttled requests never reach OnResponse for settlement, so the
	// precharge counter must not be incremented either.
	re.Equal(before, after,
		"throttled paging request should not inflate PagingPrechargeCounter")
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

// TestAcquireTokensCancelKeepsLastMonotonic checks that a stale CancelAt does
// not rewind lim.last, exercised through two concurrent acquireTokens calls
// pinned by the waitReservationsBeforeSelect failpoint: A reserves
// (lim.last = now_A) and parks; B advances lim.last to now_B (> now_A); A is
// then cancelled so CancelAt runs with the stale now_A. lim.last must stay at
// now_B, not rewind to now_A.
func TestAcquireTokensCancelKeepsLastMonotonic(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	gc.mainCfg.LTBMaxWaitDuration = 30 * time.Second
	gc.mainCfg.WaitRetryTimes = 1
	counter := gc.run.requestUnitTokens

	// Throttled bucket: a 10000-RU request reserves with a multi-second delay and
	// parks in WaitReservations rather than being granted immediately.
	counter.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{newTokens: 0, newFillRate: 1000, newBurst: 0})

	const fp = "github.com/tikv/pd/client/resource_group/controller/waitReservationsBeforeSelect"
	reached := make(chan struct{})
	release := make(chan struct{})
	re.NoError(failpoint.EnableCall(fp, func() {
		reached <- struct{}{}
		<-release
	}))
	defer func() { re.NoError(failpoint.Disable(fp)) }()

	// A reserves with a delay and parks at the hook. Only A reaches the hook;
	// the allowDebt sibling B never enters WaitReservations.
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	resultCh := make(chan error, 1)
	go func() {
		var wd time.Duration
		_, err := gc.acquireTokens(ctxA, &rmpb.Consumption{RRU: 10000}, &wd, false)
		resultCh <- err
	}()

	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the request to reserve and park")
	}
	tMid := time.Now() // now_A < tMid <= now_B

	// B advances lim.last to now_B via the allowDebt RemoveTokens path.
	var wd time.Duration
	_, err := gc.acquireTokens(context.Background(), &rmpb.Consumption{RRU: 1}, &wd, true)
	re.NoError(err)

	cancelA()
	close(release)
	select {
	case err := <-resultCh:
		re.Error(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for acquireTokens to return")
	}

	re.False(counter.limiter.last.Before(tMid), "stale CancelAt rewound lim.last")
}
