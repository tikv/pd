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
	"math"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
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

func createTestGroupCostController(re *require.Assertions, names ...string) *groupCostController {
	name := "test"
	if len(names) > 0 {
		name = names[0]
	}
	return createTestGroupCostControllerWithConfig(re, name, DefaultRUConfig())
}

func createTestGroupCostControllerWithConfig(re *require.Assertions, name string, cfg *RUConfig) *groupCostController {
	group := &rmpb.ResourceGroup{
		Name:     name,
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
	gc, err := newGroupCostController(group, 1001, cfg, ch1, ch2)
	re.NoError(err)
	return gc
}

func waitForNonZeroProcessCPUTime(re *require.Assertions) {
	deadline := time.Now().Add(2 * time.Second)
	var sink uint64
	for getSQLProcessCPUTime(true) <= 0 && time.Now().Before(deadline) {
		for i := range 100000 {
			sink += uint64(i)
		}
	}
	runtime.KeepAlive(sink)
	re.Greater(getSQLProcessCPUTime(true), 0.0)
}

func TestGroupControlBurstable(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	args := tokenBucketReconfigureArgs{
		newFillRate: 1000,
		newBurst:    -1,
	}
	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), args)
	gc.run.now = time.Now()
	gc.run.requestUnitTokens.avgLastTime = gc.run.now.Add(-time.Second)
	gc.updateAvgRequestResourcePerSec()
	re.True(gc.burstable.Load())
}

func TestCalcAvgSkipsNonPositiveDeltaDuration(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	counter := gc.run.requestUnitTokens
	counter.limiter.Reconfigure(gc.run.now, tokenBucketReconfigureArgs{
		newFillRate: 1000,
		newBurst:    -1,
	})
	re.False(gc.burstable.Load())
	// Simulate a controller that is published between the `updateRunState` and
	// `updateAvgRequestResourcePerSec` passes of the same tick: `gc.run.now`
	// still equals the `avgLastTime` set by `initRunState`, so the elapsed
	// duration is exactly zero and the sample must be skipped instead of
	// computing 0/0 = NaN.
	re.Equal(gc.run.now, counter.avgLastTime)
	gc.updateAvgRequestResourcePerSec()
	re.False(math.IsNaN(counter.avgRUPerSec))
	re.Zero(counter.avgRUPerSec)
	re.True(gc.burstable.Load())
	// The skipped sample must not advance the accounting state.
	re.Equal(gc.run.now, counter.avgLastTime)
	re.Zero(counter.avgRUPerSecLastRU)

	// Once the run state advances, sampling resumes as usual.
	gc.run.consumption.RRU = 100
	gc.run.now = gc.run.now.Add(time.Second)
	gc.updateAvgRequestResourcePerSec()
	re.False(math.IsNaN(counter.avgRUPerSec))
	re.Positive(counter.avgRUPerSec)
}

func TestSmallPositiveConsumptionDoesNotBypassThreshold(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	now := time.Now()

	gc.run.consumption.RRU = 1
	gc.initialRequestCompleted.Store(true)
	gc.run.lastRequestTime = now.Add(-defaultTargetPeriod)
	gc.run.now = now

	report := gc.collectRequestAndConsumption(periodicReport)
	re.Nil(report)
}

func TestDirectReportConsumptionReportsOnlyConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	gc.addRUConsumption(&rmpb.Consumption{
		RRU:        7,
		WRU:        3,
		WriteBytes: 1024,
	})
	gc.updateRunState()

	report := gc.collectRequestAndConsumption(periodicReport)
	re.NotNil(report)
	re.Equal(float64(7), report.GetConsumptionSinceLastRequest().GetRRU())
	re.Equal(float64(3), report.GetConsumptionSinceLastRequest().GetWRU())
	re.Equal(float64(1024), report.GetConsumptionSinceLastRequest().GetWriteBytes())
}

func TestRequestAndResponseConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
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
	gc := createTestGroupCostController(re, t.Name())

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

func TestOnResponseWaitZeroSettlementObservesSuccessfulDuration(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	re.False(gc.burstable.Load())

	samplesBefore := histogramSampleCount(re, gc.metrics.successfulRequestDuration)
	_, waitTime, err := gc.onResponseWaitImpl(context.TODO(), &TestRequestInfo{isWrite: false}, &TestResponseInfo{succeed: true})
	re.NoError(err)
	re.Zero(waitTime)
	re.Equal(samplesBefore+1, histogramSampleCount(re, gc.metrics.successfulRequestDuration))
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

func TestPagingPrechargeDoesNotEnterReportedRequestConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	cfg := DefaultRUConfig()
	kvCalc := gc.getKVCalculator()

	predictedReadBytes := uint64(4 * 1024 * 1024)
	req := &TestRequestInfo{
		isWrite:            false,
		isCop:              true,
		predictedReadBytes: predictedReadBytes,
	}

	tokenDelta, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)

	baseCost := float64(kvCalc.ReadBaseCost) + float64(kvCalc.ReadPerBatchBaseCost)*defaultAvgBatchProportion
	prechargeCost := float64(cfg.ReadBytesCost) * float64(predictedReadBytes)
	re.InDelta(baseCost+prechargeCost, tokenDelta.RRU, 1e-6,
		"request token ledger should still include paging pre-charge")

	gc.updateRunState()
	report := gc.collectRequestAndConsumption(periodicReport)
	re.NotNil(report)
	re.InDelta(baseCost, report.GetConsumptionSinceLastRequest().GetRRU(), 1e-6,
		"reported request consumption must keep metering aligned with master and exclude predicted-read reservation")
}

func TestPagingPrechargeRefundDoesNotEnterReportedResponseConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	cfg := DefaultRUConfig()

	predictedReadBytes := uint64(4 * 1024 * 1024)
	actualReadBytes := uint64(512 * 1024)
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
	gc.updateRunState()
	requestReport := gc.collectRequestAndConsumption(periodicReport)
	re.NotNil(requestReport)
	gc.handleTokenBucketResponse(&rmpb.TokenBucketResponse{})

	tokenSettlement, _, err := gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	expectedTokenSettlement := float64(cfg.ReadBytesCost) * (float64(actualReadBytes) - float64(predictedReadBytes))
	re.InDelta(expectedTokenSettlement, tokenSettlement.RRU, 1e-6,
		"response token ledger should still settle against the request pre-charge")
	re.Negative(tokenSettlement.RRU)

	gc.run.lastRequestTime = time.Now().Add(-extendedReportingPeriodFactor * defaultTargetPeriod)
	gc.updateRunState()
	responseReport := gc.collectRequestAndConsumption(periodicReport)
	re.NotNil(responseReport)
	expectedReportedResponseRU := float64(cfg.ReadBytesCost) * float64(actualReadBytes)
	re.InDelta(expectedReportedResponseRU, responseReport.GetConsumptionSinceLastRequest().GetRRU(), 1e-6,
		"reported response consumption must expose actual read usage, not the paging refund")
	re.GreaterOrEqual(responseReport.GetConsumptionSinceLastRequest().GetRRU(), 0.0)
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

	samplesBefore := histogramSampleCount(re, gc.metrics.successfulRequestDuration)
	_, _, err = gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	tokensAfterSettlement := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())
	re.Equal(samplesBefore+1, histogramSampleCount(re, gc.metrics.successfulRequestDuration))

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
	samplesBefore := histogramSampleCount(re, gc.metrics.successfulRequestDuration)
	_, waitDuration, err := gc.onResponseWaitImpl(context.Background(), req, resp)
	re.NoError(err)
	re.Zero(waitDuration)

	tokensAfter := limiter.AvailableTokens(time.Now())
	re.Less(tokensAfter, tokensBefore,
		"precharged positive settlement should be debited immediately")
	re.Equal(samplesBefore+1, histogramSampleCount(re, gc.metrics.successfulRequestDuration))
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

func TestNegativePagingSettlementDoesNotForceConsumptionReport(t *testing.T) {
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
	report := gc.collectRequestAndConsumption(periodicReport)

	cfg := DefaultRUConfig()
	expectedRRU := float64(cfg.ReadBytesCost) * (float64(actualReadBytes) - float64(predictedReadBytes))
	re.InDelta(expectedRRU, settlementDelta.RRU, 1e-6)
	re.Nil(report, "negative token settlement alone should not force a consumption report")
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

func TestFailedWriteDoesNotUsePagingRefundPath(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   100000,
		newFillRate: 0,
		newBurst:    0,
	})

	req := &TestRequestInfo{
		isWrite:     true,
		writeBytes:  4 * 1024,
		numReplicas: 3,
	}
	resp := &TestResponseInfo{
		readBytes: 0,
		succeed:   false,
	}

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	tokensAfterReservation := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	refundDelta, _, err := gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	re.Negative(refundDelta.WRU)
	tokensAfterResponse := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	re.InDelta(tokensAfterReservation, tokensAfterResponse, 1.0,
		"feature PR should only refund paging over-estimates, not failed-write reservations")
}

func TestFailedWriteOnResponseDoesNotUsePagingRefundPath(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	gc.run.requestUnitTokens.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   100000,
		newFillRate: 0,
		newBurst:    0,
	})

	req := &TestRequestInfo{
		isWrite:     true,
		writeBytes:  4 * 1024,
		numReplicas: 3,
	}
	resp := &TestResponseInfo{
		readBytes: 0,
		succeed:   false,
	}

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	tokensAfterReservation := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	refundDelta, err := gc.onResponseImpl(req, resp)
	re.NoError(err)
	re.Negative(refundDelta.WRU)
	tokensAfterResponse := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	re.InDelta(tokensAfterReservation, tokensAfterResponse, 1.0,
		"feature PR should only refund paging over-estimates, not failed-write reservations")
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
	noPrechargeBefore := counterValue(re, gc.metrics.noPrechargeCounter)
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
	re.InDelta(noPrechargeBefore, counterValue(re, gc.metrics.noPrechargeCounter), 1e-9)
}

func TestDeletePagingLabelsResetsSeries(t *testing.T) {
	re := require.New(t)
	// Use a name no other test reuses so we measure only our own series.
	name := "test-paging-cleanup-rg"

	gmc := initMetrics(name, name)
	// Push a sample through every paging counter / histogram so each label
	// series actually exists in the underlying Vec.
	gmc.observePagingRequest(100)
	gmc.observePagingResponse(100, 80)
	gmc.observePagingRequest(0)
	gmc.observePagingResponse(0, 200)

	// Sanity: cached counters are non-zero before cleanup.
	re.Positive(counterValue(re, gmc.prechargeCounter))
	re.Positive(counterValue(re, gmc.actualBytesCounter))
	re.Positive(counterValue(re, gmc.noPrechargeCounter))
	re.Positive(histogramSampleCount(re, gmc.predictionResidualBytes))

	gmc.deletePagingLabels(name)

	// After cleanup, refetching each Vec with the same label must yield a
	// fresh zero-valued series. Covers every counter declared in
	// initMetrics so a forgotten DeleteLabelValues in deletePagingLabels
	// surfaces here.
	for _, vec := range []*prometheus.CounterVec{
		metrics.CopReadPrechargeCounter,
		metrics.CopReadNoPrechargeCounter,
		metrics.PagingPrechargeBytesCounter,
		metrics.PagingActualBytesCounter,
	} {
		re.Zero(counterValue(re, vec.WithLabelValues(name)),
			"paging counter series for %q should be cleared by deletePagingLabels", name)
	}
	for _, vec := range []*prometheus.HistogramVec{
		metrics.PagingPredictionResidualBytes,
	} {
		re.Zero(histogramSampleCount(re, vec.WithLabelValues(name)),
			"paging histogram series for %q should be cleared by deletePagingLabels", name)
	}
}

func TestCopReadNoPrechargeGatedByIsCop(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	// Reads without a pre-charge hint reach onResponseImpl through the same RC
	// interceptor regardless of cmd type (CmdGet, CmdBatchGet, CmdCop, ...).
	// Only IsCop()==true requests should land in cop_read_no_precharge_total;
	// the rest must be ignored so the metric keeps its cop-read coverage
	// semantics.
	resp := &TestResponseInfo{readBytes: 1024, succeed: true}
	nonCop := &TestRequestInfo{isWrite: false, isCop: false}
	cop := &TestRequestInfo{isWrite: false, isCop: true}

	counterBefore := counterValue(re, gc.metrics.noPrechargeCounter)
	residualSamplesBefore := histogramSampleCount(re, gc.metrics.predictionResidualBytes)

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), nonCop)
	re.NoError(err)

	_, err = gc.onResponseImpl(nonCop, resp)
	re.NoError(err)
	re.InDelta(counterBefore, counterValue(re, gc.metrics.noPrechargeCounter), 1e-9,
		"non-cop reads must not increment cop_read_no_precharge_total")
	re.Equal(residualSamplesBefore, histogramSampleCount(re, gc.metrics.predictionResidualBytes),
		"non-cop reads must not be included in paging prediction residuals")

	_, _, _, _, err = gc.onRequestWaitImpl(context.TODO(), cop)
	re.NoError(err)
	re.InDelta(counterBefore+1, counterValue(re, gc.metrics.noPrechargeCounter), 1e-9,
		"cop reads without a hint must increment cop_read_no_precharge_total on the request path")
	re.Equal(residualSamplesBefore, histogramSampleCount(re, gc.metrics.predictionResidualBytes),
		"cop reads without a hint must not record prediction residuals before response")

	_, err = gc.onResponseImpl(cop, resp)
	re.NoError(err)
	re.InDelta(counterBefore+1, counterValue(re, gc.metrics.noPrechargeCounter), 1e-9,
		"cop read responses must not increment cop_read_no_precharge_total again")
	re.Equal(residualSamplesBefore, histogramSampleCount(re, gc.metrics.predictionResidualBytes),
		"cop read responses without a hint must not be included in precharge prediction residuals")
}

func TestCopReadNoPrechargeCounterObservedOnRequest(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	req := &TestRequestInfo{isWrite: false, isCop: true}

	counterBefore := counterValue(re, gc.metrics.noPrechargeCounter)
	residualSamplesBefore := histogramSampleCount(re, gc.metrics.predictionResidualBytes)

	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)

	re.InDelta(counterBefore+1, counterValue(re, gc.metrics.noPrechargeCounter), 1e-9,
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
	gc := createTestGroupCostController(re, t.Name())
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
		"throttled paging request should not inflate CopReadPrechargeCounter")
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
	gc, err := newGroupCostController(group, 1001, cfg, notifyCh, make(chan *groupCostController, 1))
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
	gc := createTestGroupCostController(re, t.Name())
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
	gc := createTestGroupCostController(re, t.Name())
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

func TestOnRequestWaitUpdatesDemandMetricOnThrottle(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	gc.mainCfg.LTBMaxWaitDuration = 0
	gc.mainCfg.WaitRetryTimes = 1
	gc.mainCfg.WaitRetryInterval = 0

	counter := gc.run.requestUnitTokens
	counter.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   0,
		newFillRate: 1,
		newBurst:    1,
	})
	now := time.Now()
	counter.avgLastTime = now.Add(-time.Second)
	counter.demandAvgLastTime = now.Add(-time.Second)
	gc.run.now = now

	before := promtestutil.ToFloat64(metrics.DemandRUPerSecGauge.WithLabelValues(gc.name))
	_, _, _, _, err := gc.onRequestWaitImpl(context.Background(), &TestRequestInfo{
		isWrite:    true,
		writeBytes: 64 * 1024,
	})
	re.Error(err)
	re.True(errs.ErrClientResourceGroupThrottled.Equal(err))
	re.Equal(before, promtestutil.ToFloat64(metrics.DemandRUPerSecGauge.WithLabelValues(gc.name)))

	gc.updateAvgRequestResourcePerSec()
	re.Greater(promtestutil.ToFloat64(metrics.DemandRUPerSecGauge.WithLabelValues(gc.name)), before)
}

func TestDemandMetricSamplesConsumptionPaths(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	resetDemandMetric := func() {
		now := time.Now()
		counter := gc.run.requestUnitTokens
		counter.avgMu.Lock()
		counter.demandRUPerSec = 0
		counter.demandRUPerSecLastRU = 0
		counter.avgLastTime = now.Add(-time.Second)
		counter.demandAvgLastTime = now.Add(-time.Second)
		counter.avgMu.Unlock()
		counter.demandTotalRUScaled.Store(0)
		gc.run.now = now
		gc.metrics.demandRUPerSecGauge.Set(0)
	}
	requireDemandObserved := func() {
		re.Equal(0.0, promtestutil.ToFloat64(metrics.DemandRUPerSecGauge.WithLabelValues(gc.name)))
		gc.updateAvgRequestResourcePerSec()
		re.Greater(promtestutil.ToFloat64(metrics.DemandRUPerSecGauge.WithLabelValues(gc.name)), 0.0)
	}

	resetDemandMetric()
	_, err := gc.onResponseImpl(&TestRequestInfo{}, &TestResponseInfo{
		readBytes: 64 * 1024,
		succeed:   true,
	})
	re.NoError(err)
	requireDemandObserved()

	resetDemandMetric()
	gc.burstable.Store(true)
	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), &TestRequestInfo{
		isWrite:    true,
		writeBytes: 64 * 1024,
	})
	re.NoError(err)
	requireDemandObserved()

	resetDemandMetric()
	gc.burstable.Store(true)
	_, _, err = gc.onResponseWaitImpl(context.Background(), &TestRequestInfo{}, &TestResponseInfo{
		readBytes: 64 * 1024,
		succeed:   true,
	})
	re.NoError(err)
	requireDemandObserved()

	resetDemandMetric()
	gc.burstable.Store(false)
	_, _, err = gc.onResponseWaitImpl(context.Background(), &TestRequestInfo{}, &TestResponseInfo{
		readBytes: 64 * 1024,
		succeed:   true,
	})
	re.NoError(err)
	requireDemandObserved()

	resetDemandMetric()
	gc.addRUConsumption(&rmpb.Consumption{RRU: 3, WRU: 5})
	requireDemandObserved()
}

func TestObserveDemandDoesNotUpdateDemandMetricImmediately(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	counter := gc.run.requestUnitTokens
	now := time.Now()
	counter.avgLastTime = now.Add(-time.Second)
	counter.demandAvgLastTime = now.Add(-time.Second)
	gc.run.now = now

	before := promtestutil.ToFloat64(metrics.DemandRUPerSecGauge.WithLabelValues(gc.name))
	gc.observeDemand(&rmpb.Consumption{RRU: 100})
	re.Equal(before, promtestutil.ToFloat64(metrics.DemandRUPerSecGauge.WithLabelValues(gc.name)))

	gc.updateAvgRequestResourcePerSec()
	re.Greater(promtestutil.ToFloat64(metrics.DemandRUPerSecGauge.WithLabelValues(gc.name)), before)
}

func TestDemandMetricDecaysWithoutNewDemand(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	counter := gc.run.requestUnitTokens

	now := time.Now()
	counter.avgLastTime = now.Add(-time.Second)
	counter.demandAvgLastTime = now.Add(-time.Second)
	gc.run.now = now
	gc.observeDemand(&rmpb.Consumption{RRU: 100})
	gc.updateAvgRequestResourcePerSec()
	first := promtestutil.ToFloat64(metrics.DemandRUPerSecGauge.WithLabelValues(gc.name))
	re.Greater(first, 0.0)

	now = now.Add(time.Second)
	counter.avgLastTime = now.Add(-time.Second)
	counter.demandAvgLastTime = now.Add(-time.Second)
	gc.run.now = now
	gc.updateAvgRequestResourcePerSec()
	re.Less(promtestutil.ToFloat64(metrics.DemandRUPerSecGauge.WithLabelValues(gc.name)), first)
}

func TestRecordDemandAccumulatesConcurrently(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	counter := gc.run.requestUnitTokens

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				gc.observeDemand(&rmpb.Consumption{RRU: 1})
			}
		}()
	}
	wg.Wait()

	re.Equal(16000.0, counter.getDemandTotalRU())
}

func TestRecordDemandPreservesSubRUDemand(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())

	gc.observeDemand(&rmpb.Consumption{RRU: 0.25, WRU: 0.5})

	re.Equal(0.75, gc.run.requestUnitTokens.getDemandTotalRU())
}

func TestDemandStatsConcurrentLogFields(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())

	counter := gc.run.requestUnitTokens
	counter.demandAvgLastTime = time.Now().Add(-time.Second)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 100 {
				waitDuration := time.Duration(0)
				gc.observeDemand(&rmpb.Consumption{RRU: 1})
				_ = gc.logFields(waitDuration, nil)
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestObserveConsumptionAndComponentMetrics(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())

	rruBefore := promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "rru", chargeDirection))
	wruBefore := promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", chargeDirection))
	readBefore := promtestutil.ToFloat64(metrics.ReadByteCost.WithLabelValues(gc.name))
	writeBefore := promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, chargeDirection))
	kvBefore := promtestutil.ToFloat64(metrics.KVCPUCost.WithLabelValues(gc.name))
	sqlBefore := promtestutil.ToFloat64(metrics.SQLCPUCost.WithLabelValues(gc.name))

	gc.addRUConsumption(&rmpb.Consumption{
		RRU:               3,
		WRU:               5,
		ReadBytes:         13,
		WriteBytes:        17,
		TotalCpuTimeMs:    11,
		SqlLayerCpuTimeMs: 7,
	})

	re.Equal(rruBefore+3, promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "rru", chargeDirection)))
	re.Equal(wruBefore+5, promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", chargeDirection)))
	re.Equal(readBefore+13, promtestutil.ToFloat64(metrics.ReadByteCost.WithLabelValues(gc.name)))
	re.Equal(writeBefore+17, promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, chargeDirection)))
	re.Equal(kvBefore+4, promtestutil.ToFloat64(metrics.KVCPUCost.WithLabelValues(gc.name)))
	re.Equal(sqlBefore+7, promtestutil.ToFloat64(metrics.SQLCPUCost.WithLabelValues(gc.name)))
}

func TestObserveConsumptionHistogramRequiresTraceLog(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())

	const histogramName = "test_observe_consumption_histogram_requires_trace_log"
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    histogramName,
		Help:    "Test histogram.",
		Buckets: []float64{1, 3},
	})
	gc.metrics.consumeTokenHistogram = histogram

	originalTraceLog := enableControllerTraceLog.Load()
	enableControllerTraceLog.Store(false)
	t.Cleanup(func() {
		enableControllerTraceLog.Store(originalTraceLog)
	})

	rruBefore := promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "rru", chargeDirection))

	gc.observeConsumption(&rmpb.Consumption{RRU: 1})
	re.NoError(promtestutil.CollectAndCompare(histogram, strings.NewReader(`
# HELP test_observe_consumption_histogram_requires_trace_log Test histogram.
# TYPE test_observe_consumption_histogram_requires_trace_log histogram
test_observe_consumption_histogram_requires_trace_log_bucket{le="1"} 0
test_observe_consumption_histogram_requires_trace_log_bucket{le="3"} 0
test_observe_consumption_histogram_requires_trace_log_bucket{le="+Inf"} 0
test_observe_consumption_histogram_requires_trace_log_sum 0
test_observe_consumption_histogram_requires_trace_log_count 0
`), histogramName))
	re.Equal(rruBefore+1, promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "rru", chargeDirection)))

	enableControllerTraceLog.Store(true)
	gc.observeConsumption(&rmpb.Consumption{RRU: 2})
	re.NoError(promtestutil.CollectAndCompare(histogram, strings.NewReader(`
# HELP test_observe_consumption_histogram_requires_trace_log Test histogram.
# TYPE test_observe_consumption_histogram_requires_trace_log histogram
test_observe_consumption_histogram_requires_trace_log_bucket{le="1"} 0
test_observe_consumption_histogram_requires_trace_log_bucket{le="3"} 1
test_observe_consumption_histogram_requires_trace_log_bucket{le="+Inf"} 1
test_observe_consumption_histogram_requires_trace_log_sum 2
test_observe_consumption_histogram_requires_trace_log_count 1
`), histogramName))
	re.Equal(rruBefore+3, promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "rru", chargeDirection)))
}

func TestAddRUConsumptionIgnoresNilConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())

	rruBefore := promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "rru", chargeDirection))
	wruBefore := promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", chargeDirection))
	readBefore := promtestutil.ToFloat64(metrics.ReadByteCost.WithLabelValues(gc.name))
	writeBefore := promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, chargeDirection))

	re.NotPanics(func() {
		gc.addRUConsumption(nil)
	})

	re.Equal(rruBefore, promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "rru", chargeDirection)))
	re.Equal(wruBefore, promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", chargeDirection)))
	re.Equal(readBefore, promtestutil.ToFloat64(metrics.ReadByteCost.WithLabelValues(gc.name)))
	re.Equal(writeBefore, promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, chargeDirection)))
}

func TestFailedWriteConsumptionRecordsRefundMetrics(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	gc.burstable.Store(true)

	req := &TestRequestInfo{
		isWrite:     true,
		writeBytes:  64 * 1024,
		numReplicas: 3,
	}
	resp := &TestResponseInfo{succeed: false}

	wruChargeBefore := promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", chargeDirection))
	wruRefundBefore := promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", refundDirection))
	writeChargeBefore := promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, chargeDirection))
	writeRefundBefore := promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, refundDirection))

	requestDelta, _, _, _, err := gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)

	responseDelta, err := gc.onResponseImpl(req, resp)
	re.NoError(err)
	re.Negative(responseDelta.WRU)
	re.Negative(responseDelta.WriteBytes)

	re.Equal(wruChargeBefore+requestDelta.WRU, promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", chargeDirection)))
	re.Equal(wruRefundBefore-responseDelta.WRU, promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", refundDirection)))
	re.Equal(writeChargeBefore+requestDelta.WriteBytes, promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, chargeDirection)))
	re.Equal(writeRefundBefore-responseDelta.WriteBytes, promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, refundDirection)))
}

func TestSuccessfulWriteConsumptionDoesNotRecordRefundMetrics(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	gc.burstable.Store(true)

	req := &TestRequestInfo{
		isWrite:     true,
		writeBytes:  64 * 1024,
		numReplicas: 3,
	}
	resp := &TestResponseInfo{succeed: true}

	wruRefundBefore := promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", refundDirection))
	writeRefundBefore := promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, refundDirection))

	_, _, _, _, err := gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	responseDelta, err := gc.onResponseImpl(req, resp)
	re.NoError(err)
	re.Zero(responseDelta.WRU)
	re.Zero(responseDelta.WriteBytes)

	re.Equal(wruRefundBefore, promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", refundDirection)))
	re.Equal(writeRefundBefore, promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, refundDirection)))
}

func TestFailedWriteResponseWaitRecordsRefundMetrics(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	gc.burstable.Store(true)

	req := &TestRequestInfo{
		isWrite:     true,
		writeBytes:  64 * 1024,
		numReplicas: 3,
	}
	resp := &TestResponseInfo{succeed: false}

	wruRefundBefore := promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", refundDirection))
	writeRefundBefore := promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, refundDirection))

	_, _, _, _, err := gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	responseDelta, _, err := gc.onResponseWaitImpl(context.Background(), req, resp)
	re.NoError(err)
	re.Negative(responseDelta.WRU)
	re.Negative(responseDelta.WriteBytes)

	re.Equal(wruRefundBefore-responseDelta.WRU, promtestutil.ToFloat64(metrics.TokenConsumedByTypeCounter.WithLabelValues(gc.name, "wru", refundDirection)))
	re.Equal(writeRefundBefore-responseDelta.WriteBytes, promtestutil.ToFloat64(metrics.WriteByteCost.WithLabelValues(gc.name, refundDirection)))
}

func TestInitializesSQLCPUConsumptionBaseline(t *testing.T) {
	re := require.New(t)
	waitForNonZeroProcessCPUTime(re)
	cfg := DefaultRUConfig()
	cfg.isSingleGroupByKeyspace = true
	gc := createTestGroupCostControllerWithConfig(re, t.Name(), cfg)

	gc.mu.Lock()
	consumption := *gc.mu.consumption
	gc.mu.Unlock()

	re.Greater(consumption.SqlLayerCpuTimeMs, 0.0)
	re.Equal(consumption.SqlLayerCpuTimeMs, consumption.TotalCpuTimeMs)
	re.Equal(consumption, *gc.run.consumption)
	re.Equal(consumption, *gc.run.lastRequestConsumption)
}

func TestModifyTokenCounterUpdatesActualGrantMetric(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re, t.Name())
	counter := gc.run.requestUnitTokens
	gc.run.now = time.Now()
	counter.lastDeadline = gc.run.now.Add(10 * time.Second)
	counter.lastRate = 100
	before := promtestutil.ToFloat64(metrics.ActualGrantTokensCounter.WithLabelValues(gc.name))

	gc.modifyTokenCounter(counter, &rmpb.TokenBucket{
		Tokens: 250,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   100,
			BurstLimit: 1000,
		},
	}, 0)

	re.Equal(before+250, promtestutil.ToFloat64(metrics.ActualGrantTokensCounter.WithLabelValues(gc.name)))
}
