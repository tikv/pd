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
	"runtime"
	"sync"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/resource_group/controller/metrics"
)

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
