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

	"github.com/pingcap/failpoint"
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

func TestFailedWriteReservationDoesNotEnterReportedConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	req := &TestRequestInfo{
		isWrite:     true,
		writeBytes:  1024,
		numReplicas: 3,
	}
	resp := &TestResponseInfo{
		readBytes: 0,
		succeed:   false,
	}

	tokenDelta, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
	re.NoError(err)
	re.Positive(tokenDelta.WRU)
	gc.updateRunState()
	requestReport := gc.collectRequestAndConsumption(periodicReport)
	re.NotNil(requestReport)
	re.Zero(requestReport.GetConsumptionSinceLastRequest().GetWRU(),
		"write reservation should not be reported as actual usage before the response succeeds")
	gc.handleTokenBucketResponse(&rmpb.TokenBucketResponse{})

	refundDelta, _, err := gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.NoError(err)
	re.Negative(refundDelta.WRU)
	gc.run.lastRequestTime = time.Now().Add(-extendedReportingPeriodFactor * defaultTargetPeriod)
	gc.updateRunState()
	refundReport := gc.collectRequestAndConsumption(periodicReport)
	re.NotNil(refundReport)
	re.Zero(refundReport.GetConsumptionSinceLastRequest().GetWRU(),
		"failed write refund must not make actual usage negative")
}

func TestFailedWriteReservationRefundsLimiterOnResponseWait(t *testing.T) {
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
	expectedRefund := -getRUValueFromConsumption(refundDelta)
	re.Positive(expectedRefund)
	tokensAfterResponse := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	re.InDelta(tokensAfterReservation+expectedRefund, tokensAfterResponse, 1.0,
		"failed write payback should refund the local write reservation")
}

func TestFailedWriteReservationRefundsLimiterOnResponse(t *testing.T) {
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
	expectedRefund := -getRUValueFromConsumption(refundDelta)
	re.Positive(expectedRefund)
	tokensAfterResponse := gc.run.requestUnitTokens.limiter.AvailableTokens(time.Now())

	re.InDelta(tokensAfterReservation+expectedRefund, tokensAfterResponse, 1.0,
		"failed write payback should refund the local write reservation")
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
