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

func TestDemandRUTracking(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	// Simulate requests arriving: demand should accumulate regardless of throttling.
	req := &TestRequestInfo{
		isWrite:    true,
		writeBytes: 100,
	}
	resp := &TestResponseInfo{
		readBytes: 100,
		succeed:   true,
	}

	// Issue several successful requests.
	for range 5 {
		consumption, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), req)
		re.NoError(err)
		re.NotNil(consumption)
		_, err = gc.onResponseImpl(req, resp)
		re.NoError(err)
	}

	// demandRUTotal should have accumulated all pre-request and post-response RU.
	gc.mu.Lock()
	demandTotal := gc.mu.demandRUTotal
	gc.mu.Unlock()
	re.Positive(demandTotal, "demand should be accumulated after requests")

	// Now issue a request that gets throttled (rejected) on `onRequestWaitImpl`.
	bigReq := &TestRequestInfo{
		isWrite:    true,
		writeBytes: 10000000,
	}
	_, _, _, _, err := gc.onRequestWaitImpl(context.TODO(), bigReq)
	re.Error(err)
	re.True(errs.ErrClientResourceGroupThrottled.Equal(err))

	// demandRUTotal should still include the throttled request's RU.
	gc.mu.Lock()
	demandAfterThrottle := gc.mu.demandRUTotal
	gc.mu.Unlock()
	re.Greater(demandAfterThrottle, demandTotal,
		"demand should increase even for throttled requests")

	// Verify the demand EMA math directly. We deliberately avoid going through
	// `updateRunState` here because that method overwrites `gc.run.now` with
	// `time.Now()` on every call, which makes any caller-side time control a
	// no-op. Instead, snapshot demand into `gc.run` once and drive `calcDemandAvg`
	// with hand-set timestamps so the EMA's behavior is observable.
	gc.updateRunState() // copy mu.demandRUTotal into gc.run.demandRUTotal once.

	counter := gc.run.requestUnitTokens
	// Reset the EMA bookkeeping so we can measure a clean two-tick trajectory.
	counter.avgDemandRUPerSec = 0
	counter.avgDemandRUPerSecLastRU = 0
	base := time.Unix(0, 0)
	counter.avgDemandLastTime = base
	gc.run.now = base.Add(time.Second)

	re.True(gc.calcDemandAvg(counter, gc.run.demandRUTotal))
	// First tick: avg = movingAvgFactor*0 + (1-movingAvgFactor) * (demandTotal/1s).
	expectedFirst := (1 - movingAvgFactor) * gc.run.demandRUTotal
	re.InEpsilon(expectedFirst, counter.avgDemandRUPerSec, 1e-9,
		"first EMA tick should equal (1-movingAvgFactor) * demand-rate")

	// Second tick: same demand snapshot, one more second elapsed -> rate is 0,
	// so the EMA must decay toward zero by movingAvgFactor.
	gc.run.now = base.Add(2 * time.Second)
	prev := counter.avgDemandRUPerSec
	re.True(gc.calcDemandAvg(counter, gc.run.demandRUTotal))
	re.InEpsilon(movingAvgFactor*prev, counter.avgDemandRUPerSec, 1e-9,
		"with no new demand the EMA should decay by movingAvgFactor")

	// Same `gc.run.now` -> calcDemandAvg must report no update and leave state alone.
	re.False(gc.calcDemandAvg(counter, gc.run.demandRUTotal))
}

// TestDemandRUCapturedOnResponseWaitThrottle locks in the invariant that
// `demand_ru_per_sec` reflects rejected responses too. Without the
// `recordDemand` hoist in `onResponseWaitImpl`, throttle-rejected responses
// would be silently absent from the demand counter -- defeating the metric.
func TestDemandRUCapturedOnResponseWaitThrottle(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	// Short retry budget so the test fails fast.
	gc.mainCfg.WaitRetryInterval = 5 * time.Millisecond
	gc.mainCfg.WaitRetryTimes = 2
	gc.mainCfg.LTBMaxWaitDuration = 10 * time.Millisecond

	// Stop the bucket from refilling. The limiter still carries its initial
	// tokens (FillRate=1000 -> 1000 RU seeded in initRunState), so the request
	// below must demand strictly more than that to provoke a throttle error.
	counter := gc.run.requestUnitTokens
	counter.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		newTokens:   0,
		newFillRate: 0,
		newBurst:    0,
	})
	// `allowDebt` in `onResponseWaitImpl` is false only when the response is
	// "big" (read+write bytes >= bigRequestThreshold) AND the group is already
	// throttled. Force both.
	gc.isThrottled.Store(true)

	gc.mu.Lock()
	demandBefore := gc.mu.demandRUTotal
	gc.mu.Unlock()

	const readBytes = 128 * 1024 * 1024 // 128 MiB -> 2048 RRU at default 1/64KiB cost
	req := &TestRequestInfo{isWrite: false}
	resp := &TestResponseInfo{
		readBytes: readBytes,
		succeed:   true,
	}
	_, _, err := gc.onResponseWaitImpl(context.TODO(), req, resp)
	re.Error(err)
	re.True(errs.ErrClientResourceGroupThrottled.Equal(err))

	gc.mu.Lock()
	demandAfter := gc.mu.demandRUTotal
	gc.mu.Unlock()
	re.Greater(demandAfter, demandBefore,
		"demand should be recorded for responses rejected by the limiter")
}
