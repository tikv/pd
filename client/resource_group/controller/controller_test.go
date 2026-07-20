// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
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
		NewRate:  1000,
		NewBurst: -1,
	}
	for _, counter := range gc.run.requestUnitTokens {
		counter.limiter.Reconfigure(time.Now(), args)
	}
	gc.updateAvgRequestResourcePerSec()
	re.True(gc.burstable.Load())
}

func TestCalcAvgSkipsNonPositiveDeltaDuration(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	counter := gc.run.requestUnitTokens[rmpb.RequestUnitType_RU]
	// Simulate a controller that is published between the `updateRunState` and
	// `updateAvgRequestResourcePerSec` passes of the same tick: `gc.run.now`
	// still equals the `avgLastTime` set by `initRunState`, so the elapsed
	// duration is exactly zero and the sample must be skipped instead of
	// computing 0/0 = NaN.
	re.Equal(gc.run.now, counter.avgLastTime)
	gc.updateAvgRequestResourcePerSec()
	re.False(math.IsNaN(counter.avgRUPerSec))
	re.Zero(counter.avgRUPerSec)
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
				isWrite:    true,
				writeBytes: 100,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				succeed:   true,
			},
		},
		// Read request
		{
			req: &TestRequestInfo{
				isWrite:    false,
				writeBytes: 0,
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
		}
		consumption, err = gc.onResponseImpl(testCase.req, testCase.resp)
		re.NoError(err, caseNum)
		kvCalculator.calculateReadCost(expectedConsumption, testCase.resp)
		kvCalculator.calculateCPUCost(expectedConsumption, testCase.resp)
		re.Equal(expectedConsumption.RRU, consumption.RRU, caseNum)
		re.Equal(expectedConsumption.TotalCpuTimeMs, consumption.TotalCpuTimeMs, caseNum)
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
	counter := gc.run.requestUnitTokens[rmpb.RequestUnitType_RU]
	gc.modifyTokenCounter(counter, &rmpb.TokenBucket{
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
	counter := gc.run.requestUnitTokens[rmpb.RequestUnitType_RU]

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
	counter := gc.run.requestUnitTokens[rmpb.RequestUnitType_RU]
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
	counter := gc.run.requestUnitTokens[rmpb.RequestUnitType_RU]
	counter.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		NewTokens: 1000,
		NewRate:   0,
		NewBurst:  0,
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
		NewTokens: 100000,
		NewRate:   100000,
		NewBurst:  0,
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
	counter := gc.run.requestUnitTokens[rmpb.RequestUnitType_RU]
	counter.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{
		NewTokens: 1000,
		NewRate:   0,
		NewBurst:  0,
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
	counter := gc.run.requestUnitTokens[rmpb.RequestUnitType_RU]

	// Throttled bucket: a 10000-RU request reserves with a multi-second delay and
	// parks in WaitReservations rather than being granted immediately.
	counter.limiter.Reconfigure(time.Now(), tokenBucketReconfigureArgs{NewTokens: 0, NewRate: 1000, NewBurst: 0})

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

// MockResourceGroupProvider is a mock implementation of the ResourceGroupProvider interface.
type MockResourceGroupProvider struct {
	mock.Mock
}

func newMockResourceGroupProvider() *MockResourceGroupProvider {
	mockProvider := &MockResourceGroupProvider{}
	mockProvider.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta_storagepb.GetResponse{}, nil)
	mockProvider.On("LoadResourceGroups", mock.Anything).Return([]*rmpb.ResourceGroup{}, int64(0), nil)
	mockProvider.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(chan []*meta_storagepb.Event), nil)
	return mockProvider
}

func (m *MockResourceGroupProvider) GetResourceGroup(ctx context.Context, resourceGroupName string, opts ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error) {
	args := m.Called(ctx, resourceGroupName, opts)
	return args.Get(0).(*rmpb.ResourceGroup), args.Error(1)
}

func (m *MockResourceGroupProvider) ListResourceGroups(ctx context.Context, opts ...pd.GetResourceGroupOption) ([]*rmpb.ResourceGroup, error) {
	args := m.Called(ctx, opts)
	return args.Get(0).([]*rmpb.ResourceGroup), args.Error(1)
}

func (m *MockResourceGroupProvider) AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	args := m.Called(ctx, metaGroup)
	return args.String(0), args.Error(1)
}

func (m *MockResourceGroupProvider) ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	args := m.Called(ctx, metaGroup)
	return args.String(0), args.Error(1)
}

func (m *MockResourceGroupProvider) DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error) {
	args := m.Called(ctx, resourceGroupName)
	return args.String(0), args.Error(1)
}

func (m *MockResourceGroupProvider) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	args := m.Called(ctx, request)
	return args.Get(0).([]*rmpb.TokenBucketResponse), args.Error(1)
}

func (m *MockResourceGroupProvider) LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*rmpb.ResourceGroup), args.Get(1).(int64), args.Error(2)
}

func (m *MockResourceGroupProvider) Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(chan []*meta_storagepb.Event), args.Error(1)
}

func (m *MockResourceGroupProvider) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(*meta_storagepb.GetResponse), args.Error(1)
}

func (m *MockResourceGroupProvider) Put(ctx context.Context, key []byte, value []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	args := m.Called(ctx, key, value, opts)
	return args.Get(0).(*meta_storagepb.PutResponse), args.Error(1)
}

func TestControllerWithTwoGroupRequestConcurrency(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/triggerPeriodicReport", fmt.Sprintf("return(\"%s\")", defaultResourceGroupName)))
	defer failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/triggerPeriodicReport")
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/triggerLowRUReport", fmt.Sprintf("return(\"%s\")", "test-group")))
	defer failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/triggerLowRUReport")

	mockProvider := newMockResourceGroupProvider()
	controller, _ := NewResourceGroupController(ctx, 1, mockProvider, nil)
	controller.Start(ctx)

	defaultResourceGroup := &rmpb.ResourceGroup{Name: defaultResourceGroupName, Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	testResourceGroup := &rmpb.ResourceGroup{Name: "test-group", Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	mockProvider.On("GetResourceGroup", mock.Anything, defaultResourceGroupName, mock.Anything).Return(defaultResourceGroup, nil)
	mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).Return(testResourceGroup, nil)

	c1, err := controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
	re.NoError(err)
	re.Equal(defaultResourceGroup, c1.meta)

	c2, err := controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.NoError(err)
	re.Equal(testResourceGroup, c2.meta)

	var expectResp []*rmpb.TokenBucketResponse
	recTestGroupAcquireTokenRequest := make(chan bool)
	mockProvider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		request := args.Get(1).(*rmpb.TokenBucketsRequest)
		var responses []*rmpb.TokenBucketResponse
		for _, req := range request.Requests {
			if req.ResourceGroupName == defaultResourceGroupName {
				// no response the default group request, that's mean `len(c.run.currentRequests) != 0` always.
				time.Sleep(100 * time.Second)
				responses = append(responses, &rmpb.TokenBucketResponse{
					ResourceGroupName: defaultResourceGroupName,
					GrantedRUTokens: []*rmpb.GrantedRUTokenBucket{
						{
							GrantedTokens: &rmpb.TokenBucket{
								Tokens: 100000,
							},
						},
					},
				})
			} else {
				responses = append(responses, &rmpb.TokenBucketResponse{
					ResourceGroupName: req.ResourceGroupName,
					GrantedRUTokens: []*rmpb.GrantedRUTokenBucket{
						{
							GrantedTokens: &rmpb.TokenBucket{
								Tokens: 100000,
							},
						},
					},
				})
			}
		}
		// receive test-group request
		if len(request.Requests) == 1 && request.Requests[0].ResourceGroupName == "test-group" {
			recTestGroupAcquireTokenRequest <- true
		}
		expectResp = responses
	}).Return(expectResp, nil)
	// wait default group request token by PeriodicReport.
	time.Sleep(2 * time.Second)
	counter := c2.run.requestUnitTokens[0]
	counter.limiter.mu.Lock()
	counter.limiter.notify()
	counter.limiter.mu.Unlock()
	select {
	case res := <-recTestGroupAcquireTokenRequest:
		re.True(res)
	case <-time.After(5 * time.Second):
		re.Fail("timeout")
	}
}

func TestTryGetController(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := newMockResourceGroupProvider()
	controller, _ := NewResourceGroupController(ctx, 1, mockProvider, nil)
	controller.Start(ctx)

	defaultResourceGroup := &rmpb.ResourceGroup{Name: defaultResourceGroupName, Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	testResourceGroup := &rmpb.ResourceGroup{Name: "test-group", Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	mockProvider.On("GetResourceGroup", mock.Anything, defaultResourceGroupName, mock.Anything).Return(defaultResourceGroup, nil)
	mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).Return(testResourceGroup, nil)
	mockProvider.On("GetResourceGroup", mock.Anything, "test-group-non-existent", mock.Anything).Return((*rmpb.ResourceGroup)(nil), nil)

	gc, err := controller.tryGetResourceGroupController(ctx, "test-group-non-existent", false)
	re.Error(err)
	re.Nil(gc)
	gc, err = controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
	re.NoError(err)
	re.Equal(defaultResourceGroup, gc.getMeta())
	gc, err = controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.NoError(err)
	re.Equal(testResourceGroup, gc.getMeta())
	requestInfo, responseInfo := NewTestRequestInfo(true, 1, 1), NewTestResponseInfo(1, time.Millisecond, true)
	_, _, _, _, err = controller.OnRequestWait(ctx, "test-group", requestInfo)
	re.NoError(err)
	consumption, err := controller.OnResponse("test-group", requestInfo, responseInfo)
	re.NoError(err)
	re.NotEmpty(consumption)
	// Mark the tombstone manually to test the fallback case.
	gc, err = controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.NoError(err)
	re.NotNil(gc)
	controller.tombstoneGroupCostController("test-group")
	gc, err = controller.tryGetResourceGroupController(ctx, "test-group", false)
	re.Error(err)
	re.Nil(gc)
	gc, err = controller.tryGetResourceGroupController(ctx, "test-group", true)
	re.NoError(err)
	re.Equal(defaultResourceGroup, gc.getMeta())
	_, _, _, _, err = controller.OnRequestWait(ctx, "test-group", requestInfo)
	re.NoError(err)
	consumption, err = controller.OnResponse("test-group", requestInfo, responseInfo)
	re.NoError(err)
	re.NotEmpty(consumption)
	// Test the default group protection.
	gc, err = controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
	re.NoError(err)
	re.Equal(defaultResourceGroup, gc.getMeta())
	controller.tombstoneGroupCostController(defaultResourceGroupName)
	gc, err = controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
	re.NoError(err)
	re.Equal(defaultResourceGroup, gc.getMeta())
	gc, err = controller.tryGetResourceGroupController(ctx, defaultResourceGroupName, true)
	re.NoError(err)
	re.Equal(defaultResourceGroup, gc.getMeta())
	_, _, _, _, err = controller.OnRequestWait(ctx, defaultResourceGroupName, requestInfo)
	re.NoError(err)
	consumption, err = controller.OnResponse(defaultResourceGroupName, requestInfo, responseInfo)
	re.NoError(err)
	re.NotEmpty(consumption)
}
