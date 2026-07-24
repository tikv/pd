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
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/errs"
	controllerMetrics "github.com/tikv/pd/client/resource_group/controller/metrics"
)

func requestSourceCounterValue(t *testing.T, metric prometheus.Counter) float64 {
	t.Helper()
	pb := &dto.Metric{}
	require.NoError(t, metric.Write(pb))
	return pb.GetCounter().GetValue()
}

func collectorMetricCount(collector prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 8)
	go func() {
		collector.Collect(ch)
		close(ch)
	}()
	count := 0
	for range ch {
		count++
	}
	return count
}

type requestSourceMetricsSnapshot struct {
	rruConsume prometheus.Counter
	rruRefund  prometheus.Counter
	wruConsume prometheus.Counter
	wruRefund  prometheus.Counter
}

func requestSourceStateSnapshot(t *testing.T, gc *groupCostController, requestSource string) (*requestSourceMetricsSnapshot, int) {
	t.Helper()
	require.NotNil(t, gc.metrics.sourceState)

	gc.metrics.sourceState.mu.RLock()
	defer gc.metrics.sourceState.mu.RUnlock()

	snapshot := &requestSourceMetricsSnapshot{}
	for key, metric := range gc.metrics.sourceState.items {
		if key.requestSource != requestSource {
			continue
		}
		switch {
		case key.ruType == requestSourceRUTypeRRU && key.direction == requestSourceDirectionConsume:
			snapshot.rruConsume = metric
		case key.ruType == requestSourceRUTypeRRU && key.direction == requestSourceDirectionRefund:
			snapshot.rruRefund = metric
		case key.ruType == requestSourceRUTypeWRU && key.direction == requestSourceDirectionConsume:
			snapshot.wruConsume = metric
		case key.ruType == requestSourceRUTypeWRU && key.direction == requestSourceDirectionRefund:
			snapshot.wruRefund = metric
		}
	}
	return snapshot, len(gc.metrics.sourceState.items)
}

func TestRequestSourceMetricsRecordAndCacheAccountingDeltas(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    100,
		numReplicas:   1,
		storeID:       1,
		accessType:    AccessUnknown,
		requestSource: "internal_gc_test",
	}
	resp := &TestResponseInfo{
		readBytes: 128,
		succeed:   true,
	}

	beforeCount := collectorMetricCount(controllerMetrics.RequestSourceRUCounter)

	reqConsumption, _, _, _, err := gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	re.NotZero(reqConsumption.WRU)

	sourceMetrics, cacheSize := requestSourceStateSnapshot(t, gc, req.requestSource)
	re.Equal(1, cacheSize)
	re.NotNil(sourceMetrics.wruConsume)
	re.Nil(sourceMetrics.wruRefund)
	re.Nil(sourceMetrics.rruConsume)
	re.Equal(reqConsumption.WRU, requestSourceCounterValue(t, sourceMetrics.wruConsume))
	re.Equal(beforeCount+1, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	respConsumption, err := gc.onResponseImpl(req, resp)
	re.NoError(err)
	re.NotZero(respConsumption.RRU)

	sourceMetrics, cacheSize = requestSourceStateSnapshot(t, gc, req.requestSource)

	re.Equal(2, cacheSize)
	re.NotNil(sourceMetrics)
	re.Equal(reqConsumption.WRU, requestSourceCounterValue(t, sourceMetrics.wruConsume))
	re.Equal(respConsumption.RRU, requestSourceCounterValue(t, sourceMetrics.rruConsume))
	re.Equal(beforeCount+2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	cachedMetrics, cacheSize := requestSourceStateSnapshot(t, gc, req.requestSource)
	re.Equal(2, cacheSize)
	re.Same(sourceMetrics.wruConsume, cachedMetrics.wruConsume)
	re.Same(sourceMetrics.rruConsume, cachedMetrics.rruConsume)

	gc.metrics.sourceState.cleanup()
}

func TestRequestSourceMetricsSkipRejectedAndZeroDeltas(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    10000000,
		requestSource: "internal_rejected_request",
	}
	beforeCount := collectorMetricCount(controllerMetrics.RequestSourceRUCounter)

	_, _, _, _, err := gc.onRequestWaitImpl(context.Background(), req)
	re.Error(err)
	re.True(errs.ErrClientResourceGroupThrottled.Equal(err))
	_, cacheSize := requestSourceStateSnapshot(t, gc, req.requestSource)
	re.Zero(cacheSize)
	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	gc.metrics.addRequestSourceRUDelta(req.requestSource, &rmpb.Consumption{})
	_, cacheSize = requestSourceStateSnapshot(t, gc, req.requestSource)
	re.Zero(cacheSize)
	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))
}

func TestRequestSourceMetricsConcurrentLazyCreation(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	const workers = 64
	requestSource := "internal_concurrent_metric_creation"
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			gc.metrics.addRequestSourceRUDelta(requestSource, &rmpb.Consumption{RRU: 1, WRU: -1})
		}()
	}
	wg.Wait()

	sourceMetrics, cacheSize := requestSourceStateSnapshot(t, gc, requestSource)
	re.Equal(2, cacheSize)
	re.NotNil(sourceMetrics.rruConsume)
	re.NotNil(sourceMetrics.wruRefund)
	re.Equal(float64(workers), requestSourceCounterValue(t, sourceMetrics.rruConsume))
	re.Equal(float64(workers), requestSourceCounterValue(t, sourceMetrics.wruRefund))

	gc.metrics.sourceState.cleanup()
}

func TestCleanupResourceGroupRemovesRequestSourceMetrics(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 0)
	re.NoError(err)

	group := &rmpb.ResourceGroup{
		Name: "request-source-cleanup",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000000},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)

	gc, err := controller.tryGetResourceGroupController(ctx, group.Name, false)
	re.NoError(err)

	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    64,
		numReplicas:   1,
		storeID:       1,
		requestSource: "internal_gc_cleanup",
	}
	resp := &TestResponseInfo{readBytes: 64, succeed: true}
	beforeCount := collectorMetricCount(controllerMetrics.RequestSourceRUCounter)

	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = gc.onResponseImpl(req, resp)
	re.NoError(err)

	gc.mu.Lock()
	*gc.run.consumption = *gc.mu.consumption
	gc.mu.Unlock()
	gc.inactive = true

	re.Greater(collectorMetricCount(controllerMetrics.RequestSourceRUCounter), beforeCount)

	controller.cleanUpResourceGroup()

	_, ok := controller.loadGroupController(group.Name)
	re.False(ok)
	_, cacheSize := requestSourceStateSnapshot(t, gc, req.requestSource)
	re.Zero(cacheSize)
	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))
}

func TestCleanupDoesNotReexportExistingCachedHandle(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 0)
	re.NoError(err)

	group := &rmpb.ResourceGroup{
		Name: "request-source-cleanup-cached-handle",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000000},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)

	gc, err := controller.tryGetResourceGroupController(ctx, group.Name, false)
	re.NoError(err)

	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    64,
		numReplicas:   1,
		storeID:       1,
		requestSource: "internal_gc_cleanup_cached_handle",
	}
	resp := &TestResponseInfo{readBytes: 64, succeed: true}
	beforeCount := collectorMetricCount(controllerMetrics.RequestSourceRUCounter)

	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = gc.onResponseImpl(req, resp)
	re.NoError(err)

	sourceMetrics, cacheSize := requestSourceStateSnapshot(t, gc, req.requestSource)
	re.NotNil(sourceMetrics)
	re.Equal(2, cacheSize)
	re.Equal(beforeCount+2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	gc.mu.Lock()
	*gc.run.consumption = *gc.mu.consumption
	gc.mu.Unlock()
	gc.inactive = true

	controller.cleanUpResourceGroup()

	_, ok := controller.loadGroupController(group.Name)
	re.False(ok)
	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	sourceMetrics.rruConsume.Add(1)
	sourceMetrics.wruConsume.Add(1)

	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))
}

func TestCleanupPreventsRecreateRequestSourceMetrics(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 0)
	re.NoError(err)

	group := &rmpb.ResourceGroup{
		Name: "request-source-cleanup-prevent-recreate",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000000},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)

	gc, err := controller.tryGetResourceGroupController(ctx, group.Name, false)
	re.NoError(err)

	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    64,
		numReplicas:   1,
		storeID:       1,
		requestSource: "internal_gc_cleanup_prevent_recreate",
	}
	resp := &TestResponseInfo{readBytes: 64, succeed: true}
	beforeCount := collectorMetricCount(controllerMetrics.RequestSourceRUCounter)

	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = gc.onResponseImpl(req, resp)
	re.NoError(err)
	re.Equal(beforeCount+2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	gc.mu.Lock()
	*gc.run.consumption = *gc.mu.consumption
	gc.mu.Unlock()
	gc.inactive = true

	controller.cleanUpResourceGroup()

	_, ok := controller.loadGroupController(group.Name)
	re.False(ok)
	_, cacheSize := requestSourceStateSnapshot(t, gc, req.requestSource)
	re.Zero(cacheSize)
	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	gc.metrics.addRequestSourceRUDelta(req.requestSource, &rmpb.Consumption{RRU: 1, WRU: 1})

	_, cacheSize = requestSourceStateSnapshot(t, gc, req.requestSource)
	re.Zero(cacheSize)
	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))
}

func TestTombstoneCleanupRemovesExistingRequestSourceMetrics(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 0)
	re.NoError(err)

	group := &rmpb.ResourceGroup{
		Name: "request-source-tombstone-cleanup",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000000},
			},
		},
	}
	defaultGroup := &rmpb.ResourceGroup{
		Name: defaultResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000000},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)
	mockProvider.On("GetResourceGroup", mock.Anything, defaultResourceGroupName, mock.Anything).Return(defaultGroup, nil)

	gc, err := controller.tryGetResourceGroupController(ctx, group.Name, false)
	re.NoError(err)

	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    64,
		numReplicas:   1,
		storeID:       1,
		requestSource: "internal_gc_tombstone_cleanup",
	}
	resp := &TestResponseInfo{readBytes: 64, succeed: true}
	beforeCount := collectorMetricCount(controllerMetrics.RequestSourceRUCounter)

	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = gc.onResponseImpl(req, resp)
	re.NoError(err)
	re.Equal(beforeCount+2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	controller.tombstoneGroupCostController(group.Name)
	tombstoneGC, err := controller.tryGetResourceGroupController(ctx, group.Name, true)
	re.NoError(err)
	re.True(tombstoneGC.tombstone.Load())

	controller.cleanUpResourceGroup()

	_, ok := controller.loadGroupController(group.Name)
	re.False(ok)
	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))
}

func TestRevivedResourceGroupCleanupRemovesExistingRequestSourceMetrics(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 0)
	re.NoError(err)

	group := &rmpb.ResourceGroup{
		Name: "request-source-revive-cleanup",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000000},
			},
		},
	}
	defaultGroup := &rmpb.ResourceGroup{
		Name: defaultResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000000},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)
	mockProvider.On("GetResourceGroup", mock.Anything, defaultResourceGroupName, mock.Anything).Return(defaultGroup, nil)

	gc, err := controller.tryGetResourceGroupController(ctx, group.Name, false)
	re.NoError(err)

	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    64,
		numReplicas:   1,
		storeID:       1,
		requestSource: "internal_gc_revive_cleanup",
	}
	resp := &TestResponseInfo{readBytes: 64, succeed: true}
	beforeCount := collectorMetricCount(controllerMetrics.RequestSourceRUCounter)

	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = gc.onResponseImpl(req, resp)
	re.NoError(err)
	re.Equal(beforeCount+2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	controller.tombstoneGroupCostController(group.Name)
	tombstoneGC, err := controller.tryGetResourceGroupController(ctx, group.Name, true)
	re.NoError(err)
	re.True(tombstoneGC.tombstone.Load())

	revivedGC, err := newGroupCostController(
		group,
		controller.ruConfig,
		controller.lowTokenNotifyChan,
		controller.tokenBucketUpdateChan,
		controller.getOrCreateRequestSourceMetricsState(group.Name),
	)
	re.NoError(err)
	re.True(controller.groupsController.CompareAndSwap(group.Name, tombstoneGC, revivedGC))

	revivedGC.mu.Lock()
	*revivedGC.run.consumption = *revivedGC.mu.consumption
	revivedGC.mu.Unlock()
	revivedGC.inactive = true

	controller.cleanUpResourceGroup()

	_, ok := controller.loadGroupController(group.Name)
	re.False(ok)
	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))
}

// TestGetOrCreateAfterCleanupReturnsFreshState verifies the fix for the race
// rleungx identified: after cleanupRequestSourceMetricsState runs,
// getOrCreateRequestSourceMetricsState must return a fresh, non-closed state
// so that a newly created gc can record metrics normally.
func TestGetOrCreateAfterCleanupReturnsFreshState(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 0)
	re.NoError(err)

	group := &rmpb.ResourceGroup{
		Name: "request-source-create-after-cleanup",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000000},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)

	// Phase 1: create a gc, record some metrics, then clean up.
	gc, err := controller.tryGetResourceGroupController(ctx, group.Name, false)
	re.NoError(err)

	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    64,
		numReplicas:   1,
		storeID:       1,
		requestSource: "internal_create_after_cleanup",
	}
	resp := &TestResponseInfo{readBytes: 64, succeed: true}

	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = gc.onResponseImpl(req, resp)
	re.NoError(err)

	gc.mu.Lock()
	*gc.run.consumption = *gc.mu.consumption
	gc.mu.Unlock()
	gc.inactive = true
	controller.cleanUpResourceGroup()

	_, loaded := controller.loadGroupController(group.Name)
	re.False(loaded)

	// Phase 2: getOrCreateRequestSourceMetricsState must return a fresh,
	// non-closed state after cleanup has removed the old one.
	state := controller.getOrCreateRequestSourceMetricsState(group.Name)
	re.NotNil(state)

	state.mu.RLock()
	re.False(state.closed)
	state.mu.RUnlock()

	// Phase 3: create a new gc with this state and verify it can record metrics.
	newGC, err := newGroupCostController(
		group,
		controller.ruConfig,
		controller.lowTokenNotifyChan,
		controller.tokenBucketUpdateChan,
		state,
	)
	re.NoError(err)

	beforeCount := collectorMetricCount(controllerMetrics.RequestSourceRUCounter)
	_, _, _, _, err = newGC.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = newGC.onResponseImpl(req, resp)
	re.NoError(err)

	sourceMetrics, cacheSize := requestSourceStateSnapshot(t, newGC, req.requestSource)
	re.Equal(2, cacheSize)
	re.NotNil(sourceMetrics)
	re.Greater(requestSourceCounterValue(t, sourceMetrics.wruConsume), float64(0))
	re.Greater(requestSourceCounterValue(t, sourceMetrics.rruConsume), float64(0))
	re.Equal(beforeCount+2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	// Cleanup for this test.
	state.cleanup()
}

// TestCleanupThenRecreateViaFullPath exercises the full end-to-end path:
// cleanup a group, then tryGetResourceGroupController re-creates it, and
// the new gc records metrics successfully.
func TestCleanupThenRecreateViaFullPath(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := newMockResourceGroupProvider()
	controller, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 0)
	re.NoError(err)

	group := &rmpb.ResourceGroup{
		Name: "request-source-full-recreate",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000000},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)

	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    64,
		numReplicas:   1,
		storeID:       1,
		requestSource: "internal_full_recreate",
	}
	resp := &TestResponseInfo{readBytes: 64, succeed: true}

	// Create, use, and clean up the group.
	gc, err := controller.tryGetResourceGroupController(ctx, group.Name, false)
	re.NoError(err)
	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = gc.onResponseImpl(req, resp)
	re.NoError(err)

	gc.mu.Lock()
	*gc.run.consumption = *gc.mu.consumption
	gc.mu.Unlock()
	gc.inactive = true
	controller.cleanUpResourceGroup()

	_, loaded := controller.loadGroupController(group.Name)
	re.False(loaded)

	// Re-create through the normal path (simulates a new request arriving
	// after the group was cleaned up).
	gc2, err := controller.tryGetResourceGroupController(ctx, group.Name, false)
	re.NoError(err)

	beforeCount := collectorMetricCount(controllerMetrics.RequestSourceRUCounter)
	_, _, _, _, err = gc2.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = gc2.onResponseImpl(req, resp)
	re.NoError(err)

	sourceMetrics, cacheSize := requestSourceStateSnapshot(t, gc2, req.requestSource)
	re.Equal(2, cacheSize)
	re.NotNil(sourceMetrics)
	re.Greater(requestSourceCounterValue(t, sourceMetrics.wruConsume), float64(0))
	re.Greater(requestSourceCounterValue(t, sourceMetrics.rruConsume), float64(0))
	re.Equal(beforeCount+2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	// Cleanup for this test.
	gc2.metrics.sourceState.cleanup()
}

// TestStopCleansUpRequestSourceMetricsState verifies the loopCtx.Done()
// shutdown path: per-group state is closed, cached label tuples are deleted
// from the vec, and post-stop addRequestSourceRUDelta does not re-register series.
func TestStopCleansUpRequestSourceMetricsState(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockProvider := newMockResourceGroupProvider()
	c, err := NewResourceGroupController(ctx, 1, mockProvider, nil, 0)
	re.NoError(err)

	group := &rmpb.ResourceGroup{
		Name: "request-source-stop-cleanup",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 1000000},
			},
		},
	}
	mockProvider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)

	// Start is required so that the loopCtx.Done() branch actually runs.
	c.Start(ctx)

	gc, err := c.tryGetResourceGroupController(ctx, group.Name, false)
	re.NoError(err)

	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    64,
		numReplicas:   1,
		storeID:       1,
		requestSource: "internal_stop_cleanup",
	}
	resp := &TestResponseInfo{readBytes: 64, succeed: true}
	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = gc.onResponseImpl(req, resp)
	re.NoError(err)

	v, ok := c.requestSourceStates.Load(group.Name)
	re.True(ok)
	ms := v.(*requestSourceMetricsState)
	beforeCount := collectorMetricCount(controllerMetrics.RequestSourceRUCounter)

	re.NoError(c.Stop())

	// cleanup() runs asynchronously on the background loop goroutine.
	re.Eventually(func() bool {
		ms.mu.RLock()
		defer ms.mu.RUnlock()
		return ms.closed && len(ms.items) == 0
	}, 5*time.Second, 10*time.Millisecond)

	// This request registered one consume series for RRU and one for WRU.
	re.Equal(beforeCount-2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	// Post-stop accounting must not re-register any series.
	gc.metrics.addRequestSourceRUDelta("internal_stop_cleanup_later", &rmpb.Consumption{RRU: 1, WRU: 1})
	ms.mu.RLock()
	re.Empty(ms.items)
	ms.mu.RUnlock()
	re.Equal(beforeCount-2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))
}

// TestFailedWriteMatchesControllerConsumption verifies that consume minus
// refund mirrors the controller's own consumption when payBackWriteCost
// produces a negative response-side WRU delta.
func TestFailedWriteMatchesControllerConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)

	req := &TestRequestInfo{
		isWrite:       true,
		writeBytes:    100,
		numReplicas:   3,
		storeID:       1,
		accessType:    AccessUnknown,
		requestSource: "internal_failed_write",
	}
	successResp := &TestResponseInfo{readBytes: 0, succeed: true}
	failedResp := &TestResponseInfo{readBytes: 0, succeed: false}

	for i, resp := range []*TestResponseInfo{successResp, failedResp, failedResp, successResp, failedResp} {
		_, _, _, _, err := gc.onRequestWaitImpl(context.Background(), req)
		re.NoError(err, "iteration %d", i)
		_, err = gc.onResponseImpl(req, resp)
		re.NoError(err, "iteration %d", i)
	}

	sourceMetrics, _ := requestSourceStateSnapshot(t, gc, req.requestSource)
	re.NotNil(sourceMetrics)

	gc.mu.Lock()
	controllerWRU := gc.mu.consumption.WRU
	controllerRRU := gc.mu.consumption.RRU
	gc.mu.Unlock()

	re.NotNil(sourceMetrics.wruConsume)
	re.NotNil(sourceMetrics.wruRefund)
	netWRU := requestSourceCounterValue(t, sourceMetrics.wruConsume) -
		requestSourceCounterValue(t, sourceMetrics.wruRefund)
	re.InDelta(controllerWRU, netWRU, 1e-9,
		"per-source WRU diverges from controller consumption — failed-write payback dropped?")
	re.Zero(controllerRRU)
	re.Nil(sourceMetrics.rruConsume)
	re.Nil(sourceMetrics.rruRefund)

	// Sanity: failed writes still consume some WRU (calculateWriteCost adds
	// per-batch and replication terms that payBackWriteCost does not refund),
	// so the counter is non-zero.
	re.Greater(netWRU, float64(0))

	gc.metrics.sourceState.cleanup()
}
