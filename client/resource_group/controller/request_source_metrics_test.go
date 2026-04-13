package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	controllerMetrics "github.com/tikv/pd/client/resource_group/controller/metrics"
)

func counterValue(t *testing.T, metric prometheus.Counter) float64 {
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

func requestSourceStateSnapshot(t *testing.T, gc *groupCostController, requestSource string) (*requestSourceMetrics, int) {
	t.Helper()
	require.NotNil(t, gc.metrics.sourceState)

	gc.metrics.sourceState.mu.RLock()
	defer gc.metrics.sourceState.mu.RUnlock()

	return gc.metrics.sourceState.items[requestSource], len(gc.metrics.sourceState.items)
}

func TestRequestSourceMetricsCachedByResourceGroup(t *testing.T) {
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
	respConsumption, err := gc.onResponseImpl(req, resp)
	re.NoError(err)
	re.NotZero(reqConsumption.WRU)
	re.NotZero(respConsumption.RRU)

	sourceMetrics, cacheSize := requestSourceStateSnapshot(t, gc, req.requestSource)

	re.Equal(1, cacheSize)
	re.NotNil(sourceMetrics)
	re.Equal(reqConsumption.WRU, counterValue(t, sourceMetrics.wru))
	re.Equal(respConsumption.RRU, counterValue(t, sourceMetrics.rru))
	re.Equal(beforeCount+2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	cachedMetrics, cacheSize := requestSourceStateSnapshot(t, gc, req.requestSource)
	re.Equal(1, cacheSize)
	re.Same(sourceMetrics, cachedMetrics)

	controllerMetrics.RequestSourceRUCounter.DeleteLabelValues(gc.name, req.requestSource, "rru")
	controllerMetrics.RequestSourceRUCounter.DeleteLabelValues(gc.name, req.requestSource, "wru")
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
	re.Equal(1, cacheSize)
	re.Equal(beforeCount+2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	gc.mu.Lock()
	*gc.run.consumption = *gc.mu.consumption
	gc.mu.Unlock()
	gc.inactive = true

	controller.cleanUpResourceGroup()

	_, ok := controller.loadGroupController(group.Name)
	re.False(ok)
	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	sourceMetrics.rru.Add(1)
	sourceMetrics.wru.Add(1)

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

	gc.metrics.addRequestSourceRU(req.requestSource, &rmpb.Consumption{RRU: 1, WRU: 1})

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
