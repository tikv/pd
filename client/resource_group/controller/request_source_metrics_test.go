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

	gc.metrics.sourceMetricsMu.RLock()
	sourceMetrics := gc.metrics.sourceMetrics[req.requestSource]
	cacheSize := len(gc.metrics.sourceMetrics)
	gc.metrics.sourceMetricsMu.RUnlock()

	re.Equal(1, cacheSize)
	re.NotNil(sourceMetrics)
	re.Equal(reqConsumption.WRU, counterValue(t, sourceMetrics.wru))
	re.Equal(respConsumption.RRU, counterValue(t, sourceMetrics.rru))
	re.Equal(beforeCount+2, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))

	_, _, _, _, err = gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	gc.metrics.sourceMetricsMu.RLock()
	re.Equal(1, len(gc.metrics.sourceMetrics))
	re.Same(sourceMetrics, gc.metrics.sourceMetrics[req.requestSource])
	gc.metrics.sourceMetricsMu.RUnlock()

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
	gc.metrics.sourceMetricsMu.RLock()
	re.Empty(gc.metrics.sourceMetrics)
	gc.metrics.sourceMetricsMu.RUnlock()
	re.Equal(beforeCount, collectorMetricCount(controllerMetrics.RequestSourceRUCounter))
}
