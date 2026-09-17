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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/resource_group/controller/metrics"
)

func gaugeValue(re *require.Assertions, g prometheus.Gauge) float64 {
	var m dto.Metric
	re.NoError(g.Write(&m))
	return m.GetGauge().GetValue()
}

// gatherRUMaxMetrics reads the registry without recreating a deleted label set.
func gatherRUMaxMetrics(t *testing.T, name string) map[string]float64 {
	t.Helper()
	re := require.New(t)
	registry := prometheus.NewRegistry()
	re.NoError(registry.Register(metrics.RUMaxPerSecGauge))
	families, err := registry.Gather()
	re.NoError(err)
	values := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			var groupName, ruType string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "resource_group":
					groupName = label.GetValue()
				case "type":
					ruType = label.GetValue()
				}
			}
			if groupName == name {
				values[ruType] = metric.GetGauge().GetValue()
			}
		}
	}
	return values
}

func TestRUMaxTracker(t *testing.T) {
	type observation struct {
		at       time.Duration
		rru, wru float64
		want     [3]float64
	}
	tests := []struct {
		name         string
		observations []observation
	}{
		{"first sample includes initial consumption", []observation{
			{time.Second, 100, 50, [3]float64{100, 50, 150}},
		}},
		{"partial first interval preserves subsequent actual intervals", []observation{
			{5 * time.Millisecond, 100, 50, [3]float64{100, 50, 150}},
			{505 * time.Millisecond, 200, 100, [3]float64{200, 100, 300}},
			{1505 * time.Millisecond, 500, 250, [3]float64{300, 150, 450}},
		}},
		{"partial first interval expires at its actual timestamp", []observation{
			{5 * time.Millisecond, 100, 50, [3]float64{100, 50, 150}},
			{60005*time.Millisecond - time.Nanosecond, 100, 50, [3]float64{100, 50, 150}},
			{60005 * time.Millisecond, 100, 50, [3]float64{}},
		}},
		{"idle first sample does not extend normalization to later samples", []observation{
			{5 * time.Millisecond, 0, 0, [3]float64{}},
			{505 * time.Millisecond, 100, 50, [3]float64{200, 100, 300}},
		}},
		{"actual interval normalization", []observation{
			{3 * time.Second, 300, 150, [3]float64{100, 50, 150}},
			{3500 * time.Millisecond, 400, 200, [3]float64{200, 100, 300}},
		}},
		{"total uses simultaneous rates", []observation{
			{time.Second, 100, 0, [3]float64{100, 0, 100}},
			{2 * time.Second, 100, 80, [3]float64{100, 80, 100}},
			{3 * time.Second, 170, 150, [3]float64{100, 80, 140}},
		}},
		{"expiry and recovery after a minute", []observation{
			{time.Second, 100, 0, [3]float64{100, 0, 100}},
			{61*time.Second - time.Nanosecond, 100, 0, [3]float64{100, 0, 100}},
			{61 * time.Second, 100, 0, [3]float64{}},
			{62 * time.Second, 110, 0, [3]float64{10, 0, 10}},
			{63 * time.Second, 110, 0, [3]float64{10, 0, 10}},
			{123 * time.Second, 110, 0, [3]float64{}},
		}},
		{"long pause expires old peaks and normalizes new consumption", []observation{
			{time.Second, 100, 0, [3]float64{100, 0, 100}},
			{101 * time.Second, 300, 100, [3]float64{2, 1, 3}},
		}},
		{"negative increments are clamped independently and advance the baseline", []observation{
			{time.Second, 100, 100, [3]float64{100, 100, 200}},
			{61 * time.Second, 100, 100, [3]float64{}},
			{62 * time.Second, 80, 150, [3]float64{0, 50, 50}},
			{63 * time.Second, 85, 140, [3]float64{5, 50, 50}},
			{123 * time.Second, 75, 130, [3]float64{}},
		}},
		{"nonpositive intervals preserve the baseline", []observation{
			{0, 10, 0, [3]float64{}},
			{-time.Second, 20, 0, [3]float64{}},
			{time.Second, 30, 0, [3]float64{30, 0, 30}},
			{time.Second, 100, 0, [3]float64{30, 0, 30}},
			{500 * time.Millisecond, 90, 0, [3]float64{30, 0, 30}},
			{2 * time.Second, 100, 0, [3]float64{70, 0, 70}},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			base := time.Unix(1700000000, 0)
			tracker := newRUMaxTracker(t.Name(), base)
			t.Cleanup(func() { deleteRUMaxMetricLabels(t.Name()) })
			for _, observation := range tc.observations {
				tracker.observe(observation.rru, observation.wru, base.Add(observation.at))
				got := []float64{gaugeValue(re, tracker.rruGauge), gaugeValue(re, tracker.wruGauge), gaugeValue(re, tracker.ruGauge)}
				re.InDeltaSlice(observation.want[:], got, 1e-9, "at %s", observation.at)
			}
		})
	}
}

func TestRUMaxTrackerFrequentSamples(t *testing.T) {
	re := require.New(t)
	base := time.Unix(1700000000, 0)
	tracker := newRUMaxTracker(t.Name(), base)
	t.Cleanup(func() { deleteRUMaxMetricLabels(t.Name()) })
	tracker.observe(100, 0, base.Add(time.Second))
	// Even more than 60 positive samples must not evict a peak before 60s.
	for i := 1; i <= 600; i++ {
		tracker.observe(100+float64(i), 0, base.Add(time.Second+time.Duration(i)*100*time.Millisecond))
		want := 100.0
		if i == 600 {
			want = 10
		}
		re.InDelta(want, gaugeValue(re, tracker.ruGauge), 1e-9, "sample %d", i)
	}
}

func TestRUMaxSamplerTombstoneLifecycle(t *testing.T) {
	for _, path := range []string{"replace and recreate", "default missing", "default invalid"} {
		t.Run(path, func(t *testing.T) {
			re := require.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			provider := newMockResourceGroupProvider()
			c, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID)
			re.NoError(err)
			// Exercise the event handlers synchronously; the loop test covers scheduling.
			c.loopCtx = ctx
			group := &rmpb.ResourceGroup{
				Name: t.Name(), Mode: rmpb.GroupMode_RUMode,
				RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 10000, BurstLimit: -1}}},
			}
			provider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)
			gc, err := c.tryGetResourceGroupController(ctx, group.Name, false)
			re.NoError(err)
			t.Cleanup(func() {
				gc.metrics.deletePagingLabels(group.Name)
				gc.metrics.deletePagingLabels(defaultResourceGroupName)
				c.cleanupRequestSourceMetricsState(group.Name)
				c.cleanupRequestSourceMetricsState(defaultResourceGroupName)
			})
			re.Empty(gatherRUMaxMetrics(t, group.Name), "construction must not create peak series")
			sampler := make(ruMaxSampler)
			t.Cleanup(sampler.clear)
			sampler.sample(c)
			sampler[group.Name].tracker.observe(100, 0, gc.createdAt.Add(sampler[group.Name].tracker.last+time.Second))
			def := *group
			def.Name = defaultResourceGroupName
			if path == "default missing" {
				provider.On("GetResourceGroup", mock.Anything, defaultResourceGroupName, mock.Anything).Return((*rmpb.ResourceGroup)(nil), nil)
			} else {
				provider.On("GetResourceGroup", mock.Anything, defaultResourceGroupName, mock.Anything).Return(&def, nil)
				defaultGC, err := c.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
				re.NoError(err)
				sampler.sample(c)
				sampler[defaultResourceGroupName].tracker.observe(42, 0, defaultGC.createdAt.Add(sampler[defaultResourceGroupName].tracker.last+time.Second))
				if path == "default invalid" {
					def.Mode = rmpb.GroupMode_RawMode
				}
			}
			c.tombstoneGroupCostController(group.Name)
			sampler.sample(c)
			if path != "replace and recreate" {
				_, ok := c.loadGroupController(group.Name)
				re.False(ok)
				re.Empty(gatherRUMaxMetrics(t, group.Name))
				return
			}
			tombstone, ok := c.loadGroupController(group.Name)
			re.True(ok)
			re.True(tombstone.tombstone.Load())
			re.Equal(defaultResourceGroupName, tombstone.name)
			re.Zero(gatherRUMaxMetrics(t, group.Name)[ruTypeTotal])
			re.InDelta(42, gatherRUMaxMetrics(t, defaultResourceGroupName)[ruTypeTotal], 1e-9)

			// A replacement uses its own cumulative baseline and window. Constructing
			// it must not reset the live series before it wins the cache replacement.
			sampler[group.Name].tracker.observe(200, 0, tombstone.createdAt.Add(sampler[group.Name].tracker.last+time.Second))
			revived, err := newGroupCostController(group, c.ruConfig, c.lowTokenNotifyChan, c.tokenBucketUpdateChan, c.getOrCreateRequestSourceMetricsState(group.Name))
			re.NoError(err)
			re.InDelta(200, gatherRUMaxMetrics(t, group.Name)[ruTypeTotal], 1e-9)
			re.True(c.groupsController.CompareAndSwap(group.Name, tombstone, revived))
			sampler.sample(c)
			re.Zero(gatherRUMaxMetrics(t, group.Name)[ruTypeTotal])
			revived.inactive = true
			oldGauge := sampler[group.Name].tracker.ruGauge
			c.cleanUpResourceGroup()
			sampler.sample(c)
			re.Empty(gatherRUMaxMetrics(t, group.Name))

			fresh, err := c.tryGetResourceGroupController(ctx, group.Name, false)
			re.NoError(err)
			sampler.sample(c)
			sampler[group.Name].tracker.observe(7, 3, fresh.createdAt.Add(sampler[group.Name].tracker.last+time.Second))
			// Cached children from deleted controllers must remain detached.
			oldGauge.Set(999)
			got := gatherRUMaxMetrics(t, group.Name)
			re.Len(got, 3)
			re.InDelta(10, got[ruTypeTotal], 1e-9)
		})
	}
}

func TestRUMaxSamplerControllerLoop(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider := newMockResourceGroupProvider()
	provider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).Return([]*rmpb.TokenBucketResponse{}, nil)
	group := &rmpb.ResourceGroup{
		Name: t.Name(), Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000, BurstLimit: -1}}},
	}
	provider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)
	c, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	gc, err := c.tryGetResourceGroupController(ctx, group.Name, false)
	re.NoError(err)
	start := gc.createdAt
	// Complete requests before the first tick to exercise initialization too.
	for _, isWrite := range []bool{true, false} {
		for range 50 {
			req := NewTestRequestInfo(isWrite, 4096, 1, AccessCrossZone)
			_, _, _, _, err := c.OnRequestWait(ctx, group.Name, req)
			re.NoError(err)
			_, err = c.OnResponse(group.Name, req, NewTestResponseInfo(8192, time.Millisecond, true))
			re.NoError(err)
		}
	}
	rru, wru := gc.mu.consumption.RRU, gc.mu.consumption.WRU
	re.Positive(rru)
	re.Positive(wru)
	c.Start(ctx)
	t.Cleanup(func() { re.NoError(c.Stop()) })
	re.Eventually(func() bool {
		return gatherRUMaxMetrics(t, group.Name)[ruTypeTotal] > 0
	}, 5*time.Second, 10*time.Millisecond)
	peak := gatherRUMaxMetrics(t, group.Name)
	// Exercise the actual notification branch, not just its helper functions.
	for range 100 {
		select {
		case c.lowTokenNotifyChan <- notifyMsg{}:
		case <-time.After(5 * time.Second):
			t.Fatal("controller did not process low-token notifications")
		}
	}
	re.Equal(peak, gatherRUMaxMetrics(t, group.Name))
	re.NoError(c.Stop())
	re.Empty(gatherRUMaxMetrics(t, group.Name))
	// The first interval starts at construction. Independently bound its duration
	// by wall time and check the read/write ratio from the actual charged RUs.
	seconds := rru / peak[requestSourceRUTypeRRU]
	re.GreaterOrEqual(seconds, defaultGroupStateUpdateInterval.Seconds())
	re.LessOrEqual(seconds, time.Since(start).Seconds())
	re.InDelta(wru/seconds, peak[requestSourceRUTypeWRU], 1e-6)
	re.InDelta((rru+wru)/seconds, peak[ruTypeTotal], 1e-6)
	t.Logf("charged RRU=%f WRU=%f; interval=%fs; peak=%v", rru, wru, seconds, peak)
}

// Recreate between cache removal and old-controller metric cleanup. The new
// owner must keep exporting even when it reuses the same live Gauge children.
func TestRUMaxSamplerRecreateDuringCleanup(t *testing.T) {
	re := require.New(t)
	c := &ResourceGroupsController{}
	old := createTestGroupCostController(re)
	name := t.Name()
	c.groupsController.Store(name, old)
	sampler := make(ruMaxSampler)
	defer sampler.clear()
	sampler.sample(c)
	sampler[name].tracker.observe(100, 0, old.createdAt.Add(time.Second))
	c.groupsController.Delete(name)
	fresh := createTestGroupCostController(re)
	c.groupsController.Store(name, fresh)
	fresh.mu.consumption.RRU = 7
	fresh.mu.consumption.WRU = 3
	sampler.sample(c)
	old.metrics.deletePagingLabels(name)
	got := gatherRUMaxMetrics(t, name)
	re.Len(got, 3)
	// A replacement starts a new first interval, while retaining live gauges.
	re.Less(sampler[name].tracker.last, defaultGroupStateUpdateInterval)
	re.InDelta(10, got[ruTypeTotal], 1e-9)
	re.Equal(fresh, sampler[name].gc)
	re.Len(sampler[name].tracker.samples, 1)
	c.groupsController.Delete(name)
	sampler.sample(c)
	re.Empty(gatherRUMaxMetrics(t, name))
	re.Empty(sampler)
}

func TestRUMaxSamplerIndependentOfStateUpdates(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	// Simulate a controller created well before the sampler first sees it.
	gc.createdAt = gc.createdAt.Add(-time.Second)
	c := &ResourceGroupsController{}
	c.groupsController.Store(t.Name(), gc)
	sampler := make(ruMaxSampler)
	defer sampler.clear()
	gc.burstable.Store(true)
	req := NewTestRequestInfo(true, 4096, 1, AccessCrossZone)
	_, _, _, _, err := gc.onRequestWaitImpl(context.Background(), req)
	re.NoError(err)
	_, err = gc.onResponseImpl(req, NewTestResponseInfo(0, time.Millisecond, true))
	re.NoError(err)
	for range 100 {
		gc.updateRunState()
		gc.updateAvgRequestResourcePerSec()
	}
	re.Empty(gatherRUMaxMetrics(t, t.Name()))
	minimum := time.Since(gc.createdAt).Seconds()
	sampler.sample(c)
	maximum := time.Since(gc.createdAt).Seconds()
	tracker := &sampler[t.Name()].tracker
	seconds := tracker.last.Seconds()
	re.GreaterOrEqual(seconds, minimum)
	re.LessOrEqual(seconds, maximum)
	re.Positive(gc.mu.consumption.WRU)
	re.InDelta(gc.mu.consumption.WRU/seconds, gaugeValue(re, tracker.wruGauge), 1e-6)
	before := tracker.last
	for range 100 {
		gc.updateRunState()
		gc.updateAvgRequestResourcePerSec()
	}
	re.Equal(before, tracker.last)
}

func TestRUMaxSamplerSlowCollectDoesNotBlockController(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider := newMockResourceGroupProvider()
	provider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).Return([]*rmpb.TokenBucketResponse{}, nil)
	c, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	c.tokenResponseChan = make(chan []*rmpb.TokenBucketResponse)
	// An unbuffered Collect holds the family's read lock while waiting for its
	// remaining children to be drained. New peak children must wait for it.
	seed := newRUMaxTracker(t.Name()+"/seed", time.Now())
	t.Cleanup(func() { deleteRUMaxMetricLabels(t.Name() + "/seed") })
	seed.observe(1, 0, seed.base.Add(time.Second))
	ch := make(chan prometheus.Metric)
	go func() {
		metrics.RUMaxPerSecGauge.Collect(ch)
		close(ch)
	}()
	<-ch
	// Always release Collect before stopping a sampler that could be waiting
	// for it, including when an assertion fails.
	t.Cleanup(func() {
		drained := 0
		for range ch {
			drained++
		}
		re.Positive(drained, "Collect must have held its read lock waiting for more children")
		if c.loopCancel != nil {
			re.NoError(c.Stop())
		}
	})
	group := &rmpb.ResourceGroup{
		Name: t.Name(), Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000, BurstLimit: -1}}},
	}
	provider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)
	created := make(chan error, 1)
	go func() {
		_, err := c.tryGetResourceGroupController(ctx, group.Name, false)
		created <- err
	}()
	select {
	case err := <-created:
		re.NoError(err)
	case <-time.After(5 * time.Second):
		t.Fatal("peak collection blocked controller construction")
	}
	c.Start(ctx)
	// Continue through multiple real ticks while the sampler cannot create
	// children. Unbuffered sends acknowledge that the main loop is running.
	deadline := time.NewTimer(2 * defaultGroupStateUpdateInterval)
	defer deadline.Stop()
	events := time.NewTicker(10 * time.Millisecond)
	defer events.Stop()
	for {
		select {
		case <-deadline.C:
			return
		case <-events.C:
			select {
			case c.tokenResponseChan <- nil:
			case <-time.After(5 * time.Second):
				t.Fatal("peak collection blocked token responses")
			}
		}
	}
}

func TestRUMaxSamplerConcurrentRecreation(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	provider := newMockResourceGroupProvider()
	provider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).Return([]*rmpb.TokenBucketResponse{}, nil)
	group := &rmpb.ResourceGroup{
		Name: t.Name(), Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000, BurstLimit: -1}}},
	}
	provider.On("GetResourceGroup", mock.Anything, group.Name, mock.Anything).Return(group, nil)
	c, err := NewResourceGroupController(ctx, 1, provider, nil, constants.NullKeyspaceID)
	re.NoError(err)
	c.Start(ctx)
	registry := prometheus.NewRegistry()
	re.NoError(registry.Register(metrics.RUMaxPerSecGauge))
	done := make(chan error, 1)
	go func() {
		req := NewTestRequestInfo(true, 64, 1, AccessCrossZone)
		for ctx.Err() == nil {
			if _, _, _, _, err := c.OnRequestWait(ctx, group.Name, req); err != nil {
				if ctx.Err() != nil {
					break
				}
				done <- err
				return
			}
			if _, err := c.OnResponse(group.Name, req, NewTestResponseInfo(64, time.Millisecond, true)); err != nil {
				done <- err
				return
			}
			if _, err := registry.Gather(); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	t.Cleanup(func() {
		cancel()
		re.NoError(<-done)
		re.NoError(c.Stop())
		re.Empty(gatherRUMaxMetrics(t, group.Name))
	})
	// Exercise requests, scraping and sampling across cache deletion/recreation.
	// The last generation remains present long enough to be reconciled.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for range 220 {
		<-ticker.C
		c.groupsController.Delete(group.Name)
		_, err := c.tryGetResourceGroupController(ctx, group.Name, false)
		re.NoError(err)
	}
	re.Eventually(func() bool {
		return gatherRUMaxMetrics(t, group.Name)[ruTypeTotal] > 0
	}, 5*time.Second, 10*time.Millisecond)
}
