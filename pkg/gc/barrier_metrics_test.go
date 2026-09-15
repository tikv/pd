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

package gc

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
)

func TestBarrierMetricsAgeBoundaries(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	cases := []struct {
		age             time.Duration
		metric, warning bool
	}{
		{24*time.Hour - time.Millisecond, false, false},
		{24 * time.Hour, false, false},
		{24*time.Hour + time.Millisecond, true, false},
		{72 * time.Hour, true, false},
		{72*time.Hour + time.Millisecond, true, true},
		{-time.Hour, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.age.String(), func(t *testing.T) {
			m := newBarrierMetrics(func() time.Time { return now }, func() time.Duration { return 72 * time.Hour })
			registry := prometheus.NewRegistry()
			registry.MustRegister(m)
			ts := uint64(now.Add(-tc.age).UnixMilli())<<18 | 123
			barriers := []*endpoint.GCBarrier{endpoint.NewGCBarrier("ticdc", ts, nil), endpoint.NewGCBarrier("gc_worker", ts, nil)}
			warnings := m.observeMetrics(m.generation(), 42, "tenant-a", barriers, nil, now)
			require.Len(t, warnings, boolCount(tc.warning))
			families, err := registry.Gather()
			require.NoError(t, err)
			if !tc.metric {
				require.Empty(t, families)
				return
			}
			require.Len(t, families, 1)
			require.Equal(t, "pd_gc_barrier_timestamp_seconds", families[0].GetName())
			require.Len(t, families[0].GetMetric(), 1)
			metric := families[0].GetMetric()[0]
			require.Equal(t, float64(now.Add(-tc.age).UnixMilli())/1000, metric.GetGauge().GetValue())
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			require.Equal(t, map[string]string{"scope": "keyspace", "keyspace_id": "42", "keyspace_name": "tenant-a", "barrier_id": "ticdc"}, labels)
		})
	}
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestBarrierMetricsNameRefreshPreservesIdentityAndWarnings(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	m := newBarrierMetrics(func() time.Time { return now }, func() time.Duration { return 72 * time.Hour })
	registry := prometheus.NewRegistry()
	registry.MustRegister(m)
	ts := uint64(now.Add(-80*time.Hour).UnixMilli()) << 18
	barrier := endpoint.NewGCBarrier("backup", ts, nil)
	observe := func(name string) []barrierWarning {
		return m.observeMetrics(m.generation(), 42, name, []*endpoint.GCBarrier{barrier}, nil, now)
	}
	require.Len(t, observe("tenant-a"), 1)
	require.Equal(t, map[string]string{"keyspace/42/backup": "tenant-a"}, gatherBarrierMetricNames(t, registry))
	now = now.Add(time.Minute)
	require.Empty(t, observe("tenant-b"), "name refresh must retain warning suppression")
	require.Equal(t, map[string]string{"keyspace/42/backup": "tenant-b"}, gatherBarrierMetricNames(t, registry))
	barrier = endpoint.NewGCBarrier("backup", ts+1, nil)
	m.updateMetrics(barrierMetricScope{keyspaceID: 42}, barrier, now)
	require.Equal(t, map[string]string{"keyspace/42/backup": "tenant-b"}, gatherBarrierMetricNames(t, registry))
	now = now.Add(9 * time.Minute)
	require.Len(t, observe("tenant-c"), 1, "name refresh must not restart the ten-minute interval")
	require.Equal(t, map[string]string{"keyspace/42/backup": "tenant-c"}, gatherBarrierMetricNames(t, registry))
	m.invalidateKeyspaceMetrics(42)
	require.Empty(t, gatherBarrierMetricNames(t, registry), "removal is keyed by ID")
	require.Empty(t, m.entries)
}

func TestBarrierMetricsRenewalExpiryAndRecovery(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	m := newBarrierMetrics(func() time.Time { return now }, func() time.Duration { return 72 * time.Hour })
	registry := prometheus.NewRegistry()
	registry.MustRegister(m)
	oldTS := uint64(now.Add(-80*time.Hour).UnixMilli()) << 18
	barrier := endpoint.NewGCBarrier("backup", oldTS, nil)
	observe := func() []barrierWarning {
		return m.observeMetrics(m.generation(), 42, "tenant-a", []*endpoint.GCBarrier{barrier}, nil, now)
	}
	require.Len(t, observe(), 1)
	now = now.Add(time.Minute)
	expiry := now.Add(time.Hour)
	barrier = endpoint.NewGCBarrier("backup", oldTS+1, &expiry)
	m.updateMetrics(barrierMetricScope{keyspaceID: 42}, barrier, now)
	require.Empty(t, observe())
	now = now.Add(9 * time.Minute)
	require.Len(t, observe(), 1)
	barrier = endpoint.NewGCBarrier("backup", uint64(now.Add(-48*time.Hour).UnixMilli())<<18, nil)
	m.updateMetrics(barrierMetricScope{keyspaceID: 42}, barrier, now)
	require.Empty(t, observe())
	barrier = endpoint.NewGCBarrier("backup", oldTS, &expiry)
	require.Len(t, observe(), 1)
	now = expiry
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1, "expiration remains valid at equality")
	now = now.Add(time.Nanosecond)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
	require.Empty(t, m.entries)
	barrier = endpoint.NewGCBarrier("backup", oldTS, nil)
	m.updateMetrics(barrierMetricScope{keyspaceID: 42}, barrier, now)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families, "set cannot rediscover an expired entry")
	require.Len(t, observe(), 1, "expiry removes warning suppression")
}

func TestBarrierMetricsHealthySetAndGlobalIdentity(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	m := newBarrierMetrics(func() time.Time { return now }, func() time.Duration { return 72 * time.Hour })
	barriers := make([]*endpoint.GCBarrier, 10000)
	for i := range barriers {
		barriers[i] = endpoint.NewGCBarrier(strconv.Itoa(i), uint64(now.UnixMilli())<<18, nil)
	}
	require.Empty(t, m.observeMetrics(m.generation(), 42, "tenant-a", barriers, nil, now))
	require.Empty(t, m.entries)
	global := []*endpoint.GlobalGCBarrier{endpoint.NewGlobalGCBarrier("br", uint64(now.Add(-80*time.Hour).UnixMilli())<<18, nil)}
	require.Len(t, m.observeMetrics(m.generation(), 42, "tenant-a", barriers, global, now), 1)
	require.Empty(t, m.observeMetrics(m.generation(), 43, "tenant-b", nil, global, now))
	registry := prometheus.NewRegistry()
	registry.MustRegister(m)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Len(t, families[0].GetMetric(), 1)
	labels := make(map[string]string)
	for _, label := range families[0].GetMetric()[0].GetLabel() {
		labels[label.GetName()] = label.GetValue()
	}
	require.Equal(t, map[string]string{"scope": "global", "keyspace_id": "", "keyspace_name": "", "barrier_id": "br"}, labels)
}

func TestBarrierMetricsRegistrationAndLiveWarningAge(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	cfg := config.NewConfig()
	require.NoError(t, cfg.Adjust(nil, false))
	options := config.NewPersistOptions(cfg)
	first := NewGCStateManager(endpoint.GCStateProvider{}, cfg.PDServerCfg, nil)
	second := NewGCStateManager(endpoint.GCStateProvider{}, cfg.PDServerCfg, nil)
	first.barrierMetrics.now = func() time.Time { return now }
	second.barrierMetrics.now = func() time.Time { return now }
	age := func() time.Duration { return options.GetPDServerConfig().GCBarrierWarningAge.Duration }
	first.EnableBarrierMetrics(age)
	second.EnableBarrierMetrics(age)
	t.Cleanup(first.DisableBarrierMetrics)
	t.Cleanup(second.DisableBarrierMetrics)
	barriers := []*endpoint.GCBarrier{endpoint.NewGCBarrier("old", uint64(now.Add(-80*time.Hour).UnixMilli())<<18, nil)}
	observe := func(m *GCStateManager, id uint32) []barrierWarning {
		return m.barrierMetrics.observeMetrics(m.barrierMetrics.generation(), id, "tenant", barriers, nil, now)
	}
	first.OnNodeBecomesLeader()
	require.Len(t, observe(first, 42), 1)
	require.Contains(t, gatherBarrierMetrics(t, prometheus.DefaultGatherer), "keyspace/42/old")
	updated := options.GetPDServerConfig().Clone()
	updated.GCBarrierWarningAge = typeutil.NewDuration(96 * time.Hour)
	options.SetPDServerConfig(updated)
	now = now.Add(10 * time.Minute)
	require.Empty(t, observe(first, 42))
	updated = updated.Clone()
	updated.GCBarrierWarningAge = typeutil.NewDuration(72 * time.Hour)
	options.SetPDServerConfig(updated)
	require.Len(t, observe(first, 42), 1, "live age reduction sees reset warning state")
	generation := first.barrierMetrics.generation()
	first.OnNodeBecomesLeader()
	require.Empty(t, gatherBarrierMetrics(t, prometheus.DefaultGatherer))
	require.Empty(t, first.barrierMetrics.observeMetrics(generation, 42, "tenant-a", barriers, nil, now))
	require.Empty(t, gatherBarrierMetrics(t, prometheus.DefaultGatherer), "stale pre-leadership read cannot publish")
	first.OnNodeBecomesFollower() // Previous lease ends, one leadership remains.
	require.Len(t, observe(first, 42), 1)
	require.Contains(t, gatherBarrierMetrics(t, prometheus.DefaultGatherer), "keyspace/42/old")
	second.OnNodeBecomesLeader()
	require.Len(t, observe(second, 43), 1)
	first.DisableBarrierMetrics()
	require.Equal(t, map[string]float64{"keyspace/43/old": 1_999_712_000}, gatherBarrierMetrics(t, prometheus.DefaultGatherer), "old owner cleanup must preserve replacement")
	second.OnNodeBecomesFollower()
	require.Empty(t, gatherBarrierMetrics(t, prometheus.DefaultGatherer))
	require.Nil(t, productionBarrierMetrics.current.Load(), "registry must not retain a stopped manager")
}

func TestBarrierMetricsConcurrentLifecycleAndScrapes(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	m := newBarrierMetrics(func() time.Time { return now }, func() time.Duration { return 72 * time.Hour })
	registry := prometheus.NewRegistry()
	registry.MustRegister(m)
	barriers := []*endpoint.GCBarrier{endpoint.NewGCBarrier("old", uint64(now.Add(-80*time.Hour).UnixMilli())<<18, nil)}
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for range 100 {
				switch i {
				case 0:
					m.observeMetrics(m.generation(), 42, "tenant-a", barriers, nil, now)
				case 1:
					m.updateMetrics(barrierMetricScope{keyspaceID: 42}, barriers[0], now)
					m.deleteMetrics(barrierMetricScope{keyspaceID: 42}, "old")
				case 2:
					m.invalidateKeyspaceMetrics(42)
					m.clearMetrics()
				case 3:
					_, err := registry.Gather()
					assert.NoError(t, err)
				}
			}
		})
	}
	wg.Wait()
	m.clearMetrics()
	require.Empty(t, gatherBarrierMetrics(t, registry))
}

func TestBarrierMetricsManagerDefaultWarningAge(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	m := NewGCStateManager(endpoint.GCStateProvider{}, config.PDServerConfig{}, nil)
	barriers := []*endpoint.GCBarrier{endpoint.NewGCBarrier("old", uint64(now.Add(-48*time.Hour).UnixMilli())<<18, nil)}
	require.Empty(t, m.barrierMetrics.observeMetrics(m.barrierMetrics.generation(), 42, "tenant-a", barriers, nil, now), "zero constructor config uses the 72-hour default")
}

func TestBarrierMetricsScrapeUnlocksBeforeSending(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	m := newBarrierMetrics(func() time.Time { return now }, func() time.Duration { return 72 * time.Hour })
	ts := uint64(now.Add(-80*time.Hour).UnixMilli()) << 18
	m.observeMetrics(m.generation(), 42, "tenant-a", []*endpoint.GCBarrier{endpoint.NewGCBarrier("one", ts, nil), endpoint.NewGCBarrier("two", ts, nil)}, nil, now)
	samples := make(chan prometheus.Metric)
	done := make(chan struct{})
	go func() { m.Collect(samples); close(done) }()
	<-samples // Collect is now sending its snapshot, with another send pending.
	m.deleteMetrics(barrierMetricScope{keyspaceID: 42}, "one")
	m.invalidateKeyspaceMetrics(42)
	<-samples
	<-done
	registry := prometheus.NewRegistry()
	registry.MustRegister(m)
	require.Empty(t, gatherBarrierMetrics(t, registry))
}

func BenchmarkBarrierMetricsHealthy(b *testing.B) {
	now := time.Unix(2_000_000_000, 0)
	m := newBarrierMetrics(func() time.Time { return now }, func() time.Duration { return 72 * time.Hour })
	barriers := make([]*endpoint.GCBarrier, 10000)
	for i := range barriers {
		barriers[i] = endpoint.NewGCBarrier(strconv.Itoa(i), uint64(now.UnixMilli())<<18, nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		m.observeMetrics(m.generation(), 42, "tenant-a", barriers, nil, now)
	}
}
