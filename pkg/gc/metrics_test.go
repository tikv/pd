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
	"context"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server/config"
)

func gatherBarrierMetrics(t testing.TB, registry prometheus.Gatherer) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	result := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != "pd_gc_barrier_timestamp_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			result[labels["scope"]+"/"+labels["keyspace_id"]+"/"+labels["barrier_id"]] = metric.GetGauge().GetValue()
		}
	}
	return result
}

// gatherBarrierMetricNames checks the exported label contract and rejects duplicate
// identities even when different names would otherwise hide them in a map.
func gatherBarrierMetricNames(t testing.TB, registry prometheus.Gatherer) map[string]string {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	result := make(map[string]string)
	for _, family := range families {
		if family.GetName() != "pd_gc_barrier_timestamp_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			require.Len(t, labels, 4)
			require.Contains(t, labels, "keyspace_name")
			identity := labels["scope"] + "/" + labels["keyspace_id"] + "/" + labels["barrier_id"]
			require.NotContains(t, result, identity, "name changes must replace the same barrier")
			result[identity] = labels["keyspace_name"]
		}
	}
	return result
}

// The wrapper keeps real etcd reads/transactions and controls only the commit
// boundary, allowing assertions about failure publication and added storage IO.
type barrierMetricsTestKV struct {
	kv.Base
	fail          bool
	beforeCommit  func()
	loads, ranges int
}

func (s *barrierMetricsTestKV) Load(key string) (string, error) { s.loads++; return s.Base.Load(key) }
func (s *barrierMetricsTestKV) LoadRange(key, end string, limit int) (keys, values []string, err error) {
	s.ranges++
	return s.Base.LoadRange(key, end, limit)
}
func (s *barrierMetricsTestKV) CreateRawTxn() kv.RawTxn {
	return &barrierMetricsTestTxn{RawTxn: s.Base.(kv.RawTxnCapable).CreateRawTxn(), store: s}
}

type barrierMetricsTestTxn struct {
	kv.RawTxn
	store *barrierMetricsTestKV
}

func (t *barrierMetricsTestTxn) If(conditions ...kv.RawTxnCondition) kv.RawTxn {
	t.RawTxn = t.RawTxn.If(conditions...)
	return t
}
func (t *barrierMetricsTestTxn) Then(ops ...kv.RawTxnOp) kv.RawTxn {
	t.RawTxn = t.RawTxn.Then(ops...)
	return t
}
func (t *barrierMetricsTestTxn) Commit() (kv.RawTxnResponse, error) {
	if t.store.beforeCommit != nil {
		t.store.beforeCommit()
	}
	if t.store.fail {
		return kv.RawTxnResponse{Succeeded: false}, nil
	}
	return t.RawTxn.Commit()
}

func TestBarrierMetricsAgeBoundaries(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	cases := []struct {
		age             time.Duration
		metric, warning bool
	}{
		{24*time.Hour - time.Millisecond, false, false},
		{24 * time.Hour, false, false},
		{24*time.Hour + time.Millisecond, true, true},
		{48 * time.Hour, true, true},
		{72*time.Hour + time.Millisecond, true, true},
		{-time.Hour, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.age.String(), func(t *testing.T) {
			m := newBarrierMetrics(func() time.Time { return now })
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
	m := newBarrierMetrics(func() time.Time { return now })
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
	m := newBarrierMetrics(func() time.Time { return now })
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
	barrier = endpoint.NewGCBarrier("backup", uint64(now.Add(-12*time.Hour).UnixMilli())<<18, nil)
	m.updateMetrics(barrierMetricScope{keyspaceID: 42}, barrier, now)
	require.Empty(t, observe())
	require.Empty(t, gatherBarrierMetrics(t, registry), "a recovered barrier clears both metrics and warning suppression")
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
	m := newBarrierMetrics(func() time.Time { return now })
	barriers := make([]*endpoint.GCBarrier, 10000)
	for i := range barriers {
		barriers[i] = endpoint.NewGCBarrier(strconv.Itoa(i), uint64(now.UnixMilli())<<18, nil)
	}
	require.Empty(t, m.observeMetrics(m.generation(), 42, "tenant-a", barriers, nil, now))
	require.Empty(t, m.entries)
	global := []*endpoint.GlobalGCBarrier{endpoint.NewGlobalGCBarrier("br", uint64(now.Add(-48*time.Hour).UnixMilli())<<18, nil)}
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

func TestBarrierMetricsRegistrationAndLeadership(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	cfg := config.NewConfig()
	require.NoError(t, cfg.Adjust(nil, false))
	first := NewGCStateManager(endpoint.GCStateProvider{}, cfg.PDServerCfg, nil)
	second := NewGCStateManager(endpoint.GCStateProvider{}, cfg.PDServerCfg, nil)
	first.barrierMetrics.now = func() time.Time { return now }
	second.barrierMetrics.now = func() time.Time { return now }
	t.Cleanup(first.CloseBarrierMetrics)
	t.Cleanup(second.CloseBarrierMetrics)
	barriers := []*endpoint.GCBarrier{endpoint.NewGCBarrier("old", uint64(now.Add(-80*time.Hour).UnixMilli())<<18, nil)}
	observe := func(m *GCStateManager, id uint32) []barrierWarning {
		return m.barrierMetrics.observeMetrics(m.barrierMetrics.generation(), id, "tenant", barriers, nil, now)
	}
	stopFirst := first.OnNodeBecomesLeader()
	require.Len(t, observe(first, 42), 1)
	require.Contains(t, gatherBarrierMetrics(t, prometheus.DefaultGatherer), "keyspace/42/old")
	generation := first.barrierMetrics.generation()
	stopReplacement := first.OnNodeBecomesLeader()
	defer stopReplacement()
	require.Empty(t, gatherBarrierMetrics(t, prometheus.DefaultGatherer))
	require.Empty(t, first.barrierMetrics.observeMetrics(generation, 42, "tenant-a", barriers, nil, now))
	require.Empty(t, gatherBarrierMetrics(t, prometheus.DefaultGatherer), "stale pre-leadership read cannot publish")
	stopFirst() // Previous lease ends, the replacement leadership remains.
	require.Len(t, observe(first, 42), 1)
	require.Contains(t, gatherBarrierMetrics(t, prometheus.DefaultGatherer), "keyspace/42/old")
	stopSecond := second.OnNodeBecomesLeader()
	require.Len(t, observe(second, 43), 1)
	first.CloseBarrierMetrics()
	require.Equal(t, map[string]float64{"keyspace/43/old": 1_999_712_000}, gatherBarrierMetrics(t, prometheus.DefaultGatherer), "old owner cleanup must preserve replacement")
	stopSecond()
	require.Empty(t, gatherBarrierMetrics(t, prometheus.DefaultGatherer))
	require.Nil(t, productionBarrierMetrics.current.Load(), "registry must not retain a stopped manager")
}

func TestBarrierMetricsConcurrentLifecycleAndScrapes(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	m := newBarrierMetrics(func() time.Time { return now })
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

func TestBarrierMetricsManagerFixedWarningAge(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	m := NewGCStateManager(endpoint.GCStateProvider{}, config.PDServerConfig{}, nil)
	barriers := []*endpoint.GCBarrier{endpoint.NewGCBarrier("old", uint64(now.Add(-48*time.Hour).UnixMilli())<<18, nil)}
	require.Len(t, m.barrierMetrics.observeMetrics(m.barrierMetrics.generation(), 42, "tenant-a", barriers, nil, now), 1, "barriers older than 24 hours must warn without configuration")
}

func TestBarrierMetricsScrapeUnlocksBeforeSending(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	m := newBarrierMetrics(func() time.Time { return now })
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
	m := newBarrierMetrics(func() time.Time { return now })
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

func (s *gcStateManagerTestSuite) TestBarrierMetricsKeyspaceNames() {
	now := time.Unix(2_000_000_000, 0)
	m := s.manager
	m.barrierMetrics.now = func() time.Time { return now }
	registry := prometheus.NewRegistry()
	registry.MustRegister(m.barrierMetrics)
	ts := uint64(now.Add(-80*time.Hour).UnixMilli()) << 18
	_, err := m.SetGlobalGCBarrier(context.Background(), "global", ts, time.Duration(math.MaxInt64), now)
	s.Require().NoError(err)
	for _, tc := range []struct {
		name         string
		keyspaceID   uint32
		keyspaceName string
		compat       bool
	}{
		{"direct-keyspace", 2, "ks2", false},
		{"direct-null", constant.NullKeyspaceID, "", false},
		{"compat-keyspace", 2, "ks2", true},
		{"compat-null", constant.NullKeyspaceID, "", true},
	} {
		s.Run(tc.name, func() {
			re := s.Require()
			_, err := m.SetGCBarrier(tc.keyspaceID, "named", ts, time.Hour, now)
			re.NoError(err)
			if tc.compat {
				_, _, err = m.CompatibleUpdateServiceGCSafePoint(tc.keyspaceID, "gc_worker", ts, math.MaxInt64, now)
			} else {
				_, err = m.AdvanceTxnSafePoint(tc.keyspaceID, ts, now)
			}
			re.NoError(err)
			identity := "keyspace/" + strconv.FormatUint(uint64(tc.keyspaceID), 10) + "/named"
			expected := map[string]string{identity: tc.keyspaceName, "global//global": ""}
			re.Equal(expected, gatherBarrierMetricNames(s.T(), registry))
			_, err = m.SetGCBarrier(tc.keyspaceID, "named", ts+1, time.Hour, now)
			re.NoError(err)
			re.Equal(expected, gatherBarrierMetricNames(s.T(), registry), "set preserves observed name")
			_, err = m.DeleteGCBarrier(tc.keyspaceID, "named")
			re.NoError(err)
			re.Equal(map[string]string{"global//global": ""}, gatherBarrierMetricNames(s.T(), registry))
			_, err = m.SetGCBarrier(tc.keyspaceID, "named", ts, time.Hour, now)
			re.NoError(err)
			_, err = m.AdvanceTxnSafePoint(tc.keyspaceID, ts, now)
			re.NoError(err)
			re.Equal(expected, gatherBarrierMetricNames(s.T(), registry))
			now = now.Add(time.Hour + time.Second)
			re.Equal(map[string]string{"global//global": ""}, gatherBarrierMetricNames(s.T(), registry), "expiry removes named sample")
		})
	}
	_, err = m.DeleteGlobalGCBarrier(context.Background(), "global")
	s.Require().NoError(err)
	s.Require().Empty(gatherBarrierMetricNames(s.T(), registry))
}

func (s *gcStateManagerTestSuite) TestBarrierMetricsAdvancementAndWrites() {
	re := s.Require()
	now := time.Unix(2_000_000_000, 0)
	s.manager.barrierMetrics.now = func() time.Time { return now }
	registry := prometheus.NewRegistry()
	registry.MustRegister(s.manager.barrierMetrics)
	ts := func(age time.Duration) uint64 { return uint64(now.Add(-age).UnixMilli()) << 18 }
	globalTS, oldTS, newerTS := ts(100*time.Hour), ts(90*time.Hour), ts(80*time.Hour)
	_, err := s.manager.SetGlobalGCBarrier(context.Background(), "global", globalTS, time.Duration(math.MaxInt64), now)
	re.NoError(err)
	for id, stamp := range map[string]uint64{"oldest": oldTS, "hidden": newerTS} {
		_, err = s.manager.SetGCBarrier(2, id, stamp, time.Duration(math.MaxInt64), now)
		re.NoError(err)
	}
	re.Empty(gatherBarrierMetrics(s.T(), registry), "set must not discover barriers")
	counted := &barrierMetricsTestKV{Base: s.storage.Base}
	s.manager.gcMetaStorage = endpoint.NewStorageEndpoint(counted, nil).GetGCStateProvider()
	result, err := s.manager.AdvanceTxnSafePoint(2, ts(0), now)
	re.NoError(err)
	re.Equal(globalTS, result.NewTxnSafePoint)
	expected := map[string]float64{"global//global": float64(globalTS>>18) / 1000, "keyspace/2/oldest": float64(oldTS>>18) / 1000, "keyspace/2/hidden": float64(newerTS>>18) / 1000}
	re.Equal(expected, gatherBarrierMetrics(s.T(), registry))
	re.Equal(3, counted.loads, "revision and two safe points only")
	re.Equal(3, counted.ranges, "local barriers, min start TS and global barriers only")
	_, err = s.manager.AdvanceTxnSafePoint(2, globalTS, now)
	re.NoError(err)
	re.Equal(expected, gatherBarrierMetrics(s.T(), registry), "unchanged target still observes all barriers")
	// A min start TS hides every barrier without suppressing observation.
	re.NoError(s.storage.Save(keypath.CompatibleTiDBMinStartTSPrefix(2)+"tidb", strconv.FormatUint(ts(110*time.Hour), 10)))
	_, err = s.manager.AdvanceTxnSafePoint(2, ts(0), now)
	re.NoError(err)
	re.Equal(expected, gatherBarrierMetrics(s.T(), registry))
	counted.fail = true
	_, err = s.manager.SetGCBarrier(2, "hidden", ts(time.Hour), time.Duration(math.MaxInt64), now)
	re.Error(err)
	_, err = s.manager.DeleteGCBarrier(2, "oldest")
	re.Error(err)
	_, err = s.manager.DeleteGlobalGCBarrier(context.Background(), "global")
	re.Error(err)
	_, err = s.manager.SetGlobalGCBarrier(context.Background(), "global", ts(time.Hour), time.Duration(math.MaxInt64), now)
	re.Error(err)
	re.Equal(expected, gatherBarrierMetrics(s.T(), registry))
	// Storage changes visible to a failed advancement must not replace cached data.
	re.NoError(s.provider.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error { return wb.DeleteGCBarrier(2, "hidden") }))
	_, err = s.manager.AdvanceTxnSafePoint(2, ts(0), now)
	re.Error(err)
	re.Equal(expected, gatherBarrierMetrics(s.T(), registry))
	counted.fail = false
	_, err = s.manager.AdvanceTxnSafePoint(2, ts(0), now)
	re.NoError(err)
	delete(expected, "keyspace/2/hidden")
	re.Equal(expected, gatherBarrierMetrics(s.T(), registry))
	_, err = s.manager.SetGCBarrier(2, "oldest", ts(time.Hour), time.Duration(math.MaxInt64), now)
	re.NoError(err)
	delete(expected, "keyspace/2/oldest")
	re.Equal(expected, gatherBarrierMetrics(s.T(), registry))
	_, err = s.manager.DeleteGlobalGCBarrier(context.Background(), "global")
	re.NoError(err)
	re.Empty(gatherBarrierMetrics(s.T(), registry))
	// Compatibility paths share the successful set/delete/advance hooks.
	_, _, err = s.manager.CompatibleUpdateServiceGCSafePoint(constant.NullKeyspaceID, "native_br", newerTS, 3600, now)
	re.NoError(err)
	_, _, err = s.manager.CompatibleUpdateServiceGCSafePoint(2, "compat", newerTS, 3600, now)
	re.NoError(err)
	_, _, err = s.manager.CompatibleUpdateServiceGCSafePoint(2, "gc_worker", ts(0), math.MaxInt64, now)
	re.NoError(err)
	re.Len(gatherBarrierMetrics(s.T(), registry), 2)
	_, _, err = s.manager.CompatibleUpdateServiceGCSafePoint(2, "compat", ts(time.Hour), 3600, now)
	re.NoError(err)
	re.Len(gatherBarrierMetrics(s.T(), registry), 1)
	_, _, err = s.manager.CompatibleUpdateServiceGCSafePoint(constant.NullKeyspaceID, "native_br", newerTS, 0, now)
	re.NoError(err)
	re.Empty(gatherBarrierMetrics(s.T(), registry))
}

func (s *gcStateManagerTestSuite) TestBarrierMetricsDisabledKeyspace() {
	re := s.Require()
	now := time.Unix(2_000_000_000, 0)
	m := s.manager
	m.barrierMetrics.now = func() time.Time { return now }
	registry := prometheus.NewRegistry()
	registry.MustRegister(m.barrierMetrics)
	ts := uint64(now.Add(-80*time.Hour).UnixMilli()) << 18
	_, err := m.keyspaceManager.UpdateKeyspaceStateByID(2, keyspacepb.KeyspaceState_DISABLED, now.Unix())
	re.NoError(err)
	_, err = m.SetGCBarrier(2, "old", ts, time.Duration(math.MaxInt64), now)
	re.NoError(err)
	_, err = m.SetGlobalGCBarrier(context.Background(), "global", ts, time.Duration(math.MaxInt64), now)
	re.NoError(err)
	re.Empty(gatherBarrierMetrics(s.T(), registry))
	core, logs := observer.New(zapcore.WarnLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	counted := &barrierMetricsTestKV{Base: s.storage.Base}
	m.gcMetaStorage = endpoint.NewStorageEndpoint(counted, nil).GetGCStateProvider()
	// State changes after metadata resolution, including another keyspace's,
	// must not fence an otherwise successful observation.
	counted.beforeCommit = func() {
		_, err := m.keyspaceManager.UpdateKeyspaceStateByID(2, keyspacepb.KeyspaceState_ENABLED, now.Unix())
		re.NoError(err)
		_, err = m.keyspaceManager.UpdateKeyspaceStateByID(2, keyspacepb.KeyspaceState_DISABLED, now.Unix())
		re.NoError(err)
		_, err = m.keyspaceManager.UpdateKeyspaceStateByID(3, keyspacepb.KeyspaceState_DISABLED, now.Unix())
		re.NoError(err)
	}
	_, err = m.AdvanceTxnSafePoint(2, ts, now)
	re.NoError(err)
	re.Equal(map[string]float64{"keyspace/2/old": 1_999_712_000, "global//global": 1_999_712_000}, gatherBarrierMetrics(s.T(), registry))
	re.Len(logs.FilterMessage("GC barrier timestamp is too old").All(), 2)
	counted.beforeCommit = nil
	// Compatibility gc_worker advancement must discover the same disabled scope.
	m.OnNodeBecomesLeader()
	_, _, err = m.CompatibleUpdateServiceGCSafePoint(2, "gc_worker", ts, math.MaxInt64, now)
	re.NoError(err)
	re.Equal(map[string]float64{"keyspace/2/old": 1_999_712_000, "global//global": 1_999_712_000}, gatherBarrierMetrics(s.T(), registry))
	re.Len(logs.FilterMessage("GC barrier timestamp is too old").All(), 4)
}

func (s *gcStateManagerTestSuite) TestBarrierMetricsWarningCommitAndTTL() {
	re := s.Require()
	now := time.Unix(2_000_000_000, 0)
	m := s.manager
	m.barrierMetrics.now = func() time.Time { return now }
	registry := prometheus.NewRegistry()
	registry.MustRegister(m.barrierMetrics)
	ts := uint64(now.Add(-80*time.Hour).Add(-time.Millisecond).UnixMilli())<<18 | 123
	_, err := m.SetGCBarrier(2, "ttl", ts, time.Hour, now)
	re.NoError(err)
	_, err = m.SetGlobalGCBarrier(context.Background(), "global", ts, time.Duration(math.MaxInt64), now)
	re.NoError(err)
	core, logs := observer.New(zapcore.WarnLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	counted := &barrierMetricsTestKV{Base: s.storage.Base, fail: true}
	m.gcMetaStorage = endpoint.NewStorageEndpoint(counted, nil).GetGCStateProvider()
	_, err = m.AdvanceTxnSafePoint(2, ts, now)
	re.Error(err)
	re.Empty(logs.FilterMessage("GC barrier timestamp is too old").All())
	re.Empty(gatherBarrierMetrics(s.T(), registry))
	counted.fail = false
	_, err = m.AdvanceTxnSafePoint(2, ts, now)
	re.NoError(err)
	warnings := logs.FilterMessage("GC barrier timestamp is too old").All()
	re.Len(warnings, 2)
	for _, warning := range warnings {
		fields := warning.ContextMap()
		re.Equal(ts, fields["barrier-ts"])
		re.Equal(now.Add(-80*time.Hour).Add(-time.Millisecond), fields["barrier-time"])
		re.Equal(80*time.Hour+time.Millisecond, fields["lag"])
		if fields["scope"] == "global" {
			re.Empty(fields["keyspace-id"])
			re.Equal("never", fields["expiration-time"])
		} else {
			re.Equal("2", fields["keyspace-id"])
			re.Equal(now.Add(time.Hour).UTC().Format(time.RFC3339Nano), fields["expiration-time"])
		}
	}
	_, err = m.AdvanceTxnSafePoint(constant.NullKeyspaceID, ts, now)
	re.NoError(err)
	re.Len(logs.FilterMessage("GC barrier timestamp is too old").All(), 2, "global warning limiter is independent of request keyspace")
	now = now.Add(time.Minute)
	_, err = m.SetGCBarrier(2, "ttl", ts+1, time.Hour, now)
	re.NoError(err)
	_, err = m.AdvanceTxnSafePoint(2, ts, now)
	re.NoError(err)
	re.Len(logs.FilterMessage("GC barrier timestamp is too old").All(), 2, "old TS renewal preserves suppression")
	_, err = m.DeleteGlobalGCBarrier(context.Background(), "global")
	re.NoError(err)
	now = now.Add(time.Hour)
	re.Len(gatherBarrierMetrics(s.T(), registry), 1, "equality at rounded expiry remains valid")
	now = now.Add(time.Nanosecond)
	beforeLoads, beforeRanges := counted.loads, counted.ranges
	re.Empty(gatherBarrierMetrics(s.T(), registry))
	re.Equal(beforeLoads, counted.loads)
	re.Equal(beforeRanges, counted.ranges)
	// Successful delete and young global update remove already reported samples.
	_, err = m.SetGCBarrier(2, "delete", ts, time.Hour, now)
	re.NoError(err)
	_, err = m.SetGlobalGCBarrier(context.Background(), "global", ts, time.Hour, now)
	re.NoError(err)
	_, err = m.AdvanceTxnSafePoint(2, ts, now)
	re.NoError(err)
	re.Len(gatherBarrierMetrics(s.T(), registry), 2)
	_, err = m.DeleteGCBarrier(2, "delete")
	re.NoError(err)
	_, err = m.SetGlobalGCBarrier(context.Background(), "global", uint64(now.UnixMilli())<<18, time.Hour, now)
	re.NoError(err)
	re.Empty(gatherBarrierMetrics(s.T(), registry))
}

func (s *gcStateManagerTestSuite) TestBarrierMetricsForceDeleteServiceGCSafePoint() {
	re := s.Require()
	now := time.Unix(2_000_000_000, 0)
	m := s.manager
	m.barrierMetrics.now = func() time.Time { return now }
	registry := prometheus.NewRegistry()
	registry.MustRegister(m.barrierMetrics)
	ts := uint64(now.Add(-80*time.Hour).UnixMilli()) << 18
	core, logs := observer.New(zapcore.WarnLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	_, err := m.SetGCBarrier(constant.NullKeyspaceID, "force-delete", ts, time.Duration(math.MaxInt64), now)
	re.NoError(err)
	_, err = m.AdvanceTxnSafePoint(constant.NullKeyspaceID, ts, now)
	re.NoError(err)
	expected := map[string]float64{"keyspace/4294967295/force-delete": 1_999_712_000}
	re.Equal(expected, gatherBarrierMetrics(s.T(), registry))
	re.Len(logs.FilterMessage("GC barrier timestamp is too old").All(), 1)

	counted := &barrierMetricsTestKV{Base: s.storage.Base, fail: true}
	m.gcMetaStorage = endpoint.NewStorageEndpoint(counted, nil).GetGCStateProvider()
	re.Error(m.ForceDeleteServiceGCSafePoint("force-delete"))
	re.Equal(expected, gatherBarrierMetrics(s.T(), registry), "failed commit must retain the observation")
	counted.fail = false
	_, err = m.AdvanceTxnSafePoint(constant.NullKeyspaceID, ts, now)
	re.NoError(err)
	re.Len(logs.FilterMessage("GC barrier timestamp is too old").All(), 1, "failed deletion must retain warning suppression")

	beforeLoads, beforeRanges := counted.loads, counted.ranges
	re.NoError(m.ForceDeleteServiceGCSafePoint("force-delete"))
	re.Empty(gatherBarrierMetrics(s.T(), registry))
	re.Equal(beforeLoads+1, counted.loads, "force deletion only reads the transaction revision")
	re.Equal(beforeRanges, counted.ranges)
	re.NoError(m.ForceDeleteServiceGCSafePoint("force-delete"), "deleting an absent service is idempotent")
	re.Nil(s.getGCBarrier(constant.NullKeyspaceID, "force-delete"))

	_, err = m.SetGCBarrier(constant.NullKeyspaceID, "force-delete", ts, time.Duration(math.MaxInt64), now)
	re.NoError(err)
	re.Empty(gatherBarrierMetrics(s.T(), registry), "recreation alone must not discover the barrier")
	_, err = m.AdvanceTxnSafePoint(constant.NullKeyspaceID, ts, now)
	re.NoError(err)
	re.Equal(expected, gatherBarrierMetrics(s.T(), registry))
	re.Len(logs.FilterMessage("GC barrier timestamp is too old").All(), 2, "successful deletion must reset warning suppression")

	// Cleanup must also succeed when storage no longer contains an observed barrier.
	re.NoError(s.provider.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		return wb.DeleteGCBarrier(constant.NullKeyspaceID, "force-delete")
	}))
	re.NoError(m.ForceDeleteServiceGCSafePoint("force-delete"))
	re.Empty(gatherBarrierMetrics(s.T(), registry))
}

func (s *gcStateManagerTestSuite) TestForceDeleteServiceGCSafePointCompatibility() {
	re := s.Require()
	const barrierID = keypath.GCWorkerServiceSafePointID
	barrier := endpoint.NewGCBarrier(barrierID, 100, nil)
	re.NoError(s.provider.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		return wb.SetGCBarrier(constant.NullKeyspaceID, barrier)
	}))
	_, err := s.manager.DeleteGCBarrier(constant.NullKeyspaceID, barrierID)
	re.ErrorIs(err, errs.ErrReservedGCBarrierID)
	stored, err := s.provider.LoadGCBarrier(constant.NullKeyspaceID, barrierID)
	re.NoError(err)
	re.Equal(barrier, stored)
	_, err = s.manager.DeleteGCBarrier(constant.NullKeyspaceID, "")
	re.ErrorIs(err, errs.ErrInvalidArgument)
	re.NoError(s.manager.ForceDeleteServiceGCSafePoint(barrierID))
	stored, err = s.provider.LoadGCBarrier(constant.NullKeyspaceID, barrierID)
	re.NoError(err)
	re.Nil(stored)
	re.NoError(s.manager.ForceDeleteServiceGCSafePoint(barrierID))
}

func (s *gcStateManagerTestSuite) TestBarrierMetricsStateUpdatesPreserveObservations() {
	now := time.Unix(2_000_000_000, 0)
	m := s.manager
	m.barrierMetrics.now = func() time.Time { return now }
	registry := prometheus.NewRegistry()
	registry.MustRegister(m.barrierMetrics)
	ts := uint64(now.Add(-80*time.Hour).UnixMilli()) << 18
	_, err := m.SetGCBarrier(2, "old", ts, time.Duration(math.MaxInt64), now)
	s.Require().NoError(err)
	for _, byID := range []bool{true, false} {
		name := "by-name"
		if byID {
			name = "by-id"
		}
		s.Run(name, func() {
			t := s.T()
			re := require.New(t)
			_, err := m.keyspaceManager.UpdateKeyspaceStateByID(2, keyspacepb.KeyspaceState_ENABLED, now.Unix())
			re.NoError(err)
			m.OnNodeBecomesLeader()
			core, logs := observer.New(zapcore.WarnLevel)
			restore := log.ReplaceGlobals(zap.New(core), nil)
			defer restore()
			_, err = m.AdvanceTxnSafePoint(2, ts, now)
			re.NoError(err)
			expected := gatherBarrierMetrics(t, registry)
			re.Len(expected, 1)
			for _, state := range []keyspacepb.KeyspaceState{
				keyspacepb.KeyspaceState_ENABLED,
				keyspacepb.KeyspaceState_DISABLED,
				keyspacepb.KeyspaceState_DISABLED,
				keyspacepb.KeyspaceState_ENABLED,
			} {
				if byID {
					_, err = m.keyspaceManager.UpdateKeyspaceStateByID(2, state, now.Unix())
				} else {
					_, err = m.keyspaceManager.UpdateKeyspaceState("ks2", state, now.Unix())
				}
				re.NoError(err)
				re.Equal(expected, gatherBarrierMetrics(t, registry), "state updates must preserve observations: %s", state)
				_, err = m.AdvanceTxnSafePoint(2, ts, now)
				re.NoError(err)
				re.Len(logs.FilterMessage("GC barrier timestamp is too old").All(), 1, "state updates must preserve warning suppression: %s", state)
			}
		})
	}
}

func (s *gcStateManagerTestSuite) TestBarrierMetricsGlobalGCWorkerIsRealBarrier() {
	re := s.Require()
	now := time.Unix(2_000_000_000, 0)
	m := s.manager
	m.barrierMetrics.now = func() time.Time { return now }
	registry := prometheus.NewRegistry()
	registry.MustRegister(m.barrierMetrics)
	ts := uint64(now.Add(-80*time.Hour).UnixMilli()) << 18
	_, err := m.SetGlobalGCBarrier(context.Background(), "gc_worker", ts, time.Duration(math.MaxInt64), now)
	re.NoError(err)
	// Local gc_worker is the compatibility watermark; global gc_worker is an
	// ordinary, API-accepted retention barrier and must remain observable.
	re.NoError(s.provider.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		return wb.SetGCBarrier(2, endpoint.NewGCBarrier("gc_worker", ts, nil))
	}))
	core, logs := observer.New(zapcore.WarnLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	_, err = m.AdvanceTxnSafePoint(2, ts, now)
	re.NoError(err)
	re.Equal(map[string]float64{"global//gc_worker": 1_999_712_000}, gatherBarrierMetrics(s.T(), registry))
	warnings := logs.FilterMessage("GC barrier timestamp is too old").All()
	re.Len(warnings, 1)
	re.Equal("global", warnings[0].ContextMap()["scope"])
}

func (s *gcStateManagerTestSuite) TestBarrierMetricsRejectedRequestsDoNotDiscover() {
	re := s.Require()
	now := time.Unix(2_000_000_000, 0)
	m := s.manager
	m.barrierMetrics.now = func() time.Time { return now }
	registry := prometheus.NewRegistry()
	registry.MustRegister(m.barrierMetrics)
	ts := uint64(now.Add(-80*time.Hour).UnixMilli()) << 18
	_, err := m.SetGlobalGCBarrier(context.Background(), "global", ts, time.Duration(math.MaxInt64), now)
	re.NoError(err)
	// Seed unsupported metadata directly so this rejection is covered in both
	// classic and NextGen builds, independent of keyspace creation defaults.
	meta, err := m.keyspaceManager.LoadKeyspaceByID(2)
	re.NoError(err)
	meta.Config[keyspace.GCManagementType] = keyspace.UnifiedGC
	re.NoError(s.storage.RunInTxn(context.Background(), func(txn kv.Txn) error {
		return s.storage.SaveKeyspaceMeta(txn, meta)
	}))
	re.NoError(s.provider.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		for _, id := range []uint32{2, 10000} {
			if err := wb.SetGCBarrier(id, endpoint.NewGCBarrier("old", ts, nil)); err != nil {
				return err
			}
		}
		return nil
	}))
	core, logs := observer.New(zapcore.WarnLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	counted := &barrierMetricsTestKV{Base: s.storage.Base}
	m.gcMetaStorage = endpoint.NewStorageEndpoint(counted, nil).GetGCStateProvider()
	for _, id := range []uint32{2, 10000} {
		_, err = m.AdvanceTxnSafePoint(id, ts, now)
		re.Error(err)
		re.Empty(gatherBarrierMetrics(s.T(), registry))
		re.Empty(logs.FilterMessage("GC barrier timestamp is too old").All())
	}
	re.Zero(counted.loads)
	re.Zero(counted.ranges)
}

func (s *gcStateManagerTestSuite) TestBarrierMetricsRemovalFencesInflightPublication() {
	re := s.Require()
	now := time.Unix(2_000_000_000, 0)
	m := s.manager
	stop := m.OnNodeBecomesLeader()
	m.barrierMetrics.now = func() time.Time { return now }
	registry := prometheus.NewRegistry()
	registry.MustRegister(m.barrierMetrics)
	ts := uint64(now.Add(-80*time.Hour).UnixMilli()) << 18
	_, err := m.SetGCBarrier(2, "old", ts, time.Duration(math.MaxInt64), now)
	re.NoError(err)
	_, err = m.SetGlobalGCBarrier(context.Background(), "global", ts, time.Duration(math.MaxInt64), now)
	re.NoError(err)
	advance := func() { _, err := m.AdvanceTxnSafePoint(2, ts, now); re.NoError(err) }
	advance()
	expected := map[string]float64{"keyspace/2/old": 1_999_712_000, "global//global": 1_999_712_000}
	for _, state := range []keyspacepb.KeyspaceState{keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE} {
		_, err = m.keyspaceManager.UpdateKeyspaceStateByID(2, state, now.Unix())
		re.NoError(err)
		re.Equal(expected, gatherBarrierMetrics(s.T(), registry))
		advance()
		re.Equal(expected, gatherBarrierMetrics(s.T(), registry), "every accepted metadata state remains observable")
	}
	stop()
	re.Empty(gatherBarrierMetrics(s.T(), registry))
	m.OnNodeBecomesLeader()
	re.Empty(gatherBarrierMetrics(s.T(), registry))
	advance()
	groupManager := keyspace.NewKeyspaceGroupManager(context.Background(), s.storage, nil)
	defer groupManager.Close()
	re.NoError(groupManager.CreateKeyspaceGroups([]*endpoint.KeyspaceGroup{{ID: 101, UserKind: endpoint.Standard.String(), Keyspaces: []uint32{2}}}))
	counted := &barrierMetricsTestKV{Base: s.storage.Base}
	m.gcMetaStorage = endpoint.NewStorageEndpoint(counted, nil).GetGCStateProvider()
	// Remove metadata after it was resolved and the GC transaction loaded its
	// barriers. The successful GC commit must not restore the removed sample.
	counted.beforeCommit = func() {
		_, err := groupManager.RemoveKeyspacesFromGroup(101, m.keyspaceManager, []uint32{2})
		re.NoError(err)
		re.Equal(map[string]float64{"global//global": 1_999_712_000}, gatherBarrierMetrics(s.T(), registry))
	}
	core, logs := observer.New(zapcore.WarnLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	now = now.Add(10 * time.Minute)
	advance()
	re.Equal(map[string]float64{"global//global": 1_999_712_000}, gatherBarrierMetrics(s.T(), registry))
	re.Empty(logs.FilterMessage("GC barrier timestamp is too old").All())
	counted.beforeCommit = nil
	_, err = m.AdvanceTxnSafePoint(2, ts, now)
	re.Error(err)
}
