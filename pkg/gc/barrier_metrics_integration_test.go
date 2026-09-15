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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
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
	m.OnNodeBecomesFollower()
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
