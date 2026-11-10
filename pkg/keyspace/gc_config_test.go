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

package keyspace

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

func TestEffectiveGCManagementType(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config map[string]string
		want   string
	}{
		{"nil", nil, UnifiedGC},
		{"empty", map[string]string{}, UnifiedGC},
		{"legacy-v1", map[string]string{"safe_point_version": "v1"}, UnifiedGC},
		{"legacy-v2", map[string]string{"safe_point_version": "v2"}, KeyspaceLevelGC},
		{"legacy-unknown", map[string]string{"safe_point_version": "unknown"}, UnifiedGC},
		{"new-unified", map[string]string{GCManagementType: UnifiedGC}, UnifiedGC},
		{"new-keyspace", map[string]string{GCManagementType: KeyspaceLevelGC}, KeyspaceLevelGC},
		{"unified-overrides-legacy", map[string]string{GCManagementType: UnifiedGC, "safe_point_version": "v2"}, UnifiedGC},
		{"keyspace-overrides-legacy", map[string]string{GCManagementType: KeyspaceLevelGC, "safe_point_version": "v1"}, KeyspaceLevelGC},
		{"empty-suppresses-legacy", map[string]string{GCManagementType: "", "safe_point_version": "v2"}, UnifiedGC},
		{"unknown-suppresses-legacy", map[string]string{GCManagementType: "unknown", "safe_point_version": "v2"}, UnifiedGC},
		{"empty-new", map[string]string{GCManagementType: ""}, UnifiedGC},
		{"unknown-new", map[string]string{GCManagementType: "unknown"}, UnifiedGC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := maps.Clone(tc.config)
			require.Equal(t, tc.want, EffectiveGCManagementType(tc.config))
			require.Equal(t, original, tc.config)
		})
	}
}

func TestGCConfigCreationRejectsBeforeSideEffects(t *testing.T) {
	m := &Manager{} // Every external dependency is absent: rejection must precede IO.
	opposite := KeyspaceLevelGC
	if kerneltype.IsNextGen() {
		opposite = UnifiedGC
	}
	for _, tc := range []struct {
		config map[string]string
		want   error
	}{
		{map[string]string{GCManagementType: ""}, errs.ErrKeyspaceGCConfigInvalid},
		{map[string]string{GCManagementType: "unknown"}, errs.ErrKeyspaceGCConfigInvalid},
		{map[string]string{GCManagementType: opposite}, errs.ErrKeyspaceGCModeMismatch},
		{map[string]string{"safe_point_version": ""}, errs.ErrKeyspaceLegacyGCConfigUnsupported},
	} {
		_, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "gc_rejected", Config: maps.Clone(tc.config)})
		require.ErrorIs(t, err, tc.want)
		id := uint32(123)
		_, err = m.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &id, Name: "gc_rejected", Config: maps.Clone(tc.config)})
		require.ErrorIs(t, err, tc.want)
	}
}

func TestObserveGCConfigBoundAndLegacy(t *testing.T) {
	re := require.New(t)
	core, logs := observer.New(zap.ErrorLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	m := &Manager{} // Observation works without storage or any other dependency.
	m.ObserveGCConfig(nil)
	m.ObserveGCConfig(&keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: constant.NullKeyspaceID}, Config: map[string]string{GCManagementType: "invalid"}})
	legacy := "v1"
	if kerneltype.IsNextGen() {
		legacy = "v2"
	}
	m.ObserveGCConfig(&keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1}, Config: map[string]string{"safe_point_version": legacy}})
	re.Empty(logs.All(), "matching legacy metadata and NullKeyspace are not violations")
	for id := uint32(1); id <= 4097; id++ {
		meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Config: map[string]string{GCManagementType: "invalid"}}
		m.ObserveGCConfig(meta)
		re.Equal(map[string]string{GCManagementType: "invalid"}, meta.Config)
	}
	re.Len(logs.All(), 4097)
	meta := &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 4097}, Config: map[string]string{GCManagementType: "invalid"}}
	m.ObserveGCConfig(meta)
	re.Len(logs.All(), 4097, "the most recent violation remains suppressed")
	meta.Keyspace = &keyspacepb.KeyspaceMeta_Id{Id: 1}
	m.ObserveGCConfig(meta)
	re.Len(logs.All(), 4098, "an evicted violation may be reported again")
}

// saveHistoricalGCConfig constructs persisted history without using a creation API.
func (suite *keyspaceTestSuite) saveHistoricalGCConfig(id uint32, config map[string]string) *keyspacepb.KeyspaceMeta {
	meta := &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id},
		Name:     fmt.Sprintf("gc-history-%d", id),
		State:    keyspacepb.KeyspaceState_ENABLED,
		Config:   maps.Clone(config),
	}
	suite.Require().NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		if err := suite.manager.store.SaveKeyspaceID(txn, id, meta.Name); err != nil {
			return err
		}
		return suite.manager.store.SaveKeyspaceMeta(txn, meta)
	}))
	return meta
}

func (suite *keyspaceTestSuite) TestGCConfigCreationPolicy() {
	const kernelFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	suite.T().Cleanup(func() { suite.Require().NoError(failpoint.Disable(kernelFlag)) })
	allocated := uint32(0)
	for _, nextGen := range []bool{false, true} {
		suite.Require().NoError(failpoint.Enable(kernelFlag, fmt.Sprintf("return(%t)", nextGen)))
		mode, incompatible := UnifiedGC, KeyspaceLevelGC
		if nextGen {
			mode, incompatible = KeyspaceLevelGC, UnifiedGC
		}
		for _, byID := range []bool{false, true} {
			cases := []struct {
				name   string
				config map[string]string
				valid  bool
			}{
				{"nil", nil, true},
				{"absent", map[string]string{"unrelated": "retained"}, true},
				{"matching", map[string]string{GCManagementType: mode}, true},
				{"mismatched", map[string]string{GCManagementType: incompatible}, false},
				{"empty", map[string]string{GCManagementType: ""}, false},
				{"unknown", map[string]string{GCManagementType: "other"}, false},
				{"legacy-v2", map[string]string{"safe_point_version": "v2"}, false},
				{"legacy-empty", map[string]string{"safe_point_version": ""}, false},
				{"legacy-v1", map[string]string{GCManagementType: mode, "safe_point_version": "v1"}, false},
			}
			for i, tc := range cases {
				suite.Run(fmt.Sprintf("nextgen=%t/by-id=%t/%s", nextGen, byID, tc.name), func() {
					re := suite.Require()
					name := fmt.Sprintf("gc_%t_%t_%d", nextGen, byID, i)
					var meta *keyspacepb.KeyspaceMeta
					var err error
					if byID {
						id := uint32(1000 + i)
						if nextGen {
							id += 100
						}
						meta, err = suite.manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &id, Name: name, Config: maps.Clone(tc.config)})
					} else {
						meta, err = suite.manager.CreateKeyspace(&CreateKeyspaceRequest{Name: name, Config: maps.Clone(tc.config)})
					}
					if !tc.valid {
						re.Error(err)
						_, err = suite.manager.LoadKeyspace(name)
						re.ErrorIs(err, errs.ErrKeyspaceNotFound)
						return
					}
					re.NoError(err)
					re.Equal(mode, meta.Config[GCManagementType])
					if !byID {
						allocated++
						re.Equal(allocated, meta.GetId(), "rejected requests must not allocate IDs")
					}
				})
			}
		}
	}
}

func (suite *keyspaceTestSuite) TestGCConfigUpdates() {
	put := func(value string) []*Mutation { return []*Mutation{{Op: OpPut, Key: GCManagementType, Value: value}} }
	del := []*Mutation{{Op: OpDel, Key: GCManagementType}}
	cases := []struct {
		name      string
		stored    map[string]string
		mutations []*Mutation
		want      string
		wantErr   error
	}{
		{"nil", nil, nil, UnifiedGC, nil},
		{"missing", map[string]string{}, nil, UnifiedGC, nil},
		{"legacy-v2", map[string]string{"safe_point_version": "v2"}, nil, KeyspaceLevelGC, nil},
		{"legacy-v1", map[string]string{"safe_point_version": "v1"}, nil, UnifiedGC, nil},
		{"legacy-explicit", map[string]string{"safe_point_version": "v2"}, put(KeyspaceLevelGC), KeyspaceLevelGC, nil},
		{"legacy-conversion", map[string]string{"safe_point_version": "v2"}, put(UnifiedGC), "", errs.ErrKeyspaceGCModeImmutable},
		{"repeat-keyspace", map[string]string{GCManagementType: KeyspaceLevelGC}, put(KeyspaceLevelGC), KeyspaceLevelGC, nil},
		{"repeat-unified", map[string]string{GCManagementType: UnifiedGC}, put(UnifiedGC), UnifiedGC, nil},
		{"unified-to-keyspace", map[string]string{GCManagementType: UnifiedGC}, put(KeyspaceLevelGC), "", errs.ErrKeyspaceGCModeImmutable},
		{"keyspace-to-unified", map[string]string{GCManagementType: KeyspaceLevelGC}, put(UnifiedGC), "", errs.ErrKeyspaceGCModeImmutable},
		{"delete-keyspace", map[string]string{GCManagementType: KeyspaceLevelGC}, del, "", errs.ErrKeyspaceGCModeImmutable},
		{"delete-both-keyspace", map[string]string{GCManagementType: KeyspaceLevelGC, "safe_point_version": "v2"}, del, KeyspaceLevelGC, nil},
		{"delete-activates-legacy", map[string]string{GCManagementType: UnifiedGC, "safe_point_version": "v2"}, del, "", errs.ErrKeyspaceGCModeImmutable},
		{"delete-unified", map[string]string{GCManagementType: UnifiedGC}, del, UnifiedGC, nil},
		{"delete-missing-legacy", map[string]string{"safe_point_version": "v2"}, del, KeyspaceLevelGC, nil},
		{"empty-history", map[string]string{GCManagementType: "", "safe_point_version": "v2"}, nil, "", nil},
		{"unknown-history", map[string]string{GCManagementType: "unknown", "safe_point_version": "v2"}, nil, "unknown", nil},
		{"repair-empty", map[string]string{GCManagementType: "", "safe_point_version": "v2"}, put(UnifiedGC), UnifiedGC, nil},
		{"repair-unknown", map[string]string{GCManagementType: "unknown"}, put(UnifiedGC), UnifiedGC, nil},
		{"delete-empty-activates-legacy", map[string]string{GCManagementType: "", "safe_point_version": "v2"}, del, "", errs.ErrKeyspaceGCModeImmutable},
		{"delete-unknown", map[string]string{GCManagementType: "unknown"}, del, UnifiedGC, nil},
		{"put-empty", nil, put(""), "", errs.ErrKeyspaceGCConfigInvalid},
		{"put-unknown", nil, put("unknown"), "", errs.ErrKeyspaceGCConfigInvalid},
		{"put-invalid-then-valid", nil, append(put("unknown"), put(UnifiedGC)...), "", errs.ErrKeyspaceGCConfigInvalid},
		{"put-legacy-same", map[string]string{"safe_point_version": "v2"}, []*Mutation{{Op: OpPut, Key: "safe_point_version", Value: "v2"}}, "", errs.ErrKeyspaceLegacyGCConfigUnsupported},
		{"put-legacy-empty", nil, []*Mutation{{Op: OpPut, Key: "safe_point_version"}}, "", errs.ErrKeyspaceLegacyGCConfigUnsupported},
		{"delete-legacy", map[string]string{"safe_point_version": "v2"}, []*Mutation{{Op: OpDel, Key: "safe_point_version"}}, "", errs.ErrKeyspaceLegacyGCConfigUnsupported},
		{"delete-legacy-absent", nil, []*Mutation{{Op: OpDel, Key: "safe_point_version"}}, "", errs.ErrKeyspaceLegacyGCConfigUnsupported},
	}
	for i, tc := range cases {
		suite.Run(tc.name, func() {
			re := suite.Require()
			original := suite.saveHistoricalGCConfig(uint32(2000+i), tc.stored)
			mutations := append([]*Mutation{{Op: OpPut, Key: "unrelated", Value: "updated"}}, tc.mutations...)
			updated, err := suite.manager.UpdateKeyspaceConfig(original.Name, mutations)
			if tc.wantErr != nil {
				re.ErrorIs(err, tc.wantErr)
				stored, err := suite.manager.LoadKeyspace(original.Name)
				re.NoError(err)
				re.Equal(original.Config, stored.Config, "rejection must not persist any mutation or normalization")
				return
			}
			re.NoError(err)
			want := maps.Clone(tc.stored)
			if want == nil {
				want = map[string]string{}
			}
			want[GCManagementType] = tc.want
			want["unrelated"] = "updated"
			re.Equal(want, updated.Config)
			stored, err := suite.manager.LoadKeyspace(original.Name)
			re.NoError(err)
			re.Equal(want, stored.Config)
		})
	}
}

func (suite *keyspaceTestSuite) TestGCConfigOriginalPreconditionsAndAtomicity() {
	re := suite.Require()
	meta := suite.saveHistoricalGCConfig(3000, map[string]string{"safe_point_version": "v2"})
	unified := UnifiedGC
	_, err := suite.manager.UpdateKeyspaceConfigWithPreconditions(meta.Name, nil, map[string]*string{GCManagementType: &unified})
	re.ErrorIs(err, errs.ErrKeyspaceConfigPreconditionFailed)
	stored, err := suite.manager.LoadKeyspace(meta.Name)
	re.NoError(err)
	re.Equal(meta.Config, stored.Config)
	_, err = suite.manager.UpdateKeyspaceConfig(meta.Name, []*Mutation{{Op: OpPut, Key: "partial", Value: "no"}, {Op: OpType(99)}})
	re.ErrorIs(err, errs.ErrIllegalOperation)
	stored, err = suite.manager.LoadKeyspace(meta.Name)
	re.NoError(err)
	re.Equal(meta.Config, stored.Config)
	updated, err := suite.manager.UpdateKeyspaceConfigWithPreconditions(meta.Name, nil, map[string]*string{GCManagementType: nil})
	re.NoError(err)
	re.Equal(KeyspaceLevelGC, updated.Config[GCManagementType])
	_, err = suite.manager.UpdateKeyspaceConfigWithPreconditions(meta.Name, nil, map[string]*string{GCManagementType: nil})
	re.ErrorIs(err, errs.ErrKeyspaceConfigPreconditionFailed)
}

func (suite *keyspaceTestSuite) TestGCConfigEmptyUpdateNormalizes() {
	re := suite.Require()
	meta := suite.saveHistoricalGCConfig(3001, nil)
	updated, err := suite.manager.UpdateKeyspaceConfig(meta.Name, nil)
	re.NoError(err)
	re.Equal(map[string]string{GCManagementType: UnifiedGC}, updated.Config)
}

func (suite *keyspaceTestSuite) TestGCConfigReadsAndStateChangesPreserveHistory() {
	re := suite.Require()
	for i, config := range []map[string]string{nil, {"safe_point_version": "v2"}, {GCManagementType: "unknown", "safe_point_version": "v2"}} {
		meta := suite.saveHistoricalGCConfig(uint32(3100+i), config)
		byName, err := suite.manager.LoadKeyspace(meta.Name)
		re.NoError(err)
		re.Equal(meta.Config, byName.Config)
		byID, err := suite.manager.LoadKeyspaceByID(meta.GetId())
		re.NoError(err)
		re.Equal(meta.Config, byID.Config)
		_, err = suite.manager.UpdateKeyspaceState(meta.Name, keyspacepb.KeyspaceState_DISABLED, 1)
		re.NoError(err)
		updated, err := suite.manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_ARCHIVED, 2)
		re.NoError(err)
		re.Equal(meta.Config, updated.Config)
	}
}

func (suite *keyspaceTestSuite) TestGCConfigDiagnostics() {
	re := suite.Require()
	core, logs := observer.New(zap.ErrorLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	matching, mismatched := UnifiedGC, KeyspaceLevelGC
	if kerneltype.IsNextGen() {
		matching, mismatched = KeyspaceLevelGC, UnifiedGC
	}
	meta := suite.saveHistoricalGCConfig(3200, map[string]string{GCManagementType: mismatched})
	// Concurrent observations must emit one error, including for disabled history.
	meta.State = keyspacepb.KeyspaceState_DISABLED
	re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return suite.manager.store.SaveKeyspaceMeta(txn, meta) }))
	wg := &sync.WaitGroup{}
	start := make(chan struct{})
	results := make(chan error, 50)
	for range 50 {
		wg.Go(func() {
			<-start
			_, err := suite.manager.LoadKeyspaceByID(meta.GetId())
			results <- err
		})
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		re.NoError(err)
	}
	re.Len(logs.All(), 1)
	fields := logs.All()[0].ContextMap()
	re.Equal(meta.GetId(), fields["keyspace-id"])
	re.Equal(meta.Name, fields["keyspace-name"])
	re.Equal(mismatched, fields["effective-gc-mode"])
	re.Equal(true, fields["gc-management-type-present"])
	re.Equal(mismatched, fields["gc-management-type"])
	re.Equal(false, fields["safe-point-version-present"])
	re.Contains(fields, "kernel")
	re.Contains(fields, "reason")
	// Unrelated changes leave the diagnostic fingerprint unchanged.
	_, err := suite.manager.UpdateKeyspaceConfig(meta.Name, []*Mutation{{Op: OpPut, Key: "unrelated", Value: "changed"}})
	re.NoError(err)
	re.Len(logs.All(), 1)
	// A changed malformed representation logs again even if its effective mode is unchanged.
	for _, value := range []string{"", "unknown"} {
		meta.Config[GCManagementType] = value
		re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return suite.manager.store.SaveKeyspaceMeta(txn, meta) }))
		_, err = suite.manager.LoadKeyspace(meta.Name)
		re.NoError(err)
	}
	re.Len(logs.All(), 3)
	// A valid observation clears the entry, so recurrence can be reported.
	meta.Config[GCManagementType] = matching
	re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return suite.manager.store.SaveKeyspaceMeta(txn, meta) }))
	_, err = suite.manager.LoadRangeKeyspace(meta.GetId(), 1)
	re.NoError(err)
	meta.Config[GCManagementType] = "unknown"
	re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return suite.manager.store.SaveKeyspaceMeta(txn, meta) }))
	_, err = suite.manager.LoadKeyspaceByID(meta.GetId())
	re.NoError(err)
	re.Len(logs.All(), 4)
}

func (suite *keyspaceTestSuite) TestGCConfigDiagnosticsDuringMaintenance() {
	m := suite.manager
	for i, path := range []string{"revision", "group-scan", "assignment-patrol", "removal"} {
		suite.Run(path, func() {
			re := suite.Require()
			core, logs := observer.New(zap.ErrorLevel)
			restore := log.ReplaceGlobals(zap.New(core), nil)
			defer restore()
			meta := suite.saveHistoricalGCConfig(uint32(3400+i), map[string]string{GCManagementType: "malformed", "safe_point_version": "v2"})
			meta.State = keyspacepb.KeyspaceState_ARCHIVED
			re.NoError(m.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return m.store.SaveKeyspaceMeta(txn, meta) }))
			switch path {
			case "revision":
				base := m.store
				m.store = &revisionedKeyspaceStorage{KeyspaceStorage: base, load: func(context.Context, uint32) (*keyspacepb.KeyspaceMeta, int64, error) { return meta, 123, nil }}
				loaded, revision, err := m.LoadKeyspaceByIDWithRevision(suite.ctx, meta.GetId())
				m.store = base
				re.NoError(err)
				re.Equal(int64(123), revision)
				re.Equal(meta, loaded)
			case "group-scan":
				_, err := m.CountKeyspacesByMetaServiceGroup(map[string]struct{}{"group": {}})
				re.NoError(err)
			case "assignment-patrol":
				re.NoError(m.PatrolKeyspaceAssignment(meta.GetId(), meta.GetId()))
				loaded, err := m.LoadKeyspaceByID(meta.GetId())
				re.NoError(err)
				re.Equal("malformed", loaded.Config[GCManagementType])
				re.Equal("v2", loaded.Config["safe_point_version"])
			case "removal":
				re.NoError(m.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return m.RemoveKeyspace(txn, meta.GetId()) }))
			}
			re.Len(logs.All(), 1, "each existing metadata read must observe archived history")
		})
	}
}

func (suite *keyspaceTestSuite) TestGCConfigReservedAndPreallocatedDefaults() {
	re := suite.Require()
	mode := UnifiedGC
	if kerneltype.IsNextGen() {
		mode = KeyspaceLevelGC
	}
	reserved, err := suite.manager.LoadKeyspaceByID(GetBootstrapKeyspaceID())
	re.NoError(err)
	re.Equal(mode, reserved.Config[GCManagementType])
	delete(reserved.Config, GCManagementType)
	reserved.Config["safe_point_version"] = "v2"
	re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error { return suite.manager.store.SaveKeyspaceMeta(txn, reserved) }))
	suite.manager.config.(*mockConfig).PreAlloc = []string{"gc_preallocated"}
	re.NoError(suite.manager.Bootstrap())
	loaded, err := suite.manager.LoadKeyspaceByID(reserved.GetId())
	re.NoError(err)
	re.Equal(reserved.Config, loaded.Config, "restart must retain existing reserved metadata")
	re.Eventually(func() bool {
		meta, err := suite.manager.LoadKeyspace("gc_preallocated")
		return err == nil && meta.Config[GCManagementType] == mode && meta.State == keyspacepb.KeyspaceState_ENABLED
	}, time.Second, time.Millisecond)
}

func (suite *keyspaceTestSuite) TestGCConfigPolicyBeforeGroupChanges() {
	re := suite.Require()
	m := suite.manager
	created, err := m.CreateKeyspace(&CreateKeyspaceRequest{Name: "gc_group_policy"})
	re.NoError(err)
	re.NoError(m.kgm.CreateKeyspaceGroups([]*endpoint.KeyspaceGroup{{ID: 123, UserKind: endpoint.Basic.String()}}))
	oldGroups, err := m.kgm.GetKeyspaceGroups(0, 0)
	re.NoError(err)
	opposite := KeyspaceLevelGC
	if kerneltype.IsNextGen() {
		opposite = UnifiedGC
	}
	_, err = m.UpdateKeyspaceConfig(created.Name, []*Mutation{
		{Op: OpPut, Key: TSOKeyspaceGroupIDKey, Value: "123"},
		{Op: OpPut, Key: MetaServiceGroupIDKey, Value: "missing"},
		{Op: OpPut, Key: GCManagementType, Value: opposite},
	})
	re.ErrorIs(err, errs.ErrKeyspaceGCModeImmutable)
	groups, err := m.kgm.GetKeyspaceGroups(0, 0)
	re.NoError(err)
	re.Equal(oldGroups, groups)
	loaded, err := m.LoadKeyspace(created.Name)
	re.NoError(err)
	re.Equal(created.Config, loaded.Config)
	// A group validation failure must not persist successful normalization.
	history := suite.saveHistoricalGCConfig(3300, map[string]string{TSOKeyspaceGroupIDKey: "0", UserKindKey: endpoint.Basic.String()})
	_, err = m.UpdateKeyspaceConfig(history.Name, []*Mutation{{Op: OpPut, Key: TSOKeyspaceGroupIDKey, Value: "missing"}})
	re.Error(err)
	loaded, err = m.LoadKeyspace(history.Name)
	re.NoError(err)
	re.Equal(history.Config, loaded.Config)
}

type rejectedGCConfigTransaction struct{ endpoint.KeyspaceStorage }

func (s *rejectedGCConfigTransaction) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	return s.KeyspaceStorage.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := f(txn); err != nil {
			return err
		}
		return errors.New("injected transaction rejection")
	})
}

func (suite *keyspaceTestSuite) TestGCConfigTransactionFailureDoesNotNormalize() {
	re := suite.Require()
	meta := suite.saveHistoricalGCConfig(3301, map[string]string{"safe_point_version": "v2"})
	base := suite.manager.store
	suite.manager.store = &rejectedGCConfigTransaction{base}
	_, err := suite.manager.UpdateKeyspaceConfig(meta.Name, []*Mutation{{Op: OpPut, Key: "partial", Value: "no"}})
	suite.manager.store = base
	re.ErrorContains(err, "injected transaction rejection")
	loaded, err := suite.manager.LoadKeyspace(meta.Name)
	re.NoError(err)
	re.Equal(meta.Config, loaded.Config)
}
