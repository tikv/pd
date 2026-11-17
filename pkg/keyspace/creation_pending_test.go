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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

var errCreationStorage = errors.New("injected creation storage failure")

// creationTestStorage injects errors at the real storage boundary. The test
// controls below are only changed while no request is running.
type creationTestStorage struct {
	*endpoint.StorageEndpoint
	ruleReadErr      bool
	ruleReads        int
	groupReadErr     bool
	failGroupRefresh bool
	failEnable       bool
	lostResponse     string
	writePhase       string
}

func (s *creationTestStorage) LoadRegionRule(id string) (string, error) {
	s.ruleReads++
	if s.ruleReadErr {
		return "", errCreationStorage
	}
	return s.StorageEndpoint.LoadRegionRule(id)
}

func (s *creationTestStorage) LoadKeyspaceGroup(txn kv.Txn, id uint32) (*endpoint.KeyspaceGroup, error) {
	if s.groupReadErr {
		return nil, errCreationStorage
	}
	return s.StorageEndpoint.LoadKeyspaceGroup(txn, id)
}

func (s *creationTestStorage) SaveKeyspaceMeta(txn kv.Txn, meta *keyspacepb.KeyspaceMeta) error {
	s.writePhase = "create"
	if meta.GetState() == keyspacepb.KeyspaceState_ENABLED {
		s.writePhase = "enable"
		if s.failEnable {
			return errCreationStorage
		}
	}
	return s.StorageEndpoint.SaveKeyspaceMeta(txn, meta)
}

func (s *creationTestStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	s.writePhase = ""
	if err := s.StorageEndpoint.RunInTxn(ctx, f); err != nil {
		return err
	}
	if s.writePhase == "create" && s.failGroupRefresh {
		s.groupReadErr = true
	}
	if s.writePhase != "" && s.writePhase == s.lostResponse {
		s.lostResponse = ""
		return errCreationStorage
	}
	return nil
}

func newPendingCreationManager(t *testing.T) (*Manager, *creationTestStorage, *mockcluster.Cluster, *mockConfig) {
	t.Helper()
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store := &creationTestStorage{StorageEndpoint: endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	var err error
	cluster.RegionLabeler, err = labeler.NewRegionLabeler(ctx, store, time.Hour)
	re.NoError(err)
	kgm := NewKeyspaceGroupManager(ctx, store, nil)
	re.NoError(kgm.Bootstrap(ctx))
	cfg := &mockConfig{
		WaitRegionSplit:          true,
		WaitRegionSplitTimeout:   typeutil.Duration{Duration: 5 * time.Millisecond},
		CheckRegionSplitInterval: typeutil.Duration{Duration: time.Millisecond},
	}
	manager := NewKeyspaceManager(ctx, store, cluster, mockid.NewIDAllocator(), cfg, kgm, nil)
	re.NoError(manager.Bootstrap())
	return manager, store, cluster, cfg
}

func installCreationBounds(manager *Manager, cluster *mockcluster.Cluster, id uint32) {
	bound := MakeRegionBound(id)
	left, right := bound.TxnLeftBound, bound.TxnRightBound
	if manager.getRegionBoundType() != txnRegionBound {
		left, right = bound.RawLeftBound, bound.RawRightBound
	}
	cluster.AddLeaderRegionWithRange(100, string(left), string(right), 1)
	cluster.AddLeaderRegionWithRange(101, string(right), "", 1)
}

func TestPendingCreationRetryUsesStoredConfig(t *testing.T) {
	re := require.New(t)
	manager, _, cluster, _ := newPendingCreationManager(t)
	request := &CreateKeyspaceRequest{Name: "pending_config", Config: map[string]string{"setting": "first"}, CreateTime: 123}
	_, err := manager.CreateKeyspace(request)
	re.ErrorIs(err, errs.ErrRegionSplitTimeout)
	pending, err := manager.LoadKeyspace(request.Name)
	re.NoError(err)
	re.NotEmpty(pending.GetConfig()[creationPendingKey])
	re.NotContains(request.Config, creationPendingKey)
	re.NotNil(cluster.GetLabelRule(getRegionLabelID(pending.GetId())))
	_, _, err = manager.kgm.GetGroupByKeyspaceID(pending.GetId())
	re.NoError(err)

	_, err = manager.UpdateKeyspaceConfig(request.Name, []*Mutation{{Op: OpPut, Key: "setting", Value: "patched"}})
	re.NoError(err)
	installCreationBounds(manager, cluster, pending.GetId())
	id := pending.GetId()
	wrongID := id + 1
	_, err = manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &wrongID, Name: request.Name})
	re.ErrorIs(err, errs.ErrKeyspaceExists)
	created, err := manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &id, Name: request.Name, Config: map[string]string{"setting": "ignored"}})
	re.NoError(err)
	re.Equal(id, created.GetId())
	re.Equal("patched", created.GetConfig()["setting"])
	re.Equal(int64(123), created.GetCreatedAt())
	re.NotContains(created.GetConfig(), creationPendingKey)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, created.GetState())
	_, err = manager.CreateKeyspace(request)
	re.ErrorIs(err, errs.ErrKeyspaceExists)
}

func TestPendingCreationCacheReadFailure(t *testing.T) {
	for _, kind := range []string{"rule", "group"} {
		t.Run(kind, func(t *testing.T) {
			re := require.New(t)
			manager, store, cluster, _ := newPendingCreationManager(t)
			request := &CreateKeyspaceRequest{Name: "cache_error"}
			store.ruleReadErr = kind == "rule"
			store.failGroupRefresh = kind == "group"
			_, err := manager.CreateKeyspace(request)
			re.ErrorIs(err, errCreationStorage)
			re.Contains(err.Error(), "cache refresh failed")
			pending, err := manager.LoadKeyspace(request.Name)
			re.NoError(err)
			re.NotEmpty(pending.GetConfig()[creationPendingKey])
			re.Equal(keyspacepb.KeyspaceState_DISABLED, pending.GetState())
			if kind == "rule" {
				re.GreaterOrEqual(store.ruleReads, 3)
				re.Nil(cluster.GetLabelRule(getRegionLabelID(pending.GetId())))
			}
			store.ruleReadErr, store.failGroupRefresh, store.groupReadErr = false, false, false
			installCreationBounds(manager, cluster, pending.GetId())
			created, err := manager.CreateKeyspace(request)
			re.NoError(err)
			re.Equal(pending.GetId(), created.GetId())
			re.NotNil(cluster.GetLabelRule(getRegionLabelID(created.GetId())))
			_, _, err = manager.kgm.GetGroupByKeyspaceID(created.GetId())
			re.NoError(err)
		})
	}
}

func TestPendingCreationWithoutWaiting(t *testing.T) {
	re := require.New(t)
	manager, store, _, cfg := newPendingCreationManager(t)
	cfg.WaitRegionSplit = false
	store.ruleReadErr = true
	request := &CreateKeyspaceRequest{Name: "no_wait"}
	_, err := manager.CreateKeyspace(request)
	re.ErrorIs(err, errCreationStorage)
	pending, err := manager.LoadKeyspace(request.Name)
	re.NoError(err)
	re.Equal(keyspacepb.KeyspaceState_DISABLED, pending.GetState())
	re.NotEmpty(pending.GetConfig()[creationPendingKey])
	store.ruleReadErr = false
	created, err := manager.CreateKeyspace(request)
	re.NoError(err)
	re.Equal(pending.GetId(), created.GetId())
	re.Equal(keyspacepb.KeyspaceState_ENABLED, created.GetState())
	re.False(manager.CheckKeyspaceRegionBound(created))
}

func TestPendingCreationRetryAfterGroupMove(t *testing.T) {
	re := require.New(t)
	manager, _, cluster, _ := newPendingCreationManager(t)
	request := &CreateKeyspaceRequest{Name: "moved_group"}
	_, err := manager.CreateKeyspace(request)
	re.ErrorIs(err, errs.ErrRegionSplitTimeout)
	pending, err := manager.LoadKeyspace(request.Name)
	re.NoError(err)
	newID := uint32(1)
	re.NoError(manager.kgm.CreateKeyspaceGroups([]*endpoint.KeyspaceGroup{{ID: newID, UserKind: endpoint.Basic.String()}}))
	// Like a split/merge, move durable membership without updating meta.Config.
	re.NoError(manager.kgm.UpdateKeyspaceGroup(pending.GetConfig()[TSOKeyspaceGroupIDKey], "1", endpoint.Basic, endpoint.Basic, pending.GetId()))
	installCreationBounds(manager, cluster, pending.GetId())
	created, err := manager.CreateKeyspace(request)
	re.NoError(err)
	groupID, _, err := manager.kgm.GetGroupByKeyspaceID(created.GetId())
	re.NoError(err)
	re.Equal(newID, groupID)
}

func TestPendingCreationConcurrentCompletion(t *testing.T) {
	re := require.New(t)
	manager, _, cluster, _ := newPendingCreationManager(t)
	request := &CreateKeyspaceRequest{Name: "concurrent"}
	_, err := manager.CreateKeyspace(request)
	re.ErrorIs(err, errs.ErrRegionSplitTimeout)
	pending, err := manager.LoadKeyspace(request.Name)
	re.NoError(err)
	installCreationBounds(manager, cluster, pending.GetId())
	reached, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	name := "github.com/tikv/pd/pkg/keyspace/beforeEnableCreatedKeyspace"
	re.NoError(failpoint.EnableCall(name, func() {
		if calls.Add(1) == 1 {
			close(reached)
			<-release
		}
	}))
	var once sync.Once
	defer func() {
		once.Do(func() { close(release) })
		re.NoError(failpoint.Disable(name))
	}()
	result := make(chan error, 1)
	go func() {
		_, err := manager.CreateKeyspace(request)
		result <- err
	}()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach enable")
	}
	created, err := manager.CreateKeyspace(request)
	re.NoError(err)
	re.Equal(pending.GetId(), created.GetId())
	once.Do(func() { close(release) })
	select {
	case err := <-result:
		re.ErrorIs(err, errs.ErrKeyspaceExists)
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not finish")
	}
	stored, err := manager.LoadKeyspace(request.Name)
	re.NoError(err)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, stored.GetState())
	re.NotContains(stored.GetConfig(), creationPendingKey)
}

func TestPendingCreationMissingRule(t *testing.T) {
	re := require.New(t)
	manager, _, cluster, _ := newPendingCreationManager(t)
	request := &CreateKeyspaceRequest{Name: "missing_rule"}
	_, err := manager.CreateKeyspace(request)
	re.ErrorIs(err, errs.ErrRegionSplitTimeout)
	pending, err := manager.LoadKeyspace(request.Name)
	re.NoError(err)
	re.NoError(cluster.DeleteLabelRule(getRegionLabelID(pending.GetId())))
	installCreationBounds(manager, cluster, pending.GetId())
	_, err = manager.CreateKeyspace(request)
	re.ErrorContains(err, "not found")
	loaded, err := manager.LoadKeyspace(request.Name)
	re.NoError(err)
	re.Equal(pending.GetConfig()[creationPendingKey], loaded.GetConfig()[creationPendingKey])
}

func TestPendingCreationEnableErrorAndUncertainCommit(t *testing.T) {
	for _, stage := range []string{"save_enable", "commit_enable", "commit_create"} {
		t.Run(stage, func(t *testing.T) {
			re := require.New(t)
			manager, store, cluster, _ := newPendingCreationManager(t)
			request := &CreateKeyspaceRequest{Name: "uncertain_commit"}
			_, err := manager.CreateKeyspace(request)
			re.ErrorIs(err, errs.ErrRegionSplitTimeout)
			pending, err := manager.LoadKeyspace(request.Name)
			re.NoError(err)
			installCreationBounds(manager, cluster, pending.GetId())
			switch stage {
			case "commit_create":
				// Exercise the initial commit's lost response with a fresh name.
				request.Name = "lost_create_response"
				store.lostResponse = "create"
			case "commit_enable":
				store.lostResponse = "enable"
			default:
				store.failEnable = true
			}
			_, err = manager.CreateKeyspace(request)
			re.ErrorIs(err, errCreationStorage)
			store.failEnable = false
			loaded, err := manager.LoadKeyspace(request.Name)
			re.NoError(err)
			if stage == "commit_enable" {
				re.Equal(keyspacepb.KeyspaceState_ENABLED, loaded.GetState())
				re.NotContains(loaded.GetConfig(), creationPendingKey)
				state, err := manager.GetKeyspaceStateByID(loaded.GetId())
				re.NoError(err)
				re.Equal(keyspacepb.KeyspaceState_ENABLED, state)
				_, err = manager.CreateKeyspace(request)
				re.ErrorIs(err, errs.ErrKeyspaceExists)
				return
			}
			re.NotEmpty(loaded.GetConfig()[creationPendingKey])
			installCreationBounds(manager, cluster, loaded.GetId())
			created, err := manager.CreateKeyspace(request)
			re.NoError(err)
			re.Equal(loaded.GetId(), created.GetId())
		})
	}
}

func TestPendingCreationManagement(t *testing.T) {
	re := require.New(t)
	manager, _, _, cfg := newPendingCreationManager(t)
	request := &CreateKeyspaceRequest{Name: "management"}
	_, err := manager.CreateKeyspace(request)
	re.ErrorIs(err, errs.ErrRegionSplitTimeout)
	pending, err := manager.LoadKeyspace(request.Name)
	re.NoError(err)
	_, err = manager.UpdateKeyspaceState(request.Name, keyspacepb.KeyspaceState_ENABLED, 1)
	re.ErrorContains(err, "region split is pending")
	_, err = manager.UpdateKeyspaceStateByID(pending.GetId(), keyspacepb.KeyspaceState_ENABLED, 1)
	re.ErrorContains(err, "region split is pending")
	unchanged, err := manager.UpdateKeyspaceState(request.Name, keyspacepb.KeyspaceState_DISABLED, 1)
	re.NoError(err)
	re.Equal(pending.GetConfig()[creationPendingKey], unchanged.GetConfig()[creationPendingKey])
	for _, op := range []OpType{OpPut, OpDel} {
		_, err = manager.UpdateKeyspaceConfig(request.Name, []*Mutation{{Op: op, Key: creationPendingKey, Value: "forged"}})
		re.ErrorContains(err, "managed by PD")
	}
	_, err = manager.CreateKeyspace(&CreateKeyspaceRequest{Name: "forged", Config: map[string]string{creationPendingKey: "forged"}})
	re.ErrorContains(err, "managed by PD")
	// Read the current server setting, not a persisted wait_region_split value.
	cfg.WaitRegionSplit = false
	enabled, err := manager.UpdateKeyspaceState(request.Name, keyspacepb.KeyspaceState_ENABLED, 2)
	re.NoError(err)
	re.NotContains(enabled.GetConfig(), creationPendingKey)
	disabled, err := manager.UpdateKeyspaceState(request.Name, keyspacepb.KeyspaceState_DISABLED, 3)
	re.NoError(err)
	re.NotContains(disabled.GetConfig(), creationPendingKey)
	_, err = manager.CreateKeyspace(request)
	re.ErrorIs(err, errs.ErrKeyspaceExists)
}

func TestPendingCreationArchiveAndIDReuse(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "archive", true: "reuse"}[reuse], func(t *testing.T) {
			re := require.New(t)
			manager, _, cluster, _ := newPendingCreationManager(t)
			request := &CreateKeyspaceRequest{Name: "same_identity"}
			_, err := manager.CreateKeyspace(request)
			re.ErrorIs(err, errs.ErrRegionSplitTimeout)
			original, err := manager.LoadKeyspace(request.Name)
			re.NoError(err)
			installCreationBounds(manager, cluster, original.GetId())
			reached, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			name := "github.com/tikv/pd/pkg/keyspace/beforeEnableCreatedKeyspace"
			re.NoError(failpoint.EnableCall(name, func() {
				once.Do(func() { close(reached) })
				<-release
			}))
			var releaseOnce sync.Once
			defer func() {
				releaseOnce.Do(func() { close(release) })
				re.NoError(failpoint.Disable(name))
			}()
			result := make(chan error, 1)
			go func() {
				_, err := manager.CreateKeyspace(request)
				result <- err
			}()
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("create did not reach the enable barrier")
			}
			archived, err := manager.UpdateKeyspaceState(request.Name, keyspacepb.KeyspaceState_ARCHIVED, 2)
			re.NoError(err)
			re.NotContains(archived.GetConfig(), creationPendingKey)
			if reuse {
				groupID, _, err := manager.kgm.GetGroupByKeyspaceID(original.GetId())
				re.NoError(err)
				_, err = manager.kgm.RemoveKeyspacesFromGroup(groupID, manager, []uint32{original.GetId()})
				re.NoError(err)
				// Keep the replacement pending by failing its cache read, before
				// it reaches the old request's enable barrier.
				store := manager.store.(*creationTestStorage)
				store.ruleReadErr = true
				id := original.GetId()
				_, err = manager.CreateKeyspaceByID(&CreateKeyspaceByIDRequest{ID: &id, Name: request.Name})
				re.ErrorIs(err, errCreationStorage)
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-result:
				re.ErrorContains(err, "no longer pending")
			case <-time.After(5 * time.Second):
				t.Fatal("create did not finish")
			}
			loaded, err := manager.LoadKeyspace(request.Name)
			re.NoError(err)
			if reuse {
				re.Equal(keyspacepb.KeyspaceState_DISABLED, loaded.GetState())
				re.NotEqual(original.GetConfig()[creationPendingKey], loaded.GetConfig()[creationPendingKey])
			} else {
				re.Equal(keyspacepb.KeyspaceState_ARCHIVED, loaded.GetState())
			}
		})
	}
}
