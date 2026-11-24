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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

type deferredEnableWrite struct {
	key, value string
}

type deferredEnableTxn struct {
	kv.Txn
	writes *[]deferredEnableWrite
}

func (txn *deferredEnableTxn) Save(key, value string) error {
	*txn.writes = append(*txn.writes, deferredEnableWrite{key, value})
	return nil
}

// enableOutcomeStorage models commit outcomes at the storage boundary. Deferred
// writes model ordering only, not etcd's transaction conflict implementation.
type enableOutcomeStorage struct {
	endpoint.KeyspaceStorage
	armed      bool
	commitErr  error
	readErr    error
	saveErr    error
	writes     []deferredEnableWrite
	afterStage func()
}

func (s *enableOutcomeStorage) LoadKeyspaceMeta(txn kv.Txn, id uint32) (*keyspacepb.KeyspaceMeta, error) {
	if s.readErr != nil {
		err := s.readErr
		s.readErr = nil
		return nil, err
	}
	return s.KeyspaceStorage.LoadKeyspaceMeta(txn, id)
}

func (s *enableOutcomeStorage) SaveKeyspaceMeta(txn kv.Txn, meta *keyspacepb.KeyspaceMeta) error {
	if s.saveErr != nil {
		err := s.saveErr
		s.saveErr = nil
		return err
	}
	return s.KeyspaceStorage.SaveKeyspaceMeta(txn, meta)
}

func (s *enableOutcomeStorage) RunInTxn(ctx context.Context, fn func(kv.Txn) error) error {
	if !s.armed {
		return s.KeyspaceStorage.RunInTxn(ctx, fn)
	}
	s.armed = false
	err := s.KeyspaceStorage.RunInTxn(ctx, func(txn kv.Txn) error {
		return fn(&deferredEnableTxn{Txn: txn, writes: &s.writes})
	})
	if err != nil {
		return err
	}
	if s.afterStage != nil {
		s.afterStage()
	}
	return s.commitErr
}

func TestCreateEnableOutcomeClassification(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		optimistic bool
	}{
		{"deadline", context.DeadlineExceeded, true},
		{"grpc_deadline", status.Error(codes.DeadlineExceeded, "deadline exceeded"), true},
		{"transport", status.Error(codes.Unavailable, "transport is closing"), true},
		{"etcd_timeout", rpctypes.ErrTimeout, true},
		{"grpc_etcd_timeout", rpctypes.ErrGRPCTimeout, true},
		{"leader_timeout", rpctypes.ErrTimeoutDueToLeaderFail, true},
		{"connection_timeout", rpctypes.ErrTimeoutDueToConnectionLost, true},
		{"wrapped_timeout", errors.Annotate(rpctypes.ErrTimeout, "commit"), true},
		{"std_wrapped_timeout", fmt.Errorf("commit: %w", rpctypes.ErrGRPCTimeout), true},
		{"no_leader", rpctypes.ErrNoLeader, false},
		{"grpc_no_leader", rpctypes.ErrGRPCNoLeader, false},
		{"wrapped_no_leader", fmt.Errorf("commit: %w", rpctypes.ErrGRPCNoLeader), false},
		{"nested_no_leader", errors.Annotate(fmt.Errorf("commit: %w", rpctypes.ErrGRPCNoLeader), "enable"), false},
		{"leader_changed", rpctypes.ErrLeaderChanged, false},
		{"unhealthy", rpctypes.ErrUnhealthy, false},
		{"no_space", rpctypes.ErrNoSpace, false},
		{"wait_applied_index", rpctypes.ErrGRPCTimeoutWaitAppliedIndex, false},
		{"unknown_etcd_error", status.Error(codes.Unavailable, "etcdserver: unknown rejection"), false},
		{"permission", status.Error(codes.PermissionDenied, "denied"), false},
		{"conflict", errs.ErrEtcdTxnConflict.FastGenByArgs(), false},
		{"canceled", context.Canceled, false},
		{"other", errCreationStorage, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			manager, _, cluster, _ := newPendingCreationManager(t)
			_, err := manager.CreateKeyspace(&CreateKeyspaceRequest{Name: "enable_outcome"})
			re.ErrorIs(err, errs.ErrRegionSplitTimeout)
			pending, err := manager.LoadKeyspace("enable_outcome")
			re.NoError(err)
			installCreationBounds(manager, cluster, pending.GetId())
			store := &enableOutcomeStorage{KeyspaceStorage: manager.store, armed: true, commitErr: tc.err}
			manager.store = store
			_, err = manager.enableNewKeyspace(pending)
			re.ErrorIs(err, tc.err)
			re.Len(store.writes, 1)
			durable, err := manager.LoadKeyspaceByID(pending.GetId())
			re.NoError(err)
			re.Equal(keyspacepb.KeyspaceState_DISABLED, durable.GetState())
			re.Equal(pending.GetConfig()[creationPendingKey], durable.GetConfig()[creationPendingKey])
			state, err := manager.GetKeyspaceStateByID(pending.GetId())
			re.NoError(err)
			want := keyspacepb.KeyspaceState_DISABLED
			if tc.optimistic {
				want = keyspacepb.KeyspaceState_ENABLED
			}
			re.Equal(want, state)
			cached, found := manager.cache.getKeyspaceByID(pending.GetId())
			re.True(found)
			re.Equal(want, cached.state)
			if !tc.optimistic {
				return
			}
			// The enable write becomes durable only after the error-path read
			// and publication. No extra cache refresh is needed for this ordering.
			re.NoError(store.KeyspaceStorage.RunInTxn(t.Context(), func(txn kv.Txn) error {
				for _, write := range store.writes {
					if err := txn.Save(write.key, write.value); err != nil {
						return err
					}
				}
				return nil
			}))
			durable, err = manager.LoadKeyspaceByID(pending.GetId())
			re.NoError(err)
			re.Equal(keyspacepb.KeyspaceState_ENABLED, durable.GetState())
			state, err = manager.GetKeyspaceStateByID(pending.GetId())
			re.NoError(err)
			re.Equal(durable.GetState(), state)
		})
	}
}

func TestCreateEnableDoesNotPublishPreparationTimeout(t *testing.T) {
	for _, phase := range []string{"read", "save"} {
		t.Run(phase, func(t *testing.T) {
			re := require.New(t)
			manager, _, _, _ := newPendingCreationManager(t)
			_, err := manager.CreateKeyspace(&CreateKeyspaceRequest{Name: "prepare_timeout"})
			re.ErrorIs(err, errs.ErrRegionSplitTimeout)
			pending, err := manager.LoadKeyspace("prepare_timeout")
			re.NoError(err)
			store := &enableOutcomeStorage{KeyspaceStorage: manager.store, armed: true}
			if phase == "read" {
				store.readErr = context.DeadlineExceeded
			} else {
				store.saveErr = context.DeadlineExceeded
			}
			manager.store = store
			_, err = manager.enableNewKeyspace(pending)
			re.ErrorIs(err, context.DeadlineExceeded)
			re.Empty(store.writes)
			state, err := manager.GetKeyspaceStateByID(pending.GetId())
			re.NoError(err)
			re.Equal(keyspacepb.KeyspaceState_DISABLED, state)
		})
	}
}

func TestCreateEnableOptimismDoesNotOverrideNewGeneration(t *testing.T) {
	re := require.New(t)
	manager, _, _, _ := newPendingCreationManager(t)
	_, err := manager.CreateKeyspace(&CreateKeyspaceRequest{Name: "new_generation"})
	re.ErrorIs(err, errs.ErrRegionSplitTimeout)
	pending, err := manager.LoadKeyspace("new_generation")
	re.NoError(err)
	store := &enableOutcomeStorage{KeyspaceStorage: manager.store, armed: true, commitErr: context.DeadlineExceeded}
	store.afterStage = func() {
		// Model metadata replaced by an external writer after the old enable
		// attempt, before its cache reconciliation. Never adopt its token.
		re.NoError(store.KeyspaceStorage.RunInTxn(t.Context(), func(txn kv.Txn) error {
			current, err := store.LoadKeyspaceMeta(txn, pending.GetId())
			if err != nil {
				return err
			}
			current.Config[creationPendingKey] = "replacement"
			return store.SaveKeyspaceMeta(txn, current)
		}))
	}
	manager.store = store
	_, err = manager.enableNewKeyspace(pending)
	re.ErrorIs(err, context.DeadlineExceeded)
	state, err := manager.GetKeyspaceStateByID(pending.GetId())
	re.NoError(err)
	re.Equal(keyspacepb.KeyspaceState_DISABLED, state)
}

func TestCreateEnableOptimismPreservesLaterState(t *testing.T) {
	for _, archived := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "archived"}[archived], func(t *testing.T) {
			re := require.New(t)
			manager, _, _, _ := newPendingCreationManager(t)
			_, err := manager.CreateKeyspace(&CreateKeyspaceRequest{Name: "later_state"})
			re.ErrorIs(err, errs.ErrRegionSplitTimeout)
			pending, err := manager.LoadKeyspace("later_state")
			re.NoError(err)
			store := &enableOutcomeStorage{KeyspaceStorage: manager.store, armed: true, commitErr: context.DeadlineExceeded}
			want := keyspacepb.KeyspaceState_DISABLED
			if archived {
				want = keyspacepb.KeyspaceState_ARCHIVED
			}
			store.afterStage = func() {
				_, err := manager.UpdateKeyspaceState(pending.GetName(), keyspacepb.KeyspaceState_ENABLED, 2)
				re.NoError(err)
				_, err = manager.UpdateKeyspaceState(pending.GetName(), keyspacepb.KeyspaceState_DISABLED, 3)
				re.NoError(err)
				if archived {
					_, err = manager.UpdateKeyspaceState(pending.GetName(), want, 4)
					re.NoError(err)
				}
			}
			manager.store = store
			_, err = manager.enableNewKeyspace(pending)
			re.ErrorIs(err, context.DeadlineExceeded)
			state, err := manager.GetKeyspaceStateByID(pending.GetId())
			re.NoError(err)
			re.Equal(want, state)
		})
	}
}

func TestCreateEnableOptimismInvalidatesOnRefreshFailure(t *testing.T) {
	re := require.New(t)
	manager, _, _, _ := newPendingCreationManager(t)
	_, err := manager.CreateKeyspace(&CreateKeyspaceRequest{Name: "refresh_failure"})
	re.ErrorIs(err, errs.ErrRegionSplitTimeout)
	pending, err := manager.LoadKeyspace("refresh_failure")
	re.NoError(err)
	store := &enableOutcomeStorage{KeyspaceStorage: manager.store, armed: true, commitErr: context.DeadlineExceeded}
	store.afterStage = func() { store.readErr = errCreationStorage }
	manager.store = store
	_, err = manager.enableNewKeyspace(pending)
	re.ErrorIs(err, context.DeadlineExceeded)
	_, found := manager.keyspaceStateLookup.Load(pending.GetId())
	re.False(found)
	_, found = manager.cache.getKeyspaceByID(pending.GetId())
	re.False(found)
	state, err := manager.GetKeyspaceStateByID(pending.GetId())
	re.NoError(err)
	re.Equal(keyspacepb.KeyspaceState_DISABLED, state)
}
