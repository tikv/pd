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
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/sync/singleflight"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server/config"
)

// starterGCMigration belongs to a single PD leadership generation. The marker's
// creation revision distinguishes old values from T writes that have passed
// migration. Neither source etcd revisions nor the PD GC revision are comparable to it.
type starterGCMigration struct {
	markerRevision int64
	leaderKey      string
	leaderRevision int64

	groupSlots sync.Map // group ID -> chan struct{}; bound reads independently per group
	flights    singleflight.Group
}

func (s *starterGCMigration) needsCheck(id uint32, txnRevision int64) bool {
	return s != nil && id != constant.NullKeyspaceID && txnRevision <= s.markerRevision
}

// Call outside the manager lock. Shared work belongs to the generation and can
// finish after an individual waiter cancels. Failed attempts never publish a value.
func (m *GCStateManager) ensureMigratedTxnSafePoint(ctx context.Context, generation *gcStateGeneration, id uint32) error {
	if generation == nil || generation.migration == nil || id == constant.NullKeyspaceID {
		return nil
	}
	if m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	s := generation.migration
	// Only verified pairs may enter this generation's cache.
	if _, ok := m.gcStateCache.load(id); ok && m.activeGeneration.Load() == generation {
		return nil
	}
	work := s.flights.DoChan(strconv.FormatUint(uint64(id), 10), func() (any, error) {
		readCtx, cancel := context.WithTimeout(generation.ctx, etcdutil.DefaultRequestTimeout)
		defer cancel()
		var err error
		for range 3 {
			err = m.migrateStarterScope(readCtx, generation, id)
			if !errors.Is(err, errs.ErrEtcdTxnConflict) && !errors.Is(err, errs.ErrEtcdTxnInternal) {
				break
			}
			if readCtx.Err() != nil {
				return nil, readCtx.Err()
			}
		}
		return nil, err
	})
	select {
	case <-generation.done:
		return errs.ErrNotLeader
	case <-ctx.Done():
		return ctx.Err()
	case result := <-work:
		if m.activeGeneration.Load() != generation {
			return errs.ErrNotLeader
		}
		return result.Err
	}
}

type starterScopeSource struct {
	txnSafePoint   uint64
	metaRevision   int64
	configRevision int64 // -1 when this scope did not use group configuration
	missing        bool  // the source has no saved_safe_point
}

func (m *GCStateManager) migrateStarterScope(ctx context.Context, generation *gcStateGeneration, id uint32) error {
	if m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	rows, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, []uint32{id})
	if err != nil {
		return err
	}
	pair := rows[0]
	if pair.Err != nil {
		return pair.Err
	}
	if pair.TxnSafePointEmpty {
		return fmt.Errorf("empty transaction safe point for keyspace %d", id)
	}
	s := generation.migration
	if !s.needsCheck(id, pair.TxnSafePointRevision) {
		return validateStarterGCState(pair)
	}

	source, err := m.loadStarterSource(ctx, s, pair)
	if err != nil {
		return fmt.Errorf("load frozen transaction safe point for keyspace %d: %w", id, err)
	}
	failpoint.InjectCall("starterGCMigrationSourceLoaded", id)

	// A group Get may block independently of operations on other keyspaces.
	// Hold the manager lock only for the short target-side transaction.
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	// A data conflict is authoritative only if its T, G and route are still
	// current. Commit the comparisons even when there is no T to write.
	var conflict error
	err = m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		if err := wb.ExpectLeaderRevision(s.leaderKey, s.leaderRevision); err != nil {
			return err
		}
		if err := wb.ExpectTxnSafePointRevision(id, pair.TxnSafePointRevision); err != nil {
			return err
		}
		if err := wb.ExpectKeyspaceMetaRevision(id, source.metaRevision); err != nil {
			return err
		}
		if source.configRevision >= 0 {
			if err := wb.ExpectConfigRevision(source.configRevision); err != nil {
				return err
			}
		}
		gcSafePoint, err := m.gcMetaStorage.LoadGCSafePoint(id)
		if err != nil {
			return err
		}
		switch {
		case source.missing && (pair.TxnSafePoint != 0 || gcSafePoint != 0):
			conflict = fmt.Errorf("keyspace %d frozen transaction safe point is missing with local T=%d G=%d", id, pair.TxnSafePoint, gcSafePoint)
		case pair.TxnSafePoint > source.txnSafePoint:
			conflict = fmt.Errorf("keyspace %d local transaction safe point %d exceeds frozen value %d", id, pair.TxnSafePoint, source.txnSafePoint)
		case gcSafePoint > source.txnSafePoint:
			conflict = fmt.Errorf("keyspace %d GC safe point %d exceeds frozen transaction safe point %d", id, gcSafePoint, source.txnSafePoint)
		}
		if conflict != nil {
			return nil
		}
		// Stamp even an equal T (or a verified initial zero) with a new revision.
		return wb.SetTxnSafePoint(id, source.txnSafePoint)
	})
	if err != nil {
		return err
	}
	return conflict
}

func (m *GCStateManager) loadStarterSource(ctx context.Context, migration *starterGCMigration, pair endpoint.GCSafePointReadResult) (starterScopeSource, error) {
	id := pair.KeyspaceID
	resp, err := m.etcdClient.Get(ctx, keypath.KeyspaceMetaPath(id))
	if err != nil {
		return starterScopeSource{}, err
	}
	if len(resp.Kvs) != 1 || len(resp.Kvs[0].Value) == 0 {
		return starterScopeSource{}, errs.ErrKeyspaceNotFound
	}
	row := resp.Kvs[0]
	meta := &keyspacepb.KeyspaceMeta{}
	if err := proto.Unmarshal(row.Value, meta); err != nil {
		return starterScopeSource{}, err
	}
	if meta.GetId() != id {
		return starterScopeSource{}, fmt.Errorf("metadata for keyspace %d has ID %d", id, meta.GetId())
	}
	// A queued independent-scope read may outlive its routing decision. Unified
	// requests use the null scope, and tombstoning removes the old group binding.
	// Neither change proves that the pending independent T was local.
	if meta.GetState() == keyspacepb.KeyspaceState_TOMBSTONE {
		return starterScopeSource{}, errs.ErrKeyspaceNotFound
	}
	if _, _, err := gcKeyspaceScope(id, meta, false); err != nil {
		return starterScopeSource{}, err
	}
	source := starterScopeSource{metaRevision: row.ModRevision, configRevision: -1}
	groupID, hasGroup := meta.Config[keyspace.MetaServiceGroupIDKey]
	if row.CreateRevision > migration.markerRevision || !hasGroup {
		// New tenants have no v1 state. The rollout verifies that the v1 default points to PD.
		source.txnSafePoint = pair.TxnSafePoint
		source.missing = pair.TxnSafePointRevision == 0
		return source, nil
	}
	if groupID == "" || !config.IsValidMetaServiceGroupID(groupID) || m.metaServiceGroupManager == nil {
		return starterScopeSource{}, fmt.Errorf("invalid or unavailable meta-service group %q", groupID)
	}
	// Isolate slow groups: one unreachable group must not monopolize all reads.
	slots, ok := migration.groupSlots.Load(groupID)
	if !ok {
		slots, _ = migration.groupSlots.LoadOrStore(groupID, make(chan struct{}, m.getConfig().GCStateCacheWarmupConcurrency))
	}
	reads := slots.(chan struct{})
	select {
	case <-ctx.Done():
		return starterScopeSource{}, ctx.Err()
	case reads <- struct{}{}:
		defer func() { <-reads }()
	}
	client, configRevision, err := m.metaServiceGroupManager.GetEtcdClientWithConfigRevision(ctx, groupID)
	if err != nil {
		return starterScopeSource{}, err
	}
	result, err := client.Get(ctx, keypath.TxnSafePointPath(id))
	if err != nil {
		return starterScopeSource{}, fmt.Errorf("read meta-service group %q: %w", groupID, err)
	}
	source.configRevision = configRevision
	source.missing = len(result.Kvs) == 0
	if !source.missing {
		source.txnSafePoint, err = strconv.ParseUint(string(result.Kvs[0].Value), 10, 64)
		if err != nil {
			return starterScopeSource{}, fmt.Errorf("invalid transaction safe point in meta-service group %q: %w", groupID, err)
		}
	}
	return source, nil
}
