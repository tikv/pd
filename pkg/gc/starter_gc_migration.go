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
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

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
}

func (s *starterGCMigration) needsCheck(id uint32, txnRevision int64) bool {
	return s != nil && id != constant.NullKeyspaceID && txnRevision <= s.markerRevision
}

// Call outside the manager lock. Each caller may race another first use. The
// conditional write and a fresh local read on conflict decide which T to use.
func (m *GCStateManager) ensureMigratedTxnSafePoint(ctx context.Context, generation *gcStateGeneration, id uint32) error {
	if generation == nil || generation.migration == nil || id == constant.NullKeyspaceID {
		return nil
	}
	if m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	if _, ok := m.gcStateCache.load(id); ok && m.activeGeneration.Load() == generation {
		return nil
	}
	return m.migrateStarterIDs(ctx, generation, []uint32{id})
}

// ids fits in one GC state read batch.
func (m *GCStateManager) migrateStarterIDs(ctx context.Context, generation *gcStateGeneration, ids []uint32) error {
	ctx, cancel := generation.loadContext(ctx)
	defer cancel()
	pairs, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, ids)
	if err == nil {
		items := make([]starterMigrationItem, len(pairs))
		for i, pair := range pairs {
			items[i].target = pair
		}
		m.migrateStarterBatch(ctx, generation, items, nil)
		for i := range items {
			if items[i].err != nil {
				err = items[i].err
				break
			}
		}
	}
	if m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	return err
}

type starterMigrationItem struct {
	index          int // slot in the caller's original batch
	target         endpoint.GCSafePointReadResult
	metaRevision   int64
	configRevision int64 // -1 when this scope did not use group configuration
	txnSafePoint   uint64
	missing        bool
	err            error
}

// When supplied, finish publishes each result as its group completes.
func (m *GCStateManager) migrateStarterBatch(ctx context.Context, generation *gcStateGeneration, items []starterMigrationItem, finish func(int, error)) {
	pending := make([]*starterMigrationItem, len(items))
	for i := range items {
		pending[i] = &items[i]
	}
	for attempt := 1; len(pending) > 0; attempt++ {
		retry := func(item *starterMigrationItem) bool {
			return attempt < 3 && (errors.Is(item.err, errs.ErrEtcdTxnConflict) || errors.Is(item.err, errs.ErrEtcdTxnInternal))
		}
		m.migrateStarterOnce(ctx, generation, pending, func(item *starterMigrationItem, err error) {
			item.err = err
			if finish != nil && !retry(item) {
				finish(item.index, err)
			}
		})
		pending = slices.DeleteFunc(pending, func(item *starterMigrationItem) bool { return !retry(item) })
		if len(pending) == 0 {
			return
		}

		// A conditional write may have lost to a newer T, or committed despite a lost reply.
		// Check the local revisions before consulting any frozen source again.
		ids := make([]uint32, len(pending))
		for i, item := range pending {
			ids[i] = item.target.KeyspaceID
		}
		pairs, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, ids)
		for i, item := range pending {
			if err != nil {
				item.target.Err = err
			} else {
				item.target = pairs[i]
			}
		}
	}
}

func (m *GCStateManager) migrateStarterOnce(ctx context.Context, generation *gcStateGeneration, pending []*starterMigrationItem, finish func(*starterMigrationItem, error)) {
	routes := make([]*starterMigrationItem, 0, len(pending))
	for _, item := range pending {
		pair := item.target
		switch {
		case m.activeGeneration.Load() != generation:
			finish(item, errs.ErrNotLeader)
		case ctx.Err() != nil:
			finish(item, ctx.Err())
		case pair.Err != nil:
			finish(item, pair.Err)
		case pair.TxnSafePointEmpty || !generation.migration.needsCheck(pair.KeyspaceID, pair.TxnSafePointRevision):
			finish(item, validateStarterGCState(pair))
		default:
			routes = append(routes, item)
		}
	}
	if len(routes) == 0 {
		return
	}
	rows, err := readStarterKeys(ctx, m.etcdClient, routes, keypath.KeyspaceMetaPath)
	groups := make(map[string][]*starterMigrationItem)
	for i, item := range routes {
		if err != nil {
			finish(item, err)
			continue
		}
		group, err := item.route(rows[i], generation.migration.markerRevision)
		if err != nil {
			finish(item, err)
			continue
		}
		groups[group] = append(groups[group], item)
	}

	// A slow source must not hold up verified scopes in a different group.
	var workers sync.WaitGroup
	for group, items := range groups {
		run := func() {
			var err error
			if group != "" {
				err = m.readStarterGroup(ctx, group, items)
			}
			for _, item := range items {
				if err != nil {
					finish(item, err)
				} else if item.err != nil {
					finish(item, item.err)
				} else {
					finish(item, m.commitStarterScope(ctx, generation, item))
				}
			}
		}
		if len(groups) == 1 {
			run()
		} else {
			workers.Go(run)
		}
	}
	workers.Wait()
}

// readStarterKeys fetches one exact key per item at the same etcd revision.
func readStarterKeys(ctx context.Context, client *clientv3.Client, items []*starterMigrationItem, path func(uint32) string) ([]*mvccpb.KeyValue, error) {
	ctx, cancel := context.WithTimeout(ctx, etcdutil.DefaultRequestTimeout)
	defer cancel()
	ops := make([]clientv3.Op, len(items))
	for i, item := range items {
		ops[i] = clientv3.OpGet(path(item.target.KeyspaceID))
	}
	resp, err := client.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return nil, err
	}
	if len(resp.Responses) != len(ops) {
		return nil, errors.New("incomplete starter GC batch response")
	}
	rows := make([]*mvccpb.KeyValue, len(items))
	for i, op := range resp.Responses {
		data := op.GetResponseRange()
		if data == nil || len(data.Kvs) > 1 {
			return nil, fmt.Errorf("invalid starter GC range response for %q", ops[i].KeyBytes())
		}
		if len(data.Kvs) == 1 {
			row := data.Kvs[0]
			if row == nil || !bytes.Equal(row.Key, ops[i].KeyBytes()) || row.ModRevision <= 0 {
				return nil, fmt.Errorf("invalid starter GC key for %q", ops[i].KeyBytes())
			}
			rows[i] = row
		}
	}
	return rows, nil
}

// An empty group selects the verified PD default or a tenant created after the marker.
func (item *starterMigrationItem) route(row *mvccpb.KeyValue, marker int64) (string, error) {
	if row == nil || len(row.Value) == 0 {
		return "", errs.ErrKeyspaceNotFound
	}
	meta := &keyspacepb.KeyspaceMeta{}
	if err := proto.Unmarshal(row.Value, meta); err != nil {
		return "", err
	}
	id := item.target.KeyspaceID
	if meta.GetId() != id {
		return "", fmt.Errorf("metadata for keyspace %d has ID %d", id, meta.GetId())
	}
	if meta.GetState() == keyspacepb.KeyspaceState_TOMBSTONE {
		return "", errs.ErrKeyspaceNotFound
	}
	if _, _, err := gcKeyspaceScope(id, meta, false); err != nil {
		return "", err
	}
	item.err = nil
	item.metaRevision = row.ModRevision
	item.configRevision = -1
	group, hasGroup := meta.Config[keyspace.MetaServiceGroupIDKey]
	if row.CreateRevision > marker || !hasGroup {
		item.txnSafePoint = item.target.TxnSafePoint
		item.missing = item.target.TxnSafePointRevision == 0
		return "", nil
	}
	if group == "" || !config.IsValidMetaServiceGroupID(group) {
		return "", fmt.Errorf("invalid meta-service group %q for keyspace %d", group, id)
	}
	return group, nil
}

func (m *GCStateManager) readStarterGroup(ctx context.Context, group string, items []*starterMigrationItem) error {
	if m.metaServiceGroupManager == nil {
		return fmt.Errorf("unavailable meta-service group %q", group)
	}
	client, revision, err := m.metaServiceGroupManager.GetEtcdClientWithConfigRevision(ctx, group)
	if err != nil {
		return err
	}
	rows, err := readStarterKeys(ctx, client, items, keypath.TxnSafePointPath)
	if err != nil {
		return fmt.Errorf("read meta-service group %q: %w", group, err)
	}
	for i, item := range items {
		item.configRevision = revision
		item.missing = rows[i] == nil
		item.txnSafePoint = 0
		if !item.missing {
			value, err := strconv.ParseUint(string(rows[i].Value), 10, 64)
			if err != nil {
				item.err = fmt.Errorf("invalid transaction safe point for keyspace %d in meta-service group %q: %w", item.target.KeyspaceID, group, err)
			}
			item.txnSafePoint = value
		}
	}
	return nil
}

func (m *GCStateManager) commitStarterScope(ctx context.Context, generation *gcStateGeneration, source *starterMigrationItem) error {
	pair, s := source.target, generation.migration
	id := pair.KeyspaceID
	failpoint.InjectCall("starterGCMigrationSourceLoaded", id)

	// A group Get may block independently of operations on other keyspaces.
	// Hold the manager lock only for the short target-side transaction.
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	// A data conflict is authoritative only if its T, G and route are still
	// current. Commit the comparisons even when there is no T to write.
	var conflict error
	err := m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
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
