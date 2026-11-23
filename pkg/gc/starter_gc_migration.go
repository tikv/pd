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

// Only revisions in PD etcd can be compared with the upgrade marker.
func (m *GCStateManager) needsStarterCheck(id uint32, txnRevision int64) bool {
	return m.markerRevision > 0 && id != constant.NullKeyspaceID && txnRevision <= m.markerRevision
}

// Call outside the manager lock. Competing first uses are resolved by the
// conditional write on T's revision.
func (m *GCStateManager) ensureMigratedTxnSafePoint(ctx context.Context, generation *gcStateGeneration, id uint32) error {
	if m.markerRevision == 0 {
		return nil
	}
	if generation == nil || m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	if id == constant.NullKeyspaceID {
		return nil
	}
	if _, cached := m.gcStateCache.load(id); cached && m.activeGeneration.Load() == generation {
		return nil
	}
	ctx, cancel := generation.loadContext(ctx)
	defer cancel()
	pairs, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, []uint32{id})
	if err == nil {
		item := starterMigrationItem{target: pairs[0]}
		if item.needsMigration(m) {
			m.migrateStarterBatch(ctx, generation, []*starterMigrationItem{&item}, 0, nil)
		}
		err = item.err
	}
	if m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	return err
}

type starterMigrationItem struct {
	index        int // slot in the loader's original batch
	target       endpoint.GCSafePointReadResult
	metaRevision int64
	txnSafePoint uint64
	err          error
}

// A legacy local T may be behind G. Only compare the pair once T is confirmed.
func (item *starterMigrationItem) needsMigration(m *GCStateManager) bool {
	pair := item.target
	item.err = pair.Err
	if item.err == nil {
		if !pair.TxnSafePointEmpty && m.needsStarterCheck(pair.KeyspaceID, pair.TxnSafePointRevision) {
			return true
		}
		item.err = validateStarterGCState(pair)
	}
	return false
}

// Each group completes independently. The loader can publish the group as soon
// as its conditional writes finish, even if another group is unreachable.
func (m *GCStateManager) migrateStarterBatch(ctx context.Context, generation *gcStateGeneration, pending []*starterMigrationItem, attempt int, finish func([]*starterMigrationItem)) {
	rows, err := readStarterKeys(ctx, m.etcdClient, pending, keypath.KeyspaceMetaPath)
	if err != nil {
		for _, item := range pending {
			item.err = err
		}
		if finish != nil {
			finish(pending)
		}
		return
	}

	groups := make(map[string][]*starterMigrationItem)
	for i, item := range pending {
		group, err := item.route(rows[i], m.markerRevision)
		item.err = err
		groups[group] = append(groups[group], item)
	}

	var workers sync.WaitGroup
	for group, items := range groups {
		run := func() {
			m.migrateStarterGroup(ctx, generation, group, items, attempt)
			if finish != nil {
				finish(items)
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

// Read the frozen values together even on retry. After a failed commit, write
// the remaining scopes individually so a changing route cannot block its peers.
func (m *GCStateManager) migrateStarterGroup(ctx context.Context, generation *gcStateGeneration, group string, items []*starterMigrationItem, attempt int) {
	configRevision, err := m.readStarterGroup(ctx, group, items)
	ready := make([]*starterMigrationItem, 0, len(items))
	for _, item := range items {
		if err != nil {
			item.err = err
		}
		if item.err == nil {
			ready = append(ready, item)
		}
	}
	size := endpoint.MaxGCSafePointBatchSize
	if attempt > 0 {
		size = 1
	}
	for batch := range slices.Chunk(ready, size) {
		if err := m.commitStarterBatch(ctx, generation, batch, configRevision); err != nil {
			// A failed CAS invalidates data conflicts observed before the commit too.
			for _, item := range batch {
				item.err = err
			}
		}
	}
	if attempt < 2 {
		if retry := m.reloadStarterConflicts(ctx, items); len(retry) > 0 {
			m.migrateStarterBatch(ctx, generation, retry, attempt+1, nil)
		}
	}
}

// A CAS conflict or lost reply may already have installed or advanced T.
// Recheck the local revisions before looking up any frozen source again.
func (m *GCStateManager) reloadStarterConflicts(ctx context.Context, items []*starterMigrationItem) []*starterMigrationItem {
	var ids []uint32
	var conflicts []*starterMigrationItem
	for _, item := range items {
		if errors.Is(item.err, errs.ErrEtcdTxnConflict) || errors.Is(item.err, errs.ErrEtcdTxnInternal) {
			ids = append(ids, item.target.KeyspaceID)
			conflicts = append(conflicts, item)
		}
	}
	if len(conflicts) == 0 {
		return nil
	}
	pairs, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, ids)
	pending := conflicts[:0]
	for i, item := range conflicts {
		if err != nil {
			item.err = err
			continue
		}
		item.target = pairs[i]
		if item.needsMigration(m) {
			pending = append(pending, item)
		}
	}
	return pending
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
	item.metaRevision = row.ModRevision
	group, hasGroup := meta.Config[keyspace.MetaServiceGroupIDKey]
	if row.CreateRevision > marker || !hasGroup {
		item.txnSafePoint = item.target.TxnSafePoint
		return "", nil
	}
	if group == "" || !config.IsValidMetaServiceGroupID(group) {
		return "", fmt.Errorf("invalid meta-service group %q for keyspace %d", group, id)
	}
	return group, nil
}

func (m *GCStateManager) readStarterGroup(ctx context.Context, group string, items []*starterMigrationItem) (int64, error) {
	if group == "" {
		return -1, nil // No external group configuration to check.
	}
	if m.metaServiceGroupManager == nil {
		return 0, fmt.Errorf("unavailable meta-service group %q", group)
	}
	client, revision, err := m.metaServiceGroupManager.GetEtcdClientWithConfigRevision(ctx, group)
	if err != nil {
		return 0, err
	}
	rows, err := readStarterKeys(ctx, client, items, keypath.TxnSafePointPath)
	if err != nil {
		return 0, fmt.Errorf("read meta-service group %q: %w", group, err)
	}
	for i, item := range items {
		item.txnSafePoint = 0
		if rows[i] != nil {
			value, err := strconv.ParseUint(string(rows[i].Value), 10, 64)
			if err != nil {
				item.err = fmt.Errorf("invalid transaction safe point for keyspace %d in meta-service group %q: %w", item.target.KeyspaceID, group, err)
			}
			item.txnSafePoint = value
		}
	}
	return revision, nil
}

func (m *GCStateManager) commitStarterBatch(ctx context.Context, generation *gcStateGeneration, items []*starterMigrationItem, configRevision int64) error {
	ids := make([]uint32, len(items))
	for i, item := range items {
		ids[i] = item.target.KeyspaceID
	}
	failpoint.InjectCall("starterGCMigrationSourceLoaded", ids[0])
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	return m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		if configRevision >= 0 {
			if err := wb.ExpectConfigRevision(configRevision); err != nil {
				return err
			}
		}
		// Read G after the transaction captures the GC state revision. Any
		// intervening GC advancement must invalidate the batch at commit.
		current, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, ids)
		if err != nil {
			return err
		}
		for i, item := range items {
			id := ids[i]
			if err := wb.ExpectTxnSafePointRevision(id, item.target.TxnSafePointRevision); err != nil {
				return err
			}
			if err := wb.ExpectKeyspaceMetaRevision(id, item.metaRevision); err != nil {
				return err
			}
			// Keep the comparisons even for conflicts. The error is authoritative
			// only if T, G and the route are still current when this batch commits.
			switch {
			case current[i].Err != nil:
				item.err = current[i].Err
			case item.target.TxnSafePoint > item.txnSafePoint:
				item.err = fmt.Errorf("keyspace %d local transaction safe point %d exceeds frozen value %d", id, item.target.TxnSafePoint, item.txnSafePoint)
			case current[i].GCSafePoint > item.txnSafePoint:
				item.err = fmt.Errorf("keyspace %d GC safe point %d exceeds frozen transaction safe point %d", id, current[i].GCSafePoint, item.txnSafePoint)
			default:
				// Stamp even an equal T (or a verified zero) with a new revision.
				if err := wb.SetTxnSafePoint(id, item.txnSafePoint); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
