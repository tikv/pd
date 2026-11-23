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

// Only revisions in PD etcd can be compared with the migration cutoff.
func (m *GCStateManager) needsStarterCheck(id uint32, txnRevision int64) bool {
	return m.migrationCutoff > 0 && id != constant.NullKeyspaceID && txnRevision <= m.migrationCutoff
}

// Call outside the manager lock. Competing first uses are resolved by the
// conditional write on T's revision.
func (m *GCStateManager) ensureMigratedTxnSafePoint(ctx context.Context, generation *gcStateGeneration, id uint32) error {
	if m.migrationCutoff == 0 {
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
		item := migrationItem{localRead: pairs[0]}
		if item.needsMigration(m) {
			m.migrateStarterBatch(ctx, generation, []*migrationItem{&item}, 0, nil)
		}
		err = item.err
	}
	if m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	return err
}

type migrationItem struct {
	index        int                            // Position in the loader batch before grouping by source.
	localRead    endpoint.GCSafePointReadResult // Safe points read from PD etcd before looking up the source.
	metaRevision int64                          // ModRevision of the keyspace metadata used to select the source.
	txnSafePoint uint64                         // Transaction safe point to install from the selected source.
	err          error                          // Error to report for this keyspace.
}

// A legacy local T may be behind G. Only compare the pair once T is confirmed.
func (item *migrationItem) needsMigration(m *GCStateManager) bool {
	pair := item.localRead
	item.err = pair.Err
	if item.err == nil {
		if !pair.TxnSafePointEmpty && m.needsStarterCheck(pair.KeyspaceID, pair.TxnSafePointRevision) {
			return true
		}
		item.err = validateStarterGCState(pair)
	}
	return false
}

// Process one source group at a time so each warmup worker has at most one
// group read in flight. Publish each group as soon as its writes finish.
func (m *GCStateManager) migrateStarterBatch(ctx context.Context, generation *gcStateGeneration, pending []*migrationItem, attempt int, finish func([]*migrationItem)) {
	metaRows, err := readStarterKeys(ctx, m.etcdClient, pending, keypath.KeyspaceMetaPath)
	if err != nil {
		for _, item := range pending {
			item.err = err
		}
		if finish != nil {
			finish(pending)
		}
		return
	}

	groups := make(map[string][]*migrationItem)
	for i, item := range pending {
		groupID, err := item.resolveSourceGroup(metaRows[i], m.migrationCutoff)
		item.err = err
		groups[groupID] = append(groups[groupID], item)
	}

	for groupID, items := range groups {
		m.migrateStarterGroup(ctx, generation, groupID, items, attempt)
		if finish != nil {
			finish(items)
		}
	}
}

// Read the frozen values together even on retry. After a failed commit, write
// the remaining scopes individually so a changing route cannot block its peers.
func (m *GCStateManager) migrateStarterGroup(ctx context.Context, generation *gcStateGeneration, groupID string, items []*migrationItem, attempt int) {
	configRevision, err := m.readSourceTxnSafePoints(ctx, groupID, items)
	ready := make([]*migrationItem, 0, len(items))
	for _, item := range items {
		if err != nil {
			item.err = err
		}
		if item.err == nil {
			ready = append(ready, item)
		}
	}
	writeBatchSize := endpoint.MaxGCSafePointBatchSize
	if attempt > 0 {
		writeBatchSize = 1
	}
	for batch := range slices.Chunk(ready, writeBatchSize) {
		if err := m.commitStarterBatch(ctx, generation, batch, configRevision); err != nil {
			// A failed CAS invalidates data conflicts observed before the commit too.
			for _, item := range batch {
				item.err = err
			}
		}
	}
	if attempt < 2 {
		if retry := m.reloadRetryableItems(ctx, items); len(retry) > 0 {
			m.migrateStarterBatch(ctx, generation, retry, attempt+1, nil)
		}
	}
}

// A CAS conflict or lost reply may already have installed or advanced T.
// Recheck the local revisions before looking up any frozen source again.
func (m *GCStateManager) reloadRetryableItems(ctx context.Context, items []*migrationItem) []*migrationItem {
	var ids []uint32
	var retryable []*migrationItem
	for _, item := range items {
		if errors.Is(item.err, errs.ErrEtcdTxnConflict) || errors.Is(item.err, errs.ErrEtcdTxnInternal) {
			ids = append(ids, item.localRead.KeyspaceID)
			retryable = append(retryable, item)
		}
	}
	if len(retryable) == 0 {
		return nil
	}
	pairs, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, ids)
	pending := retryable[:0]
	for i, item := range retryable {
		if err != nil {
			item.err = err
			continue
		}
		item.localRead = pairs[i]
		if item.needsMigration(m) {
			pending = append(pending, item)
		}
	}
	return pending
}

// readStarterKeys fetches one exact key per item at the same etcd revision.
func readStarterKeys(ctx context.Context, client *clientv3.Client, items []*migrationItem, path func(uint32) string) ([]*mvccpb.KeyValue, error) {
	ctx, cancel := context.WithTimeout(ctx, etcdutil.DefaultRequestTimeout)
	defer cancel()
	ops := make([]clientv3.Op, len(items))
	for i, item := range items {
		ops[i] = clientv3.OpGet(path(item.localRead.KeyspaceID))
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

// An empty group ID selects PD etcd as the source for this keyspace.
func (item *migrationItem) resolveSourceGroup(row *mvccpb.KeyValue, cutoff int64) (string, error) {
	if row == nil || len(row.Value) == 0 {
		return "", errs.ErrKeyspaceNotFound
	}
	meta := &keyspacepb.KeyspaceMeta{}
	if err := proto.Unmarshal(row.Value, meta); err != nil {
		return "", err
	}
	id := item.localRead.KeyspaceID
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
	groupID, hasGroup := meta.Config[keyspace.MetaServiceGroupIDKey]
	if row.CreateRevision > cutoff || !hasGroup {
		item.txnSafePoint = item.localRead.TxnSafePoint
		return "", nil
	}
	if groupID == "" || !config.IsValidMetaServiceGroupID(groupID) {
		return "", fmt.Errorf("invalid meta-service group %q for keyspace %d", groupID, id)
	}
	return groupID, nil
}

func (m *GCStateManager) readSourceTxnSafePoints(ctx context.Context, groupID string, items []*migrationItem) (int64, error) {
	if groupID == "" {
		return -1, nil // No external group configuration to check.
	}
	if m.metaServiceGroupManager == nil {
		return 0, fmt.Errorf("unavailable meta-service group %q", groupID)
	}
	client, configRevision, err := m.metaServiceGroupManager.GetEtcdClientWithConfigRevision(ctx, groupID)
	if err != nil {
		return 0, err
	}
	rows, err := readStarterKeys(ctx, client, items, keypath.TxnSafePointPath)
	if err != nil {
		return 0, fmt.Errorf("read meta-service group %q: %w", groupID, err)
	}
	for i, item := range items {
		item.txnSafePoint = 0
		if rows[i] != nil {
			value, err := strconv.ParseUint(string(rows[i].Value), 10, 64)
			if err != nil {
				item.err = fmt.Errorf("invalid transaction safe point for keyspace %d in meta-service group %q: %w", item.localRead.KeyspaceID, groupID, err)
			}
			item.txnSafePoint = value
		}
	}
	return configRevision, nil
}

func (m *GCStateManager) commitStarterBatch(ctx context.Context, generation *gcStateGeneration, items []*migrationItem, configRevision int64) error {
	ids := make([]uint32, len(items))
	for i, item := range items {
		ids[i] = item.localRead.KeyspaceID
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
			if err := wb.ExpectTxnSafePointRevision(id, item.localRead.TxnSafePointRevision); err != nil {
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
			case item.localRead.TxnSafePoint > item.txnSafePoint:
				item.err = fmt.Errorf("keyspace %d local transaction safe point %d exceeds frozen value %d", id, item.localRead.TxnSafePoint, item.txnSafePoint)
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
