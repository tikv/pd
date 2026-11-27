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
	"strconv"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	pingcaperrors "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server/config"
)

// Only revisions in PD etcd can be compared with the cutoff. A legacy local
// transaction safe point may be behind G until its source has been checked.
func needsStarterMigration(pair endpoint.GCSafePointReadResult, cutoff int64) (bool, error) {
	if pair.Err != nil || cutoff == 0 || pair.KeyspaceID == constant.NullKeyspaceID {
		return false, pair.Err
	}
	if !pair.TxnSafePointEmpty && pair.TxnSafePointRevision <= cutoff {
		return true, nil
	}
	return false, validateStarterGCState(pair)
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
	pairs, err := m.loadLocalSafePointPairs(ctx, []uint32{id})
	if err == nil {
		item := migrationItem{localRead: pairs[0]}
		var pending bool
		pending, err = needsStarterMigration(item.localRead, m.migrationCutoff)
		if pending {
			m.migrateStarterBatch(ctx, generation, []*migrationItem{&item}, 0, nil)
			err = item.err
		}
	}
	if m.activeGeneration.Load() != generation {
		return errs.ErrNotLeader
	}
	return err
}

type migrationItem struct {
	index              int                            // Position in the loader batch before grouping by source.
	localRead          endpoint.GCSafePointReadResult // Safe points read from PD etcd before looking up the source.
	metaRevision       int64                          // ModRevision of the keyspace metadata used to select the source.
	sourceTxnSafePoint uint64                         // Frozen transaction safe point read from the selected source.
	err                error                          // Error to report for this keyspace.
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
		if err != nil {
			err = &keyspaceGCStateLoadError{err}
		}
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

// Each source group is a subset of the bounded loader batch. Retry its pending
// items together after refreshing local revisions and resolving their routes.
func (m *GCStateManager) migrateStarterGroup(ctx context.Context, generation *gcStateGeneration, groupID string, items []*migrationItem, attempt int) {
	configRevision, err := m.loadSourceTxnSafePoints(ctx, groupID, items)
	ready := make([]*migrationItem, 0, len(items))
	for _, item := range items {
		if err != nil {
			item.err = err
		} else if item.err == nil {
			ready = append(ready, item)
		}
	}
	if err := m.commitStarterBatch(ctx, generation, ready, groupID, configRevision); err != nil {
		// A failed CAS invalidates data conflicts observed before the commit too.
		for _, item := range ready {
			item.err = err
		}
	}
	// The last commit can also succeed without a reply. Recheck its revision
	// even when there are no more write attempts.
	if retry := m.reloadRetryableItems(ctx, items); attempt < 2 && len(retry) > 0 {
		m.migrateStarterBatch(ctx, generation, retry, attempt+1, nil)
	}
}

// A CAS conflict or lost reply may already have installed or advanced T.
// Recheck the local revisions before looking up any frozen source again.
// Items still pending retain their errors until another attempt.
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
	pairs, err := m.loadLocalSafePointPairs(ctx, ids)
	pending := retryable[:0]
	for i, item := range retryable {
		if err != nil {
			item.err = err
			continue
		}
		item.localRead = pairs[i]
		needsMigration, err := needsStarterMigration(item.localRead, m.migrationCutoff)
		if needsMigration {
			pending = append(pending, item)
		} else {
			item.err = err
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
	groupID := meta.Config[keyspace.MetaServiceGroupIDKey]
	if row.CreateRevision > cutoff || groupID == "" {
		return "", nil
	}
	if !config.IsValidMetaServiceGroupID(groupID) {
		return "", fmt.Errorf("invalid meta-service group %q for keyspace %d", groupID, id)
	}
	return groupID, nil
}

func (m *GCStateManager) loadSourceTxnSafePoints(ctx context.Context, groupID string, items []*migrationItem) (int64, error) {
	if groupID == "" {
		for _, item := range items {
			item.sourceTxnSafePoint = item.localRead.TxnSafePoint
		}
		return 0, nil
	}
	if m.metaServiceGroupManager == nil {
		return 0, fmt.Errorf("unavailable meta-service group %q", groupID)
	}
	client, configRevision, err := m.metaServiceGroupManager.GetEtcdClientWithConfigRevision(ctx, groupID)
	if err != nil {
		if errors.Is(err, errs.ErrUnknownMetaServiceGroup) || errors.Is(err, errs.ErrJSONUnmarshal) {
			return 0, &keyspaceGCStateLoadError{err}
		}
		return 0, err
	}
	rows, err := readStarterKeys(ctx, client, items, keypath.TxnSafePointPath)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, &keyspaceGCStateLoadError{fmt.Errorf("read meta-service group %q: %w", groupID, err)}
	}
	for i, item := range items {
		item.sourceTxnSafePoint = 0
		if rows[i] != nil {
			value, err := strconv.ParseUint(string(rows[i].Value), 10, 64)
			if err != nil {
				msg := fmt.Sprintf("invalid transaction safe point for keyspace %d in meta-service group %q", item.localRead.KeyspaceID, groupID)
				item.err = &keyspaceGCStateLoadError{pingcaperrors.WithMessage(err, msg)}
			}
			item.sourceTxnSafePoint = value
		}
	}
	return configRevision, nil
}

func (m *GCStateManager) commitStarterBatch(ctx context.Context, generation *gcStateGeneration, items []*migrationItem, groupID string, configRevision int64) error {
	if len(items) == 0 {
		return nil
	}
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
	// Retain the exact G used by each fallback. Only a successful CAS confirms
	// that those observations still describe the committed route and values.
	type migrationConflict struct {
		item        *migrationItem
		gcSafePoint uint64
	}
	var conflicts []migrationConflict
	// The source T stays frozen during migration. If this generation retires
	// before etcd commits, a successful CAS still installs a valid migration
	// result. A competing write to T changes its revision and rejects this CAS.
	err := m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		if groupID != "" {
			if err := wb.ExpectConfigRevision(configRevision); err != nil {
				return err
			}
		}
		// Read G after the transaction captures the GC state revision. Any
		// intervening GC advancement must invalidate the batch at commit.
		current, err := m.loadLocalSafePointPairs(ctx, ids)
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
			localT, frozenT := item.localRead.TxnSafePoint, item.sourceTxnSafePoint
			chosenT := max(localT, frozenT)
			switch {
			case current[i].Err != nil:
				item.err = current[i].Err
			case current[i].GCSafePoint > chosenT:
				item.err = &keyspaceGCStateLoadError{fmt.Errorf("keyspace %d GC safe point %d exceeds selected transaction safe point %d (local %d, frozen %d)", id, current[i].GCSafePoint, chosenT, localT, frozenT)}
			default:
				// Stamp even an equal T (or a verified zero) with a new revision.
				if err := wb.SetTxnSafePoint(id, chosenT); err != nil {
					return err
				}
				// The pre-cutoff local T should never exceed the frozen T.
				// Preserve the higher local value as a fallback to avoid regression.
				if localT > frozenT {
					conflicts = append(conflicts, migrationConflict{item, current[i].GCSafePoint})
				}
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errs.ErrEtcdTxnInternal) {
		return err
	}
	message := "Starter GC migration conflict"
	if err != nil {
		message = "Starter GC migration conflict with uncertain commit outcome"
	}
	for _, conflict := range conflicts {
		local := conflict.item.localRead
		log.Error(message,
			zap.Uint32("keyspace-id", local.KeyspaceID), zap.String("meta-service-group-id", groupID),
			zap.Uint64("local-txn-safe-point", local.TxnSafePoint), zap.Uint64("local-gc-safe-point", conflict.gcSafePoint),
			zap.Uint64("frozen-txn-safe-point", conflict.item.sourceTxnSafePoint), zap.Uint64("chosen-txn-safe-point", local.TxnSafePoint),
			zap.NamedError("commit-error", err))
	}
	return err
}
