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

package endpoint

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// MaxGCSafePointBatchSize is the maximum number of scopes read in one transaction.
const MaxGCSafePointBatchSize = etcdutil.MaxEtcdTxnOps / 2

// GCSafePointReadResult contains one scope's safe points. The safe points are valid
// only when Err is nil.
type GCSafePointReadResult struct {
	KeyspaceID   uint32
	TxnSafePoint uint64
	GCSafePoint  uint64
	Err          error
}

// LoadGCSafePointPairs reads both safe points for each scope in one atomic read
// transaction. Results follow the input order. Missing keys and empty values have
// zero safe points. Decode errors belong to individual results; read or response
// errors fail the whole batch. Empty input performs no read. Duplicate IDs and
// batches larger than MaxGCSafePointBatchSize are rejected.
func (p GCStateProvider) LoadGCSafePointPairs(ctx context.Context, keyspaceIDs []uint32) ([]GCSafePointReadResult, error) {
	if len(keyspaceIDs) == 0 {
		return nil, nil
	}
	if len(keyspaceIDs) > MaxGCSafePointBatchSize {
		return nil, errors.Errorf("gc safe point batch exceeds maximum size %d", MaxGCSafePointBatchSize)
	}
	seen := make(map[uint32]struct{}, len(keyspaceIDs))
	ops := make([]kv.RawTxnOp, 0, len(keyspaceIDs)*2)
	for _, id := range keyspaceIDs {
		if _, ok := seen[id]; ok {
			return nil, errors.Errorf("duplicate keyspace ID %d in GC safe point batch", id)
		}
		seen[id] = struct{}{}
		pair := gcSafePointReadOps(id)
		ops = append(ops, pair[:]...)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	txn, err := p.storage.createRawTxnWithContext(ctx)
	if err != nil {
		return nil, err
	}
	response, err := txn.Then(ops...).Commit()
	if err != nil {
		return nil, err
	}
	if !response.Succeeded || len(response.Responses) != len(ops) {
		return nil, errors.New("invalid GC safe point batch response")
	}
	// Validate every point read before returning any scope's decoded values.
	values := make([]string, len(ops))
	for i, item := range response.Responses {
		values[i], err = gcSafePointReadValue(ops[i].Key, item)
		if err != nil {
			return nil, err
		}
	}
	results := make([]GCSafePointReadResult, len(keyspaceIDs))
	for i, id := range keyspaceIDs {
		results[i] = decodeGCSafePointPair(id, values[2*i], values[2*i+1])
	}
	return results, nil
}

func gcSafePointReadOps(keyspaceID uint32) [2]kv.RawTxnOp {
	return [2]kv.RawTxnOp{
		{Key: keypath.TxnSafePointPath(keyspaceID), OpType: kv.RawTxnOpGet},
		{Key: keypath.GCSafePointPath(keyspaceID), OpType: kv.RawTxnOpGet},
	}
}

func gcSafePointReadValue(key string, item kv.RawTxnResponseItem) (string, error) {
	if len(item.KeyValuePairs) == 0 {
		return "", nil
	}
	if len(item.KeyValuePairs) != 1 || item.KeyValuePairs[0].Key != key {
		return "", errors.Errorf("invalid GC safe point read response for key %q", key)
	}
	return item.KeyValuePairs[0].Value, nil
}

func decodeGCSafePointPair(keyspaceID uint32, txnValue, gcValue string) GCSafePointReadResult {
	result := GCSafePointReadResult{KeyspaceID: keyspaceID}
	result.TxnSafePoint, result.Err = decodeTxnSafePoint(txnValue)
	if result.Err != nil {
		return result
	}
	if keyspaceID == constant.NullKeyspaceID {
		result.GCSafePoint, result.Err = decodeUnifiedGCSafePoint(gcValue)
	} else {
		result.GCSafePoint, result.Err = decodeKeyspaceGCSafePoint(gcValue)
	}
	return result
}

// GCStateSnapshot contains the persisted safe points and requested barriers from one etcd snapshot.
type GCStateSnapshot struct {
	TxnSafePoint     uint64
	GCSafePoint      uint64
	GCBarriers       []*GCBarrier
	GlobalGCBarriers []*GlobalGCBarrier
}

// LoadGCStateSnapshot reads one GC scope and its requested barriers in a single linearizable transaction.
// The local barrier list omits the legacy gc_worker service safe point.
func (p GCStateProvider) LoadGCStateSnapshot(ctx context.Context, keyspaceID uint32, includeLocal, includeGlobal bool) (GCStateSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return GCStateSnapshot{}, err
	}
	pair := gcSafePointReadOps(keyspaceID)
	ops := make([]kv.RawTxnOp, 0, 4)
	ops = append(ops, pair[:]...)
	if includeLocal {
		ops = append(ops, kv.RawTxnOp{Key: keypath.GCBarrierPrefix(keyspaceID), EndKey: "\x00", OpType: kv.RawTxnOpGetRange})
	}
	if includeGlobal {
		ops = append(ops, kv.RawTxnOp{Key: keypath.GlobalGCBarrierPrefix(), EndKey: "\x00", OpType: kv.RawTxnOpGetRange})
	}
	txn, err := p.storage.createRawTxnWithContext(ctx)
	if err != nil {
		return GCStateSnapshot{}, err
	}
	response, err := txn.Then(ops...).Commit()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return GCStateSnapshot{}, ctxErr
	}
	if err != nil {
		return GCStateSnapshot{}, err
	}
	if !response.Succeeded || len(response.Responses) != len(ops) {
		return GCStateSnapshot{}, errs.ErrEtcdTxnResponse.GenWithStackByArgs("invalid GC state snapshot response")
	}
	txnValue, err := gcSafePointReadValue(ops[0].Key, response.Responses[0])
	if err != nil {
		return GCStateSnapshot{}, err
	}
	gcValue, err := gcSafePointReadValue(ops[1].Key, response.Responses[1])
	if err != nil {
		return GCStateSnapshot{}, err
	}
	safePoints := decodeGCSafePointPair(keyspaceID, txnValue, gcValue)
	if safePoints.Err != nil {
		return GCStateSnapshot{}, safePoints.Err
	}
	snapshot := GCStateSnapshot{TxnSafePoint: safePoints.TxnSafePoint, GCSafePoint: safePoints.GCSafePoint}
	barrierResponses := response.Responses[2:]
	if includeLocal {
		snapshot.GCBarriers, err = decodeLocalGCBarrierRange(ctx, barrierResponses[0])
		if err != nil {
			return GCStateSnapshot{}, err
		}
		barrierResponses = barrierResponses[1:]
	}
	if includeGlobal {
		snapshot.GlobalGCBarriers, err = decodeGlobalGCBarrierRange(ctx, barrierResponses[0])
		if err != nil {
			return GCStateSnapshot{}, err
		}
	}
	return snapshot, nil
}

func decodeLocalGCBarrierRange(ctx context.Context, response kv.RawTxnResponseItem) ([]*GCBarrier, error) {
	if len(response.KeyValuePairs) == 0 {
		return nil, nil
	}
	barriers := make([]*GCBarrier, 0, len(response.KeyValuePairs))
	for _, pair := range response.KeyValuePairs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		serviceSafePoint, err := decodeGCBarrierValue[*ServiceSafePoint](pair.Value)
		if err != nil {
			return nil, err
		}
		if serviceSafePoint == nil {
			return nil, errors.New("invalid null GC barrier value")
		}
		if serviceSafePoint.ServiceID == keypath.GCWorkerServiceSafePointID {
			continue
		}
		barriers = append(barriers, gcBarrierFromServiceSafePoint(serviceSafePoint))
	}
	return barriers, nil
}

func decodeGlobalGCBarrierRange(ctx context.Context, response kv.RawTxnResponseItem) ([]*GlobalGCBarrier, error) {
	if len(response.KeyValuePairs) == 0 {
		return nil, nil
	}
	barriers := make([]*GlobalGCBarrier, 0, len(response.KeyValuePairs))
	for _, pair := range response.KeyValuePairs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		barrier, err := decodeGCBarrierValue[*GlobalGCBarrier](pair.Value)
		if err != nil {
			return nil, err
		}
		barriers = append(barriers, barrier)
	}
	return barriers, nil
}

func decodeGCBarrierValue[T any](value string) (T, error) {
	var barrier T
	if err := json.Unmarshal([]byte(value), &barrier); err != nil {
		return barrier, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	return barrier, nil
}
