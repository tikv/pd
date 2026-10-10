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

	"github.com/pingcap/errors"

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
		ops = append(ops,
			kv.RawTxnOp{Key: keypath.TxnSafePointPath(id), OpType: kv.RawTxnOpGet},
			kv.RawTxnOp{Key: keypath.GCSafePointPath(id), OpType: kv.RawTxnOpGet},
		)
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
		if len(item.KeyValuePairs) == 0 {
			continue
		}
		if len(item.KeyValuePairs) != 1 || item.KeyValuePairs[0].Key != ops[i].Key {
			return nil, errors.Errorf("invalid GC safe point read response for key %q", ops[i].Key)
		}
		values[i] = item.KeyValuePairs[0].Value
	}
	results := make([]GCSafePointReadResult, len(keyspaceIDs))
	for i, id := range keyspaceIDs {
		result := &results[i]
		result.KeyspaceID = id
		result.TxnSafePoint, result.Err = decodeTxnSafePoint(values[2*i])
		if result.Err != nil {
			continue
		}
		if id == constant.NullKeyspaceID {
			result.GCSafePoint, result.Err = decodeUnifiedGCSafePoint(values[2*i+1])
		} else {
			result.GCSafePoint, result.Err = decodeKeyspaceGCSafePoint(values[2*i+1])
		}
	}
	return results, nil
}
