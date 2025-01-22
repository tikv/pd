// Copyright 2017 TiKV Project Authors.
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

package kv

import (
	"context"

	"github.com/google/btree"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/utils/syncutil"
)

type memoryKV struct {
	syncutil.RWMutex
	tree *btree.BTreeG[memoryKVItem]
}

// NewMemoryKV returns an in-memory kvBase.
func NewMemoryKV() Base {
	return &memoryKV{
		tree: btree.NewG(2, func(i, j memoryKVItem) bool {
			return i.Less(&j)
		}),
	}
}

type memoryKVItem struct {
	key, value string
}

// Less compares two memoryKVItem.
func (s *memoryKVItem) Less(than *memoryKVItem) bool {
	return s.key < than.key
}

// Load loads the value for the key.
func (kv *memoryKV) Load(key string) (string, error) {
	kv.RLock()
	defer kv.RUnlock()
	item, ok := kv.tree.Get(memoryKVItem{key, ""})
	if !ok {
		return "", nil
	}
	return item.value, nil
}

// LoadRange loads the keys in the range of [key, endKey).
func (kv *memoryKV) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
	failpoint.Inject("withRangeLimit", func(val failpoint.Value) {
		rangeLimit, ok := val.(int)
		if ok && limit > rangeLimit {
			failpoint.Return(nil, nil, errors.Errorf("limit %d exceed max rangeLimit %d", limit, rangeLimit))
		}
	})
	kv.RLock()
	defer kv.RUnlock()
	keys := make([]string, 0, limit)
	values := make([]string, 0, limit)
	kv.tree.AscendRange(memoryKVItem{key, ""}, memoryKVItem{endKey, ""}, func(item memoryKVItem) bool {
		keys = append(keys, item.key)
		values = append(values, item.value)
		if limit > 0 {
			return len(keys) < limit
		}
		return true
	})
	return keys, values, nil
}

// Save saves the key-value pair.
func (kv *memoryKV) Save(key, value string) error {
	kv.Lock()
	defer kv.Unlock()
	kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
	return nil
}

// Remove removes the key.
func (kv *memoryKV) Remove(key string) error {
	kv.Lock()
	defer kv.Unlock()

	kv.tree.Delete(memoryKVItem{key, ""})
	return nil
}

// memTxn implements kv.Txn.
type memTxn struct {
	kv  *memoryKV
	ctx context.Context
	// mu protects ops.
	mu  syncutil.Mutex
	ops []*op
}

// op represents an Operation that memKV can execute.
type op struct {
	t   opType
	key string
	val string
}

type opType int

const (
	tPut opType = iota
	tDelete
)

// RunInTxn runs the user provided function f in a transaction.
// If user provided function returns error, then transaction will not be committed.
func (kv *memoryKV) RunInTxn(ctx context.Context, f func(txn Txn) error) error {
	txn := &memTxn{
		kv:  kv,
		ctx: ctx,
	}
	err := f(txn)
	if err != nil {
		return err
	}
	return txn.commit()
}

// Save appends a save operation to ops.
func (txn *memTxn) Save(key, value string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.ops = append(txn.ops, &op{
		t:   tPut,
		key: key,
		val: value,
	})
	return nil
}

// Remove appends a remove operation to ops.
func (txn *memTxn) Remove(key string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.ops = append(txn.ops, &op{
		t:   tDelete,
		key: key,
	})
	return nil
}

// Load executes base's load directly.
func (txn *memTxn) Load(key string) (string, error) {
	return txn.kv.Load(key)
}

// LoadRange executes base's load range directly.
func (txn *memTxn) LoadRange(key, endKey string, limit int) (keys []string, values []string, err error) {
	return txn.kv.LoadRange(key, endKey, limit)
}

// commit executes operations in ops.
func (txn *memTxn) commit() error {
	// Check context first to make sure transaction is not cancelled.
	select {
	default:
	case <-txn.ctx.Done():
		return txn.ctx.Err()
	}
	// Lock txn.mu to protect memTxn ops.
	txn.mu.Lock()
	defer txn.mu.Unlock()
	// Lock kv.lock to protect the execution of the batch,
	// making the execution atomic.
	txn.kv.Lock()
	defer txn.kv.Unlock()
	// Execute mutations in order.
	// Note: executions in mem_kv never fails.
	for _, op := range txn.ops {
		switch op.t {
		case tPut:
			txn.kv.tree.ReplaceOrInsert(memoryKVItem{op.key, op.val})
		case tDelete:
			txn.kv.tree.Delete(memoryKVItem{op.key, ""})
		}
	}
	return nil
}
