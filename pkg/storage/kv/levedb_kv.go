// Copyright 2018 TiKV Project Authors.
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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// LevelDBKV is a kv store using LevelDB.
type LevelDBKV struct {
	*leveldb.DB
}

// NewLevelDBKV is used to store regions information.
func NewLevelDBKV(path string) (*LevelDBKV, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, errs.ErrLevelDBOpen.Wrap(err).GenWithStackByCause()
	}
	return &LevelDBKV{db}, nil
}

// Load gets a value for a given key.
func (kv *LevelDBKV) Load(key string) (string, error) {
	v, err := kv.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return "", nil
		}
		return "", errors.WithStack(err)
	}
	return string(v), err
}

// LoadRange gets a range of value for a given key range.
func (kv *LevelDBKV) LoadRange(startKey, endKey string, limit int) ([]string, []string, error) {
	iter := kv.NewIterator(&util.Range{Start: []byte(startKey), Limit: []byte(endKey)}, nil)
	keys := make([]string, 0, limit)
	values := make([]string, 0, limit)
	count := 0
	for iter.Next() {
		if limit > 0 && count >= limit {
			break
		}
		keys = append(keys, string(iter.Key()))
		values = append(values, string(iter.Value()))
		count++
	}
	iter.Release()
	return keys, values, nil
}

// Save stores a key-value pair.
func (kv *LevelDBKV) Save(key, value string) error {
	return errors.WithStack(kv.Put([]byte(key), []byte(value), nil))
}

// Remove deletes a key-value pair for a given key.
func (kv *LevelDBKV) Remove(key string) error {
	return errors.WithStack(kv.Delete([]byte(key), nil))
}

func (kv *LevelDBKV) CreateLowLevelTxn() LowLevelTxn {
	return &levelDBLowLevelTxnSimulator{
		kv: kv,
	}
}

// levelDBTxn implements kv.Txn.
// It utilizes leveldb.Batch to batch user operations to an atomic execution unit.
type levelDBTxn struct {
	kv  *LevelDBKV
	ctx context.Context
	// mu protects batch.
	mu    syncutil.Mutex
	batch *leveldb.Batch
}

// RunInTxn runs user provided function f in a transaction.
// If user provided function returns error, then transaction will not be committed.
func (kv *LevelDBKV) RunInTxn(ctx context.Context, f func(txn Txn) error) error {
	txn := &levelDBTxn{
		kv:    kv,
		ctx:   ctx,
		batch: new(leveldb.Batch),
	}
	err := f(txn)
	if err != nil {
		return err
	}
	return txn.commit()
}

// Save puts a save operation with target key value into levelDB batch.
func (txn *levelDBTxn) Save(key, value string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.batch.Put([]byte(key), []byte(value))
	return nil
}

// Remove puts a delete operation with target key into levelDB batch.
func (txn *levelDBTxn) Remove(key string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.batch.Delete([]byte(key))
	return nil
}

// Load executes base's load.
func (txn *levelDBTxn) Load(key string) (string, error) {
	return txn.kv.Load(key)
}

// LoadRange executes base's load range.
func (txn *levelDBTxn) LoadRange(key, endKey string, limit int) (keys []string, values []string, err error) {
	return txn.kv.LoadRange(key, endKey, limit)
}

// commit writes the batch constructed into levelDB.
func (txn *levelDBTxn) commit() error {
	// Check context first to make sure transaction is not cancelled.
	select {
	default:
	case <-txn.ctx.Done():
		return txn.ctx.Err()
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	return txn.kv.Write(txn.batch, nil)
}

type levelDBLowLevelTxnSimulator struct {
	kv           *LevelDBKV
	condition    []LowLevelTxnCondition
	onSuccessOps []LowLevelTxnOp
	onFailureOps []LowLevelTxnOp
}

func (t *levelDBLowLevelTxnSimulator) If(conditions ...LowLevelTxnCondition) LowLevelTxn {
	t.condition = append(t.condition, conditions...)
	return t
}

func (t *levelDBLowLevelTxnSimulator) Then(ops ...LowLevelTxnOp) LowLevelTxn {
	t.onSuccessOps = append(t.onSuccessOps, ops...)
	return t
}

func (t *levelDBLowLevelTxnSimulator) Else(ops ...LowLevelTxnOp) LowLevelTxn {
	t.onFailureOps = append(t.onFailureOps, ops...)
	return t
}

func (t *levelDBLowLevelTxnSimulator) Commit(_ctx context.Context) (res LowLevelTxnResult, err error) {
	txn, err := t.kv.DB.OpenTransaction()
	if err != nil {
		return LowLevelTxnResult{}, err
	}
	defer func() {
		// Set txn to nil when the function finished normally.
		// When the function encounters any error and returns early, the transaction will be discarded here.
		if txn != nil {
			txn.Discard()
		}
	}()

	succeeds := true
	for _, condition := range t.condition {
		value, err := t.kv.DB.Get([]byte(condition.Key), nil)
		valueStr := string(value)
		exists := true
		if err != nil {
			if err == leveldb.ErrNotFound {
				exists = false
			} else {
				return res, errors.WithStack(err)
			}
		}

		if !condition.CheckOnValue(valueStr, exists) {
			succeeds = false
			break
		}
	}

	ops := t.onSuccessOps
	if !succeeds {
		ops = t.onFailureOps
	}

	results := make([]LowLevelTxnResultItem, 0, len(ops))

	for _, operation := range ops {
		switch operation.OpType {
		case LowLevelOpPut:
			err = txn.Put([]byte(operation.Key), []byte(operation.Value), nil)
			if err != nil {
				return res, errors.WithStack(err)
			}
			results = append(results, LowLevelTxnResultItem{})
		case LowLevelOpDelete:
			err = txn.Delete([]byte(operation.Key), nil)
			if err != nil {
				return res, errors.WithStack(err)
			}
			results = append(results, LowLevelTxnResultItem{})
		case LowLevelOpGet:
			value, err := txn.Get([]byte(operation.Key), nil)
			result := LowLevelTxnResultItem{}
			if err != nil {
				if err != leveldb.ErrNotFound {
					return res, errors.WithStack(err)
				}
			} else {
				result.KeyValuePairs = append(result.KeyValuePairs, KeyValuePair{
					Key:   operation.Key,
					Value: string(value),
				})
			}
			results = append(results, result)
		case LowLevelOpGetRange:
			iter := txn.NewIterator(&util.Range{Start: []byte(operation.Key), Limit: []byte(operation.EndKey)}, nil)
			result := LowLevelTxnResultItem{}
			count := 0
			for iter.Next() {
				if operation.Limit > 0 && count >= operation.Limit {
					break
				}
				result.KeyValuePairs = append(result.KeyValuePairs, KeyValuePair{
					Key:   string(iter.Key()),
					Value: string(iter.Value()),
				})
				count++
			}
			iter.Release()
			results = append(results, result)
		default:
			panic(fmt.Sprintf("unknown operation type %v", operation.OpType))
		}
	}

	err = txn.Commit()
	if err != nil {
		return res, errors.WithStack(err)
	}
	// Avoid being discarded again in the defer block.
	txn = nil

	return LowLevelTxnResult{
		Succeeded: succeeds,
		Items:     results,
	}, nil
}
