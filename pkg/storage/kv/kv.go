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

import "context"

// LowLevelTxnCmpType represents the comparison type that is used in the condition of LowLevelTxn.
type LowLevelTxnCmpType int

// LowLevelTxnOpType represents the operation type that is used in the operations (either `Then` branch or `Else`
// branch) of LowLevelTxn.
type LowLevelTxnOpType int

// nolint:revive
const (
	LowLevelCmpEqual LowLevelTxnCmpType = iota
	LowLevelCmpNotEqual
	LowLevelCmpLess
	LowLevelCmpGreater
	LowLevelCmpExists
	LowLevelCmpNotExists
)

// nolint:revive
const (
	LowLevelOpPut LowLevelTxnOpType = iota
	LowLevelOpDelete
	LowLevelOpGet
	LowLevelOpGetRange
)

// LowLevelTxnCondition represents a condition in a LowLevelTxn.
type LowLevelTxnCondition struct {
	Key     string
	CmpType LowLevelTxnCmpType
	// The value to compare with. It's not used when CmpType is LowLevelCmpExists or LowLevelCmpNotExists.
	Value string
}

// CheckOnValue checks whether the condition is satisfied on the given value.
func (c *LowLevelTxnCondition) CheckOnValue(value string, exists bool) bool {
	switch c.CmpType {
	case LowLevelCmpEqual:
		if exists && value == c.Value {
			return true
		}
	case LowLevelCmpNotEqual:
		if exists && value != c.Value {
			return true
		}
	case LowLevelCmpLess:
		if exists && value < c.Value {
			return true
		}
	case LowLevelCmpGreater:
		if exists && value > c.Value {
			return true
		}
	case LowLevelCmpExists:
		if exists {
			return true
		}
	case LowLevelCmpNotExists:
		if !exists {
			return true
		}
	default:
		panic("unreachable")
	}
	return false
}

// LowLevelTxnOp represents an operation in a LowLevelTxn's `Then` or `Else` branch and will be executed according to
// the result of checking conditions.
type LowLevelTxnOp struct {
	Key    string
	OpType LowLevelTxnOpType
	Value  string
	// The end key when the OpType is LowLevelOpGetRange.
	EndKey string
	// The limit of the keys to get when the OpType is LowLevelOpGetRange.
	Limit int
}

// KeyValuePair represents a pair of key and value.
type KeyValuePair struct {
	Key   string
	Value string
}

// LowLevelTxnResultItem represents a single result of a read operation in a LowLevelTxn.
type LowLevelTxnResultItem struct {
	KeyValuePairs []KeyValuePair
}

// LowLevelTxnResult represents the result of a LowLevelTxn. The results of operations in `Then` or `Else` branches
// will be listed in `ResultItems` in the same order as the operations are added.
// For Put or Delete operations, its corresponding result is the previous value before writing.
type LowLevelTxnResult struct {
	Succeeded bool
	// The results of each operation in the `Then` branch or the `Else` branch of a transaction, depending on
	// whether `Succeeded`. The i-th result belongs to the i-th operation added to the executed branch.
	// * For Put or Delete operations, the result is empty.
	// * For Get operations, the result contains a key-value pair representing the get result. In case the key
	//   does not exist, its `KeyValuePairs` field will be empty.
	// * For GetRange operations, the result is a list of key-value pairs containing key-value paris that are scanned.
	ResultItems []LowLevelTxnResultItem
}

// LowLevelTxn is a low-level transaction interface. It follows the same pattern of etcd's transaction
// API. When the backend is etcd, it simply calls etcd's equivalent APIs internally. Otherwise, the
// behavior is simulated.
// Considering that in different backends, the kv pairs may not have equivalent property of etcd's
// version, create-time, etc., the abstracted LowLevelTxn interface does not support comparing on them.
// It only supports checking the value or whether the key exists.
// Avoid reading/writing the same key multiple times in a single transaction, otherwise the behavior
// would be undefined.
type LowLevelTxn interface {
	If(conditions ...LowLevelTxnCondition) LowLevelTxn
	Then(ops ...LowLevelTxnOp) LowLevelTxn
	Else(ops ...LowLevelTxnOp) LowLevelTxn
	Commit(ctx context.Context) (LowLevelTxnResult, error)
}

// BaseReadWrite is the API set, shared by Base and Txn interfaces, that provides basic KV read and write operations.
type BaseReadWrite interface {
	Save(key, value string) error
	Remove(key string) error
	Load(key string) (string, error)
	LoadRange(key, endKey string, limit int) (keys []string, values []string, err error)
}

// Txn bundles multiple operations into a single executable unit.
// It enables kv to atomically apply a set of updates.
type Txn interface {
	BaseReadWrite
}

// Base is an abstract interface for load/save pd cluster data.
type Base interface {
	BaseReadWrite
	// RunInTxn runs the user provided function in a Transaction.
	// If user provided function f returns a non-nil error, then
	// transaction will not be committed, the same error will be
	// returned by RunInTxn.
	// Otherwise, it returns the error occurred during the
	// transaction.
	//
	// This is a highly-simplified transaction interface. As
	// etcd's transaction API is quite limited, it's hard to use it
	// to provide a complete transaction model as how a normal database
	// does. So when this API is running on etcd backend, each read on
	// `txn` implicitly constructs a condition.
	// (ref: https://etcd.io/docs/v3.5/learning/api/#transaction)
	// When reading a range using `LoadRange`, for each key found in the
	// range there will be a condition constructed. Be aware of the
	// possibility of causing phantom read.
	// RunInTxn may not suit all use cases. When RunInTxn is found not
	// improper to use, consider using CreateLowLevelTxn instead.
	//
	// Note that transaction are not committed until RunInTxn returns nil.
	// Note:
	// 1. Load and LoadRange operations provides only stale read.
	// Values saved/ removed during transaction will not be immediately
	// observable in the same transaction.
	// 2. Only when storage is etcd, does RunInTxn checks that
	// values loaded during transaction has not been modified before commit.
	RunInTxn(ctx context.Context, f func(txn Txn) error) error

	// CreateLowLevelTxn creates a transaction that provides the if-then-else
	// API pattern which is the same as how etcd does, makes it possible
	// to precisely control how etcd's transaction API is used when the
	// backend is etcd. When there's other backend types, the behavior will be
	// simulated.
	CreateLowLevelTxn() LowLevelTxn
}
