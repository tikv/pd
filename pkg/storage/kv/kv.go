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

// RawTxnCmpType represents the comparison type that is used in the condition of RawTxn.
type RawTxnCmpType int

// RawTxnOpType represents the operation type that is used in the operations (either `Then` branch or `Else`
// branch) of RawTxn.
type RawTxnOpType int

// nolint:revive
const (
	RawTxnCmpEqual RawTxnCmpType = iota
	RawTxnCmpNotEqual
	RawTxnCmpLess
	RawTxnCmpGreater
	RawTxnCmpExists
	RawTxnCmpNotExists
)

// nolint:revive
const (
	RawTxnOpPut RawTxnOpType = iota
	RawTxnOpDelete
	RawTxnOpGet
	RawTxnOpGetRange
)

// RawTxnCondition represents a condition in a RawTxn.
type RawTxnCondition struct {
	Key     string
	CmpType RawTxnCmpType
	// The value to compare with. It's not used when CmpType is RawTxnCmpExists or RawTxnCmpNotExists.
	Value string
}

// RawTxnOp represents an operation in a RawTxn's `Then` or `Else` branch and will be executed according to
// the result of checking conditions.
type RawTxnOp struct {
	Key    string
	OpType RawTxnOpType
	Value  string
	// The end key when the OpType is RawTxnOpGetRange.
	EndKey string
	// The limit of the keys to get when the OpType is RawTxnOpGetRange.
	Limit int
}

// KeyValuePair represents a pair of key and value.
type KeyValuePair struct {
	Key   string
	Value string
}

// RawTxnResponseItem represents a single result of a read operation in a RawTxn.
type RawTxnResponseItem struct {
	KeyValuePairs []KeyValuePair
}

// RawTxnResponse represents the result of a RawTxn. The results of operations in `Then` or `Else` branches
// will be listed in `Responses` in the same order as the operations are added.
// For Put or Delete operations, its corresponding result is the previous value before writing.
type RawTxnResponse struct {
	Succeeded bool
	// The results of each operation in the `Then` branch or the `Else` branch of a transaction, depending on
	// whether `Succeeded`. The i-th result belongs to the i-th operation added to the executed branch.
	// * For Put or Delete operations, the result is empty.
	// * For Get operations, the result contains a key-value pair representing the get result. In case the key
	//   does not exist, its `KeyValuePairs` field will be empty.
	// * For GetRange operations, the result is a list of key-value pairs containing key-value paris that are scanned.
	Responses []RawTxnResponseItem
}

// RawTxn is a low-level transaction interface. It follows the same pattern of etcd's transaction
// API. When the backend is etcd, it simply calls etcd's equivalent APIs internally. Otherwise, the
// behavior is simulated.
// Avoid reading/writing the same key multiple times in a single transaction, otherwise the behavior
// would be undefined.
type RawTxn interface {
	If(conditions ...RawTxnCondition) RawTxn
	Then(ops ...RawTxnOp) RawTxn
	Else(ops ...RawTxnOp) RawTxn
	Commit() (RawTxnResponse, error)
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
	// does. When this API is running on etcd backend, each read on
	// `txn` implicitly constructs a condition.
	// (ref: https://etcd.io/docs/v3.5/learning/api/#transaction)
	// When reading a range using `LoadRange`, for each key found in the
	// range there will be a condition constructed. Be aware of the
	// possibility of causing phantom read.
	// RunInTxn may not suit all use cases. When RunInTxn is found
	// improper to use, consider using CreateRawTxn instead, which
	// is available when the backend is etcd.
	//
	// Note that transaction are not committed until RunInTxn returns nil.
	// Note:
	// 1. Load and LoadRange operations provides only stale read.
	// Values saved/ removed during transaction will not be immediately
	// observable in the same transaction.
	// 2. Only when storage is etcd, does RunInTxn checks that
	// values loaded during transaction has not been modified before commit.
	RunInTxn(ctx context.Context, f func(txn Txn) error) error

	// CreateRawTxn creates a transaction that provides the if-then-else
	// API pattern when the backend is etcd, makes it possible
	// to precisely control how etcd's transaction API is used. When the
	// backend is not etcd, it panics.
	CreateRawTxn() RawTxn
}
