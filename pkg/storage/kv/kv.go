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

// RawTxnCmpTarget selects which attribute of a key a RawTxnCondition compares.
type RawTxnCmpTarget int

// nolint:revive
const (
	// RawTxnCmpTargetValue compares the key's value. It is the default and is
	// used when CmpType is RawTxnCmpExists or RawTxnCmpNotExists too.
	RawTxnCmpTargetValue RawTxnCmpTarget = iota
	// RawTxnCmpTargetModRevision compares the key's modification revision,
	// which changes on every write regardless of whether the value returns to
	// one it held before. Prefer it over RawTxnCmpTargetValue to guard against
	// ABA: a value-only compare cannot tell a key that was never touched
	// apart from one that changed and changed back to the same value.
	RawTxnCmpTargetModRevision
)

// RawTxnCondition represents a condition in a RawTxn.
type RawTxnCondition struct {
	Key     string
	CmpType RawTxnCmpType
	// Target selects what CmpType compares against. Defaults to
	// RawTxnCmpTargetValue.
	Target RawTxnCmpTarget
	// The value to compare with, used when Target is RawTxnCmpTargetValue.
	// It's not used when CmpType is RawTxnCmpExists or RawTxnCmpNotExists.
	Value string
	// The modification revision to compare with, used when Target is
	// RawTxnCmpTargetModRevision.
	Revision int64
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
// API.
// Avoid reading/writing the same key multiple times in a single transaction, otherwise the behavior
// would be undefined.
type RawTxn interface {
	If(conditions ...RawTxnCondition) RawTxn
	Then(ops ...RawTxnOp) RawTxn
	Else(ops ...RawTxnOp) RawTxn
	Commit() (RawTxnResponse, error)
}

// RawTxnCapable is implemented by KV backends that support creating raw transactions.
type RawTxnCapable interface {
	CreateRawTxn() RawTxn
}

// ModRevisionReader is implemented by KV backends that can report a key's
// modification revision alongside its value, for use as the anchor of a
// RawTxnCmpTargetModRevision condition. Backends without MVCC (LevelDB, the
// in-memory KV) don't implement it.
type ModRevisionReader interface {
	// LoadModRevision loads key's value and modification revision. A missing
	// key returns an empty value and a modification revision of 0.
	LoadModRevision(key string) (value string, modRevision int64, err error)
}

// RangeOptions carries optional, backend-specific behavior for LoadRange.
type RangeOptions struct {
	// Revision, if non-zero, pins the read to a specific historical MVCC
	// revision instead of the latest value. Backends without MVCC (LevelDB,
	// the in-memory KV) have no notion of revision and ignore it.
	Revision int64
}

// RangeOption configures a RangeOptions.
type RangeOption func(*RangeOptions)

// WithRevision pins a LoadRange call to a specific historical MVCC revision,
// so a caller issuing multiple LoadRange calls (e.g. to page through a large
// range) sees one consistent point in time across all of them instead of a
// mix of before/after states for keys mutated while paging.
//
// Avoid using it on a Txn obtained from RunInTxn: on the etcd backend, LoadRange
// inside a transaction also adds a condition comparing each returned key's
// current value against the value just read (see RunInTxn's doc), which is
// checked at commit time against the latest value, not the pinned revision. A
// revision pinned to the past will then spuriously fail that condition as soon
// as any returned key changes before commit. Call it on Base directly instead.
func WithRevision(revision int64) RangeOption {
	return func(o *RangeOptions) { o.Revision = revision }
}

// BaseReadWrite is the API set, shared by Base and Txn interfaces, that provides basic KV read and write operations.
type BaseReadWrite interface {
	Save(key, value string) error
	Remove(key string) error
	Load(key string) (string, error)
	LoadRange(key, endKey string, limit int, opts ...RangeOption) (keys []string, values []string, err error)
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
	// improper to use, require RawTxnCapable explicitly and use
	// CreateRawTxn instead.
	//
	// Note that transaction are not committed until RunInTxn returns nil.
	// Note:
	// 1. Load and LoadRange operations provides only stale read.
	// Values saved/ removed during transaction will not be immediately
	// observable in the same transaction.
	// 2. Only when storage is etcd, does RunInTxn checks that
	// values loaded during transaction has not been modified before commit.
	RunInTxn(ctx context.Context, f func(txn Txn) error) error
	// CurrentRevision returns the backend's current MVCC revision, for use
	// with WithRevision. Backends without MVCC (LevelDB, the in-memory KV)
	// return 0, which WithRevision treats as "unpinned".
	CurrentRevision(ctx context.Context) (int64, error)
}
