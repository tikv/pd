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

// Txn bundles multiple operations into a single executable unit.
// It enables kv to atomically apply a set of updates.
type Txn interface {
	Save(key, value string) error
	Remove(key string) error
	Load(key string) (string, error)
	LoadRange(key, endKey string, limit int) (keys []string, values []string, err error)
}

// Base is an abstract interface for load/save pd cluster data.
type Base interface {
	Txn
	// RunInTxn runs the user provided function in a Transaction.
	// If user provided function f returns a non-nil error, then
	// transaction will not be committed, the same error will be
	// returned by RunInTxn.
	// Otherwise, it returns the error occurred during the
	// transaction.
	// Note that transaction are not committed until RunInTxn returns nil.
	// Note:
	// 1. Load and LoadRange operations provides only stale read.
	// Values saved/ removed during transaction will not be immediately
	// observable in the same transaction.
	// 2. Only when storage is etcd, does RunInTxn checks that
	// values loaded during transaction has not been modified before commit.
	RunInTxn(ctx context.Context, f func(txn Txn) error) error
}
