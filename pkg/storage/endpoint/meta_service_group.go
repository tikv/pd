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

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// MetaServiceGroupStorage defines storage operations on meta-service group related data.
type MetaServiceGroupStorage interface {
	SaveMetaServiceGroupStatus(txn kv.Txn, id string, status *MetaServiceGroupStatus) error
	LoadMetaServiceGroupStatus(txn kv.Txn, ids map[string]string) (map[string]*MetaServiceGroupStatus, error)
	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
	// LoadMetaServiceGroupStatusModRevision loads a single meta-service
	// group's status along with its modification revision, for use with
	// CASMetaServiceGroupStatus.
	LoadMetaServiceGroupStatusModRevision(id string) (status *MetaServiceGroupStatus, modRevision int64, err error)
	// CASMetaServiceGroupStatus persists status for id, guarded by a
	// modification-revision compare-and-swap against expectedModRevision.
	// See the StorageEndpoint implementation's doc for why this is needed
	// instead of a value-based compare.
	CASMetaServiceGroupStatus(id string, expectedModRevision int64, status *MetaServiceGroupStatus) (committed bool, err error)
}

// MetaServiceGroupStatus represents the status of a meta-service group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaServiceGroupStatus struct {
	AssignmentCount int  `json:"assignment_count"`
	Enabled         bool `json:"enabled"`
}

// SaveMetaServiceGroupStatus saves the meta service group status to the storage.
func (*StorageEndpoint) SaveMetaServiceGroupStatus(txn kv.Txn, id string, status *MetaServiceGroupStatus) error {
	statusPath := keypath.MetaServiceGroupStatusPath(id)
	statusVal, err := json.Marshal(status)
	if err != nil {
		return err
	}
	return txn.Save(statusPath, string(statusVal))
}

// LoadMetaServiceGroupStatus returns the status of the designated meta-service group.
func (*StorageEndpoint) LoadMetaServiceGroupStatus(txn kv.Txn, ids map[string]string) (map[string]*MetaServiceGroupStatus, error) {
	statusMap := make(map[string]*MetaServiceGroupStatus)
	for id := range ids {
		status, err := loadMetaServiceGroupStatus(txn, id)
		if err != nil {
			return nil, err
		}
		statusMap[id] = status
	}
	return statusMap, nil
}

func loadMetaServiceGroupStatus(txn kv.Txn, id string) (*MetaServiceGroupStatus, error) {
	statusVal, err := txn.Load(keypath.MetaServiceGroupStatusPath(id))
	if err != nil {
		return nil, err
	}
	return unmarshalMetaServiceGroupStatus(statusVal)
}

func unmarshalMetaServiceGroupStatus(statusVal string) (*MetaServiceGroupStatus, error) {
	status := &MetaServiceGroupStatus{}
	if statusVal == "" {
		return status, nil
	}
	if err := json.Unmarshal([]byte(statusVal), status); err != nil {
		return nil, err
	}
	return status, nil
}

// LoadMetaServiceGroupStatusModRevision loads a single meta-service group's
// status along with its modification revision, for use with
// CASMetaServiceGroupStatus. The revision is 0 on backends without MVCC
// (LevelDB, the in-memory KV) and for a group with no persisted status yet.
func (se *StorageEndpoint) LoadMetaServiceGroupStatusModRevision(id string) (*MetaServiceGroupStatus, int64, error) {
	statusPath := keypath.MetaServiceGroupStatusPath(id)
	reader, ok := se.Base.(kv.ModRevisionReader)
	if !ok {
		status, err := se.loadMetaServiceGroupStatusNoTxn(statusPath)
		return status, 0, err
	}
	statusVal, modRevision, err := reader.LoadModRevision(statusPath)
	if err != nil {
		return nil, 0, err
	}
	status, err := unmarshalMetaServiceGroupStatus(statusVal)
	return status, modRevision, err
}

func (se *StorageEndpoint) loadMetaServiceGroupStatusNoTxn(statusPath string) (*MetaServiceGroupStatus, error) {
	statusVal, err := se.Load(statusPath)
	if err != nil {
		return nil, err
	}
	return unmarshalMetaServiceGroupStatus(statusVal)
}

// CASMetaServiceGroupStatus persists status for id, guarded by a
// modification-revision compare-and-swap against expectedModRevision when the
// backend supports raw transactions (etcd). This fences a write from a leader
// whose term has already ended: a value-only compare can't tell a key that
// was never touched apart from one that changed and changed back to the same
// value (ABA), but the modification revision advances on every write
// regardless of the value, so a stale expectedModRevision reliably fails the
// compare. Returns committed=false with a nil error when the compare fails;
// the caller should reload and decide whether to retry or surface a
// conflict.
//
// Backends without raw-transaction support (LevelDB, the in-memory KV used in
// unit tests) can't be fenced this way and always commit: they're
// single-process, so there is no concurrent stale-term writer to guard
// against there.
func (se *StorageEndpoint) CASMetaServiceGroupStatus(id string, expectedModRevision int64, status *MetaServiceGroupStatus) (committed bool, err error) {
	statusPath := keypath.MetaServiceGroupStatusPath(id)
	statusVal, err := json.Marshal(status)
	if err != nil {
		return false, err
	}
	rawTxn, err := se.createRawTxn()
	if err != nil {
		if err := se.Save(statusPath, string(statusVal)); err != nil {
			return false, err
		}
		return true, nil
	}
	resp, err := rawTxn.If(kv.RawTxnCondition{
		Key:      statusPath,
		CmpType:  kv.RawTxnCmpEqual,
		Target:   kv.RawTxnCmpTargetModRevision,
		Revision: expectedModRevision,
	}).Then(kv.RawTxnOp{
		Key:    statusPath,
		OpType: kv.RawTxnOpPut,
		Value:  string(statusVal),
	}).Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}
