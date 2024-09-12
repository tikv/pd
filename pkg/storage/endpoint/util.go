// Copyright 2022 TiKV Project Authors.
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
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (se *StorageEndpoint) loadProto(key string, msg proto.Message) (bool, error) {
	value, err := se.Load(key)
	if err != nil || value == "" {
		return false, err
	}
	err = proto.Unmarshal([]byte(value), msg)
	if err != nil {
		return false, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return true, nil
}

func (se *StorageEndpoint) saveProto(key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return se.Save(key, string(value))
}

func (se *StorageEndpoint) saveJSON(key string, data any) error {
	return saveJSONInTxn(se /* use the same interface */, key, data)
}

func saveJSONInTxn(txn kv.Txn, key string, data any) error {
	value, err := json.Marshal(data)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return txn.Save(key, string(value))
}

// loadRangeByPrefix iterates all key-value pairs in the storage that has the prefix.
func (se *StorageEndpoint) loadRangeByPrefix(prefix string, f func(k, v string)) error {
	nextKey := prefix
	endKey := clientv3.GetPrefixRangeEnd(prefix)
	for {
		keys, values, err := se.LoadRange(nextKey, endKey, MinKVRangeLimit)
		if err != nil {
			return err
		}
		for i := range keys {
			f(strings.TrimPrefix(keys[i], prefix), values[i])
		}
		if len(keys) < MinKVRangeLimit {
			return nil
		}
		nextKey = keys[len(keys)-1] + "\x00"
	}
}

// TxnStorage is the interface with RunInTxn
type TxnStorage interface {
	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
}

// RunBatchOpInTxn runs a batch of operations in transaction.
// The batch is split into multiple transactions if it exceeds the maximum number of operations per transaction.
func RunBatchOpInTxn(ctx context.Context, storage TxnStorage, batch []func(kv.Txn) error) error {
	for start := 0; start < len(batch); start += etcdutil.MaxEtcdTxnOps {
		end := start + etcdutil.MaxEtcdTxnOps
		if end > len(batch) {
			end = len(batch)
		}
		err := storage.RunInTxn(ctx, func(txn kv.Txn) (err error) {
			for _, op := range batch[start:end] {
				if err = op(txn); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
