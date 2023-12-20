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
	"encoding/json"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"go.etcd.io/etcd/clientv3"
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

func (se *StorageEndpoint) saveJSON(key string, data interface{}) error {
	return saveJSONInTxn(se /* use the same interface */, key, data)
}

func saveJSONInTxn(txn kv.Txn, key string, data interface{}) error {
	value, err := json.Marshal(data)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return txn.Save(key, string(value))
}

// loadRangeByPrefix iterates all key-value pairs in the storage that has the prefix.
func (se *StorageEndpoint) loadRangeByPrefix(prefix string, f func(k, v string)) error {
	return loadRangeByPrefixInTxn(se /* use the same interface */, prefix, f)
}

func loadRangeByPrefixInTxn(txn kv.Txn, prefix string, f func(k, v string)) error {
	nextKey := prefix
	endKey := clientv3.GetPrefixRangeEnd(prefix)
	for {
		keys, values, err := txn.LoadRange(nextKey, endKey, MinKVRangeLimit)
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
