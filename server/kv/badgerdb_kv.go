// Copyright 2021 TiKV Project Authors.
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
	"bytes"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/errs"
)

// BadgerDBKV is a kv store using BadgerDB.
type BadgerDBKV struct {
	*badger.DB
}

// NewBadgerDBKV is used to store regions information.
func NewBadgerDBKV(path string) (*BadgerDBKV, error) {
	opts := badger.DefaultOptions(path)
	// TODO: pass a logger correctly.
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errs.ErrBadgerDBOpen.Wrap(err).GenWithStackByCause()
	}
	return &BadgerDBKV{db}, nil
}

// Load gets a value for a given key.
func (kv *BadgerDBKV) Load(key string) (string, error) {
	var value string
	err := kv.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		valueByte, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		value = string(valueByte)
		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return "", nil
		}
		return "", errors.WithStack(err)
	}
	return value, nil
}

// LoadRange gets a range of value for a given key range.
func (kv *BadgerDBKV) LoadRange(startKey, endKey string, limit int) ([]string, []string, error) {
	keys := make([]string, 0, limit)
	values := make([]string, 0, limit)
	err := kv.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		count := 0
		endKeyByte := []byte(endKey)
		for it.Seek([]byte(startKey)); it.Valid(); it.Next() {
			if limit > 0 && count >= limit {
				break
			}
			item := it.Item()
			key := item.Key()
			if bytes.Compare(key, endKeyByte) > 0 {
				break
			}
			keys = append(keys, string(key))
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			values = append(values, string(value))
			count++
		}
		return nil
	})
	if err != nil {
		return []string{}, []string{}, errors.WithStack(err)
	}
	return keys, values, nil
}

// Save stores a key-value pair.
func (kv *BadgerDBKV) Save(key, value string) error {
	return errors.WithStack(kv.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(value))
	}))
}

// Remove deletes a key-value pair for a given key.
func (kv *BadgerDBKV) Remove(key string) error {
	return errors.WithStack(kv.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	}))
}

// SaveRegions stores some regions.
func (kv *BadgerDBKV) SaveRegions(regions map[string]*metapb.Region) error {
	wb := kv.NewWriteBatch()

	for key, r := range regions {
		value, err := proto.Marshal(r)
		if err != nil {
			return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
		}
		wb.Set([]byte(key), value)
	}

	if err := wb.Flush(); err != nil {
		return errs.ErrBadgerDBWrite.Wrap(err).GenWithStackByCause()
	}
	return nil
}
