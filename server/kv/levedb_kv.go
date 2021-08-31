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
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/tikv/pd/pkg/errs"
)

// LeveldbKV is a kv store using leveldb.
type LeveldbKV struct {
	*leveldb.DB
}

// NewLeveldbKV is used to store regions information.
func NewLeveldbKV(path string) (*LeveldbKV, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, errs.ErrLevelDBOpen.Wrap(err).GenWithStackByCause()
	}
	return &LeveldbKV{db}, nil
}

// Load gets a value for a given key.
func (kv *LeveldbKV) Load(key string) (string, error) {
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
func (kv *LeveldbKV) LoadRange(startKey, endKey string, limit int) ([]string, []string, error) {
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
func (kv *LeveldbKV) Save(key, value string) error {
	return errors.WithStack(kv.Put([]byte(key), []byte(value), nil))
}

// Remove deletes a key-value pair for a given key.
func (kv *LeveldbKV) Remove(key string) error {
	return errors.WithStack(kv.Delete([]byte(key), nil))
}

// SaveRegions stores some regions.
func (kv *LeveldbKV) SaveRegions(regions map[string]*metapb.Region) error {
	batch := new(leveldb.Batch)

	for key, r := range regions {
		value, err := proto.Marshal(r)
		if err != nil {
			return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
		}
		batch.Put([]byte(key), value)
	}

	if err := kv.Write(batch, nil); err != nil {
		return errs.ErrLevelDBWrite.Wrap(err).GenWithStackByCause()
	}
	return nil
}
