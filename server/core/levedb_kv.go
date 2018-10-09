// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type leveldbKV struct {
	db *leveldb.DB
}

// newLeveldbKV is used to store regions information.
func newLeveldbKV(path string) (*leveldbKV, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &leveldbKV{db: db}, nil
}

func (kv *leveldbKV) Load(key string) (string, error) {
	v, err := kv.db.Get([]byte(key), nil)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return string(v), err
}

func (kv *leveldbKV) LoadRange(startKey, endKey string, limit int) ([]string, error) {
	iter := kv.db.NewIterator(&util.Range{Start: []byte(startKey), Limit: []byte(endKey)}, nil)
	values := make([]string, 0, limit)
	count := 0
	for iter.Next() {
		if count >= limit {
			break
		}
		values = append(values, string(iter.Value()))
		count++
	}
	iter.Release()
	return values, nil
}

func (kv *leveldbKV) Save(key, value string) error {
	return errors.WithStack(kv.db.Put([]byte(key), []byte(value), nil))
}

func (kv *leveldbKV) Delete(key string) error {
	return errors.WithStack(kv.db.Delete([]byte(key), nil))
}

func (kv *leveldbKV) SaveRegions(regions map[string]*metapb.Region) error {
	batch := new(leveldb.Batch)

	for key, r := range regions {
		value, err := proto.Marshal(r)
		if err != nil {
			return errors.WithStack(err)
		}
		batch.Put([]byte(key), value)
	}
	return errors.WithStack(kv.db.Write(batch, nil))
}

func (kv *leveldbKV) Close() error {
	return errors.WithStack(kv.db.Close())
}
