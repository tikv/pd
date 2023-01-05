// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package informer

import (
	"github.com/tikv/pd/pkg/storage/kv"
)

// Store provides interface for managing the information we need.
type Store interface {
	// Add adds the given object.
	Add(item interface{}) error

	// Update updates the given object.
	Update(item interface{}) error

	// Delete deletes the given object.
	Delete(item interface{}) error

	// List returns a list of all object within a given range.
	List(start, end string, limit int) ([]interface{}, error)

	// GetByKey returns the object with the given key
	GetByKey(key string) (item interface{}, err error)
}

// KeyExtractFunc knows how to make a key from an object.
type KeyExtractFunc func(obj interface{}) (string, error)

// ValueEncodeFunc knows how to make a value from an object.
type ValueEncodeFunc func(obj interface{}) (string, error)

// ValueDecodeFunc knows how to decode a object from a string.
type ValueDecodeFunc func(string) (interface{}, error)

type store struct {
	keyExtractFunc  KeyExtractFunc
	valueEncodeFunc ValueEncodeFunc
	valueDecodeFunc ValueDecodeFunc
	cacheStorage    kv.Base
}

// NewStore creates a store instance.
func NewStore(s kv.Base, keyExtractFunc KeyExtractFunc, encodeFunc ValueEncodeFunc, decodeFunc ValueDecodeFunc) *store {
	return &store{cacheStorage: s, keyExtractFunc: keyExtractFunc, valueEncodeFunc: encodeFunc, valueDecodeFunc: decodeFunc}
}

// Add inserts an item into the store.
func (s *store) Add(obj interface{}) error {
	key, err := s.keyExtractFunc(obj)
	if err != nil {
		return err
	}
	value, err := s.valueEncodeFunc(obj)
	if err != nil {
		return err
	}

	return s.cacheStorage.Save(key, value)
}

// Update sets an item in the store to its updated state.
func (s *store) Update(obj interface{}) error {
	key, err := s.keyExtractFunc(obj)
	if err != nil {
		return err
	}
	value, err := s.valueEncodeFunc(obj)
	if err != nil {
		return err
	}
	return s.cacheStorage.Save(key, value)
}

// Delete removes an item from the store.
func (s *store) Delete(obj interface{}) error {
	key, err := s.keyExtractFunc(obj)
	if err != nil {
		return err
	}
	return s.cacheStorage.Remove(key)
}

// List returns a list of all the items.
func (s *store) List(start, end string, limit int) ([]interface{}, error) {
	_, values, err := s.cacheStorage.LoadRange(start, end, limit)
	if err != nil {
		return nil, err
	}
	items := make([]interface{}, 0, len(values))
	for _, v := range values {
		item, err := s.valueDecodeFunc(v)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

// GetByKey returns the request item.
func (s *store) GetByKey(key string) (interface{}, error) {
	value, err := s.cacheStorage.Load(key)
	if err != nil {
		return nil, err
	}
	item, err := s.valueDecodeFunc(value)
	if err != nil {
		return nil, err
	}
	return item, nil
}
