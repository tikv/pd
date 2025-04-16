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

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
)

// StorageEndpoint is the base underlying storage endpoint for all other upper
// specific storage backends. It should define some common storage interfaces and operations,
// which provides the default implementations for all kinds of storages.
type StorageEndpoint struct {
	kv.Base
	encryptionKeyManager *encryption.Manager
	// nextRegionID is the next region ID to be reloaded
	nextRegionID uint64
}

// NewStorageEndpoint creates a new base storage endpoint with the given KV and encryption key manager.
// It should be embedded inside a storage backend.
func NewStorageEndpoint(
	kvBase kv.Base,
	encryptionKeyManager *encryption.Manager,
) *StorageEndpoint {
	return &StorageEndpoint{
		kvBase,
		encryptionKeyManager,
		0,
	}
}

// loadJSON loads a specific key from the StorageEndpoint, and parses it as JSON into type T.
func loadJSON[T any](se *StorageEndpoint, key string) (T, error) {
	value, err := se.Load(key)
	if err != nil {
		var empty T
		return empty, err
	}
	if value == "" {
		var empty T
		return empty, nil
	}
	var data T
	if err = json.Unmarshal([]byte(value), &data); err != nil {
		return data, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	return data, nil
}

// loadJSON loads keys with the given prefix from the StorageEndpoint, and parses them as JSON into type T.
// Returns the loaded keys and the parsed values as type T.
// If `limit` is non-zero, it at most returns `limit` items.
func loadJSONByPrefix[T any](se *StorageEndpoint, prefix string, limit int) ([]string, []T, error) {
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, limit)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) == 0 {
		return nil, nil, nil
	}

	data := make([]T, 0, len(keys))
	for i := range keys {
		var item T
		if err := json.Unmarshal([]byte(values[i]), &item); err != nil {
			return nil, nil, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
		}
		data = append(data, item)
	}
	return keys, data, nil
}

// NextRegionID returns the next region ID to be reloaded, only for test
func (se *StorageEndpoint) NextRegionID() uint64 {
	return se.nextRegionID
}
