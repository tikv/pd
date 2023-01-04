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

package storage

import (
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

// memoryStorage is a storage that stores data in a memory B-Tree without any locks,
// which should only be used in tests.
type memoryStorage struct {
	*endpoint.StorageEndpoint
}

// newMemoryBackend is used to create a new memory storage.
func newMemoryBackend() *memoryStorage {
	return &memoryStorage{
		endpoint.NewStorageEndpoint(
			kv.NewMemoryKV(),
			nil,
		),
	}
}
