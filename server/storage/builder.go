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

package storage

import (
	"github.com/tikv/pd/server/kv"
	storage "github.com/tikv/pd/server/storage/base_storage"
	"go.etcd.io/etcd/clientv3"
)

// Storage is the interface for the backend storage of the PD.
// TODO: replace the core.Storage with this interface later.
type Storage interface {
	// Introducing the kv.Base here is to provide
	// the basic key-value read/write ability for the Storage.
	kv.Base
	storage.ConfigStorage
	storage.MetaStorage
	storage.RuleStorage
	storage.ComponentStorage
	storage.ReplicationStatusStorage
	storage.GCSafePointStorage
}

// Builder is used to build a storage with some options.
type Builder struct {
	client   *clientv3.Client
	rootPath string
}

// NewBuilder creates a new storage builder.
func NewBuilder() *Builder {
	return &Builder{}
}

// WithEtcdBackend sets the backend of the storage to etcd with the given client.
func (b *Builder) WithEtcdBackend(client *clientv3.Client) *Builder {
	b.client = client
	return b
}

// WithMemoryBackend sets the backend of the storage to memory.
func (b *Builder) WithMemoryBackend() *Builder {
	b.client = nil
	return b
}

// WithKeyRootPath sets the storage key root path.
func (b *Builder) WithKeyRootPath(rootPath string) *Builder {
	b.rootPath = rootPath
	return b
}

// Build creates a new storage with the given options.
// TODO: build different storages, e.g, RegionStorage.
func (b *Builder) Build() Storage {
	if b.client != nil {
		return newEtcdBackend(b.client, b.rootPath)
	}
	// TODO: support setting the key root path for other storage backends.
	return newMemoryBackend()
}
