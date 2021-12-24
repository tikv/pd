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
	"github.com/tikv/pd/server/storage/base"
	"go.etcd.io/etcd/clientv3"
)

// Storage is the interface for the backend storage of the PD.
// TODO: replace the core.Storage with this interface later.
type Storage interface {
	kv.Base
	base.ConfigStorage
	base.MetaStorage
	base.RuleStorage
	base.ComponentStorage
	base.ReplicationStatusStorage
	base.GCSafePointStorage
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
func (sb *Builder) WithEtcdBackend(client *clientv3.Client) *Builder {
	sb.client = client
	return sb
}

// WithMemoryBackend sets the backend of the storage to memory.
func (sb *Builder) WithMemoryBackend() *Builder {
	sb.client = nil
	return sb
}

// WithKeyRootPath sets the storage key root path.
func (sb *Builder) WithKeyRootPath(rootPath string) *Builder {
	sb.rootPath = rootPath
	return sb
}

// Build creates a new storage with the given options.
func (sb *Builder) Build() Storage {
	if sb.client != nil {
		return newEtcdStorage(sb.client, sb.rootPath)
	}
	// TODO: support setting the key root path for the memory storage backend.
	return newMemoryStorage()
}
