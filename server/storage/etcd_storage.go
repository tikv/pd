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

var _ Storage = (*EtcdStorage)(nil)

// EtcdStorage is a storage that stores data in etcd,
// which is used by the PD server.
type EtcdStorage struct {
	*base.Storage
}

// newEtcdStorage is used to create a new etcd storage.
func newEtcdStorage(client *clientv3.Client, rootPatch string) *EtcdStorage {
	return &EtcdStorage{base.NewStorage(kv.NewEtcdKVBase(client, rootPatch), nil)}
}
