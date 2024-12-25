// Copyright 2023 TiKV Project Authors.
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

package server

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

// Manager is the manager of resource group.
type Manager struct {
	srv     bs.Server
	client  *clientv3.Client
	storage *endpoint.StorageEndpoint
}

// NewManager returns a new Manager.
func NewManager(srv bs.Server) *Manager {
	m := &Manager{}
	// The first initialization after the server is started.
	srv.AddStartCallback(func() {
		log.Info("meta storage starts to initialize", zap.String("name", srv.Name()))
		m.storage = endpoint.NewStorageEndpoint(
			kv.NewEtcdKVBase(srv.GetClient(), "meta_storage"),
			nil,
		)
		m.client = srv.GetClient()
		m.srv = srv
	})
	return m
}

// GetClient returns the client of etcd.
func (m *Manager) GetClient() *clientv3.Client {
	return m.client
}
