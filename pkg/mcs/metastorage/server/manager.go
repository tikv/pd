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
	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Manager is the manager of resource group.
type Manager struct {
	srv       bs.Server
	clusterID uint64
	client    *clientv3.Client
	storage   *endpoint.StorageEndpoint
}

// ClusterIDProvider is used to get cluster ID from the given `bs.server`
type ClusterIDProvider interface {
	ClusterID() uint64
}

// NewManager returns a new Manager.
func NewManager[T ClusterIDProvider](srv bs.Server) *Manager {
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
		m.clusterID = srv.(T).ClusterID()
	})
	return m
}

// GetClient returns the client of etcd.
func (m *Manager) GetClient() *clientv3.Client {
	return m.client
}

// ClusterID returns the cluster ID.
func (m *Manager) ClusterID() uint64 {
	return m.clusterID
}
