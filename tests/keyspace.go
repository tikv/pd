// Copyright 2026 TiKV Project Authors.
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

package tests

import (
	"context"
	"maps"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/server"
)

// CreateKeyspaceWithHistoricalGCMode creates a test fixture with a persisted GC
// configuration that may predate the current write policy. Public creation still
// validates the initial metadata; only test storage writes model historical modes.
func CreateKeyspaceWithHistoricalGCMode(srv *server.Server, req *keyspace.CreateKeyspaceRequest) (*keyspacepb.KeyspaceMeta, error) {
	requestCopy := *req
	requestCopy.Config = maps.Clone(req.Config)
	delete(requestCopy.Config, keyspace.GCManagementType)
	delete(requestCopy.Config, "safe_point_version")
	meta, err := srv.GetKeyspaceManager().CreateKeyspace(&requestCopy)
	if err != nil {
		return nil, err
	}
	if mode, ok := req.Config[keyspace.GCManagementType]; ok {
		meta.Config[keyspace.GCManagementType] = mode
	}
	if legacy, ok := req.Config["safe_point_version"]; ok {
		if _, present := req.Config[keyspace.GCManagementType]; !present {
			delete(meta.Config, keyspace.GCManagementType)
		}
		meta.Config["safe_point_version"] = legacy
	}
	storage := srv.GetStorage()
	err = storage.RunInTxn(context.Background(), func(txn kv.Txn) error { return storage.SaveKeyspaceMeta(txn, meta) })
	return meta, err
}
