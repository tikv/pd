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

package gc

import (
	"fmt"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/server/config"
)

// loadTiDBMinStartTS routes only TiDB's report read; GC state writes remain on PD's provider.
func (m *GCStateManager) loadTiDBMinStartTS(keyspaceID uint32, meta *keyspacepb.KeyspaceMeta) (string, uint64, error) {
	groupID := meta.GetConfig()[keyspace.MetaServiceGroupIDKey]
	if keyspaceID == constant.NullKeyspaceID || groupID == "" {
		return m.gcMetaStorage.CompatibleLoadTiDBMinStartTS(keyspaceID)
	}
	if !config.IsValidMetaServiceGroupID(groupID) {
		return "", 0, fmt.Errorf("invalid meta-service group ID %q", groupID)
	}
	if m.metaServiceGroupManager == nil {
		return "", 0, fmt.Errorf("meta-service group manager unavailable for group %s", groupID)
	}
	client, err := m.metaServiceGroupManager.GetEtcdClient(groupID)
	if err != nil {
		return "", 0, err
	}
	// This provider borrows the cached client and is used only for this read.
	provider := endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(client), nil).GetGCStateProvider()
	return provider.CompatibleLoadTiDBMinStartTS(keyspaceID)
}
