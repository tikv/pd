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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage"
)

func TestLogStoreCapabilityCannotBeChangedByLabels(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	meta := newTestStores(1, "6.6.0")[0].GetMeta()
	meta.Labels = []*metapb.StoreLabel{{Key: core.StoreCapabilityLabel, Value: core.LogOnlyStoreCapability}}
	re.NoError(cluster.PutMetaStore(meta))
	re.NoError(cluster.UpdateStoreLabels(1, []*metapb.StoreLabel{{Key: "zone", Value: "z1"}}, false))
	re.ErrorContains(cluster.UpdateStoreLabels(1, []*metapb.StoreLabel{{Key: core.StoreCapabilityLabel, Value: core.FullStoreCapability}}, false), "immutable")
	re.ErrorContains(cluster.UpdateStoreLabels(1, []*metapb.StoreLabel{{Key: "zone", Value: "z2"}}, true), "immutable")
	re.ErrorContains(cluster.DeleteStoreLabel(1, core.StoreCapabilityLabel), "immutable")
	re.ErrorContains(cluster.UpdateStoreLabels(1, []*metapb.StoreLabel{
		{Key: core.StoreCapabilityLabel, Value: core.FullStoreCapability},
		{Key: core.StoreCapabilityLabel, Value: core.LogOnlyStoreCapability},
	}, false), "duplicate")
	re.Equal(core.LogOnlyStoreCapability, cluster.GetStore(1).GetLabelValue(core.StoreCapabilityLabel))
}

func TestStoreCapabilityIsReservedUnderStrictLabelValidation(t *testing.T) {
	ctx := t.Context()
	_, opt, err := newTestScheduleConfig()
	require.NoError(t, err)
	replication := opt.GetReplicationConfig().Clone()
	replication.StrictlyMatchLabel = true
	replication.LocationLabels = []string{"zone"}
	opt.SetReplicationConfig(replication)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	for i, capability := range []string{core.FullStoreCapability, core.LogOnlyStoreCapability} {
		meta := newTestStores(2, "6.6.0")[i].GetMeta()
		meta.Labels = []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: core.StoreCapabilityLabel, Value: capability}}
		require.NoError(t, cluster.PutMetaStore(meta))
	}
}
