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

package checker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

type heartbeatTargetWriter struct {
	targets []affinity.GroupTarget
}

func (w *heartbeatTargetWriter) WriteGroupTargets(_ context.Context, targets []affinity.GroupTarget) ([]string, error) {
	w.targets = append(w.targets, targets...)
	ids := make([]string, 0, len(targets))
	for _, target := range targets {
		ids = append(ids, target.Expected.ID)
	}
	return ids, nil
}

func newMissingHeartbeatBalanceTest(t *testing.T) (*affinityBalanceTest, *core.RegionInfo, *heartbeatTargetWriter) {
	t.Helper()
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3)
	group := &affinity.Group{ID: "_tidb_pt_100_p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	re.NoError(createAffinityGroupForTest(f.affinityManager, group, []byte("a"), []byte("c")))
	f.AddLeaderRegionWithRange(1, "a", "b", 1, 2, 3)
	f.group("_tidb_pt_100_p1", 3, 1, []uint64{1, 2, 3}, "d", "e")
	f.group("_tidb_pt_100_p2", 4, 2, []uint64{1, 2, 3}, "f", "g")
	f.policy(affinity.BalancePolicyFixed, "_tidb_pt_100_p1", "_tidb_pt_100_p2")
	for _, region := range f.GetRegions() {
		region = region.Clone(core.SetSource(core.Heartbeat))
		f.PutRegion(region)
		f.affinityManager.GetAndCacheRegionAffinityGroupState(region)
	}
	// This Region has never entered the cluster cache; the reported [a,b)
	// Region keeps its original boundaries throughout the test.
	missing := f.GetRegion(1).Clone(core.WithNewRegionID(2), core.WithStartKey([]byte("b")), core.WithEndKey([]byte("c")), core.SetSource(core.Heartbeat))
	writer := &heartbeatTargetWriter{}
	f.affinityManager.SetTargetWriter(writer)
	return f, missing, writer
}

func TestAffinityBalanceWaitsForMissingRegionHeartbeat(t *testing.T) {
	re := require.New(t)
	f, missing, writer := newMissingHeartbeatBalanceTest(t)
	re.Nil(f.GetRegion(missing.GetID()))
	before := f.snapshot()
	group := before.Group("_tidb_pt_100_p0")
	target := group.Group
	target.LeaderStoreID = 3
	status, _ := f.validateBalancePlanTarget(t.Context(), before, group, &target, make(map[uint64]balanceCheckedRegion))
	re.Equal(balanceUnready, status)
	for range 3 {
		re.False(f.step())
		re.Empty(writer.targets)
		re.Equal(before.Groups, f.snapshot().Groups)
	}
	f.PutRegion(missing)
	f.affinityManager.GetAndCacheRegionAffinityGroupState(missing)
	re.True(f.step())
	re.Len(writer.targets, 1)
	re.Equal(group.ID, writer.targets[0].Expected.ID)
	re.Equal(uint64(3), writer.targets[0].LeaderStoreID)
}

func TestAffinityBalanceWaitsForStorageRegionHeartbeat(t *testing.T) {
	re := require.New(t)
	f, heartbeat, writer := newMissingHeartbeatBalanceTest(t)
	// Region storage persists metadata, not the leader supplied by a heartbeat.
	stored := core.NewRegionInfo(heartbeat.GetMeta(), nil, core.SetSource(core.Storage))
	f.PutRegion(stored)
	re.True(f.GetRegion(stored.GetID()).LoadedFromStorage())
	re.Nil(f.GetRegion(stored.GetID()).GetLeader())
	before := f.snapshot()
	for range 3 {
		re.False(f.step())
		re.Empty(writer.targets)
		re.Equal(before.Groups, f.snapshot().Groups)
	}
	f.PutRegion(heartbeat)
	f.affinityManager.GetAndCacheRegionAffinityGroupState(heartbeat)
	re.False(f.GetRegion(heartbeat.GetID()).LoadedFromStorage())
	re.True(f.step())
	re.Len(writer.targets, 1)
	re.Equal(uint64(3), writer.targets[0].LeaderStoreID)
}
