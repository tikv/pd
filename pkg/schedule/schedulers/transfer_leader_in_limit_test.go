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

package schedulers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
)

func TestLeaderSchedulersRespectTransferLeaderInLimit(t *testing.T) {
	for _, typ := range []types.CheckerSchedulerType{types.BalanceLeaderScheduler, types.ShuffleLeaderScheduler, types.LabelScheduler} {
		t.Run(string(typ), func(t *testing.T) {
			re := require.New(t)
			cancel, _, tc, oc := prepareSchedulersTest(false)
			defer cancel()
			tc.AddLeaderStore(1, 100)
			tc.AddLeaderStore(2, 0)
			tc.AddLeaderStore(3, 0)
			tc.AddLeaderRegion(1, 1, 2, 3)
			tc.AddLeaderRegion(2, 2, 1, 3)
			if typ == types.LabelScheduler {
				tc.SetLabelProperty(config.RejectLeader, "noleader", "true")
				tc.AddLabelsStore(1, 100, map[string]string{"noleader": "true"})
				tc.UpdateLeaderCount(1, 100)
			}
			exhaustTransferLeaderInLimit(t, tc, 1, 2, 3)
			scheduler, err := CreateScheduler(typ, oc, storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(typ, []string{"", ""}))
			re.NoError(err)
			ops, _ := scheduler.Schedule(tc, false)
			re.Empty(ops)

			// The source remains exhausted. Only store 2 can receive a leader.
			tc.SetStoreLimit(2, storelimit.TransferLeaderIn, storelimit.Unlimited)
			ops, _ = scheduler.Schedule(tc, false)
			re.Len(ops, 1)
			step := ops[0].Step(0).(operator.TransferLeader)
			re.Equal(uint64(1), step.FromStore)
			re.Equal(uint64(2), step.ToStore)

			// A budget change after selection must still be enforced at admission.
			tc.SetStoreLimit(2, storelimit.TransferLeaderIn, 0.00006)
			re.True(oc.ExceedStoreLimit(ops[0]))
			re.False(oc.AddOperator(ops[0]))
		})
	}
}

func TestGrantHotRegionTransferLeaderInLimit(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	for _, id := range []uint64{1, 2, 3} {
		tc.AddLeaderStore(id, 0)
	}
	tc.AddLeaderRegion(1, 1, 2, 3)
	scheduler := newGrantHotRegionScheduler(oc, &grantHotRegionSchedulerConfig{
		StoreLeaderID: 2,
		StoreIDs:      []uint64{1, 2, 3},
	})
	exhaustTransferLeaderInLimit(t, tc, 2)
	op, err := scheduler.transfer(tc, 1, 1, true)
	re.Error(err)
	re.Nil(op)

	tc.SetStoreLimit(2, storelimit.TransferLeaderIn, storelimit.Unlimited)
	op, err = scheduler.transfer(tc, 1, 1, true)
	re.NoError(err)
	re.NotNil(op)
	re.Equal(uint64(2), op.Step(0).(operator.TransferLeader).ToStore)
}

func exhaustTransferLeaderInLimit(t *testing.T, cluster *mockcluster.Cluster, storeIDs ...uint64) {
	t.Helper()
	// One token takes long enough to refill that no sleeps or timing assertions are needed.
	for _, id := range storeIDs {
		cluster.SetStoreLimit(id, storelimit.TransferLeaderIn, 0.00006)
		cluster.ResetStoreLimit(id, storelimit.TransferLeaderIn, 0.000001)
		limiter := cluster.GetStore(id).GetStoreLimit()
		require.True(t, limiter.Take(storelimit.RegionInfluence[storelimit.TransferLeaderIn], storelimit.TransferLeaderIn, constant.Medium))
		require.False(t, cluster.GetStore(id).IsAvailable(storelimit.TransferLeaderIn, constant.Medium))
	}
}
