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

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
)

func TestAddWaitingOperatorsSkipsSchedulingHalted(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	for storeID := uint64(1); storeID <= 3; storeID++ {
		tc.AddRegionStore(storeID, 0)
	}
	region := tc.AddLeaderRegion(1, 1, 2)

	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	defer stream.Close()
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	controller := NewController(ctx, tc, tc.GetCheckerConfig(), oc)
	result := checkRegionResult{
		ops: []*operator.Operator{
			operator.NewTestOperator(region.GetID(), region.GetRegionEpoch(), operator.OpReplica),
		},
		limit: &operatorControllerLimit{
			kind:        operator.OpReplica,
			checkerType: types.ReplicaChecker,
		},
	}

	tc.SetHaltScheduling(true, "test")
	re.False(controller.addWaitingOperators(result))
	re.Empty(oc.GetOperators())
	re.Empty(oc.GetWaitingOperators())

	tc.SetHaltScheduling(false, "test")
	re.True(controller.addWaitingOperators(result))
	re.Len(oc.GetOperators(), 1)
}
