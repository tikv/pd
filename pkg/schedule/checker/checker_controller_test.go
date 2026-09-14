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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/schedule/operator"
)

func TestCheckRegionMigratesLegacyWitnessPeer(t *testing.T) {
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()
	tc.SetEnablePlacementRules(false)

	peers := []*metapb.Peer{
		{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter, IsWitness: true},
		{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
	}
	region := core.NewRegionInfo(&metapb.Region{Id: 900, Peers: peers}, peers[0])
	tc.PutRegion(region)

	ops := controller.CheckRegion(region)
	require.Len(t, ops, 1)
	op := ops[0]
	require.Equal(t, "migrate-deprecated-witness-peer", op.Desc())
	require.NotZero(t, op.Kind()&operator.OpReplica)
	require.Equal(t, constant.High, op.GetPriorityLevel())

	for i := range op.Len() {
		step, ok := op.Step(i).(operator.BecomeNonWitness)
		if !ok {
			continue
		}
		cmd := step.GetCmd(region, true)
		require.Len(t, cmd.SwitchWitnesses.GetSwitchWitnesses(), 1)
		require.False(t, cmd.SwitchWitnesses.GetSwitchWitnesses()[0].GetIsWitness())
		return
	}
	require.Fail(t, "migration operator has no conversion step")
}
