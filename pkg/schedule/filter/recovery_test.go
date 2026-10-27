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

package filter

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/plan"
)

func TestRecoveryFreezesDiscretionaryRegionSelection(t *testing.T) {
	region := core.NewRegionInfo(&metapb.Region{Id: 1}, nil, core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_RECOVERING_LEADER}))
	require.False(t, IsRegionHealthy(region))
	require.False(t, IsRegionHealthyAllowPending(region))
	require.Equal(t, plan.StatusCode(plan.StatusRegionRecovering), NewRegionDownFilter().Select(region).StatusCode)
	region = region.Clone(core.WithRecoveryStatus(nil))
	require.True(t, IsRegionHealthy(region))
	require.True(t, NewRegionDownFilter().Select(region).IsOK())
}
