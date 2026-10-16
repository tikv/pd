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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unrolled/render"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
)

func TestGrantHotRegionUpdateConfigWithInvalidLeaderIDType(t *testing.T) {
	re := require.New(t)
	handler := &grantHotRegionHandler{
		config: &grantHotRegionSchedulerConfig{},
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	body, err := json.Marshal(map[string]any{
		"store-id":        "1,2",
		"store-leader-id": 1,
	})
	re.NoError(err)
	req := httptest.NewRequest(http.MethodPost, "/config", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	re.NotPanics(func() {
		handler.updateConfig(resp, req)
	})
	re.Equal(http.StatusBadRequest, resp.Code)
}

func TestGrantHotRegionTransferLeaderInLimit(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	for _, id := range []uint64{1, 2, 3} {
		tc.AddLeaderStore(id, 0)
	}
	tc.AddLeaderRegion(1, 1, 2, 3)
	scheduler, err := CreateScheduler(types.GrantHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.GrantHotRegionScheduler, []string{"2", "1,2,3"}))
	re.NoError(err)
	grant := scheduler.(*grantHotRegionScheduler)
	exhaustTransferLeaderInLimit(t, tc, 2)
	op, err := grant.transfer(tc, 1, 1, true)
	re.Error(err)
	re.Nil(op)

	tc.SetStoreLimit(2, storelimit.TransferLeaderIn, storelimit.Unlimited)
	tc.ResetStoreLimit(2, storelimit.TransferLeaderIn, storelimit.Unlimited/time.Minute.Seconds())
	op, err = grant.transfer(tc, 1, 1, true)
	re.NoError(err)
	operatorutil.CheckTransferLeader(re, op, operator.OpLeader, 1, 2)
}

func TestGrantHotRegionTransferPeerRole(t *testing.T) {
	for _, learner := range []bool{false, true} {
		name := "voter"
		if learner {
			name = "learner"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			cancel, _, tc, oc := prepareSchedulersTest()
			defer cancel()
			tc.SetEnablePlacementRules(true)
			for id := uint64(1); id <= 4; id++ {
				engine := "tikv"
				if learner && id >= 3 {
					engine = "tiflash"
				}
				tc.AddLabelsStore(id, 0, map[string]string{"engine": engine})
			}
			rm := tc.GetRuleManager()
			voter := rm.GetRule("pd", "default").Clone()
			voter.Count = 3
			voter.LabelConstraints = []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tikv"}}}
			if learner {
				voter.Count = 2
			}
			re.NoError(rm.SetRule(voter))
			if learner {
				rule := voter.Clone()
				rule.ID = "learner"
				rule.Role = placement.Learner
				rule.Count = 1
				rule.LabelConstraints = []placement.LabelConstraint{{Key: "engine", Op: placement.In, Values: []string{"tiflash"}}}
				re.NoError(rm.SetRule(rule))
			}
			region := tc.AddLeaderRegion(1, 1, 2, 3)
			if learner {
				region = region.Clone(core.WithRole(region.GetStorePeer(3).GetId(), metapb.PeerRole_Learner))
				tc.PutRegion(region)
			}
			re.True(rm.FitRegionWithoutCache(tc, region).IsSatisfied())
			scheduler, err := CreateScheduler(types.GrantHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(),
				ConfigSliceDecoder(types.GrantHotRegionScheduler, []string{"1", "1,3,4"}))
			re.NoError(err)
			grant := scheduler.(*grantHotRegionScheduler)
			op, err := grant.transfer(tc, region.GetID(), 3, false)
			re.NoError(err)
			if learner {
				operatorutil.CheckTransferLearner(re, op, operator.OpRegion, 3, 4)
			} else {
				operatorutil.CheckTransferPeer(re, op, operator.OpRegion, 3, 4)
			}
			// Apply the role implied by the checked operator steps to the final layout.
			role := metapb.PeerRole_Voter
			if learner {
				role = metapb.PeerRole_Learner
			}
			after := region.Clone(core.WithRemoveStorePeer(3), core.WithAddPeer(&metapb.Peer{Id: 40, StoreId: 4, Role: role}))
			re.True(rm.FitRegionWithoutCache(tc, after).IsSatisfied())
			// The store exists, but this Region has no peer on it.
			op, err = grant.transfer(tc, region.GetID(), 4, false)
			re.Error(err)
			re.Nil(op)
		})
	}
}
