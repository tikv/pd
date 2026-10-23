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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unrolled/render"

	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/errs"
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

func TestGrantHotRegionLocationSafeguard(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		for _, test := range []struct {
			name     string
			target   uint64
			isLeader bool
			allowed  bool
		}{
			{"existing-peer-leader", 2, true, true},
			{"non-peer-leader", 5, true, false},
			{"same-host-peer", 4, false, false},
			{"distinct-host-peer", 5, false, true},
		} {
			t.Run(fmt.Sprintf("rules=%t/%s", enabled, test.name), func(t *testing.T) {
				re := require.New(t)
				cancel, _, tc, oc := prepareSchedulersTest()
				defer cancel()
				tc.SetEnablePlacementRules(enabled)
				tc.SetMaxReplicasWithLabel(enabled, 3, "host")
				for id, host := range map[uint64]string{1: "h1", 2: "h2", 3: "h3", 4: "h2", 5: "h5"} {
					tc.AddLabelsStore(id, 1, map[string]string{"host": host})
				}
				tc.AddLeaderRegion(1, 1, 2, 3)
				target := strconv.FormatUint(test.target, 10)
				scheduler, err := CreateScheduler(types.GrantHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(),
					ConfigSliceDecoder(types.GrantHotRegionScheduler, []string{target, target}))
				re.NoError(err)
				op, err := scheduler.(*grantHotRegionScheduler).transfer(tc, 1, 1, test.isLeader)
				if !test.allowed {
					re.Error(err)
					re.Nil(op)
					return
				}
				re.NoError(err)
				if test.isLeader {
					operatorutil.CheckTransferLeader(re, op, operator.OpLeader, 1, test.target)
				} else {
					operatorutil.CheckTransferPeerWithLeaderTransfer(re, op, operator.OpRegion|operator.OpLeader, 1, test.target)
				}
			})
		}
	}
}

func TestGrantHotRegionTransferLeaderWithPlacementRules(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()
	tc.SetEnablePlacementRules(true)
	for id := uint64(1); id <= 3; id++ {
		tc.AddLabelsStore(id, 1, map[string]string{"host": fmt.Sprintf("h%d", id)})
	}
	tc.AddLeaderRegion(1, 1, 2, 3)
	re.NoError(tc.SetRules([]*placement.Rule{
		{GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID, Role: placement.Follower, Count: 2},
		{GroupID: placement.DefaultGroupID, ID: "leader", Role: placement.Leader, Count: 1,
			LabelConstraints: []placement.LabelConstraint{{Key: "host", Op: placement.In, Values: []string{"h1", "h3"}}}},
	}))
	scheduler, err := CreateScheduler(types.GrantHotRegionScheduler, oc, storage.NewStorageWithMemoryBackend(),
		ConfigSliceDecoder(types.GrantHotRegionScheduler, []string{"2", "1,2,3"}))
	re.NoError(err)
	grant := scheduler.(*grantHotRegionScheduler)
	op, err := grant.transfer(tc, 1, 1, true)
	re.ErrorIs(err, errs.ErrCheckerNotFound)
	re.Nil(op)
	op, err = grant.transfer(tc, 1, 2, true)
	re.ErrorIs(err, errs.ErrCheckerNotFound)
	re.Nil(op)

	grant.conf.setStoreLeaderID(3)
	op, err = grant.transfer(tc, 1, 1, true)
	re.NoError(err)
	operatorutil.CheckTransferLeader(re, op, operator.OpLeader, 1, 3)

	// The selected hot peer can be a follower; validate the actual leader's rule.
	op, err = grant.transfer(tc, 1, 2, true)
	re.NoError(err)
	operatorutil.CheckTransferLeader(re, op, operator.OpLeader, 1, 3)
}
