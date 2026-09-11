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

	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/schedule/operator"
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
