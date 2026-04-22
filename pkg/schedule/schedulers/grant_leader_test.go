// Copyright 2024 TiKV Project Authors.
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

	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
)

func TestGrantLeaderScheduler(t *testing.T) {
	re := require.New(t)
	cancel, _, tc, oc := prepareSchedulersTest()
	defer cancel()

	tc.AddLeaderStore(1, 1)
	tc.AddLeaderStore(2, 2)
	tc.AddLeaderStore(3, 16)
	tc.AddLeaderRegion(1, 3, 1, 2)
	tc.AddLeaderRegion(2, 3, 1, 2)
	tc.AddLeaderRegion(3, 1, 2, 3)
	tc.AddLeaderRegion(4, 2, 1, 3)
	storage := storage.NewStorageWithMemoryBackend()

	// balance leader scheduler should add operator from store 3 to store 1
	bls, err := CreateScheduler(types.BalanceLeaderScheduler, oc, storage, ConfigSliceDecoder(types.BalanceLeaderScheduler, []string{}))
	re.NoError(err)
	ops, _ := bls.Schedule(tc, false)
	re.NotEmpty(ops)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpKind(0), 3, 1)

	// add grant leader scheduler for store 3
	re.True(tc.GetStore(3).AllowLeaderTransferOut())
	gls, err := CreateScheduler(types.GrantLeaderScheduler, oc, storage, ConfigSliceDecoder(types.GrantLeaderScheduler, []string{"3"}), nil)
	re.NoError(err)
	re.True(gls.IsScheduleAllowed(tc))
	re.NoError(gls.PrepareConfig(tc))
	ops, _ = gls.Schedule(tc, false)
	operatorutil.CheckMultiSourceTransferLeader(re, ops[0], operator.OpLeader, []uint64{1, 2})
	re.False(tc.GetStore(3).AllowLeaderTransferOut())

	// balance leader scheduler should not add operator from store 3 to store 1
	ops, _ = bls.Schedule(tc, false)
	re.Empty(ops)
}
