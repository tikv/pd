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

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
)

func TestDiagnosticPendingTransitions(t *testing.T) {
	for _, tp := range []types.CheckerSchedulerType{types.BalanceRegionScheduler, types.BalanceLeaderScheduler} {
		t.Run(tp.String(), func(t *testing.T) {
			newPlans := func(storeID uint64) []plan.Plan {
				return []plan.Plan{&plan.BalanceSchedulerPlan{
					Source: core.NewStoreInfo(&metapb.Store{Id: storeID}),
					Status: plan.NewStatus(plan.StatusStoreRemoving),
				}}
			}
			t.Run("reach limit", func(t *testing.T) {
				re := require.New(t)
				recorder := NewDiagnosticRecorder(tp, nil)
				recorder.SetResultFromPlans(nil, newPlans(1))
				re.Equal("1 store(s) StoreRemoving; ", recorder.GetLastResult().Summary)
				recorder.SetResultFromStatus(Pending)
				result := recorder.GetLastResult()
				re.Equal(Pending, result.Status)
				re.Equal(tp.String()+" reach limit", result.Summary)
			})
			t.Run("resume scheduling", func(t *testing.T) {
				re := require.New(t)
				recorder := NewDiagnosticRecorder(tp, nil)
				recorder.SetResultFromPlans(nil, newPlans(1))
				recorder.SetResultFromStatus(Pending)
				recorder.SetResultFromPlans(nil, newPlans(2))
				result := recorder.GetLastResult()
				re.Equal(Pending, result.Status)
				re.Equal("1 store(s) StoreRemoving; ", result.Summary)
			})
			t.Run("continuous store diagnostics", func(t *testing.T) {
				re := require.New(t)
				recorder := NewDiagnosticRecorder(tp, nil)
				recorder.SetResultFromPlans(nil, newPlans(1))
				recorder.SetResultFromPlans(nil, newPlans(2))
				result := recorder.GetLastResult()
				re.Equal(Pending, result.Status)
				re.Equal("2 store(s) StoreRemoving; ", result.Summary)
			})
		})
	}
}
