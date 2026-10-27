// Copyright 2022 TiKV Project Authors.
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

package server

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

func TestPatchResourceGroup(t *testing.T) {
	re := require.New(t)
	rg := &ResourceGroup{Name: testResourceGroupName, Mode: rmpb.GroupMode_RUMode, RUSettings: NewRequestUnitSettings(testResourceGroupName, nil)}
	testCaseRU := []struct {
		patchJSONString  string
		expectJSONString string
	}{
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000},"state":{"initialized":false}}},"priority":0}`},
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000, "burst_limit": -1}}}}`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000,"burst_limit":-1},"state":{"initialized":false}}},"priority":0}`},
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000, "burst_limit": -1}}}, "priority": 8, "runaway_settings": {"rule":{"exec_elapsed_time_ms":10000}, "action":1} }`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000,"burst_limit":-1},"state":{"initialized":false}}},"priority":8,"runaway_settings":{"rule":{"exec_elapsed_time_ms":10000},"action":1}}`},
	}

	for _, ca := range testCaseRU {
		patch := &rmpb.ResourceGroup{}
		err := json.Unmarshal([]byte(ca.patchJSONString), patch)
		re.NoError(err)
		err = rg.PatchSettings(patch)
		re.NoError(err)
		res, err := json.Marshal(rg.Clone(false))
		re.NoError(err)
		re.Equal(ca.expectJSONString, string(res))
	}
}

func TestClone(t *testing.T) {
	re := require.New(t)
	for range 11 {
		var rg ResourceGroup
		re.NoError(gofakeit.Struct(&rg))
		rgClone := rg.Clone(true)
		require.Equal(t, &rg, rgClone)
	}
}

func TestApplySettingsWithoutTokenPatch(t *testing.T) {
	re := require.New(t)
	rg := &ResourceGroup{
		Name: testResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &RequestUnitSettings{
			RU: &GroupTokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
				GroupTokenBucketState: GroupTokenBucketState{
					Tokens:             123.0,
					overrideFillRate:   -1,
					overrideBurstLimit: -1,
				},
			},
		},
	}
	patch := &rmpb.ResourceGroup{
		Name: testResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   500,
					BurstLimit: 800,
				},
				Tokens: 999.0,
			},
		},
	}
	re.NoError(rg.ApplySettings(patch))
	re.Equal(float64(500), rg.getFillRate())
	re.Equal(int64(800), rg.getBurstLimit())
	re.Equal(123.0, rg.getRUToken())
}

func TestResourceGroupConsumptionSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name            string
		settings, state *rmpb.Consumption
		want            float64
	}{
		{name: "state_only", state: &rmpb.Consumption{RRU: 5}, want: 5},
		{name: "overlapping_snapshots", settings: &rmpb.Consumption{RRU: 5}, state: &rmpb.Consumption{RRU: 5}, want: 5},
		{name: "newer_state", settings: &rmpb.Consumption{RRU: 3}, state: &rmpb.Consumption{RRU: 5}, want: 5},
		{name: "newer_settings", settings: &rmpb.Consumption{RRU: 5}, state: &rmpb.Consumption{RRU: 3}, want: 5},
		{name: "missing_state_usage", settings: &rmpb.Consumption{RRU: 5}, want: 5},
		{name: "zero_state_usage", settings: &rmpb.Consumption{RRU: 5}, state: &rmpb.Consumption{}, want: 5},
		{name: "zero_usage", state: &rmpb.Consumption{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := FromProtoResourceGroup(&rmpb.ResourceGroup{Name: "snapshots", Mode: rmpb.GroupMode_RUMode, RUStats: tc.settings})
			states := &GroupStates{RUConsumption: tc.state}
			rg.SetStatesIntoResourceGroup(states)
			require.Equal(t, tc.want, rg.Clone(true).RUConsumption.RRU)
			rg.SetStatesIntoResourceGroup(states)
			require.Equal(t, tc.want, rg.Clone(true).RUConsumption.RRU)
		})
	}

	t.Run("all_counters", func(t *testing.T) {
		re := require.New(t)
		initial := &rmpb.Consumption{
			RRU: 5, WRU: 20, ReadBytes: 30, WriteBytes: 4,
			TotalCpuTimeMs: 5, SqlLayerCpuTimeMs: 60, KvReadRpcCount: 70, KvWriteRpcCount: 8,
			ReadCrossAzTrafficBytes: 9, WriteCrossAzTrafficBytes: 100,
			TikvRUV2: 1.5, TidbRUV2: 25, TiflashRUV2: 35,
		}
		snapshot := &rmpb.Consumption{
			RRU: 10, WRU: 2, ReadBytes: 3, WriteBytes: 40,
			TotalCpuTimeMs: 50, SqlLayerCpuTimeMs: 6, KvReadRpcCount: 7, KvWriteRpcCount: 80,
			ReadCrossAzTrafficBytes: 90, WriteCrossAzTrafficBytes: 10,
			TikvRUV2: 15, TidbRUV2: 2.5, TiflashRUV2: 3.5,
		}
		want := &rmpb.Consumption{
			RRU: 10, WRU: 20, ReadBytes: 30, WriteBytes: 40,
			TotalCpuTimeMs: 50, SqlLayerCpuTimeMs: 60, KvReadRpcCount: 70, KvWriteRpcCount: 80,
			ReadCrossAzTrafficBytes: 90, WriteCrossAzTrafficBytes: 100,
			TikvRUV2: 15, TidbRUV2: 25, TiflashRUV2: 35,
		}
		rg := FromProtoResourceGroup(&rmpb.ResourceGroup{Name: "all-counters", Mode: rmpb.GroupMode_RUMode, RUStats: initial})
		rg.SetStatesIntoResourceGroup(&GroupStates{RUConsumption: snapshot})
		re.Equal(want, rg.Clone(true).RUConsumption)
		rg.SetStatesIntoResourceGroup(&GroupStates{RUConsumption: snapshot})
		re.Equal(want, rg.Clone(true).RUConsumption)
		rg.UpdateRUConsumption(&rmpb.Consumption{RRU: 2})
		re.Equal(12.0, rg.Clone(true).RUConsumption.RRU)
		re.Equal(10.0, snapshot.RRU, "restoring must not retain the input consumption pointer")
	})

	t.Run("concurrent_live_usage_and_stale_snapshot", func(t *testing.T) {
		rg := FromProtoResourceGroup(&rmpb.ResourceGroup{Name: "concurrent", Mode: rmpb.GroupMode_RUMode})
		rg.UpdateRUConsumption(&rmpb.Consumption{RRU: 5})
		snapshot := rg.GetGroupStates()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range 100 {
				rg.SetStatesIntoResourceGroup(snapshot)
			}
		}()
		go func() {
			defer wg.Done()
			for range 100 {
				rg.UpdateRUConsumption(&rmpb.Consumption{RRU: 1})
			}
		}()
		wg.Wait()
		require.Equal(t, 105.0, rg.Clone(true).RUConsumption.RRU)
	})
}
