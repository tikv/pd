// Copyright 2019 TiKV Project Authors.
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

package tests

import (
	"bytes"
	"sort"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/response"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// ExecuteCommand is used for test purpose.
func ExecuteCommand(root *cobra.Command, args ...string) (output []byte, err error) {
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs(args)
	err = root.Execute()
	return buf.Bytes(), err
}

// CheckStoresInfo is used to check the test results.
// CheckStoresInfo will not check Store.State because this field has been omitted pd-ctl output
func CheckStoresInfo(re *require.Assertions, stores []*response.StoreInfo, want []*response.StoreInfo) {
	re.Len(stores, len(want))
	mapWant := make(map[uint64]*response.StoreInfo)
	for _, s := range want {
		if _, ok := mapWant[s.Store.Id]; !ok {
			mapWant[s.Store.Id] = s
		}
	}
	for _, s := range stores {
		obtained := typeutil.DeepClone(s.Store.Store, core.StoreFactory)
		expected := typeutil.DeepClone(mapWant[obtained.Id].Store.Store, core.StoreFactory)
		// Ignore state
		obtained.State, expected.State = 0, 0
		obtained.NodeState, expected.NodeState = 0, 0
		// Ignore lastHeartbeat
		obtained.LastHeartbeat, expected.LastHeartbeat = 0, 0
		re.Equal(expected, obtained)

		obtainedStateName := s.Store.StateName
		expectedStateName := mapWant[obtained.Id].Store.StateName
		re.Equal(expectedStateName, obtainedStateName)
	}
}

// CheckRegionInfo is used to check the test results.
func CheckRegionInfo(re *require.Assertions, output *response.RegionInfo, expected *core.RegionInfo) {
	region := response.NewAPIRegionInfo(expected)
	output.Adjust()
	re.Equal(region, output)
}

// CheckRegionsInfo is used to check the test results.
func CheckRegionsInfo(re *require.Assertions, output *response.RegionsInfo, expected []*core.RegionInfo) {
	re.Len(expected, output.Count)
	got := output.Regions
	sort.Slice(got, func(i, j int) bool {
		return got[i].ID < got[j].ID
	})
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].GetID() < expected[j].GetID()
	})
	for i, region := range expected {
		CheckRegionInfo(re, &got[i], region)
	}
}

// CheckRegionsInfoWithoutSort is used to check the test results without sort.
func CheckRegionsInfoWithoutSort(re *require.Assertions, output *response.RegionsInfo, expected []*core.RegionInfo) {
	re.Len(expected, output.Count)
	got := output.Regions
	for i, region := range expected {
		CheckRegionInfo(re, &got[i], region)
	}
}
