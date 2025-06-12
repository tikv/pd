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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeCheck(t *testing.T) {
	re := require.New(t)
	testdata := []struct {
		size      uint64
		mergeSize uint64
		keys      uint64
		mergeKeys uint64
		pass      bool
	}{{
		// case 1: the merged region size is smaller than the max region size
		size:      96 + 20,
		mergeSize: 20,
		keys:      1440000 + 200000,
		mergeKeys: 200000,
		pass:      true,
	}, {
		// case 2: the smallest region is 68MiB, it can't be merged again.
		size:      144 + 20,
		mergeSize: 20,
		keys:      1440000 + 200000,
		mergeKeys: 200000,
		pass:      true,
	}, {
		// case 3: the smallest region is 50MiB, it can be merged again.
		size:      144 + 2,
		mergeSize: 50,
		keys:      1440000 + 20000,
		mergeKeys: 500000,
		pass:      false,
	}, {
		// case4: the smallest region is 51MiB, it can't be merged again.
		size:      144 + 3,
		mergeSize: 50,
		keys:      1440000 + 30000,
		mergeKeys: 500000,
		pass:      true,
	}}
	config := &StoreConfig{}
	for _, v := range testdata {
		if v.pass {
			re.NoError(config.CheckRegionSize(v.size, v.mergeSize))
			re.NoError(config.CheckRegionKeys(v.keys, v.mergeKeys))
		} else {
			re.Error(config.CheckRegionSize(v.size, v.mergeSize))
			re.Error(config.CheckRegionKeys(v.keys, v.mergeKeys))
		}
	}
	// Test CheckRegionSize when the region split size is 0.
	config.RegionSplitSize = "100KiB"
	config.Adjust()
	re.Empty(config.GetRegionSplitSize())
	re.NoError(config.CheckRegionSize(defaultRegionMaxSize, 50))
}
