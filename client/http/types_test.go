// Copyright 2023 TiKV Project Authors.
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

package http

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeRegionsInfo(t *testing.T) {
	re := require.New(t)
	regionsInfo1 := &RegionsInfo{
		Count: 1,
		Regions: []RegionInfo{
			{
				ID:       1,
				StartKey: "",
				EndKey:   "a",
			},
		},
	}
	regionsInfo2 := &RegionsInfo{
		Count: 1,
		Regions: []RegionInfo{
			{
				ID:       2,
				StartKey: "a",
				EndKey:   "",
			},
		},
	}
	regionsInfo := regionsInfo1.Merge(regionsInfo2)
	re.Equal(int64(2), regionsInfo.Count)
	re.Equal(2, len(regionsInfo.Regions))
	re.Equal(append(regionsInfo1.Regions, regionsInfo2.Regions...), regionsInfo.Regions)
}
