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

package response

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

func TestAPIRegionInfoSize(t *testing.T) {
	re := require.New(t)

	// Region with 2048 KiB (2 MiB)
	region := core.NewRegionInfo(&metapb.Region{Id: 1}, nil, core.SetApproximateSizeKb(2048))
	apiInfo := NewAPIRegionInfo(region)

	// approximate_size should be 2 (MiB)
	re.Equal(int64(2), apiInfo.ApproximateSize)
	// approximate_size_kb should be 2048
	re.Equal(int64(2048), apiInfo.ApproximateSizeKb)

	// Region with 512 KiB (0.5 MiB)
	region = core.NewRegionInfo(&metapb.Region{Id: 1}, nil, core.SetApproximateSizeKb(512))
	apiInfo = NewAPIRegionInfo(region)

	// approximate_size should be 0 (MiB) due to integer division (wait, floored to 1?)
	// Actually, NewRegionInfo with SetApproximateSizeKb(512) will have field approximateSize=0.
	re.Equal(int64(0), apiInfo.ApproximateSize)
	// approximate_size_kb should be 512
	re.Equal(int64(512), apiInfo.ApproximateSizeKb)
}
