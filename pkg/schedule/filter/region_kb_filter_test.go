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

package filter

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
)

func TestRegionKBEmptyFilter(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockconfig.NewTestOptions()
	testCluster := mockcluster.NewCluster(ctx, opt)
	filter := NewRegionEmptyFilter(testCluster)

	// Test 1: Region with 2 KiB (> 1KB threshold)
	// It should NOT be filtered as empty because GetApproximateSizeKb() = 2 > 1 (threshold)
	region := core.NewRegionInfo(&metapb.Region{Id: 1}, nil, core.SetApproximateSizeKb(2))
	for i := range uint64(200) {
		id := i + 100
		testCluster.PutRegion(core.NewRegionInfo(&metapb.Region{
			Id:       id,
			StartKey: []byte(fmt.Sprintf("%20d", id)),
			EndKey:   []byte(fmt.Sprintf("%20d", id+1)),
		}, nil))
	}
	re.Equal(200, testCluster.GetTotalRegionCount())
	re.Equal(statusOK, filter.Select(region))

	// Test 2: Truly empty region (0 KiB)
	region = core.NewRegionInfo(&metapb.Region{Id: 1}, nil, core.SetApproximateSizeKb(0))
	re.Equal(statusRegionEmpty, filter.Select(region))

	// Test 3: Region with 1 KiB (<= 1KB threshold)
	// Should be empty (statusRegionEmpty)
	region = core.NewRegionInfo(&metapb.Region{Id: 1}, nil, core.SetApproximateSizeKb(1))
	re.Equal(statusRegionEmpty, filter.Select(region))
}
