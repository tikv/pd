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

package statistics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestIsHot(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := utils.RWType(0); i < utils.RWTypeLen; i++ {
		cluster := core.NewBasicCluster()
		cache := NewHotCache(ctx, cluster)
		region := buildRegion(cluster, i, 3, 60)
		stats := cache.CheckReadPeerSync(region, region.GetPeers(), []float64{100000000, 1000, 1000}, 60)
		cache.Update(stats[0], i)
		for range 100 {
			re.True(cache.IsRegionHot(region, 1))
		}
	}
}
