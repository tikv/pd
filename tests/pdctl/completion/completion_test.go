// Copyright 2020 TiKV Project Authors.
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

package completion_test

import (
	"testing"

	"github.com/stretchr/testify/require"
<<<<<<< HEAD:tests/pdctl/completion/completion_test.go
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
=======
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
>>>>>>> 20087e290 (statistics: add gc in hot peer cache (#8702)):pkg/statistics/hot_cache_test.go
)

func TestCompletion(t *testing.T) {
	re := require.New(t)
<<<<<<< HEAD:tests/pdctl/completion/completion_test.go
	cmd := pdctlCmd.GetRootCmd()

	// completion command
	args := []string{"completion", "bash"}
	_, err := pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)

	// completion command
	args = []string{"completion", "zsh"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
=======
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
>>>>>>> 20087e290 (statistics: add gc in hot peer cache (#8702)):pkg/statistics/hot_cache_test.go
}
