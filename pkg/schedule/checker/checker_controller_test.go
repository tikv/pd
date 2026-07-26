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

package checker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/preparecheck"
)

func TestTryAddOperatorsRespectsPrepareChecker(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	cluster.SetEnablePlacementRules(false)
	cluster.SetMaxReplicas(3)
	cluster.AddRegionStore(1, 1)
	cluster.AddRegionStore(2, 0)
	cluster.AddRegionStore(3, 0)
	region := cluster.AddLeaderRegion(1, 1)

	stream := hbstream.NewTestHeartbeatStreams(ctx, cluster, false /* no need to run */)
	defer stream.Close()
	opController := operator.NewController(ctx, cluster.GetBasicCluster(), cluster.GetSharedConfig(), stream)
	prepareChecker := preparecheck.NewChecker(cluster.GetPrepareRegionCount)
	controller := NewController(ctx, cluster, cluster.GetCheckerConfig(), opController, prepareChecker)

	controller.tryAddOperators(region)
	re.Empty(opController.GetWaitingOperators())
	re.Empty(opController.GetOperators())

	prepareChecker.SetPrepared()
	controller.tryAddOperators(region)
	re.NotEmpty(append(opController.GetWaitingOperators(), opController.GetOperators()...))
}
