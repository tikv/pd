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

package schedule

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestRemoveSchedulerConfigsKeepsConcurrentUpdates(t *testing.T) {
	re := require.New(t)

	invalidOldCfg := sc.SchedulerConfig{
		Type: types.SchedulerTypeCompatibleMap[types.GrantLeaderScheduler],
		Args: []string{"1"},
	}
	concurrentUpdatedCfg := sc.SchedulerConfig{
		Type: types.SchedulerTypeCompatibleMap[types.GrantLeaderScheduler],
		Args: []string{"2"},
	}
	concurrentAddedCfg := sc.SchedulerConfig{
		Type: types.SchedulerTypeCompatibleMap[types.ShuffleRegionScheduler],
	}
	latest := sc.SchedulerConfigs{
		sc.DefaultSchedulers[0],
		invalidOldCfg,
		concurrentUpdatedCfg,
		concurrentAddedCfg,
	}

	re.Equal(sc.SchedulerConfigs{
		sc.DefaultSchedulers[0],
		concurrentUpdatedCfg,
		concurrentAddedCfg,
	}, removeSchedulerConfigs(latest, sc.SchedulerConfigs{invalidOldCfg}))
}
