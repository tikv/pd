// Copyright 2025 TiKV Project Authors.
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

package schedulers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/storage"
)

// BenchmarkBalanceRangeLeaderJob measures repeated job checks, including the
// configuration lock and preparation, without executing scheduling operators.
func BenchmarkBalanceRangeLeaderJob(b *testing.B) {
	for _, cfg := range []struct {
		stores, eligible, ranges int
		voter                    bool
	}{
		{64, 3, 1, false},
		{512, 3, 1, false},
		{512, 3, 32, false},
		{512, 512, 32, false},
		{512, 512, 32, true},
	} {
		role := "leader"
		if cfg.voter {
			role = "voter"
		}
		b.Run(fmt.Sprintf("%s/stores=%d/eligible=%d/ranges=%d", role, cfg.stores, cfg.eligible, cfg.ranges), func(b *testing.B) {
			re := require.New(b)
			cancel, _, tc, oc := prepareSchedulersTest()
			defer cancel()
			tc.SetEnablePlacementRules(true)
			for i := 1; i <= cfg.stores; i++ {
				pool := "other"
				if i <= cfg.eligible {
					pool = "target"
				}
				tc.AddLabelsStore(uint64(i), 0, map[string]string{"pool": pool})
			}
			const regionsPerRange = 64
			key := func(i int) string { return fmt.Sprintf("%08d", i) }
			rm := tc.GetRuleManager()
			re.NoError(rm.SetRuleGroup(&placement.RuleGroup{ID: "bench", Index: 1, Override: true}))
			rules := make([]*placement.Rule, 0, 2*cfg.ranges)
			for i := range cfg.ranges {
				rule := &placement.Rule{
					GroupID: "bench", ID: fmt.Sprintf("leader-%d", i),
					StartKeyHex: fmt.Sprintf("%x", key(i*regionsPerRange)), EndKeyHex: fmt.Sprintf("%x", key((i+1)*regionsPerRange)),
					Role: placement.Leader, Count: 1,
					LabelConstraints: []placement.LabelConstraint{{Key: "pool", Op: placement.In, Values: []string{"target"}}},
				}
				if cfg.voter {
					rule.Role, rule.Count = placement.Voter, 3
				}
				rules = append(rules, rule)
				if !cfg.voter {
					rules = append(rules, &placement.Rule{
						GroupID: "bench", ID: fmt.Sprintf("follower-%d", i),
						StartKeyHex: rule.StartKeyHex, EndKeyHex: rule.EndKeyHex,
						Role: placement.Follower, Count: 2,
					})
				}
			}
			re.NoError(rm.SetRules(rules))
			regionCount := cfg.ranges * regionsPerRange
			re.Len(rm.GetSplitKeys([]byte(key(0)), []byte(key(regionCount))), cfg.ranges-1)
			for i := range regionCount {
				tc.AddLeaderRegionWithRange(uint64(i+1), key(i), key(i+1), 1, 2, 3)
			}
			re.True(rm.FitRegionWithoutCache(tc, tc.GetRegion(1)).IsSatisfied())
			scheduler, err := CreateScheduler(types.BalanceRangeScheduler, oc, storage.NewStorageWithMemoryBackend(),
				ConfigSliceDecoder(types.BalanceRangeScheduler, []string{"leader-scatter", "tikv", "1h", "bench", key(0), key(regionCount)}))
			re.NoError(err)
			sc := scheduler.(*balanceRangeScheduler)
			re.True(sc.IsScheduleAllowed(tc))
			re.Len(sc.expectScoreMap, cfg.eligible)
			for _, expect := range sc.expectScoreMap {
				re.InDelta(float64(regionCount)/float64(cfg.eligible), expect, 1e-9)
			}
			re.Equal(running, sc.job.Status)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if !sc.IsScheduleAllowed(tc) {
					b.Fatal("leader job stopped before its leaders were balanced")
				}
			}
		})
	}
}
