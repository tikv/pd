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

package balanceplan

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// revertChain lets greedy save a neutral chain shift together with an
// independent improvement. Only the last shifted group can revert per pass.
func revertChain(groups int) *Input {
	n := groups + 3
	in := &Input{Stores: n, BaseVoters: make([]int, n), BaseLeaders: make([]int, n)}
	can := allTrue(n)
	for i := range groups {
		in.Groups = append(in.Groups, Group{Rules: []*Rule{{Count: 1, Stores: []int{i, i + 1}}},
			Voters: []int{i + 1}, Leader: i + 1, CanLead: can})
	}
	in.BaseVoters[groups+1], in.BaseLeaders[groups+1] = 1, 1
	in.Groups = append(in.Groups, Group{Rules: []*Rule{{Count: 1, Stores: []int{groups + 1, groups + 2}}},
		Voters: []int{groups + 1}, Leader: groups + 1, CanLead: can})
	return in
}

func TestRevertChainStops(t *testing.T) {
	const groups = 1000
	for _, reason := range []StopReason{StopBudget, StopCanceled} {
		for _, trials := range []int{0, 17, groups} {
			t.Run(string(reason)+"/"+strconv.Itoa(trials), func(t *testing.T) {
				re := require.New(t)
				in := revertChain(groups)
				p := newPlanner(in, Options{Sweeps: 1, WorkBudget: DefaultWorkBudget})
				origV, origL := p.layout()
				voters, leaders, best := p.greedy(0)
				for i := range groups {
					re.Equal([]int{i}, voters[i], "the saved snapshot must contain the whole chain shift")
				}
				work, visits := p.work, p.visits
				if reason == StopCanceled {
					p.opt.Cancel = func() bool { return p.visits-visits >= trials }
				} else {
					// Each one-voter move processes four load entries. A
					// rejected trial also restores those four entries.
					p.opt.WorkBudget = work + int64(8*trials)
					if trials == groups {
						p.opt.WorkBudget -= 4 // the last chain group reverts
					} else if trials > 0 {
						p.opt.WorkBudget -= 3 // interrupt only between complete trials
					}
				}
				score := p.revert(origV, origL, voters, leaders)
				re.Equal(reason, p.stop)
				re.Equal(trials, p.visits-visits)
				re.Equal(best, score)
				re.Equal(scoreOf(in, voters, leaders), score)
				for i := range in.Groups {
					re.True(matchesTarget(legalTargets(in, i), voters[i], nil, leaders[i]))
				}
				if trials == groups {
					re.Equal(origV[groups-1], voters[groups-1], "a successful reversion must stay committed at cutoff")
					re.Equal(origL[groups-1], leaders[groups-1])
				}
				if reason == StopBudget {
					re.LessOrEqual(p.work, p.opt.WorkBudget+7)
				}
				expectedWork := int64(8 * trials)
				if trials == groups {
					expectedWork -= 4
				}
				re.Equal(expectedWork, p.work-work)
				t.Logf("stop=%s trials=%d work=%d", p.stop, p.visits-visits, p.work-work)
				stoppedWork, stoppedVisits := p.work, p.visits
				re.Equal(score, p.revert(origV, origL, voters, leaders))
				re.Equal(scoreOf(in, voters, leaders), score)
				re.Equal(stoppedWork, p.work, "an already stopped cleanup must not process another trial")
				re.Equal(stoppedVisits, p.visits)
			})
		}
	}
}

func TestPlanRevertStops(t *testing.T) {
	const groups = 32
	in := revertChain(groups)
	searchChecks := 0
	opt := Options{Sweeps: 1, WorkBudget: DefaultWorkBudget, Cancel: func() bool { searchChecks++; return false }}
	p := newPlanner(in, opt)
	_, _, best := p.greedy(opt.Seed)
	for _, reason := range []StopReason{StopBudget, StopCanceled} {
		for _, trials := range []int{0, groups} {
			t.Run(string(reason)+"/"+strconv.Itoa(trials), func(t *testing.T) {
				re := require.New(t)
				run := func() Result {
					checks := 0
					options := opt
					options.Cancel = nil
					if reason == StopBudget {
						options.WorkBudget = p.work + int64(8*trials)
						if trials > 0 {
							options.WorkBudget -= 4
						}
					} else {
						cancelAfter := searchChecks
						if trials > 0 {
							cancelAfter += 1 + trials
						}
						options.Cancel = func() bool {
							checks++
							// Cleanup checks once at entry, then before
							// every trial in this first sweep.
							return checks > cancelAfter
						}
					}
					return Plan(in, options)
				}
				r := run()
				re.Equal(reason, r.Stop)
				re.Equal(p.visits+trials, r.Visits)
				expectedWork := p.work + int64(8*trials)
				if trials > 0 {
					expectedWork -= 4
				}
				re.Equal(expectedWork, r.Work)
				re.Equal(best, r.Score)
				re.Equal(scoreOf(in, r.Voters, r.Leaders), r.Score)
				re.False(r.Baseline.Less(r.Score))
				re.Equal(r, run(), "the same seed and cutoff must produce the same result")
			})
		}
	}
}
