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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// initialize places the groups one by one in input order, each on its best
// target given only the groups placed before it, like the first targets of a
// new table.
func initialize(in *Input) {
	placed := &Input{Stores: in.Stores, BaseVoters: slices.Clone(in.BaseVoters), BaseLeaders: slices.Clone(in.BaseLeaders)}
	for i := range in.Groups {
		one := *placed
		one.Groups = []Group{in.Groups[i]}
		r := Plan(&one, Options{Sweeps: 1, MaxAttempts: -1})
		in.Groups[i].Voters, in.Groups[i].Leader = r.Voters[0], r.Leaders[0]
		for _, s := range r.Voters[0] {
			placed.BaseVoters[s]++
		}
		placed.BaseLeaders[r.Leaders[0]]++
	}
}

// TestBroadLifecycle enables balancing on tables whose groups all start
// on the same three stores, and plans again after the groups were placed one
// by one, as for the first targets of a new table. Every plan reaches its
// lower bound, which proves it optimal.
func TestBroadLifecycle(t *testing.T) {
	for _, c := range []struct {
		n       int
		leaders []int
		want    Score
	}{
		{36, nil, Score{972, 108}},
		{36, []int{0, 4, 8}, Score{1008, 432}},
		{128, []int{0, 4, 8}, Score{12746, 5462}},
	} {
		in := broad(c.n, 4, c.leaders, true)
		r := Plan(in, Options{})
		require.Equal(t, StopBound, r.Stop)
		require.Equal(t, c.want, r.Score)
		initialize(in)
		again := Plan(in, Options{Seed: 1})
		require.Equal(t, StopBound, again.Stop)
		require.Equal(t, c.want, again.Score)
		t.Logf("broad %d leaders=%v: %v -> %v changing %d; placed one by one %v -> %v changing %d",
			c.n, c.leaders, r.Baseline, r.Score, r.Changed, again.Baseline, again.Score, again.Changed)
	}
}
