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

package affinity

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBalanceScorerCountsFixedGroupsAndSkipsEmptyGroups(t *testing.T) {
	scorer, err := NewBalanceScorer([]*Group{
		{ID: "fixed", BalancePolicy: BalancePolicyFixed, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
		{ID: "auto", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}},
		{ID: "empty"},
	})
	require.NoError(t, err)
	require.Equal(t, BalanceScore{Leader: 4, Voter: 8}, scorer.Score())
	require.Equal(t, 2, scorer.LeaderCount(1))
	require.Equal(t, 0, scorer.LeaderCount(3))
	require.Equal(t, 2, scorer.VoterCount(1))
	require.Equal(t, 2, scorer.VoterCount(2))

	_, ok := scorer.Evaluate(BalanceMove{GroupID: "fixed", LeaderStoreID: 2})
	require.False(t, ok)
	next, ok := scorer.Evaluate(BalanceMove{GroupID: "auto", LeaderStoreID: 2})
	require.True(t, ok)
	require.Equal(t, BalanceScore{Leader: 2, Voter: 8}, next)
}

func TestBalanceScorerEvaluatesLeaderAndVoterMoves(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p1", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p2", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p3", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p4", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	scorer, err := NewBalanceScorer(groups)
	require.NoError(t, err)
	require.Equal(t, BalanceScore{Leader: 11, Voter: 75}, scorer.Score())

	for _, tc := range []struct {
		name     string
		move     BalanceMove
		score    BalanceScore
		improves bool
	}{
		{"leader", BalanceMove{GroupID: "p0", LeaderStoreID: 2}, BalanceScore{Leader: 9, Voter: 75}, true},
		{"voter", BalanceMove{GroupID: "p0", LeaderStoreID: 1, FromStoreID: 3, ToStoreID: 4}, BalanceScore{Leader: 11, Voter: 67}, true},
		// The old leader remains a voter while a different voter is replaced.
		{"leader and voter", BalanceMove{GroupID: "p0", LeaderStoreID: 2, FromStoreID: 3, ToStoreID: 4}, BalanceScore{Leader: 9, Voter: 67}, true},
		{"worse", BalanceMove{GroupID: "p3", LeaderStoreID: 1}, BalanceScore{Leader: 17, Voter: 75}, false},
		// Voter gains cannot compensate for a worse leader distribution,
		// even when Leader+Voter would be smaller.
		{"voter gain with leader loss", BalanceMove{GroupID: "p3", LeaderStoreID: 1, FromStoreID: 3, ToStoreID: 4}, BalanceScore{Leader: 17, Voter: 67}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			score, improves := scorer.Evaluate(tc.move)
			require.Equal(t, tc.score, score)
			require.Equal(t, tc.improves, improves)
		})
	}
}

func TestBalanceScorerLeaderGainTakesPriorityOverVoterLoss(t *testing.T) {
	scorer, err := NewBalanceScorer([]*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p1", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4}},
		{ID: "p2", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 4}},
	})
	require.NoError(t, err)
	require.Equal(t, BalanceScore{Leader: 5, Voter: 23}, scorer.Score())
	score, improves := scorer.Evaluate(BalanceMove{GroupID: "p0", LeaderStoreID: 4, FromStoreID: 3, ToStoreID: 4})
	require.True(t, improves)
	require.Equal(t, BalanceScore{Leader: 3, Voter: 27}, score)
}

func TestBalanceScorerStrictImprovementsTerminate(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p1", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p2", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "p3", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3}},
	}
	stores := []uint64{1, 2, 3, 4}
	initial, err := NewBalanceScorer(groups)
	require.NoError(t, err)
	initialScore := initial.Score()
	accepted := 0

	for range 1000 {
		scorer, err := NewBalanceScorer(groups)
		require.NoError(t, err)
		move, next, ok := firstImprovingBalanceMove(scorer, groups, stores)
		if !ok {
			require.Positive(t, accepted)
			require.Negative(t, scorer.Score().Compare(initialScore))
			return
		}
		require.Negative(t, next.Compare(scorer.Score()))
		applyBalanceMove(groups, move)
		accepted++
	}
	t.Fatal("strictly improving balance moves did not terminate")
}

func TestBalanceScorerDoesNotMutateInput(t *testing.T) {
	groups := []*Group{
		{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{3, 1, 2}},
		{ID: "p1", LeaderStoreID: 2, VoterStoreIDs: []uint64{3, 1, 2}},
		{ID: "p2", LeaderStoreID: 3, VoterStoreIDs: []uint64{3, 1, 2}},
	}
	originalVoters := make([][]uint64, len(groups))
	for i, group := range groups {
		originalVoters[i] = slices.Clone(group.VoterStoreIDs)
	}

	scorer, err := NewBalanceScorer(groups)
	require.NoError(t, err)
	next, ok := scorer.Evaluate(BalanceMove{
		GroupID: "p0", LeaderStoreID: 1, FromStoreID: 3, ToStoreID: 4,
	})
	require.True(t, ok)
	require.Equal(t, BalanceScore{Leader: 3, Voter: 23}, next)
	for i, group := range groups {
		require.Equal(t, originalVoters[i], group.VoterStoreIDs)
	}
}

func TestNewBalanceScorerRejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		groups []*Group
		want   string
	}{
		{name: "nil group", groups: []*Group{nil}, want: "nil affinity group"},
		{
			name:   "duplicate IDs",
			groups: []*Group{{ID: "p0"}, {ID: "p0"}},
			want:   `duplicate affinity group ID "p0"`,
		},
		{
			name:   "invalid policy",
			groups: []*Group{{ID: "p0", BalancePolicy: BalancePolicy("invalid")}},
			want:   `group "p0" has invalid balance policy "invalid"`,
		},
		{
			name:   "partial placement",
			groups: []*Group{{ID: "p0", LeaderStoreID: 1}},
			want:   `group "p0" must have both leader and voter stores`,
		},
		{
			name:   "duplicate voters",
			groups: []*Group{{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 1, 2}}},
			want:   `group "p0" has duplicate voter store ID`,
		},
		{
			name:   "leader is not a voter",
			groups: []*Group{{ID: "p0", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2}}},
			want:   `group "p0" leader store ID is not a voter`,
		},
		{
			name:   "zero voter store",
			groups: []*Group{{ID: "p0", LeaderStoreID: 1, VoterStoreIDs: []uint64{0, 1}}},
			want:   `group "p0" has invalid voter store ID 0`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewBalanceScorer(tc.groups)
			require.EqualError(t, err, tc.want)
		})
	}
}

func TestBalanceScorerRejectsInvalidMoveShapes(t *testing.T) {
	scorer, err := NewBalanceScorer([]*Group{
		{ID: "auto", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		{ID: "fixed", BalancePolicy: BalancePolicyFixed, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
	})
	require.NoError(t, err)
	for _, move := range []BalanceMove{
		{GroupID: "missing", LeaderStoreID: 2},
		{GroupID: "fixed", LeaderStoreID: 2},
		{GroupID: "auto", LeaderStoreID: 0},
		{GroupID: "auto", LeaderStoreID: 4},
		{GroupID: "auto", LeaderStoreID: 2, FromStoreID: 1},
		{GroupID: "auto", LeaderStoreID: 2, FromStoreID: 4, ToStoreID: 5},
		{GroupID: "auto", LeaderStoreID: 2, FromStoreID: 1, ToStoreID: 3},
		{GroupID: "auto", LeaderStoreID: 1, FromStoreID: 1, ToStoreID: 4},
		{GroupID: "auto", LeaderStoreID: 6, FromStoreID: 3, ToStoreID: 4},
	} {
		_, ok := scorer.Evaluate(move)
		require.False(t, ok, "move: %+v", move)
	}
}

func firstImprovingBalanceMove(scorer *BalanceScorer, groups []*Group, stores []uint64) (BalanceMove, BalanceScore, bool) {
	for _, group := range groups {
		for _, leader := range group.VoterStoreIDs {
			move := BalanceMove{GroupID: group.ID, LeaderStoreID: leader}
			if score, ok := scorer.Evaluate(move); ok {
				return move, score, true
			}
		}
		for _, from := range group.VoterStoreIDs {
			for _, to := range stores {
				if slices.Contains(group.VoterStoreIDs, to) {
					continue
				}
				for _, leader := range group.VoterStoreIDs {
					if leader == from {
						continue
					}
					move := BalanceMove{GroupID: group.ID, LeaderStoreID: leader, FromStoreID: from, ToStoreID: to}
					if score, ok := scorer.Evaluate(move); ok {
						return move, score, true
					}
				}
				move := BalanceMove{GroupID: group.ID, LeaderStoreID: to, FromStoreID: from, ToStoreID: to}
				if score, ok := scorer.Evaluate(move); ok {
					return move, score, true
				}
			}
		}
	}
	return BalanceMove{}, BalanceScore{}, false
}

func applyBalanceMove(groups []*Group, move BalanceMove) {
	for _, group := range groups {
		if group.ID != move.GroupID {
			continue
		}
		if move.FromStoreID != 0 {
			for i, storeID := range group.VoterStoreIDs {
				if storeID == move.FromStoreID {
					group.VoterStoreIDs[i] = move.ToStoreID
					break
				}
			}
		}
		group.LeaderStoreID = move.LeaderStoreID
		return
	}
}
