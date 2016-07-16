// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

// Scorer is an interface to calculate the score.
type Scorer interface {
	Score(store *storeInfo) int
}

type leaderScorer struct {
	leaderCount int
	regionCount int
}

func newLeaderScorer(leaderCount int, regionCount int) *leaderScorer {
	return &leaderScorer{
		leaderCount: leaderCount,
		regionCount: regionCount,
	}
}

func (ls *leaderScorer) Score(store *storeInfo) int {
	if ls.regionCount == 0 {
		return 0
	}

	return ls.leaderCount * 100 / ls.regionCount
}

type capacityScorer struct {
}

func newCapacityScorer() *capacityScorer {
	return &capacityScorer{}
}

func (cs *capacityScorer) Score(store *storeInfo) int {
	return int(store.usedRatio() * 100)
}

type resourceScorer struct {
	cfg *BalanceConfig

	ls *leaderScorer
	cs *capacityScorer
}

func newResourceScorer(cfg *BalanceConfig, leaderCount int, regionCount int) *resourceScorer {
	return &resourceScorer{
		cfg: cfg,
		ls:  newLeaderScorer(leaderCount, regionCount),
		cs:  newCapacityScorer(),
	}
}

func (rs *resourceScorer) Score(store *storeInfo) int {
	capacityScore := rs.ls.Score(store)
	leaderScore := rs.cs.Score(store)
	return int(float64(capacityScore)*rs.cfg.CapacityScoreWeight + float64(leaderScore)*rs.cfg.LeaderScoreWeight)
}
