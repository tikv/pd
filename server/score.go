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

import (
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type score struct {
	from      int
	to        int
	diff      int
	threshold int
	st        string
}

func priorityScore(cfg *BalanceConfig, scores []*score) (int, *score) {
	var (
		maxPriority int
		idx         int
		resultScore *score
	)

	for i, score := range scores {
		priority := score.diff
		if score.threshold != -1 {
			// If the score diff is close to threshold value, we should add the priority weight.
			if score.threshold-score.from <= int(100*cfg.MaxDiffScoreFraction) {
				priority += 10
			}
		}

		if priority > maxPriority {
			idx = i
			resultScore = score
		}
	}

	return idx, resultScore
}

func scoreThreshold(cfg *BalanceConfig, st scoreType) int {
	switch st {
	case capacityScore:
		return int(cfg.MaxCapacityUsedRatio * 100)
	default:
		return -1
	}
}

type scoreType byte

const (
	leaderScore scoreType = iota + 1
	capacityScore
)

func (st scoreType) String() string {
	switch st {
	case leaderScore:
		return "leader score"
	case capacityScore:
		return "capacity score"
	default:
		return "unknown"
	}
}

// Scorer is an interface to calculate the score.
type Scorer interface {
	// Score calculates the score of store.
	Score(store *storeInfo) int
}

type leaderScorer struct {
}

func newLeaderScorer() *leaderScorer {
	return &leaderScorer{}
}

func (ls *leaderScorer) Score(store *storeInfo) int {
	return int(store.leaderRatio() * 100)
}

type capacityScorer struct {
}

func newCapacityScorer() *capacityScorer {
	return &capacityScorer{}
}

func (cs *capacityScorer) Score(store *storeInfo) int {
	return int(store.usedRatio() * 100)
}

func newScorer(st scoreType) Scorer {
	switch st {
	case leaderScore:
		return newLeaderScorer()
	case capacityScore:
		return newCapacityScorer()
	}

	return nil
}

func checkAndGetDiffScore(cluster *clusterInfo, oldPeer *metapb.Peer, newPeer *metapb.Peer, st scoreType, cfg *BalanceConfig) (*score, bool) {
	oldStore := cluster.getStore(oldPeer.GetStoreId())
	newStore := cluster.getStore(newPeer.GetStoreId())
	if oldStore == nil || newStore == nil {
		log.Debugf("check score failed - old peer: %v, new peer: %v", oldPeer, newPeer)
		return nil, false
	}

	// TODO: we should check the diff score of pre-balance `from store` and post balance `to store`.
	scorer := newScorer(st)
	oldStoreScore := scorer.Score(oldStore)
	newStoreScore := scorer.Score(newStore)

	// Check whether the diff score is in MaxDiffScoreFraction range.
	diffScore := oldStoreScore - newStoreScore
	if diffScore <= int(float64(oldStoreScore)*cfg.MaxDiffScoreFraction) {
		log.Debugf("check score failed - diff score is too small - score type: %v, old peer: %v, new peer: %v, old store score: %d, new store score: %d, diif score: %d",
			st, oldPeer, newPeer, oldStoreScore, newStoreScore, diffScore)
		return nil, false
	}

	score := &score{
		from:      oldStoreScore,
		to:        newStoreScore,
		diff:      diffScore,
		threshold: scoreThreshold(cfg, st),
		st:        st.String(),
	}

	return score, true
}
