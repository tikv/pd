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

type scoreType byte

const (
	leaderScore scoreType = iota + 1
	capacityScore
	resourceScore
)

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

type resourceScorer struct {
	cfg *BalanceConfig

	ls *leaderScorer
	cs *capacityScorer
}

func newResourceScorer(cfg *BalanceConfig, leaderCount int, regionCount int) *resourceScorer {
	return &resourceScorer{
		cfg: cfg,
		ls:  newLeaderScorer(),
		cs:  newCapacityScorer(),
	}
}

func (rs *resourceScorer) Score(store *storeInfo) int {
	capacityScore := rs.ls.Score(store)
	leaderScore := rs.cs.Score(store)
	return int(float64(capacityScore)*rs.cfg.CapacityScoreWeight + float64(leaderScore)*rs.cfg.LeaderScoreWeight)
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

func checkScore(cluster *clusterInfo, oldPeer *metapb.Peer, newPeer *metapb.Peer, st scoreType, cfg *BalanceConfig) bool {
	oldStore := cluster.getStore(oldPeer.GetStoreId())
	newStore := cluster.getStore(newPeer.GetStoreId())
	if oldStore == nil || newStore == nil {
		log.Debugf("check score failed - old peer: %v, new peer: %v", oldPeer, newPeer)
		return false
	}

	// TODO: we should check the diff score of pre-balance `from store` and post balance `to store`.
	scorer := newScorer(st)
	oldStoreScore := scorer.Score(oldStore)
	newStoreScore := scorer.Score(newStore)

	// Check whether the diff score is in MaxDiffScoreFraction range.
	diffScore := oldStoreScore - newStoreScore
	if diffScore <= int(float64(oldStoreScore)*cfg.MaxDiffScoreFraction) {
		log.Debugf("check score failed - diff score is too small - old peer: %v, new peer: %v, old store score: %d, new store score: %d, diif score: %d",
			oldPeer, newPeer, oldStoreScore, newStoreScore, diffScore)
		return false
	}

	return true
}
