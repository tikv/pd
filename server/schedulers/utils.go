// Copyright 2017 PingCAP, Inc.
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

package schedulers

import (
	"math"
	"sort"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/pingcap/pd/pkg/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
)

const (
	// adjustRatio is used to adjust TolerantSizeRatio according to region count.
	adjustRatio          float64 = 0.005
	minTolerantSizeRatio float64 = 1.0
)

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func isRegionUnhealthy(region *core.RegionInfo) bool {
	return len(region.GetDownPeers()) != 0 || len(region.GetLearners()) != 0
}

func shouldBalance(cluster schedule.Cluster, source, target *core.StoreInfo, region *core.RegionInfo, kind core.ResourceKind, opInfluence operator.OpInfluence) bool {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	regionSize := region.GetApproximateSize()
	if regionSize < cluster.GetAverageRegionSize() {
		regionSize = cluster.GetAverageRegionSize()
	}

	regionSize = int64(float64(regionSize) * adjustTolerantRatio(cluster))
	sourceDelta := opInfluence.GetStoreInfluence(source.GetID()).ResourceSize(kind) - regionSize
	targetDelta := opInfluence.GetStoreInfluence(target.GetID()).ResourceSize(kind) + regionSize

	// Make sure after move, source score is still greater than target score.
	return source.ResourceScore(kind, cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), sourceDelta) >
		target.ResourceScore(kind, cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), targetDelta)
}

func adjustTolerantRatio(cluster schedule.Cluster) float64 {
	tolerantSizeRatio := cluster.GetTolerantSizeRatio()
	if tolerantSizeRatio == 0 {
		var maxRegionCount float64
		stores := cluster.GetStores()
		for _, store := range stores {
			regionCount := float64(cluster.GetStoreRegionCount(store.GetID()))
			if maxRegionCount < regionCount {
				maxRegionCount = regionCount
			}
		}
		tolerantSizeRatio = maxRegionCount * adjustRatio
		if tolerantSizeRatio < minTolerantSizeRatio {
			tolerantSizeRatio = minTolerantSizeRatio
		}
	}
	return tolerantSizeRatio
}

func adjustBalanceLimit(cluster schedule.Cluster, kind core.ResourceKind) uint64 {
	stores := cluster.GetStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		if s.IsUp() {
			counts = append(counts, float64(s.ResourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(stats.Float64Data(counts))
	return maxUint64(1, uint64(limit))
}

const (
	taintCacheGCInterval = time.Second * 5
	taintCacheTTL        = time.Minute * 5
)

// newTaintCache creates a TTL cache to hold stores that are not able to
// schedule operators.
func newTaintCache() *cache.TTLUint64 {
	return cache.NewIDTTL(taintCacheGCInterval, taintCacheTTL)
}

// ScorePair stores storeID and record of a store.
type ScorePair struct {
	storeID uint64
	score   float64
}

// NewScorePair returns a ScorePair.
func NewScorePair(storeID uint64, score float64) *ScorePair {
	return &ScorePair{
		storeID: storeID,
		score:   score,
	}
}

// GetStoreID returns the storeID.
func (s *ScorePair) GetStoreID() uint64 {
	return s.storeID
}

// GetScore returns the score.
func (s *ScorePair) GetScore() float64 {
	return s.score
}

// SetScore sets the score.
func (s *ScorePair) SetScore(score float64) {
	s.score = score
}

// ScorePairSlice is used for sorting Score Pairs.
type ScorePairSlice struct {
	pairs    []*ScorePair
	isSorted bool
}

// NewScorePairSlice returns a ScorePairSlice.
func NewScorePairSlice() *ScorePairSlice {
	return &ScorePairSlice{
		pairs: make([]*ScorePair, 0),
	}
}

// Add adds a pair into the slice.
func (s *ScorePairSlice) Add(pair *ScorePair) {
	s.pairs = append(s.pairs, pair)
}

// Len returns length of slice.
func (s *ScorePairSlice) Len() int { return len(s.pairs) }

// Less returns if one number is less than another.
func (s *ScorePairSlice) Less(i, j int) bool { return s.pairs[i].score < s.pairs[j].score }

// Swap switches out two numbers in slice.
func (s *ScorePairSlice) Swap(i, j int) { s.pairs[i], s.pairs[j] = s.pairs[j], s.pairs[i] }

// Sort sorts the slice.
func (s *ScorePairSlice) Sort() {
	sort.Sort(s)
	s.isSorted = true
}

// GetPairs returns the pairs.
func (s *ScorePairSlice) GetPairs() []*ScorePair {
	return s.pairs
}

// GetMin returns the min of the slice.
func (s *ScorePairSlice) GetMin() *ScorePair {
	if !s.isSorted {
		sort.Sort(s)
	}
	return s.pairs[0]
}

// Mean returns the mean of the slice.
func (s *ScorePairSlice) Mean() float64 {
	if s.Len() == 0 {
		return 0
	}

	var sum float64
	for _, pair := range s.pairs {
		sum += pair.score
	}

	return sum / float64(s.Len())
}

// StdDeviation returns the standard deviation of the slice.
func (s *ScorePairSlice) StdDeviation() float64 {
	if s.Len() == 0 {
		return 0
	}

	var res float64
	mean := s.Mean()
	for _, pair := range s.GetPairs() {
		res += (pair.GetScore() - mean) * (pair.GetScore() - mean)
	}
	res /= float64(s.Len())
	res = math.Sqrt(res)

	return res
}

// MeanStoresStats returns the mean of stores' stats.
func MeanStoresStats(storesStats map[uint64]float64) float64 {
	if len(storesStats) == 0 {
		return 0.0
	}

	var sum float64
	for _, stats := range storesStats {
		sum += stats
	}
	return sum / float64(len(storesStats))
}

// NormalizeStoresStats returns the normalized score pairs. Normolize: x_i => (x_i - x_min)/x_avg.
func NormalizeStoresStats(storesStats map[uint64]float64) *ScorePairSlice {
	scorePairSlice := NewScorePairSlice()

	for storeID, stats := range storesStats {
		pair := NewScorePair(storeID, stats)
		scorePairSlice.Add(pair)
	}

	mean := scorePairSlice.Mean()
	if mean == 0 {
		return scorePairSlice
	}

	scorePairSlice.Sort()
	minScore := scorePairSlice.GetMin().GetScore()

	for _, pair := range scorePairSlice.GetPairs() {
		pair.SetScore((pair.GetScore() - minScore) / mean)
	}

	return scorePairSlice
}

// AggregateScores aggregates stores' scores by using their weights.
func AggregateScores(scorePairSliceVec []*ScorePairSlice, weights []float64) *ScorePairSlice {
	scoreMap := make(map[uint64]float64, 0)
	for i, scorePairSlice := range scorePairSliceVec {
		for _, pair := range scorePairSlice.GetPairs() {
			scoreMap[pair.GetStoreID()] += pair.GetScore() * weights[i]
		}
	}

	res := NewScorePairSlice()
	for storeID, score := range scoreMap {
		pair := NewScorePair(storeID, score)
		res.Add(pair)
	}

	res.Sort()

	return res
}
