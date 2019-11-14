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
	"net/url"
	"sort"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	// adjustRatio is used to adjust TolerantSizeRatio according to region count.
	adjustRatio             float64 = 0.005
	leaderTolerantSizeRatio float64 = 5.0
	minTolerantSizeRatio    float64 = 1.0
)

// ErrScheduleConfigNotExist the config is not correct.
var ErrScheduleConfigNotExist = errors.New("the config does not exist")

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

func shouldBalance(cluster opt.Cluster, source, target *core.StoreInfo, region *core.RegionInfo, kind core.ScheduleKind, opInfluence operator.OpInfluence, scheduleName string) bool {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	sourceID := source.GetID()
	targetID := target.GetID()
	tolerantResource := getTolerantResource(cluster, region, kind)
	sourceInfluence := opInfluence.GetStoreInfluence(sourceID).ResourceProperty(kind)
	targetInfluence := opInfluence.GetStoreInfluence(targetID).ResourceProperty(kind)
	sourceScore := source.ResourceScore(kind, cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), sourceInfluence-tolerantResource)
	targetScore := target.ResourceScore(kind, cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), targetInfluence+tolerantResource)

	// Make sure after move, source score is still greater than target score.
	shouldBalance := sourceScore > targetScore

	if !shouldBalance {
		log.Debug("skip balance "+kind.Resource.String(),
			zap.String("scheduler", scheduleName), zap.Uint64("region-id", region.GetID()), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID),
			zap.Int64("source-size", source.GetRegionSize()), zap.Float64("source-score", sourceScore),
			zap.Int64("source-influence", sourceInfluence),
			zap.Int64("target-size", target.GetRegionSize()), zap.Float64("target-score", targetScore),
			zap.Int64("target-influence", targetInfluence),
			zap.Int64("average-region-size", cluster.GetAverageRegionSize()),
			zap.Int64("tolerant-resource", tolerantResource))
	}
	return shouldBalance
}

func getTolerantResource(cluster opt.Cluster, region *core.RegionInfo, kind core.ScheduleKind) int64 {
	if kind.Resource == core.LeaderKind && kind.Strategy == core.ByCount {
		tolerantSizeRatio := cluster.GetTolerantSizeRatio()
		if tolerantSizeRatio == 0 {
			tolerantSizeRatio = leaderTolerantSizeRatio
		}
		leaderCount := int64(1.0 * tolerantSizeRatio)
		return leaderCount
	}

	regionSize := region.GetApproximateSize()
	if regionSize < cluster.GetAverageRegionSize() {
		regionSize = cluster.GetAverageRegionSize()
	}
	regionSize = int64(float64(regionSize) * adjustTolerantRatio(cluster))
	return regionSize
}

func adjustTolerantRatio(cluster opt.Cluster) float64 {
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

func adjustBalanceLimit(cluster opt.Cluster, kind core.ResourceKind) uint64 {
	stores := cluster.GetStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		if s.IsUp() {
			counts = append(counts, float64(s.ResourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(counts)
	return maxUint64(1, uint64(limit))
}

// ScoreInfo stores storeID and score of a store.
type ScoreInfo struct {
	storeID uint64
	score   float64
}

// NewScoreInfo returns a ScoreInfo.
func NewScoreInfo(storeID uint64, score float64) *ScoreInfo {
	return &ScoreInfo{
		storeID: storeID,
		score:   score,
	}
}

// GetStoreID returns the storeID.
func (s *ScoreInfo) GetStoreID() uint64 {
	return s.storeID
}

// GetScore returns the score.
func (s *ScoreInfo) GetScore() float64 {
	return s.score
}

// SetScore sets the score.
func (s *ScoreInfo) SetScore(score float64) {
	s.score = score
}

// ScoreInfos is used for sorting ScoreInfo.
type ScoreInfos struct {
	scoreInfos []*ScoreInfo
	isSorted   bool
}

// NewScoreInfos returns a ScoreInfos.
func NewScoreInfos() *ScoreInfos {
	return &ScoreInfos{
		scoreInfos: make([]*ScoreInfo, 0),
		isSorted:   true,
	}
}

// Add adds a scoreInfo into the slice.
func (s *ScoreInfos) Add(scoreInfo *ScoreInfo) {
	infosLen := len(s.scoreInfos)
	if s.isSorted == true && infosLen != 0 && s.scoreInfos[infosLen-1].score > scoreInfo.score {
		s.isSorted = false
	}
	s.scoreInfos = append(s.scoreInfos, scoreInfo)
}

// Len returns length of slice.
func (s *ScoreInfos) Len() int { return len(s.scoreInfos) }

// Less returns if one number is less than another.
func (s *ScoreInfos) Less(i, j int) bool { return s.scoreInfos[i].score < s.scoreInfos[j].score }

// Swap switches out two numbers in slice.
func (s *ScoreInfos) Swap(i, j int) {
	s.scoreInfos[i], s.scoreInfos[j] = s.scoreInfos[j], s.scoreInfos[i]
}

// Sort sorts the slice.
func (s *ScoreInfos) Sort() {
	if !s.isSorted {
		sort.Sort(s)
		s.isSorted = true
	}
}

// ToSlice returns the scoreInfo slice.
func (s *ScoreInfos) ToSlice() []*ScoreInfo {
	return s.scoreInfos
}

// Min returns the min of the slice.
func (s *ScoreInfos) Min() *ScoreInfo {
	s.Sort()
	return s.scoreInfos[0]
}

// Mean returns the mean of the slice.
func (s *ScoreInfos) Mean() float64 {
	if s.Len() == 0 {
		return 0
	}

	var sum float64
	for _, info := range s.scoreInfos {
		sum += info.score
	}

	return sum / float64(s.Len())
}

// StdDev returns the standard deviation of the slice.
func (s *ScoreInfos) StdDev() float64 {
	if s.Len() == 0 {
		return 0
	}

	var res float64
	mean := s.Mean()
	for _, info := range s.ToSlice() {
		diff := info.GetScore() - mean
		res += diff * diff
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
	for _, storeStat := range storesStats {
		sum += storeStat
	}
	return sum / float64(len(storesStats))
}

// NormalizeStoresStats returns the normalized score scoreInfos. Normalize: x_i => (x_i - x_min)/x_avg.
func NormalizeStoresStats(storesStats map[uint64]float64) *ScoreInfos {
	scoreInfos := NewScoreInfos()

	for storeID, score := range storesStats {
		scoreInfos.Add(NewScoreInfo(storeID, score))
	}

	mean := scoreInfos.Mean()
	if mean == 0 {
		return scoreInfos
	}

	minScore := scoreInfos.Min().GetScore()

	for _, info := range scoreInfos.ToSlice() {
		info.SetScore((info.GetScore() - minScore) / mean)
	}

	return scoreInfos
}

// AggregateScores aggregates stores' scores by using their weights.
func AggregateScores(storesStats []*ScoreInfos, weights []float64) *ScoreInfos {
	num := len(storesStats)
	if num > len(weights) {
		num = len(weights)
	}

	scoreMap := make(map[uint64]float64, 0)
	for i := 0; i < num; i++ {
		scoreInfos := storesStats[i]
		for _, info := range scoreInfos.ToSlice() {
			scoreMap[info.GetStoreID()] += info.GetScore() * weights[i]
		}
	}

	res := NewScoreInfos()
	for storeID, score := range scoreMap {
		res.Add(NewScoreInfo(storeID, score))
	}

	res.Sort()
	return res
}

func getKeyRanges(args []string) ([]core.KeyRange, error) {
	var ranges []core.KeyRange
	for len(args) > 1 {
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, err
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, err
		}
		args = args[2:]
		ranges = append(ranges, core.NewKeyRange(startKey, endKey))
	}
	if len(ranges) == 0 {
		return []core.KeyRange{core.NewKeyRange("", "")}, nil
	}
	return ranges, nil
}
