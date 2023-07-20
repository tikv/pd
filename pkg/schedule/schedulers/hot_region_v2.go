// Copyright 2022 TiKV Project Authors.
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

// The v2 selection algorithm related code is placed in this file.

package schedulers

import (
	"fmt"
	"math"

	"github.com/tikv/pd/pkg/statistics"
)

const (
	firstPriorityPerceivedRatio = 0.2  // PeerRate needs to be 20% above what needs to be balanced.
	firstPriorityMinHotRatio    = 0.02 // PeerRate needs to be greater than 1% lowRate

	secondPriorityPerceivedRatio = 0.3  // PeerRate needs to be 20% above what needs to be balanced.
	secondPriorityMinHotRatio    = 0.03 // PeerRate needs to be greater than 1.5% lowRate
)

// isAvailable returns the solution is available.
// If the solution has no revertRegion, progressiveRank should < 0.
// If the solution has some revertRegion, progressiveRank should equal to -4 or -3.
func isAvailableV2(s *solution) bool {
	// TODO: Test if revert region can be enabled for -1.
	return s.progressiveRank <= -3 || (s.progressiveRank < 0 && s.revertRegion == nil)
}

// TODO: Unified with stddevThreshold.
type rankV2Ratios struct {
	preBalancedRatio      float64
	balancedRatio         float64
	preBalancedCheckRatio float64
	balancedCheckRatio    float64
	perceivedRatio        float64
	minHotRatio           float64
}

func newRankV2Ratios(balancedRatio, perceivedRatio, minHotRatio float64) *rankV2Ratios {
	// limit 0.7 <= balancedRatio <= 0.95
	if balancedRatio < 0.7 {
		balancedRatio = 0.7
	}
	if balancedRatio > 0.95 {
		balancedRatio = 0.95
	}

	rs := &rankV2Ratios{balancedRatio: balancedRatio, perceivedRatio: perceivedRatio, minHotRatio: minHotRatio}
	// preBalancedRatio = 1.0 - 2*(1.0-balancedRatio)
	// The maximum value with `balancedRatio-0.1` is to prevent the preBalance range becoming too large.
	rs.preBalancedRatio = math.Max(2.0*balancedRatio-1.0, balancedRatio-0.1)
	rs.balancedCheckRatio = balancedRatio - 0.02
	rs.preBalancedCheckRatio = rs.preBalancedRatio - 0.03
	return rs
}

func (bs *balanceSolver) initRankV2() {
	bs.firstPriorityV2Ratios = newRankV2Ratios(bs.sche.conf.GetGreatDecRatio(), firstPriorityPerceivedRatio, firstPriorityMinHotRatio)
	// The second priority is less demanding. Set the preBalancedRatio of the first priority to the balancedRatio of the second dimension.
	bs.secondPriorityV2Ratios = newRankV2Ratios(bs.firstPriorityV2Ratios.preBalancedRatio, secondPriorityPerceivedRatio, secondPriorityMinHotRatio)

	bs.isAvailable = isAvailableV2
	bs.filterUniformStore = bs.filterUniformStoreV2
	bs.needSearchRevertRegions = bs.needSearchRevertRegionsV2
	bs.setSearchRevertRegions = bs.setSearchRevertRegionsV2
	bs.calcProgressiveRank = bs.calcProgressiveRankV2
	bs.betterThan = bs.betterThanV2
	bs.rankToDimString = bs.rankToDimStringV2
	bs.pickCheckPolicyV2()
}

func (bs *balanceSolver) pickCheckPolicyV2() {
	switch {
	case bs.resourceTy == writeLeader:
		bs.checkByPriorityAndTolerance = bs.checkByPriorityAndToleranceFirstOnly
		bs.checkHistoryLoadsByPriority = bs.checkHistoryLoadsByPriorityAndToleranceFirstOnly
	default:
		bs.checkByPriorityAndTolerance = bs.checkByPriorityAndToleranceAnyOf
		bs.checkHistoryLoadsByPriority = bs.checkHistoryByPriorityAndToleranceAnyOf
	}
}

func (bs *balanceSolver) filterUniformStoreV2() (string, bool) {
	if !bs.enableExpectation() {
		return "", false
	}
	// Because region is available for src and dst, so stddev is the same for both, only need to calcurate one.
	isUniformFirstPriority, isUniformSecondPriority := bs.isUniformFirstPriority(bs.cur.srcStore), bs.isUniformSecondPriority(bs.cur.srcStore)
	if isUniformFirstPriority && isUniformSecondPriority {
		// If both dims are enough uniform, any schedule is unnecessary.
		return "all-dim", true
	}
	if isUniformFirstPriority && (bs.cur.progressiveRank == -2 || bs.cur.progressiveRank == -3) {
		// If first priority dim is enough uniform, -2 is unnecessary and maybe lead to worse balance for second priority dim
		return statistics.DimToString(bs.firstPriority), true
	}
	if isUniformSecondPriority && bs.cur.progressiveRank == -1 {
		// If second priority dim is enough uniform, -1 is unnecessary and maybe lead to worse balance for first priority dim
		return statistics.DimToString(bs.secondPriority), true
	}
	return "", false
}

// The search-revert-regions is performed only when the following conditions are met to improve performance.
// * `searchRevertRegions` is true. It depends on the result of the last `solve`.
// * The current solution is not good enough. progressiveRank == -2/0
// * The current best solution is not good enough.
//   - The current best solution has progressiveRank < -2 , but contain revert regions.
//   - The current best solution has progressiveRank >= -2.
func (bs *balanceSolver) needSearchRevertRegionsV2() bool {
	if !bs.sche.searchRevertRegions[bs.resourceTy] {
		return false
	}
	return (bs.cur.progressiveRank == -2 || bs.cur.progressiveRank == 0) &&
		(bs.best == nil || bs.best.progressiveRank >= -2 || bs.best.revertRegion != nil)
}

func (bs *balanceSolver) setSearchRevertRegionsV2() {
	// The next solve is allowed to search-revert-regions only when the following conditions are met.
	// * No best solution was found this time.
	// * The progressiveRank of the best solution == -2. (first is better, second is worsened)
	// * The best solution contain revert regions.
	searchRevertRegions := bs.best == nil || bs.best.progressiveRank == -2 || bs.best.revertRegion != nil
	bs.sche.searchRevertRegions[bs.resourceTy] = searchRevertRegions
	if searchRevertRegions {
		event := fmt.Sprintf("%s-%s-allow-search-revert-regions", bs.rwTy.String(), bs.opTy.String())
		schedulerCounter.WithLabelValues(bs.sche.GetName(), event).Inc()
	}
}

// calcProgressiveRank calculates `bs.cur.progressiveRank`.
// See the comments of `solution.progressiveRank` for more about progressive rank.
// isBetter: score > 0
// isNotWorsened: score == 0
// isWorsened: score < 0
// | ↓ firstPriority \ secondPriority → | isBetter | isNotWorsened | isWorsened |
// |   isBetter                         | -4       | -3            | -2         |
// |   isNotWorsened                    | -1       | 1             | 1          |
// |   isWorsened                       | 0        | 1             | 1          |
func (bs *balanceSolver) calcProgressiveRankV2() {
	bs.cur.progressiveRank = 1
	bs.cur.calcPeersRate(bs.firstPriority, bs.secondPriority)
	if bs.cur.getPeersRateFromCache(bs.firstPriority) < bs.getMinRate(bs.firstPriority) &&
		bs.cur.getPeersRateFromCache(bs.secondPriority) < bs.getMinRate(bs.secondPriority) {
		return
	}

	if bs.resourceTy == writeLeader {
		// For write leader, only compare the first priority.
		// If the first priority is better, the progressiveRank is -3.
		// Because it is not a solution that needs to be optimized.
		if bs.getScoreByPriorities(bs.firstPriority, bs.firstPriorityV2Ratios) > 0 {
			bs.cur.progressiveRank = -3
		}
		return
	}

	firstScore := bs.getScoreByPriorities(bs.firstPriority, bs.firstPriorityV2Ratios)
	secondScore := bs.getScoreByPriorities(bs.secondPriority, bs.secondPriorityV2Ratios)
	bs.cur.firstScore, bs.cur.secondScore = firstScore, secondScore
	switch {
	case firstScore > 0 && secondScore > 0:
		// If belonging to the case, all two dim will be more balanced, the best choice.
		bs.cur.progressiveRank = -4
	case firstScore > 0 && secondScore == 0:
		// If belonging to the case, the first priority dim will be more balanced, the second priority dim will be not worsened.
		bs.cur.progressiveRank = -3
	case firstScore > 0:
		// If belonging to the case, the first priority dim will be more balanced, ignore the second priority dim.
		bs.cur.progressiveRank = -2
	case firstScore == 0 && secondScore > 0:
		// If belonging to the case, the first priority dim will be not worsened, the second priority dim will be more balanced.
		bs.cur.progressiveRank = -1
	case secondScore > 0:
		// If belonging to the case, the second priority dim will be more balanced, ignore the first priority dim.
		// It's a solution that cannot be used directly, but can be optimized.
		bs.cur.progressiveRank = 0
	}
}

func (bs *balanceSolver) getScoreByPriorities(dim int, rs *rankV2Ratios) int {
	// minNotWorsenedRate, minBetterRate, minBalancedRate, maxBalancedRate, maxBetterRate, maxNotWorsenedRate can be determined from src and dst.
	// * peersRate < minNotWorsenedRate                   ====> score == -2
	// * minNotWorsenedRate <= peersRate < minBetterRate  ====> score == 0
	// * minBetterRate <= peersRate < minBalancedRate     ====> score == 2
	// * minBalancedRate <= peersRate <= maxBalancedRate  ====> score == 3
	// * maxBalancedRate < peersRate <= maxBetterRate     ====> score == 1
	// * maxBetterRate < peersRate <= maxNotWorsenedRate  ====> score == -1
	// * peersRate > maxNotWorsenedRate                   ====> score == -2
	// The higher the score, the better.

	srcRate, dstRate := bs.cur.getExtremeLoad(dim)
	srcPendingRate, dstPendingRate := bs.cur.getPendingLoad(dim)
	peersRate := bs.cur.getPeersRateFromCache(dim)
	highRate, lowRate := srcRate, dstRate
	topnHotPeer := bs.nthHotPeer[bs.cur.srcStore.GetID()][dim]
	reverse := false
	if srcRate < dstRate {
		highRate, lowRate = dstRate, srcRate
		peersRate = -peersRate
		reverse = true
		topnHotPeer = bs.nthHotPeer[bs.cur.dstStore.GetID()][dim]
	}
	topnRate := math.MaxFloat64
	if topnHotPeer != nil {
		topnRate = topnHotPeer.GetLoad(dim)
	}

	if highRate*rs.balancedCheckRatio <= lowRate {
		// At this time, it is considered to be in the balanced state, and score > 1 will not be judged.
		// If the balanced state is not broken, score == 0.
		// If the balanced state is broken, score = -2.
		// minNotWorsenedRate == minBetterRate == minBalancedRate <= maxBalancedRate == maxBetterRate == maxNotWorsenedRate

		// highRate - (highRate+lowRate)/(1.0+balancedRatio)
		minNotWorsenedRate := (highRate*rs.balancedRatio - lowRate) / (1.0 + rs.balancedRatio)
		// highRate - (highRate+lowRate)/(1.0+balancedRatio)*balancedRatio
		maxNotWorsenedRate := (highRate - lowRate*rs.balancedRatio) / (1.0 + rs.balancedRatio)

		if minNotWorsenedRate > 0 {
			minNotWorsenedRate = 0
		}

		if peersRate >= minNotWorsenedRate && peersRate <= maxNotWorsenedRate {
			return 0
		}
		return -2
	}

	var minNotWorsenedRate, minBetterRate, maxBetterRate, maxNotWorsenedRate float64
	minBalancedRate := (highRate*rs.balancedRatio - lowRate) / (1.0 + rs.balancedRatio)
	maxBalancedRate := (highRate - lowRate*rs.balancedRatio) / (1.0 + rs.balancedRatio)
	pendingRateLimit := false

	if highRate*rs.preBalancedCheckRatio <= lowRate {
		// At this time, it is considered to be in pre-balanced state.
		// Only the schedules that reach the balanced state will be judged as 2,
		// and the schedules that do not destroy the pre-balanced state will be judged as 0.
		// minNotWorsenedRate <= minBetterRate <= maxBalancedRate == maxBetterRate <= maxNotWorsenedRate
		// To generate a score of -2 at this state, the pendingRate needs to be 0.
		minNotWorsenedRate = (highRate*rs.preBalancedRatio - lowRate) / (1.0 + rs.preBalancedRatio)
		minBetterRate = (highRate*rs.balancedRatio - lowRate) / (1.0 + rs.balancedRatio)
		maxBetterRate = maxBalancedRate
		maxNotWorsenedRate = (highRate - lowRate*rs.preBalancedRatio) / (1.0 + rs.preBalancedRatio)
		if minNotWorsenedRate > 0 {
			minNotWorsenedRate = 0
		}
		// When approaching the balanced state, wait for pending influence to zero before scheduling to reduce jitter.
		pendingRateLimit = true
	} else {
		// At this time, it is considered to be in the unbalanced state.
		// As long as the balance is significantly improved, it is judged as 1.
		// If the balance is not reduced, it is judged as 0.
		// If the rate relationship between src and dst is reversed, there will be a certain penalty.
		// maxBetterRate may be less than minBetterRate, in which case a positive fraction cannot be produced.
		minNotWorsenedRate = -bs.getMinRate(dim)
		minBetterRate = math.Min(minBalancedRate*rs.perceivedRatio, lowRate*rs.minHotRatio)
		minBetterRate = math.Min(minBetterRate, topnRate)
		maxBetterRate = maxBalancedRate + (highRate-lowRate-minBetterRate-maxBalancedRate)*rs.perceivedRatio
		maxNotWorsenedRate = maxBalancedRate + (highRate-lowRate-minNotWorsenedRate-maxBalancedRate)*rs.perceivedRatio
	}

	switch {
	case minBetterRate <= peersRate && peersRate <= maxBetterRate:
		// Positive score requires some restrictions.
		if peersRate >= bs.getMinRate(dim) && bs.isTolerance(dim, reverse) &&
			(!pendingRateLimit || math.Abs(srcPendingRate)+math.Abs(dstPendingRate) < 1) {
			switch {
			case peersRate < minBalancedRate:
				return 2
			case peersRate > maxBalancedRate:
				return 1
			default: // minBalancedRate <= peersRate <= maxBalancedRate
				return 3
			}
		}
		return 0
	case minNotWorsenedRate <= peersRate && peersRate < minBetterRate:
		return 0
	case maxBetterRate < peersRate && peersRate <= maxNotWorsenedRate:
		return -1
	default: // peersRate < minNotWorsenedRate || peersRate > maxNotWorsenedRate
		return -2
	}
}

// betterThan checks if `bs.cur` is a better solution than `old`.
func (bs *balanceSolver) betterThanV2(old *solution) bool {
	if old == nil || bs.cur.progressiveRank <= splitProgressiveRank {
		return true
	}
	if bs.cur.progressiveRank != old.progressiveRank {
		// Smaller rank is better.
		return bs.cur.progressiveRank < old.progressiveRank
	}
	if (bs.cur.revertRegion == nil) != (old.revertRegion == nil) {
		// Fewer revertRegions are better.
		return bs.cur.revertRegion == nil
	}

	if r := bs.compareSrcStore(bs.cur.srcStore, old.srcStore); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if r := bs.compareDstStore(bs.cur.dstStore, old.dstStore); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if bs.cur.mainPeerStat != old.mainPeerStat {
		// We will firstly consider ensuring converge faster, secondly reduce oscillation
		if bs.resourceTy == writeLeader {
			return bs.getRkCmpByPriorityV2(bs.firstPriority, bs.cur.firstScore, old.firstScore,
				bs.cur.getPeersRateFromCache(bs.firstPriority), old.getPeersRateFromCache(bs.firstPriority)) > 0
		}

		firstCmp := bs.getRkCmpByPriorityV2(bs.firstPriority, bs.cur.firstScore, old.firstScore,
			bs.cur.getPeersRateFromCache(bs.firstPriority), old.getPeersRateFromCache(bs.firstPriority))
		secondCmp := bs.getRkCmpByPriorityV2(bs.secondPriority, bs.cur.secondScore, old.secondScore,
			bs.cur.getPeersRateFromCache(bs.secondPriority), old.getPeersRateFromCache(bs.secondPriority))
		switch bs.cur.progressiveRank {
		case -4, -3, -2: // firstPriority
			if firstCmp != 0 {
				return firstCmp > 0
			}
			return secondCmp > 0
		case -1: // secondPriority
			if secondCmp != 0 {
				return secondCmp > 0
			}
			return firstCmp > 0
		}
	}

	return false
}

func (bs *balanceSolver) getRkCmpByPriorityV2(dim int, curScore, oldScore int, curPeersRate, oldPeersRate float64) int {
	switch {
	case curScore > oldScore:
		return 1
	case curScore < oldScore:
		return -1
	// curScore == oldScore
	case curScore == 3, curScore <= 1:
		// curScore == 3: When the balance state can be reached, the smaller the influence, the better.
		// curScore == 1: When maxBalancedRate is exceeded, the smaller the influence, the better.
		// curScore <= 0: When the score is less than 0, the smaller the influence, the better.
		return -rankCmp(curPeersRate, oldPeersRate, stepRank(0, dimToStep[dim]))
	default: // curScore == 2
		// On the way to balance state, the bigger the influence, the better.
		return rankCmp(curPeersRate, oldPeersRate, stepRank(0, dimToStep[dim]))
	}
}

func (bs *balanceSolver) rankToDimStringV2() string {
	switch bs.cur.progressiveRank {
	case -4:
		return "all"
	case -3:
		return statistics.DimToString(bs.firstPriority)
	case -2:
		return statistics.DimToString(bs.firstPriority) + "-only"
	case -1:
		return statistics.DimToString(bs.secondPriority)
	default:
		return "none"
	}
}
