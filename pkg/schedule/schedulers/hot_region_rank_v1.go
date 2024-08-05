// Copyright 2024 TiKV Project Authors.
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
	"math"

	"github.com/tikv/pd/pkg/statistics/utils"
)

type rankV1 struct {
	*balanceSolver
}

func initRankV1(r *balanceSolver) *rankV1 {
	return &rankV1{balanceSolver: r}
}

// isAvailable returns the solution is available.
// The solution should progressiveRank > 0.
// v1 does not support revert regions, so no need to check revertRegions.
func (*rankV1) isAvailable(s *solution) bool {
	return s.progressiveRank > 0
}

func (r *rankV1) checkByPriorityAndTolerance(loads []float64, f func(int) bool) bool {
	switch {
	case r.resourceTy == writeLeader:
		return r.checkByPriorityAndToleranceFirstOnly(loads, f)
	case r.sche.conf.isStrictPickingStoreEnabled():
		return r.checkByPriorityAndToleranceAllOf(loads, f)
	default:
		return r.checkByPriorityAndToleranceFirstOnly(loads, f)
	}
}

func (r *rankV1) checkHistoryLoadsByPriority(loads [][]float64, f func(int) bool) bool {
	switch {
	case r.resourceTy == writeLeader:
		return r.checkHistoryLoadsByPriorityAndToleranceFirstOnly(loads, f)
	case r.sche.conf.isStrictPickingStoreEnabled():
		return r.checkHistoryLoadsByPriorityAndToleranceAllOf(loads, f)
	default:
		return r.checkHistoryLoadsByPriorityAndToleranceFirstOnly(loads, f)
	}
}

func (r *rankV1) filterUniformStore() (string, bool) {
	if !r.enableExpectation() {
		return "", false
	}
	// Because region is available for src and dst, so stddev is the same for both, only need to calculate one.
	isUniformFirstPriority, isUniformSecondPriority := r.isUniformFirstPriority(r.cur.srcStore), r.isUniformSecondPriority(r.cur.srcStore)
	if isUniformFirstPriority && isUniformSecondPriority {
		// If both dims are enough uniform, any schedule is unnecessary.
		return "all-dim", true
	}
	if isUniformFirstPriority && (r.cur.progressiveRank == 1 || r.cur.progressiveRank == 3) {
		// If first priority dim is enough uniform, rank 1 is unnecessary and maybe lead to worse balance for second priority dim
		return utils.DimToString(r.firstPriority), true
	}
	if isUniformSecondPriority && r.cur.progressiveRank == 2 {
		// If second priority dim is enough uniform, rank 2 is unnecessary and maybe lead to worse balance for first priority dim
		return utils.DimToString(r.secondPriority), true
	}
	return "", false
}

// calcProgressiveRank calculates `r.cur.progressiveRank`.
// See the comments of `solution.progressiveRank` for more about progressive rank.
// | ↓ firstPriority \ secondPriority → | isBetter | isNotWorsened | Worsened  |
// |   isBetter                         | 4        | 3             | 1         |
// |   isNotWorsened                    | 2        | -1            | -1        |
// |   Worsened                         | 0        | -1            | -1        |
func (r *rankV1) calcProgressiveRank() {
	r.cur.progressiveRank = -1
	r.cur.calcPeersRate(r.firstPriority, r.secondPriority)
	if r.cur.getPeersRateFromCache(r.firstPriority) < r.getMinRate(r.firstPriority) &&
		r.cur.getPeersRateFromCache(r.secondPriority) < r.getMinRate(r.secondPriority) {
		return
	}

	if r.resourceTy == writeLeader {
		// For write leader, only compare the first priority.
		// If the first priority is better, the progressiveRank is 3.
		// Because it is not a solution that needs to be optimized.
		if r.isBetterForWriteLeader() {
			r.cur.progressiveRank = 3
		}
		return
	}

	isFirstBetter, isSecondBetter := r.isBetter(r.firstPriority), r.isBetter(r.secondPriority)
	isFirstNotWorsened := isFirstBetter || r.isNotWorsened(r.firstPriority)
	isSecondNotWorsened := isSecondBetter || r.isNotWorsened(r.secondPriority)
	switch {
	case isFirstBetter && isSecondBetter:
		// If belonging to the case, all two dim will be more balanced, the best choice.
		r.cur.progressiveRank = 4
	case isFirstBetter && isSecondNotWorsened:
		// If belonging to the case, the first priority dim will be more balanced, the second priority dim will be not worsened.
		r.cur.progressiveRank = 3
	case isFirstNotWorsened && isSecondBetter:
		// If belonging to the case, the first priority dim will be not worsened, the second priority dim will be more balanced.
		r.cur.progressiveRank = 2
	case isFirstBetter:
		// If belonging to the case, the first priority dim will be more balanced, ignore the second priority dim.
		r.cur.progressiveRank = 1
	case isSecondBetter:
		// If belonging to the case, the second priority dim will be more balanced, ignore the first priority dim.
		// It's a solution that cannot be used directly, but can be optimized.
		r.cur.progressiveRank = 0
	}
}

// betterThan checks if `r.cur` is a better solution than `old`.
func (r *rankV1) betterThan(old *solution) bool {
	if old == nil || r.cur.progressiveRank >= splitProgressiveRank {
		return true
	}
	if r.cur.progressiveRank != old.progressiveRank {
		// Bigger rank is better.
		return r.cur.progressiveRank > old.progressiveRank
	}
	if (r.cur.revertRegion == nil) != (old.revertRegion == nil) {
		// Fewer revertRegions are better.
		return r.cur.revertRegion == nil
	}

	if r := r.compareSrcStore(r.cur.srcStore, old.srcStore); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if r := r.compareDstStore(r.cur.dstStore, old.dstStore); r < 0 {
		return true
	} else if r > 0 {
		return false
	}

	if r.cur.mainPeerStat != old.mainPeerStat {
		// compare region
		if r.resourceTy == writeLeader {
			return r.cur.getPeersRateFromCache(r.firstPriority) > old.getPeersRateFromCache(r.firstPriority)
		}

		// We will firstly consider ensuring converge faster, secondly reduce oscillation
		firstCmp, secondCmp := r.getRkCmpPriorities(old)
		switch r.cur.progressiveRank {
		case 4: // isBetter(firstPriority) && isBetter(secondPriority)
			// Both are better, prefer the one with higher first priority rate.
			// If the first priority rate is the similar, prefer the one with higher second priority rate.
			if firstCmp != 0 {
				return firstCmp > 0
			}
			return secondCmp > 0
		case 3: // isBetter(firstPriority) && isNotWorsened(secondPriority)
			// The first priority is better, prefer the one with higher first priority rate.
			if firstCmp != 0 {
				return firstCmp > 0
			}
			// prefer smaller second priority rate, to reduce oscillation
			return secondCmp < 0
		case 2: // isNotWorsened(firstPriority) && isBetter(secondPriority)
			// The second priority is better, prefer the one with higher second priority rate.
			if secondCmp != 0 {
				return secondCmp > 0
			}
			// prefer smaller first priority rate, to reduce oscillation
			return firstCmp < 0
		case 1: // isBetter(firstPriority)
			return firstCmp > 0
			// TODO: The smaller the difference between the value and the expectation, the better.
		}
	}

	return false
}

func (r *rankV1) getRkCmpPriorities(old *solution) (firstCmp int, secondCmp int) {
	firstCmp = rankCmp(r.cur.getPeersRateFromCache(r.firstPriority), old.getPeersRateFromCache(r.firstPriority), stepRank(0, dimToStep[r.firstPriority]))
	secondCmp = rankCmp(r.cur.getPeersRateFromCache(r.secondPriority), old.getPeersRateFromCache(r.secondPriority), stepRank(0, dimToStep[r.secondPriority]))
	return
}

func (r *rankV1) rankToDimString() string {
	switch r.cur.progressiveRank {
	case 4:
		return "all"
	case 3:
		return utils.DimToString(r.firstPriority)
	case 2:
		return utils.DimToString(r.secondPriority)
	case 1:
		return utils.DimToString(r.firstPriority) + "-only"
	default:
		return "none"
	}
}

func (*rankV1) needSearchRevertRegions() bool {
	return false
}

func (*rankV1) setSearchRevertRegions() {}

func (r *rankV1) isBetterForWriteLeader() bool {
	srcRate, dstRate := r.cur.getExtremeLoad(r.firstPriority)
	peersRate := r.cur.getPeersRateFromCache(r.firstPriority)
	return srcRate-peersRate >= dstRate+peersRate && r.isTolerance(r.firstPriority, false)
}

func (r *rankV1) isBetter(dim int) bool {
	isHot, decRatio := r.getHotDecRatioByPriorities(dim)
	return isHot && decRatio <= r.greatDecRatio && r.isTolerance(dim, false)
}

// isNotWorsened must be true if isBetter is true.
func (r *rankV1) isNotWorsened(dim int) bool {
	isHot, decRatio := r.getHotDecRatioByPriorities(dim)
	return !isHot || decRatio <= r.minorDecRatio
}

func (r *rankV1) getHotDecRatioByPriorities(dim int) (isHot bool, decRatio float64) {
	// we use DecRatio(Decline Ratio) to expect that the dst store's rate should still be less
	// than the src store's rate after scheduling one peer.
	srcRate, dstRate := r.cur.getExtremeLoad(dim)
	peersRate := r.cur.getPeersRateFromCache(dim)
	// Rate may be negative after adding revertRegion, which should be regarded as moving from dst to src.
	if peersRate >= 0 {
		isHot = peersRate >= r.getMinRate(dim)
		decRatio = (dstRate + peersRate) / math.Max(srcRate-peersRate, 1)
	} else {
		isHot = -peersRate >= r.getMinRate(dim)
		decRatio = (srcRate - peersRate) / math.Max(dstRate+peersRate, 1)
	}
	return
}
