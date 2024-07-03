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

	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/statistics/utils"
)

const (
	firstPriorityPerceivedRatio = 0.2  // PeerRate needs to be 20% above what needs to be balanced.
	firstPriorityMinHotRatio    = 0.02 // PeerRate needs to be greater than 2% lowRate

	secondPriorityPerceivedRatio = 0.3  // PeerRate needs to be 30% above what needs to be balanced.
	secondPriorityMinHotRatio    = 0.03 // PeerRate needs to be greater than 3% lowRate
)

// isAvailable returns the solution is available.
// If the solution has no revertRegion, progressiveRank should < 0.
// If the solution has some revertRegion, progressiveRank should equal to -4 or -3.
func isAvailableV2(s *solution) bool {
	// TODO: Test if revert region can be enabled for -1.
	return s.progressiveRank <= -3 || (s.progressiveRank < 0 && s.revertRegion == nil)
}

type balanceChecker struct {
	// We use the following example to illustrate the calculation process.
	// Suppose preBalancedRatio is 0.9, balancedRatio is 0.95.
	// If 0.95<=low/high, the two stores are considered balanced after the operator is completed.
	// If low/high<0.9, the two stores are considered unbalanced after the operator is completed.
	// If 0.9<=low/high<0.95, the two stores are considered pre-balanced after the operator is completed.
	preBalancedRatio float64
	balancedRatio    float64
}

// rankV2Ratios is used to calculate the balanced state.
// every rankV2Ratios only effect one dim.

// There are three states of balance: balanced, pre-balanced, and unbalanced.
// It is determined by the ratio of the high and low values of the two stores.
// If the ratio is greater than the balancedRatio(0.95), it is considered to be in the balanced state.
// If the ratio is less than the preBalancedRatio(0.9), it is considered to be in the unbalanced state.
// If the ratio is between the two, it is considered to be in the pre-balanced state.

// TODO: Unified with stddevThreshold.
type rankV2Ratios struct {
	// futureChecker is used to calculate the balanced state after the operator is completed.
	// It is stricter than the currentChecker.
	futureChecker *balanceChecker
	// currentChecker is used to calculate the balanced state in the currentChecker state, which means that the operator is not triggered.
	currentChecker *balanceChecker

	// perceivedRatio avoid to not worse in a state with a large region.
	// For example, if the region is 20MB, the high store is 100MB, the low store is 80MB, the low/high is 0.8.
	// If scheduling to the low store, the high store will be 80MB, the low store will be 100MB, the low/high still be 0.8, it is not worse.
	// we need to avoid scheduling to the low store, so introduce perceivedRatio.
	perceivedRatio float64
	// minHotRatio is the minimum ratio for the hot region to be scheduled.
	minHotRatio float64
}

func newRankV2Ratios(balancedRatio, perceivedRatio, minHotRatio float64) *rankV2Ratios {
	// limit 0.7 <= balancedRatio <= 0.95
	if balancedRatio < 0.7 {
		balancedRatio = 0.7
	}
	if balancedRatio > 0.95 {
		balancedRatio = 0.95
	}

	futureStateChecker := &balanceChecker{
		balancedRatio: balancedRatio,
		// preBalancedRatio = 1.0 - 2*(1.0-balancedRatio)
		// The maximum value with `balancedRatio-0.1` is to prevent the preBalance range becoming too large.
		preBalancedRatio: math.Max(2.0*balancedRatio-1.0, balancedRatio-0.1),
	}
	currentStateChecker := &balanceChecker{
		balancedRatio:    balancedRatio - 0.02,
		preBalancedRatio: futureStateChecker.preBalancedRatio - 0.03,
	}

	rs := &rankV2Ratios{
		futureChecker:  futureStateChecker,
		currentChecker: currentStateChecker,
		perceivedRatio: perceivedRatio, minHotRatio: minHotRatio}

	return rs
}

func (bs *balanceSolver) initRankV2() {
	bs.firstPriorityV2Ratios = newRankV2Ratios(bs.sche.conf.GetGreatDecRatio(), firstPriorityPerceivedRatio, firstPriorityMinHotRatio)
	// The second priority is less demanding. Set the preBalancedRatio of the first priority to the balancedRatio of the second dimension.
	bs.secondPriorityV2Ratios = newRankV2Ratios(bs.firstPriorityV2Ratios.futureChecker.preBalancedRatio, secondPriorityPerceivedRatio, secondPriorityMinHotRatio)

	bs.isAvailable = isAvailableV2
	bs.filterUniformStore = bs.filterUniformStoreV2
	bs.needSearchRevertRegions = bs.needSearchRevertRegionsV2
	bs.setSearchRevertRegions = bs.setSearchRevertRegionsV2
	bs.calcProgressiveRank = bs.calcProgressiveRankV2
	bs.betterThan = bs.betterThanV2
	bs.rankToDimString = bs.rankToDimStringV2
	bs.pickCheckPolicyV2()
}

// pickCheckPolicyV2 will set checkByPriorityAndTolerance to the corresponding function.
// Note: PolicyV2 will search more possible solutions than PolicyV1.
// so it allows to schedule when any of the two dimensions is not balanced.
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

// filterUniformStoreV2 filters stores by stddev.
// stddev is the standard deviation of the store's load for all stores.
func (bs *balanceSolver) filterUniformStoreV2() (string, bool) {
	if !bs.enableExpectation() {
		return "", false
	}
	// Because region is available for src and dst, so stddev is the same for both, only need to calculate one.
	isUniformFirstPriority, isUniformSecondPriority := bs.isUniformFirstPriority(bs.cur.srcStore), bs.isUniformSecondPriority(bs.cur.srcStore)
	if isUniformFirstPriority && isUniformSecondPriority {
		// If both dims are enough uniform, any schedule is unnecessary.
		return "all-dim", true
	}
	if isUniformFirstPriority && (bs.cur.progressiveRank == -2 || bs.cur.progressiveRank == -3) {
		// If first priority dim is enough uniform, -2 is unnecessary and maybe lead to worse balance for second priority dim
		return utils.DimToString(bs.firstPriority), true
	}
	if isUniformSecondPriority && bs.cur.progressiveRank == -1 {
		// If second priority dim is enough uniform, -1 is unnecessary and maybe lead to worse balance for first priority dim
		return utils.DimToString(bs.secondPriority), true
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
		counterWithEvent(config.HotRegionScheduler, event).Inc()
	}
}

// calcProgressiveRankV2 calculates `bs.cur.progressiveRank`.
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
	// For unbalanced state,
	// roughly speaking, as long as the diff is reduced, it is either better or not worse.
	// To distinguish the small regions, the one where the diff is reduced too little is defined as not worse,
	// and the one where the diff is reversed is regarded as worse.
	// For pre-balanced state,
	// it is better only if it reach the balanced state,
	// and it is not worse if it still in the pre-balanced state.
	// and it is worse if it becomes the unbalanced state.
	// For balanced state,
	// there is no better state to move to,
	// it is not worse if it still in the balanced state.
	// and it is worse if it becomes the pre-balanced state or unbalanced state.

	// minNotWorsenedRate, minBetterRate, minBalancedRate, maxBalancedRate, maxBetterRate, maxNotWorsenedRate
	// can be determined from src, dst and peer. The higher the score, the better.
	// The closer to the center the higher the score, the higher the score for symmetrical zones without revert than with revert.
	// so d is higher than c and e, c is higher than e, only when state is better, the score is positive.
	// so c and e are higher than b and f, b are higher than f, only when state is not worsened and not revert, the score is zero.
	// so b and f are higher than a and g, the worse tate have the same score.
	// * a: peersRate < minNotWorsenedRate                   ====> score == -2
	// * b: minNotWorsenedRate <= peersRate < minBetterRate  ====> score == 0
	// * c: minBetterRate <= peersRate < minBalancedRate     ====> score == 2
	// * d: minBalancedRate <= peersRate <= maxBalancedRate  ====> score == 3
	// * e: maxBalancedRate < peersRate <= maxBetterRate     ====> score == 1
	// * f: maxBetterRate < peersRate <= maxNotWorsenedRate  ====> score == -1
	// * g: peersRate > maxNotWorsenedRate                   ====> score == -2

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

	if highRate*rs.currentChecker.balancedRatio <= lowRate {
		// At this time, it is considered to be in the balanced state.
		// Because rs.currentChecker.balancedRatio <= lowRate/highRate.

		// We use the following example to illustrate the calculation process.
		// Suppose futureChecker.balancedRatio is 0.95, and currentChecker.balancedRatio is 0.93.
		// Suppose the low and high are 94 and 101.
		// So their ratio is 94/101≈0.9306, 0.93<0.9306, it is considered to be in the balanced state.
		// If we hope the future stores are not worse than the current stores
		// we need to ensure that the ratio of the future stores is 0.95.
		// So the future stores need to be 94+1, 101-1, that is 95/100=0.95.
		// Or the future stores need to be 94+6, 101-6, that is 95/100=0.95.
		// So not-worse peer range is [1,6]

		// Because it has been balanced state, there is no better state to move to, so there's no 1 or 2 or 3 score.
		// And there is no balanced state to move to.
		// If the balanced state is not broken, but the loads are closer, score == 0, that is, peer range is [1,6].
		// If the balanced state is broken, score = -2.

		// (lowRate+minNotWorsenedRate) / (highRate-minNotWorsenedRate) = futureChecker.balancedRatio
		minNotWorsenedRate := (highRate*rs.futureChecker.balancedRatio - lowRate) / (1.0 + rs.futureChecker.balancedRatio)
		// (highRate-maxNotWorsenedRate) / (lowRate+maxNotWorsenedRate) = futureChecker.balancedRatio
		maxNotWorsenedRate := (highRate - lowRate*rs.futureChecker.balancedRatio) / (1.0 + rs.futureChecker.balancedRatio)

		if minNotWorsenedRate > -bs.getMinRate(dim) { // use min rate as 0 value
			minNotWorsenedRate = -bs.getMinRate(dim)
		}

		if peersRate >= minNotWorsenedRate && peersRate <= maxNotWorsenedRate {
			return 0
		}
		return -2
	}

	// When it is not in the balanced state, it is considered to be in the unbalanced state or pre-balanced state.
	// We use the following example to illustrate the calculation process.
	// Suppose futureChecker.balancedRatio is 0.95
	// Suppose the low and high are 75 and 120.
	// If we hope it can reach the balanced state, we need to ensure that the ratio of the future stores is greater than 0.95.
	// So the future stores need to be 75+20, 120-20, that is 95/100=0.95.
	// Or the future stores need to be 75+25, 120-25, that is 95/100=0.95.
	// So balanced peer range is [20,25]

	// (lowRate+minBalancedRate) / (highRate-minBalancedRate) = futureChecker.balancedRatio
	minBalancedRate := (highRate*rs.futureChecker.balancedRatio - lowRate) / (1.0 + rs.futureChecker.balancedRatio)
	// (highRate-maxBalancedRate) / (lowRate+maxBalancedRate) = futureChecker.balancedRatio
	maxBalancedRate := (highRate - lowRate*rs.futureChecker.balancedRatio) / (1.0 + rs.futureChecker.balancedRatio)

	pendingRateLimit := false
	var minNotWorsenedRate, minBetterRate, maxBetterRate, maxNotWorsenedRate float64
	if highRate*rs.currentChecker.preBalancedRatio <= lowRate {
		// At this time, it is considered to be in pre-balanced state.
		// Because rs.currentChecker.preBalancedRatio <= lowRate/highRate < rs.currentChecker.balancedRatio.

		// We use the following example to illustrate the calculation process.
		// Suppose futureChecker.balancedRatio is 0.95, and currentChecker.balancedRatio is 0.93.
		// Suppose futureChecker.preBalancedRatio is 0.9, and currentChecker.preBalancedRatio is 0.87.
		// Suppose the low and high are 93 and 102.
		// So their ratio is 93/102≈0.91, 0.87<0.91<0.93, it is considered to be in the pre-balanced state.
		// For the pre-balanced state, only the schedules that reach the balanced state is considered to be better.
		// So minBetterRate is minBalancedRate, maxBetterRate is maxBalancedRate.
		// If we hope the future stores are better than the current stores
		// we need to ensure that the ratio of the future stores is greater than 0.95.
		// So the future stores need to be 93+2, 102-2, that is 95/100=0.95.
		// Or the future stores need to be 93+7, 102-7, that is 95/100=0.95.
		// So better range is [2,7]
		// If we hope the future stores are not worse than the current stores,
		// we need to ensure that the ratio of the future stores is 0.9.
		// So the future stores need to be 93+(-1), 102-(-1), that is 92/103≈0.9.
		// Or the future stores need to be 93+10, 102-10, that is 92/103≈0.9.
		// So not-worse peer range is [-1,2) and (7,10],
		// [-1,2) means there is no revert region, (7,10] means there is revert region.
		// And we need to avoid scheduling with negative operators.
		// not-worse peer range is [max(0,-1),2), which is [0,2).

		minBetterRate, maxBetterRate = minBalancedRate, maxBalancedRate
		// (lowRate+minNotWorsenedRate) / (highRate-minNotWorsenedRate) = futureChecker.preBalancedRatio
		minNotWorsenedRate = (highRate*rs.futureChecker.preBalancedRatio - lowRate) / (1.0 + rs.futureChecker.preBalancedRatio)
		// (highRate-maxNotWorsenedRate) / (lowRate+maxNotWorsenedRate) = futureChecker.preBalancedRatio
		maxNotWorsenedRate = (highRate - lowRate*rs.futureChecker.preBalancedRatio) / (1.0 + rs.futureChecker.preBalancedRatio)
		if minNotWorsenedRate > -bs.getMinRate(dim) { // use min rate as 0 value
			minNotWorsenedRate = -bs.getMinRate(dim)
		}
		// When approaching the balanced state, wait for pending influence to zero before scheduling to reduce jitter.
		// From pre-balanced state to balanced state, we don't need other more schedule.
		pendingRateLimit = true
	} else {
		// At this time, it is considered to be in the unbalanced state.
		// Because lowRate/highRate < rs.currentChecker.balancedRatio.

		// We use the following example to illustrate the calculation process.
		// Suppose futureChecker.balancedRatio is 0.95, and currentChecker.balancedRatio is 0.93.
		// Suppose futureChecker.preBalancedRatio is 0.9, and currentChecker.preBalancedRatio is 0.87.
		// Suppose the low and high are 75 and 120.
		// So their ratio is 75/120=0.625, 0.625<0.87, it is considered to be in the unbalanced state.
		// If we hope the future stores are balanced,
		// we need to ensure that the ratio of the future stores is 0.95.
		// So the future stores need to be 75+20, 120-20, that is 95/100=0.95.
		// Or the future stores need to be 75+25, 120-25, that is 95/100=0.95.
		// So balanced peer range is [20,25]

		// For the unbalanced state, as long as the diff is reduced, it is better.
		// And we need to ensure that the ratio of the two store are not reversed,
		// so the future stores need to be 75+45, 120-45.
		// So better peer range is [0,17) and (28,45].
		// If that's all it is,
		// min better is too small, and we don't want to give too high a score to a region that's too small.
		// To avoid scheduling small regions, we take the minimum value of the three,
		// which are according some of minBalancedRate, some of the low store and the top10 peer.
		// Suppose perceivedRatio is 0.2, minHotRatio is 0.02, top10 is 5.
		// So minBetterRate is min(0.2*20,0.02*75,1)=4
		// So minNotWorsenedRate is 0
		// Similarly,
		// we don't want to dispatch a particularly large region to reverse the high store and the low store, which is worse for us.
		// From the above, we know maxBetterRate is 25, and max rate which not reverse high store are low store is 45,
		// we reduce it by a factor, namely perceivedRatio.
		// So maxBetterRate is 25+(45-25-4)*0.2=28.2
		// So maxNotWorsenedRate is 25+(45-25-0)*0.2=29

		minBetterRate = math.Min(minBalancedRate*rs.perceivedRatio, lowRate*rs.minHotRatio)
		minBetterRate = math.Min(minBetterRate, topnRate)
		maxBetterRate = maxBalancedRate + rs.perceivedRatio*(highRate-lowRate-maxBalancedRate-minBetterRate)

		maxNotWorsenedRate = maxBalancedRate + rs.perceivedRatio*(highRate-lowRate-maxBalancedRate-minNotWorsenedRate)
		minNotWorsenedRate = -bs.getMinRate(dim) // use min rate as 0 value
	}

	switch {
	case minBetterRate <= peersRate && peersRate <= maxBetterRate:
		// Positive score requires some restrictions.
		if peersRate >= bs.getMinRate(dim) && bs.isTolerance(dim, reverse) &&
			(!pendingRateLimit || math.Abs(srcPendingRate)+math.Abs(dstPendingRate) < 1 /*byte*/) { // avoid with pending influence when approaching the balanced state
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
			return getRkCmpByPriorityV2(bs.firstPriority, bs.cur.firstScore, old.firstScore,
				bs.cur.getPeersRateFromCache(bs.firstPriority), old.getPeersRateFromCache(bs.firstPriority)) > 0
		}

		firstCmp := getRkCmpByPriorityV2(bs.firstPriority, bs.cur.firstScore, old.firstScore,
			bs.cur.getPeersRateFromCache(bs.firstPriority), old.getPeersRateFromCache(bs.firstPriority))
		secondCmp := getRkCmpByPriorityV2(bs.secondPriority, bs.cur.secondScore, old.secondScore,
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

func getRkCmpByPriorityV2(dim int, curScore, oldScore int, curPeersRate, oldPeersRate float64) int {
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
		return utils.DimToString(bs.firstPriority)
	case -2:
		return utils.DimToString(bs.firstPriority) + "-only"
	case -1:
		return utils.DimToString(bs.secondPriority)
	default:
		return "none"
	}
}
