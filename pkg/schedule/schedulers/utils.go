// Copyright 2017 TiKV Project Authors.
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
	"net/url"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/statistics"
	"go.uber.org/zap"
)

const (
	// adjustRatio is used to adjust TolerantSizeRatio according to region count.
	adjustRatio                  float64 = 0.005
	leaderTolerantSizeRatio      float64 = 5.0
	minTolerantSizeRatio         float64 = 1.0
	influenceAmp                 int64   = 5
	defaultMaxRetryLimit                 = 10
	defaultMinRetryLimit                 = 1
	defaultRetryQuotaAttenuation         = 2
)

type solver struct {
	*plan.BalanceSchedulerPlan
	sche.SchedulerCluster
	kind              constant.ScheduleKind
	opInfluence       operator.OpInfluence
	tolerantSizeRatio float64
	tolerantSource    int64
	fit               *placement.RegionFit

	sourceScore float64
	targetScore float64
}

func newSolver(basePlan *plan.BalanceSchedulerPlan, kind constant.ScheduleKind, cluster sche.SchedulerCluster, opInfluence operator.OpInfluence) *solver {
	return &solver{
		BalanceSchedulerPlan: basePlan,
		SchedulerCluster:     cluster,
		kind:                 kind,
		opInfluence:          opInfluence,
		tolerantSizeRatio:    adjustTolerantRatio(cluster, kind),
	}
}

func (p *solver) getOpInfluence(storeID uint64) int64 {
	return p.opInfluence.GetStoreInfluence(storeID).ResourceProperty(p.kind)
}

func (p *solver) sourceStoreID() uint64 {
	return p.Source.GetID()
}

func (p *solver) sourceMetricLabel() string {
	return strconv.FormatUint(p.sourceStoreID(), 10)
}

func (p *solver) targetStoreID() uint64 {
	return p.Target.GetID()
}

func (p *solver) targetMetricLabel() string {
	return strconv.FormatUint(p.targetStoreID(), 10)
}

func (p *solver) sourceStoreScore(scheduleName string) float64 {
	sourceID := p.Source.GetID()
	tolerantResource := p.getTolerantResource()
	// to avoid schedule too much, if A's core greater than B and C a little
	// we want that A should be moved out one region not two
	influence := p.getOpInfluence(sourceID)
	if influence > 0 {
		influence = -influence
	}

	if p.GetSchedulerConfig().IsDebugMetricsEnabled() {
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), "source").Set(float64(influence))
		tolerantResourceStatus.WithLabelValues(scheduleName).Set(float64(tolerantResource))
	}
	var score float64
	switch p.kind.Resource {
	case constant.LeaderKind:
		sourceDelta := influence - tolerantResource
		score = p.Source.LeaderScore(p.kind.Policy, sourceDelta)
	case constant.RegionKind:
		sourceDelta := influence*influenceAmp - tolerantResource
		score = p.Source.RegionScore(p.GetSchedulerConfig().GetRegionScoreFormulaVersion(), p.GetSchedulerConfig().GetHighSpaceRatio(), p.GetSchedulerConfig().GetLowSpaceRatio(), sourceDelta)
	case constant.WitnessKind:
		sourceDelta := influence - tolerantResource
		score = p.Source.WitnessScore(sourceDelta)
	}
	return score
}

func (p *solver) targetStoreScore(scheduleName string) float64 {
	targetID := p.Target.GetID()
	// to avoid schedule too much, if A's score less than B and C in small range,
	// we want that A can be moved in one region not two
	tolerantResource := p.getTolerantResource()
	// to avoid schedule call back
	// A->B, A's influence is negative, so A will be target, C may move region to A
	influence := p.getOpInfluence(targetID)
	if influence < 0 {
		influence = -influence
	}

	if p.GetSchedulerConfig().IsDebugMetricsEnabled() {
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(targetID, 10), "target").Set(float64(influence))
	}
	var score float64
	switch p.kind.Resource {
	case constant.LeaderKind:
		targetDelta := influence + tolerantResource
		score = p.Target.LeaderScore(p.kind.Policy, targetDelta)
	case constant.RegionKind:
		targetDelta := influence*influenceAmp + tolerantResource
		score = p.Target.RegionScore(p.GetSchedulerConfig().GetRegionScoreFormulaVersion(), p.GetSchedulerConfig().GetHighSpaceRatio(), p.GetSchedulerConfig().GetLowSpaceRatio(), targetDelta)
	case constant.WitnessKind:
		targetDelta := influence + tolerantResource
		score = p.Target.WitnessScore(targetDelta)
	}
	return score
}

// Both of the source store's score and target store's score should be calculated before calling this function.
// It will not calculate the score again.
func (p *solver) shouldBalance(scheduleName string) bool {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	sourceID := p.Source.GetID()
	targetID := p.Target.GetID()
	// Make sure after move, source score is still greater than target score.
	shouldBalance := p.sourceScore > p.targetScore

	if !shouldBalance && log.GetLevel() <= zap.DebugLevel {
		log.Debug("skip balance "+p.kind.Resource.String(),
			zap.String("scheduler", scheduleName), zap.Uint64("region-id", p.Region.GetID()), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID),
			zap.Int64("source-size", p.Source.GetRegionSize()), zap.Float64("source-score", p.sourceScore),
			zap.Int64("target-size", p.Target.GetRegionSize()), zap.Float64("target-score", p.targetScore),
			zap.Int64("average-region-size", p.GetAverageRegionSize()),
			zap.Int64("tolerant-resource", p.getTolerantResource()))
	}
	return shouldBalance
}

func (p *solver) getTolerantResource() int64 {
	if p.tolerantSource > 0 {
		return p.tolerantSource
	}

	if (p.kind.Resource == constant.LeaderKind || p.kind.Resource == constant.WitnessKind) && p.kind.Policy == constant.ByCount {
		p.tolerantSource = int64(p.tolerantSizeRatio)
	} else {
		regionSize := p.GetAverageRegionSize()
		p.tolerantSource = int64(float64(regionSize) * p.tolerantSizeRatio)
	}
	return p.tolerantSource
}

func adjustTolerantRatio(cluster sche.SchedulerCluster, kind constant.ScheduleKind) float64 {
	var tolerantSizeRatio float64
	switch c := cluster.(type) {
	case *rangeCluster:
		// range cluster use a separate configuration
		tolerantSizeRatio = c.GetTolerantSizeRatio()
	default:
		tolerantSizeRatio = cluster.GetSchedulerConfig().GetTolerantSizeRatio()
	}
	if kind.Resource == constant.LeaderKind && kind.Policy == constant.ByCount {
		if tolerantSizeRatio == 0 {
			return leaderTolerantSizeRatio
		}
		return tolerantSizeRatio
	}

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

func getKeyRanges(args []string) ([]core.KeyRange, error) {
	var ranges []core.KeyRange
	for len(args) > 1 {
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, errs.ErrQueryUnescape.Wrap(err)
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, errs.ErrQueryUnescape.Wrap(err)
		}
		args = args[2:]
		ranges = append(ranges, core.NewKeyRange(startKey, endKey))
	}
	if len(ranges) == 0 {
		return []core.KeyRange{core.NewKeyRange("", "")}, nil
	}
	return ranges, nil
}

type pendingInfluence struct {
	op                *operator.Operator
	froms             []uint64
	to                uint64
	origin            statistics.Influence
	maxZombieDuration time.Duration
}

func newPendingInfluence(op *operator.Operator, froms []uint64, to uint64, infl statistics.Influence, maxZombieDur time.Duration) *pendingInfluence {
	return &pendingInfluence{
		op:                op,
		froms:             froms,
		to:                to,
		origin:            infl,
		maxZombieDuration: maxZombieDur,
	}
}

// stLdRate returns a function to get the load rate of the store with the specified dimension.
func stLdRate(dim int) func(ld *statistics.StoreLoad) float64 {
	return func(ld *statistics.StoreLoad) float64 {
		return ld.Loads[dim]
	}
}

func stLdCount(ld *statistics.StoreLoad) float64 {
	return ld.Count
}

type storeLoadCmp func(ld1, ld2 *statistics.StoreLoad) int

// negLoadCmp returns a cmp that returns the negation of cmps.
func negLoadCmp(cmp storeLoadCmp) storeLoadCmp {
	return func(ld1, ld2 *statistics.StoreLoad) int {
		return -cmp(ld1, ld2)
	}
}

// sliceLoadCmp returns function with running cmps in order.
// If the cmp returns 0, which means equal, the next cmp will be used.
// If all cmps return 0, the two loads are considered equal.
func sliceLoadCmp(cmps ...storeLoadCmp) storeLoadCmp {
	return func(ld1, ld2 *statistics.StoreLoad) int {
		for _, cmp := range cmps {
			if r := cmp(ld1, ld2); r != 0 {
				return r
			}
		}
		return 0
	}
}

// stLdRankCmp returns a cmp that compares the two loads with discretized data.
// For example, if the rank function discretize data by step 10 , the load 11 and 19 will be considered equal.
func stLdRankCmp(dim func(ld *statistics.StoreLoad) float64, rank func(value float64) int64) storeLoadCmp {
	return func(ld1, ld2 *statistics.StoreLoad) int {
		return rankCmp(dim(ld1), dim(ld2), rank)
	}
}

// rankCmp compares the two values with discretized data.
func rankCmp(a, b float64, rank func(value float64) int64) int {
	aRk, bRk := rank(a), rank(b)
	if aRk < bRk {
		return -1
	} else if aRk > bRk {
		return 1
	}
	return 0
}

type storeLPCmp func(lp1, lp2 *statistics.StoreLoadPred) int

// sliceLPCmp returns function with running cmps in order.
// If the cmp returns 0, which means equal, the next cmp will be used.
// If all cmps return 0, the two loads are considered equal.
func sliceLPCmp(cmps ...storeLPCmp) storeLPCmp {
	return func(lp1, lp2 *statistics.StoreLoadPred) int {
		for _, cmp := range cmps {
			if r := cmp(lp1, lp2); r != 0 {
				return r
			}
		}
		return 0
	}
}

// minLPCmp is a function to select the min load of the store between current and future when comparing.
func minLPCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *statistics.StoreLoadPred) int {
		return ldCmp(lp1.Min(), lp2.Min())
	}
}

// maxLPCmp is a function to select the max load of the store between current and future when comparing.
func maxLPCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *statistics.StoreLoadPred) int {
		return ldCmp(lp1.Max(), lp2.Max())
	}
}

// diffCmp is a function to select the diff load of the store between current and future when comparing.
func diffCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *statistics.StoreLoadPred) int {
		return ldCmp(lp1.Diff(), lp2.Diff())
	}
}

type retryQuota struct {
	initialLimit int
	minLimit     int
	attenuation  int

	limits map[uint64]int
}

func newRetryQuota() *retryQuota {
	return &retryQuota{
		initialLimit: defaultMaxRetryLimit,
		minLimit:     defaultMinRetryLimit,
		attenuation:  defaultRetryQuotaAttenuation,
		limits:       make(map[uint64]int),
	}
}

func (q *retryQuota) getLimit(store *core.StoreInfo) int {
	id := store.GetID()
	if limit, ok := q.limits[id]; ok {
		return limit
	}
	q.limits[id] = q.initialLimit
	return q.initialLimit
}

func (q *retryQuota) resetLimit(store *core.StoreInfo) {
	q.limits[store.GetID()] = q.initialLimit
}

func (q *retryQuota) attenuate(store *core.StoreInfo) {
	newLimit := q.getLimit(store) / q.attenuation
	if newLimit < q.minLimit {
		newLimit = q.minLimit
	}
	q.limits[store.GetID()] = newLimit
}

func (q *retryQuota) gc(keepStores []*core.StoreInfo) {
	set := make(map[uint64]struct{}, len(keepStores))
	for _, store := range keepStores {
		set[store.GetID()] = struct{}{}
	}
	for id := range q.limits {
		if _, ok := set[id]; !ok {
			delete(q.limits, id)
		}
	}
}

// pauseAndResumeLeaderTransfer checks the old and new store IDs, and pause or resume the leader transfer.
func pauseAndResumeLeaderTransfer[T any](cluster *core.BasicCluster, old, new map[uint64]T) {
	for id := range old {
		if _, ok := new[id]; ok {
			continue
		}
		cluster.ResumeLeaderTransfer(id)
	}
	for id := range new {
		if _, ok := old[id]; ok {
			continue
		}
		if err := cluster.PauseLeaderTransfer(id); err != nil {
			log.Error("pause leader transfer failed", zap.Uint64("store-id", id), errs.ZapError(err))
		}
	}
}
