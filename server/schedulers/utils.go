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
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"math"
	"net/url"
	"strconv"

	"github.com/montanaflynn/stats"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/statistics"
	"go.uber.org/zap"
)

const (
	// adjustRatio is used to adjust TolerantSizeRatio according to region count.
	adjustRatio             float64 = 0.005
	leaderTolerantSizeRatio float64 = 5.0
	minTolerantSizeRatio    float64 = 1.0
)

func shouldBalance(cluster opt.Cluster, source, target *core.StoreInfo, region *core.RegionInfo, kind core.ScheduleKind, opInfluence operator.OpInfluence, scheduleName string) (shouldBalance bool, sourceScore float64, targetScore float64) {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	sourceID := source.GetID()
	targetID := target.GetID()
	tolerantResource := getTolerantResource(cluster, region, kind)
	sourceInfluence := opInfluence.GetStoreInfluence(sourceID).ResourceProperty(kind)
	targetInfluence := opInfluence.GetStoreInfluence(targetID).ResourceProperty(kind)
	sourceDelta, targetDelta := sourceInfluence-tolerantResource, targetInfluence+tolerantResource
	opts := cluster.GetOpts()
	switch kind.Resource {
	case core.LeaderKind:
		sourceScore = source.LeaderScore(kind.Policy, sourceDelta)
		targetScore = target.LeaderScore(kind.Policy, targetDelta)
	case core.RegionKind:
		sourceScore = source.RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), sourceDelta, -1)
		targetScore = target.RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), targetDelta, 1)
	}
	if opts.IsDebugMetricsEnabled() {
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), "source").Set(float64(sourceInfluence))
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(targetID, 10), "target").Set(float64(targetInfluence))
		tolerantResourceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), strconv.FormatUint(targetID, 10)).Set(float64(tolerantResource))
	}
	// Make sure after move, source score is still greater than target score.
	shouldBalance = sourceScore > targetScore

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
	return shouldBalance, sourceScore, targetScore
}

func getTolerantResource(cluster opt.Cluster, region *core.RegionInfo, kind core.ScheduleKind) int64 {
	if kind.Resource == core.LeaderKind && kind.Policy == core.ByCount {
		tolerantSizeRatio := cluster.GetOpts().GetTolerantSizeRatio()
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
	tolerantSizeRatio := cluster.GetOpts().GetTolerantSizeRatio()
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
	return typeutil.MaxUint64(1, uint64(limit))
}

func getKeyRanges(args []string) ([]core.KeyRange, error) {
	var ranges []core.KeyRange
	for len(args) > 1 {
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, errs.ErrQueryUnescape.Wrap(err).FastGenWithCause()
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, errs.ErrQueryUnescape.Wrap(err).FastGenWithCause()
		}
		args = args[2:]
		ranges = append(ranges, core.NewKeyRange(startKey, endKey))
	}
	if len(ranges) == 0 {
		return []core.KeyRange{core.NewKeyRange("", "")}, nil
	}
	return ranges, nil
}

// Influence records operator influence.
type Influence struct {
	Loads []float64
	Count float64
}

func (lhs *Influence) add(rhs *Influence, w float64) *Influence {
	var infl Influence
	for i := range lhs.Loads {
		infl.Loads = append(infl.Loads, lhs.Loads[i]+rhs.Loads[i]*w)
	}
	infl.Count = infl.Count + rhs.Count*w
	return &infl
}

// TODO: merge it into OperatorInfluence.
type pendingInfluence struct {
	op       *operator.Operator
	from, to uint64
	origin   Influence
}

func newPendingInfluence(op *operator.Operator, from, to uint64, infl Influence) *pendingInfluence {
	return &pendingInfluence{
		op:     op,
		from:   from,
		to:     to,
		origin: infl,
	}
}

// summaryPendingInfluence calculate the summary pending Influence for each store and return storeID -> Influence
// It makes each key/byte rate or count become (1+w) times to the origin value while f is the function to provide w(weight)
func summaryPendingInfluence(pendings map[*pendingInfluence]struct{}, f func(*operator.Operator) float64) map[uint64]*Influence {
	ret := make(map[uint64]*Influence)
	for p := range pendings {
		w := f(p.op)
		if w == 0 {
			delete(pendings, p)
		}
		if _, ok := ret[p.to]; !ok {
			ret[p.to] = &Influence{Loads: make([]float64, len(p.origin.Loads))}
		}
		ret[p.to] = ret[p.to].add(&p.origin, w)
		if _, ok := ret[p.from]; !ok {
			ret[p.from] = &Influence{Loads: make([]float64, len(p.origin.Loads))}
		}
		ret[p.from] = ret[p.from].add(&p.origin, -w)
	}
	return ret
}

type storeLoad struct {
	Loads []float64
	Count float64
}

func (load storeLoad) ToLoadPred(rwTy rwType, infl *Influence) *storeLoadPred {
	future := storeLoad{
		Loads: append(load.Loads[:0:0], load.Loads...),
		Count: load.Count,
	}
	if infl != nil {
		switch rwTy {
		case read:
			future.Loads[statistics.ByteDim] += infl.Loads[statistics.RegionReadBytes]
			future.Loads[statistics.KeyDim] += infl.Loads[statistics.RegionReadKeys]
		case write:
			future.Loads[statistics.ByteDim] += infl.Loads[statistics.RegionWriteBytes]
			future.Loads[statistics.KeyDim] += infl.Loads[statistics.RegionWriteKeys]
		}
		future.Count += infl.Count
	}
	return &storeLoadPred{
		Current: load,
		Future:  future,
	}
}

func stLdByteRate(ld *storeLoad) float64 {
	return ld.Loads[statistics.ByteDim]
}

func stLdKeyRate(ld *storeLoad) float64 {
	return ld.Loads[statistics.KeyDim]
}

func stLdCount(ld *storeLoad) float64 {
	return ld.Count
}

type storeLoadCmp func(ld1, ld2 *storeLoad) int

func negLoadCmp(cmp storeLoadCmp) storeLoadCmp {
	return func(ld1, ld2 *storeLoad) int {
		return -cmp(ld1, ld2)
	}
}

func sliceLoadCmp(cmps ...storeLoadCmp) storeLoadCmp {
	return func(ld1, ld2 *storeLoad) int {
		for _, cmp := range cmps {
			if r := cmp(ld1, ld2); r != 0 {
				return r
			}
		}
		return 0
	}
}

func stLdRankCmp(dim func(ld *storeLoad) float64, rank func(value float64) int64) storeLoadCmp {
	return func(ld1, ld2 *storeLoad) int {
		return rankCmp(dim(ld1), dim(ld2), rank)
	}
}

func rankCmp(a, b float64, rank func(value float64) int64) int {
	aRk, bRk := rank(a), rank(b)
	if aRk < bRk {
		return -1
	} else if aRk > bRk {
		return 1
	}
	return 0
}

// store load prediction
type storeLoadPred struct {
	Current storeLoad
	Future  storeLoad
	Expect  storeLoad
}

func (lp *storeLoadPred) min() *storeLoad {
	return minLoad(&lp.Current, &lp.Future)
}

func (lp *storeLoadPred) max() *storeLoad {
	return maxLoad(&lp.Current, &lp.Future)
}

func (lp *storeLoadPred) diff() *storeLoad {
	mx, mn := lp.max(), lp.min()
	loads := make([]float64, len(mx.Loads))
	for i := range loads {
		loads[i] = mx.Loads[i] - mn.Loads[i]
	}
	return &storeLoad{
		Loads: loads,
		Count: mx.Count - mn.Count,
	}
}

type storeLPCmp func(lp1, lp2 *storeLoadPred) int

func sliceLPCmp(cmps ...storeLPCmp) storeLPCmp {
	return func(lp1, lp2 *storeLoadPred) int {
		for _, cmp := range cmps {
			if r := cmp(lp1, lp2); r != 0 {
				return r
			}
		}
		return 0
	}
}

func minLPCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *storeLoadPred) int {
		return ldCmp(lp1.min(), lp2.min())
	}
}

func maxLPCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *storeLoadPred) int {
		return ldCmp(lp1.max(), lp2.max())
	}
}

func diffCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *storeLoadPred) int {
		return ldCmp(lp1.diff(), lp2.diff())
	}
}

func minLoad(a, b *storeLoad) *storeLoad {
	loads := make([]float64, len(a.Loads))
	for i := range loads {
		loads[i] = math.Min(a.Loads[i], b.Loads[i])
	}
	return &storeLoad{
		Loads: loads,
		Count: math.Min(a.Count, b.Count),
	}
}

func maxLoad(a, b *storeLoad) *storeLoad {
	loads := make([]float64, len(a.Loads))
	for i := range loads {
		loads[i] = math.Max(a.Loads[i], b.Loads[i])
	}
	return &storeLoad{
		Loads: loads,
		Count: math.Max(a.Count, b.Count),
	}
}

type storeLoadDetail struct {
	LoadPred *storeLoadPred
	HotPeers []*statistics.HotPeerStat
}

func (li *storeLoadDetail) toHotPeersStat() *statistics.HotPeersStat {
	peers := make([]statistics.HotPeerStat, 0, len(li.HotPeers))
	totalLoads := make([]float64, statistics.RegionStatCount)
	for _, peer := range li.HotPeers {
		if peer.HotDegree > 0 {
			peers = append(peers, *peer.Clone())
			for i := range totalLoads {
				totalLoads[i] += peer.GetLoad(statistics.RegionStatKind(i))
			}
		}
	}
	return &statistics.HotPeersStat{
		TotalLoads: totalLoads,
		Count:      len(peers),
		Stats:      peers,
	}
}
