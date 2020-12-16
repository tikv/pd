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
	"fmt"
	"math"
	"net/url"
	"sort"
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

func shouldBalance(cluster opt.Cluster, source, target *core.StoreInfo, region *core.RegionInfo, kind core.ScheduleKind, opInfluence operator.OpInfluence, scheduleName string) bool {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	sourceID := source.GetID()
	targetID := target.GetID()
	tolerantResource := getTolerantResource(cluster, region, kind)
	sourceInfluence := opInfluence.GetStoreInfluence(sourceID).ResourceProperty(kind)
	targetInfluence := opInfluence.GetStoreInfluence(targetID).ResourceProperty(kind)
	opts := cluster.GetOpts()
	sourceScore := source.ResourceScore(kind, opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), sourceInfluence-tolerantResource)
	targetScore := target.ResourceScore(kind, opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), targetInfluence+tolerantResource)
	if opts.IsDebugMetricsEnabled() {
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), "source").Set(float64(sourceInfluence))
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(targetID, 10), "target").Set(float64(targetInfluence))
		tolerantResourceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), strconv.FormatUint(targetID, 10)).Set(float64(tolerantResource))
	}
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
	ByteRate float64
	KeyRate  float64
	Count    float64
	Ops      float64
}

func (infl Influence) add(rhs *Influence, w float64) Influence {
	infl.ByteRate += rhs.ByteRate * w
	infl.KeyRate += rhs.KeyRate * w
	infl.Count += rhs.Count * w
	infl.Ops += rhs.Ops * w
	return infl
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
func summaryPendingInfluence(pendings map[*pendingInfluence]struct{}, f func(*operator.Operator) float64) map[uint64]Influence {
	ret := map[uint64]Influence{}
	for p := range pendings {
		w := f(p.op)
		if w == 0 {
			delete(pendings, p)
		}
		ret[p.to] = ret[p.to].add(&p.origin, w)
		ret[p.from] = ret[p.from].add(&p.origin, -w)
	}
	return ret
}

type storeLoad struct {
	ByteRate float64
	KeyRate  float64
	Count    float64
	Ops      float64

	// Exp means Expectation, it is calculated from the average value from summary of byte/key rate, count.
	ExpByteRate float64
	ExpKeyRate  float64
	ExpCount    float64
	ExpOps      float64
}

func (load *storeLoad) ToLoadPred(infl Influence) *storeLoadPred {
	future := *load
	future.ByteRate += infl.ByteRate
	future.KeyRate += infl.KeyRate
	future.Count += infl.Count
	future.Ops += infl.Ops
	return &storeLoadPred{
		Current: *load,
		Future:  future,
	}
}

func stLdByteRate(ld *storeLoad) float64 {
	return ld.ByteRate
}

func stLdKeyRate(ld *storeLoad) float64 {
	return ld.KeyRate
}

func stLdCount(ld *storeLoad) float64 {
	return ld.Count
}

func stLdOps(ld *storeLoad) float64 {
	return ld.Ops
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
}

func (lp *storeLoadPred) min() *storeLoad {
	return minLoad(&lp.Current, &lp.Future)
}

func (lp *storeLoadPred) max() *storeLoad {
	return maxLoad(&lp.Current, &lp.Future)
}

func (lp *storeLoadPred) diff() *storeLoad {
	mx, mn := lp.max(), lp.min()
	return &storeLoad{
		ByteRate: mx.ByteRate - mn.ByteRate,
		KeyRate:  mx.KeyRate - mn.KeyRate,
		Count:    mx.Count - mn.Count,
		Ops:      mx.Ops - mn.Ops,
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
	return &storeLoad{
		ByteRate: math.Min(a.ByteRate, b.ByteRate),
		KeyRate:  math.Min(a.KeyRate, b.KeyRate),
		Count:    math.Min(a.Count, b.Count),
		Ops:      math.Min(a.Ops, b.Ops),
	}
}

func maxLoad(a, b *storeLoad) *storeLoad {
	return &storeLoad{
		ByteRate: math.Max(a.ByteRate, b.ByteRate),
		KeyRate:  math.Max(a.KeyRate, b.KeyRate),
		Count:    math.Max(a.Count, b.Count),
		Ops:      math.Max(a.Ops, b.Ops),
	}
}

type storeLoadDetail struct {
	LoadPred *storeLoadPred
	HotPeers []*statistics.HotPeerStat
}

func (li *storeLoadDetail) toHotPeersStat() *statistics.HotPeersStat {
	peers := make([]statistics.HotPeerStat, 0, len(li.HotPeers))
	for _, peer := range li.HotPeers {
		peers = append(peers, *peer.Clone())
	}
	return &statistics.HotPeersStat{
		TotalBytesRate: li.LoadPred.Current.ByteRate,
		TotalKeysRate:  li.LoadPred.Current.KeyRate,
		TotalOps:       li.LoadPred.Current.Ops,
		Count:          len(li.HotPeers),
		Stats:          peers,
	}
}

func calcCV(data []float64) float64 {
	var stdVar, mean float64
	for _, d := range data {
		mean += d
	}
	if len(data) <= 1 || mean == 0 {
		return 0
	}
	mean /= float64(len(data))
	for _, d := range data {
		delta := d - mean
		stdVar += delta * delta
	}
	stdVar /= float64(len(data) - 1)
	return math.Sqrt(stdVar) / mean
}

type stableAnalysis struct {
	capacity  int
	threshold float64
	datas     []float64
}

func newStableAnalysis(capacity int, threshold float64) *stableAnalysis {
	return &stableAnalysis{
		capacity:  capacity,
		threshold: threshold,
	}
}

func (d *stableAnalysis) add(data float64) {
	d.datas = append(d.datas, data)
	if len(d.datas) > d.capacity {
		d.datas = d.datas[1:]
	}
}

func (d *stableAnalysis) isStable() bool {
	if len(d.datas) == d.capacity && calcCV(d.datas) <= d.threshold {
		return true
	}
	return false
}

func (d *stableAnalysis) last() float64 {
	if len(d.datas) == 0 {
		return 0
	}
	return d.datas[len(d.datas)-1]
}

func (d *stableAnalysis) reset() {
	d.datas = nil
}

func (d *stableAnalysis) toString() string {
	return fmt.Sprintf("stableAnalysis, len %d, cur_val %lf, cv %lf", len(d.datas), d.last(), calcCV(d.datas))
}

// DimensionCount defines the number of dimensions
const DimensionCount = 2

type regionInfo struct {
	id              uint64
	regionID        uint64
	srcStoreID      uint64
	dstStoreID      uint64
	loads           [DimensionCount]float64
	diffLoad        float64
	splitRatio      float64
	splitDimID      uint64
	splittedIDs     []uint64
	splittedRegions map[uint64]*regionInfo

	peerStat *statistics.HotPeerStat
}

func newRegionInfo(id uint64, regionID uint64, srcStoreID uint64, load1 float64, load2 float64) *regionInfo {
	return &regionInfo{
		id:         id,
		regionID:   regionID,
		srcStoreID: srcStoreID,
		dstStoreID: srcStoreID,
		loads:      [DimensionCount]float64{load1, load2},
		diffLoad:   math.Abs(load1 - load2),
	}
}

func (r *regionInfo) NeedSplit() bool {
	return r.splitRatio != 0
}

type regionInfoByDiff []*regionInfo

func (r regionInfoByDiff) Len() int           { return len(r) }
func (r regionInfoByDiff) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r regionInfoByDiff) Less(i, j int) bool { return r[i].diffLoad < r[j].diffLoad }

type regionInfoByLoad0 []*regionInfo

func (r regionInfoByLoad0) Len() int           { return len(r) }
func (r regionInfoByLoad0) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r regionInfoByLoad0) Less(i, j int) bool { return r[i].loads[0] < r[j].loads[0] }

type regionInfoByLoad1 []*regionInfo

func (r regionInfoByLoad1) Len() int           { return len(r) }
func (r regionInfoByLoad1) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r regionInfoByLoad1) Less(i, j int) bool { return r[i].loads[1] < r[j].loads[1] }

type regionContainer struct {
	candiRegions    [DimensionCount]map[uint64][]*regionInfo
	migratedRegions []*regionInfo
	count           [DimensionCount]uint64
}

func newRegionContainer() *regionContainer {
	ret := &regionContainer{}
	for i := range ret.candiRegions {
		ret.candiRegions[i] = make(map[uint64][]*regionInfo, 0)
	}
	return ret
}

func (rc *regionContainer) push(dimID uint64, region *regionInfo) {
	rc.candiRegions[dimID][region.srcStoreID] = append(rc.candiRegions[dimID][region.srcStoreID], region)
	rc.count[dimID]++
}

func (rc *regionContainer) pop(dimID uint64, candidateStoreID uint64) *regionInfo {
	sid := candidateStoreID
	if len(rc.candiRegions[dimID][sid]) == 0 { // pick region from other nodes
		for sid1, candiRegioins := range rc.candiRegions[dimID] {
			if len(candiRegioins) != 0 {
				sid = sid1
				break
			}
		}
	}

	if len(rc.candiRegions[dimID][sid]) == 0 {
		log.Info("candiRegions should not empty",
			zap.Uint64("dimID", dimID),
			zap.Uint64("candidateStoreID", candidateStoreID),
			zap.Uint64("sid", sid),
			zap.Uint64("count", rc.count[dimID]),
		)
		return nil
	}

	var selectedRegion *regionInfo
	{
		length := len(rc.candiRegions[dimID][sid])
		selectedRegion = rc.candiRegions[dimID][sid][length-1]
		rc.candiRegions[dimID][sid] = rc.candiRegions[dimID][sid][:length-1]
		rc.count[dimID]--
		if len(rc.candiRegions[dimID][sid]) == 0 {
			delete(rc.candiRegions[dimID], sid)
		}
	}

	selectedRegion.dstStoreID = candidateStoreID
	// track region that needs to be migrated
	if selectedRegion.dstStoreID != selectedRegion.srcStoreID {
		rc.migratedRegions = append(rc.migratedRegions, selectedRegion)
	}
	return selectedRegion
}

func (rc *regionContainer) getMigratedRegions() []*regionInfo {
	return rc.migratedRegions
}

func (rc *regionContainer) getSplitFailedRegions(dimID uint64, storeID uint64, highLoadRegions map[uint64]*regionInfo, ratio float64) []*regionInfo {
	var splitFailedRegions, normalRegions []*regionInfo
	for _, region := range rc.candiRegions[dimID][storeID] {
		if region.NeedSplit() {
			splitFailedRegions = append(splitFailedRegions, region)
		} else {
			normalRegions = append(normalRegions, region)

			if dimID == 0 && region.loads[dimID] > ratio*2 {
				highLoadRegions[region.id] = region
				log.Info("highLoadRegion", zap.String("region", fmt.Sprintf("%+v", region)))
			}
		}
	}
	rc.candiRegions[dimID][storeID] = normalRegions
	rc.count[dimID] -= uint64(len(splitFailedRegions))
	return splitFailedRegions
}

func (rc *regionContainer) empty(dimID uint64) bool {
	return rc.count[dimID] == 0
}

type storeInfo struct {
	id            uint64
	loads         []float64
	regions       map[uint64]*regionInfo
	candiRegions  [][]*regionInfo
	sortedRegions []*regionInfo
}

type storeInfoByLoad0 []*storeInfo

func (s storeInfoByLoad0) Len() int           { return len(s) }
func (s storeInfoByLoad0) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s storeInfoByLoad0) Less(i, j int) bool { return s[i].loads[0] < s[j].loads[0] }

type storeInfoByLoad1 []*storeInfo

func (s storeInfoByLoad1) Len() int           { return len(s) }
func (s storeInfoByLoad1) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s storeInfoByLoad1) Less(i, j int) bool { return s[i].loads[1] < s[j].loads[1] }

func newStoreInfo(id uint64, loads []float64, regions map[uint64]*regionInfo) *storeInfo {
	ret := &storeInfo{
		id:           id,
		loads:        loads,
		regions:      regions,
		candiRegions: make([][]*regionInfo, DimensionCount),
	}
	for i := range ret.candiRegions {
		ret.candiRegions[i] = make([]*regionInfo, 0)
	}
	return ret
}

func (si *storeInfo) add(region *regionInfo) {
	si.regions[region.id] = region
	for i := range si.loads {
		si.loads[i] += region.loads[i]
	}
}

func (si *storeInfo) remove(region *regionInfo) {
	delete(si.regions, region.id)
	for i := range si.loads {
		si.loads[i] -= region.loads[i]
	}
}

func (si *storeInfo) classifyRegion() {
	for _, region := range si.regions {
		if region.loads[0] == region.loads[1] {
			si.candiRegions[0] = append(si.candiRegions[0], region)
			si.candiRegions[1] = append(si.candiRegions[1], region)
		} else if region.loads[0] > region.loads[1] {
			si.candiRegions[0] = append(si.candiRegions[0], region)
		} else {
			si.candiRegions[1] = append(si.candiRegions[1], region)
		}
	}
	sort.Sort(regionInfoByDiff(si.candiRegions[0]))
	sort.Sort(regionInfoByDiff(si.candiRegions[1]))
}

func (si *storeInfo) sortBy(dimID uint64) {
	for _, region := range si.regions {
		si.sortedRegions = append(si.sortedRegions, region)
	}
	if dimID == 0 {
		sort.Sort(regionInfoByLoad0(si.sortedRegions))
	} else {
		sort.Sort(regionInfoByLoad1(si.sortedRegions))
	}
}

func calcMaxMeanRatio(storeInfos []*storeInfo) (ratio1 float64, ratio2 float64) {
	var ratios []float64
	for dimID := uint64(0); dimID < DimensionCount; dimID++ {
		var maxLoad, avgLoad float64
		for _, store := range storeInfos {
			avgLoad += store.loads[dimID]
			maxLoad = math.Max(maxLoad, store.loads[dimID])
		}
		avgLoad /= float64(len(storeInfos))
		ratios = append(ratios, maxLoad/avgLoad)
	}
	ratio1, ratio2 = ratios[0], ratios[1]
	return
}

func loadBalanced(loads []float64, balanceRatio float64) bool {
	var maxLoad, avgLoad float64
	for _, load := range loads {
		avgLoad += load
		maxLoad = math.Max(maxLoad, load)
	}
	avgLoad /= float64(len(loads))
	ratio := maxLoad / avgLoad
	return ratio <= 1+2*balanceRatio
}

func greedySingle(storeInfos []*storeInfo, ratio float64, dimID uint64, candidateRegions map[uint64]*regionInfo) (ret []*regionInfo) {
	if dimID == 0 {
		sort.Sort(storeInfoByLoad0(storeInfos))
	} else {
		sort.Sort(storeInfoByLoad1(storeInfos))
	}

	for _, store := range storeInfos {
		log.Info("store load", zap.Float64("load", store.loads[dimID]))
		store.sortBy(dimID)
	}

	low := 0
	high := len(storeInfos) - 1
	for low < high {
		hStore := storeInfos[high]
		if hStore.loads[dimID] <= 1+ratio {
			break
		}

		for i := len(hStore.sortedRegions) - 1; i >= 0 && hStore.loads[dimID] > 1+ratio; i-- {
			curRegion := hStore.sortedRegions[i]
			if hStore.loads[dimID]-curRegion.loads[dimID] < 1-ratio {
				continue
			}
			if len(candidateRegions) != 0 {
				if _, exist := candidateRegions[curRegion.id]; !exist {
					continue
				}
			}

			for j := low; j < high; j++ {
				lStore := storeInfos[j]
				if lStore.loads[dimID]+curRegion.loads[dimID] <= 1+ratio {
					hStore.remove(curRegion)
					lStore.add(curRegion)
					curRegion.dstStoreID = lStore.id
					ret = append(ret, curRegion)
					break
				}
			}
		}

		high--
	}
	balanceRatio0, balanceRatio1 := calcMaxMeanRatio(storeInfos)
	log.Info("greedySingle: summary",
		zap.Int("migrationCost", len(ret)),
		zap.Float64("balanceRatio0", balanceRatio0),
		zap.Float64("balanceRatio1", balanceRatio1),
	)
	return
}

type storeLoadStat int

const (
	above storeLoadStat = iota
	cross
	under
)

func getStoreLoadStat(store *storeInfo) storeLoadStat {
	if store.loads[0] > 1.0 && store.loads[1] > 1.0 {
		return above
	} else if (store.loads[0]-1.0)*(store.loads[1]-1.0) < 0 {
		return cross
	} else {
		return under
	}
}

// pick region according to dimension with higher load
func pickRegion(store *storeInfo) (dimID uint64, region *regionInfo) {
	if store.loads[0] < store.loads[1] {
		dimID = 1
	}
	length := len(store.candiRegions[dimID])
	if length == 0 {
		log.Warn("can not choose region from pickRegion",
			zap.Uint64("storeID", store.id),
			zap.Int("candiRegionLen0", len(store.candiRegions[0])),
			zap.Int("candiRegionLen1", len(store.candiRegions[1])),
			zap.Float64("storeLoadRatio0", store.loads[0]),
			zap.Float64("storeLoadRatio1", store.loads[1]),
		)
		return
	}
	region = store.candiRegions[dimID][length-1]
	store.candiRegions[dimID] = store.candiRegions[dimID][:length-1]
	return
}

func higherLoadDimID(store *storeInfo) (higher uint64) {
	if store.loads[0] < store.loads[1] {
		higher = 1
	}
	return
}

func checkRegionSplit(region *regionInfo, ratio float64) bool {
	var splitDimID uint64
	if region.loads[0] < region.loads[1] {
		splitDimID = 1
	}
	region.splitRatio = ratio / region.loads[splitDimID]
	region.splitDimID = splitDimID

	if region.splitRatio > 0.5 && region.loads[splitDimID]*(1-region.splitRatio) < ratio/2 { // avoid splitting small regions
		region.splitRatio = 0.0
		return false
	}
	log.Info("split region",
		zap.Uint64("regionID", region.id),
		zap.Float64("load0", region.loads[0]),
		zap.Float64("load1", region.loads[1]),
		zap.Float64("splitRatio", region.splitRatio),
		zap.Uint64("splitDimID", splitDimID),
	)
	return true
}

func splitProcedure(storeInfos []*storeInfo, candidateRegions *regionContainer, ratio float64) []*regionInfo {
	splitRegions := make([]*regionInfo, 0)

	balanceRatio0, balanceRatio1 := calcMaxMeanRatio(storeInfos)
	log.Info("greedyBalance: before split",
		zap.Float64("balanceRatio0", balanceRatio0),
		zap.Float64("balanceRatio1", balanceRatio1),
	)
	for _, store := range storeInfos {
		log.Info("store load before scheduling", zap.Uint64("storeID", store.id), zap.Float64("dim0", store.loads[0]), zap.Float64("dim1", store.loads[1]))
	}

	// mark regions that can be removed so that the difference of two dimensions' loads does not exceed `ratio`
	// identify regions that need to be split
	for _, store := range storeInfos {
		store.classifyRegion()
		pickCount := 0
		for math.Abs(store.loads[0]-store.loads[1]) > ratio || getStoreLoadStat(store) == above {
			preHigherDimID := higherLoadDimID(store)
			dimID, region := pickRegion(store)
			if region == nil { // statistics error or no hotspots were recognized
				break
			}

			// check whether the region needs to be split
			if checkRegionSplit(region, ratio) {
				splitRegions = append(splitRegions, region)
			}

			store.remove(region)
			curHigherDimID := higherLoadDimID(store)
			candidateRegions.push(dimID, region)
			pickCount++

			if getStoreLoadStat(store) != under {
				continue
			} else if math.Abs(store.loads[0]-store.loads[1]) <= ratio ||
				preHigherDimID != curHigherDimID {
				break
			}
		}

		log.Info("split procedure",
			zap.Uint64("storeID", store.id),
			zap.Int("# of picked regions", pickCount),
		)
	}

	return splitRegions
}

func migrationProcedure(storeInfos []*storeInfo, candidateRegions *regionContainer, ratio float64) []*regionInfo {
	highLoadRegions := make(map[uint64]*regionInfo)
	// step 1: fill back the regions that can not be split
	for _, store := range storeInfos {
		for dimID := uint64(0); dimID < DimensionCount; dimID++ {
			splitFailedRegions := candidateRegions.getSplitFailedRegions(dimID, store.id, highLoadRegions, ratio)
			for _, region := range splitFailedRegions {
				store.add(region)
				log.Info("region can not be split",
					zap.Uint64("storeID", store.id),
					zap.Uint64("regionID", region.id),
					zap.Float64("load0", region.loads[0]),
					zap.Float64("load1", region.loads[1]),
					zap.Float64("splitRatio", region.splitRatio),
					zap.Uint64("splitDimID", region.splitDimID),
				)
			}
		}
	}

	// step 2: carefully fill candidate regions into stores to ensure that each store does not overflow too much
	sort.Sort(storeInfoByLoad0(storeInfos))
	for _, store := range storeInfos {
		log.Info("store load start step 2", zap.Uint64("storeID", store.id), zap.Float64("dim0", store.loads[0]), zap.Float64("dim1", store.loads[1]))
		for getStoreLoadStat(store) != above {
			var dimID uint64
			if store.loads[0] > store.loads[1] {
				dimID = 1
			}

			if !candidateRegions.empty(dimID) {
				region := candidateRegions.pop(dimID, store.id)
				if region != nil {
					log.Info("step 2 add region", zap.Uint64("storeID", store.id), zap.Uint64("whichDim", dimID), zap.Float64("dim0", store.loads[0]), zap.Float64("dim1", store.loads[1]), zap.String("region", fmt.Sprintf("%+v", region)))
					store.add(region)
				}
			} else {
				break
			}
		}
		log.Info("store load end step 2", zap.Uint64("storeID", store.id), zap.Float64("dim0", store.loads[0]), zap.Float64("dim1", store.loads[1]))
	}

	// step 3: fill remaining regions
	for dimID := uint64(0); dimID < DimensionCount; dimID++ {
		if dimID == 0 {
			sort.Sort(storeInfoByLoad0(storeInfos))
		} else {
			sort.Sort(storeInfoByLoad1(storeInfos))
		}
		for _, store := range storeInfos {
			log.Info("store load start step 3", zap.Uint64("storeID", store.id), zap.Uint64("wichDim", dimID), zap.Float64("dim0", store.loads[0]), zap.Float64("dim1", store.loads[1]))
			for store.loads[dimID] < 1 && !candidateRegions.empty(dimID) {
				region := candidateRegions.pop(dimID, store.id)
				if region != nil {
					log.Info("step 3 add region", zap.Uint64("storeID", store.id), zap.Uint64("whichDim", dimID), zap.Float64("dim0", store.loads[0]), zap.Float64("dim1", store.loads[1]), zap.String("region", fmt.Sprintf("%+v", region)))
					store.add(region)
				}
			}
			log.Info("store load end step 3", zap.Uint64("storeID", store.id), zap.Uint64("wichDim", dimID), zap.Float64("dim0", store.loads[0]), zap.Float64("dim1", store.loads[1]))
		}
	}

	for _, store := range storeInfos {
		log.Info("store load after migration procedure", zap.Uint64("storeID", store.id), zap.Float64("dim0", store.loads[0]), zap.Float64("dim1", store.loads[1]))
	}

	ret := candidateRegions.getMigratedRegions()
	balanceRatio0, balanceRatio1 := calcMaxMeanRatio(storeInfos)
	log.Info("greedyBalance: summary",
		zap.Int("migrationCost", len(ret)),
		zap.Float64("balanceRatio0", balanceRatio0),
		zap.Float64("balanceRatio1", balanceRatio1),
	)

	if balanceRatio0 > 1+2*ratio && balanceRatio1 <= 1+2*ratio && len(highLoadRegions) > 0 {
		extra := greedySingle(storeInfos, ratio*2, 0, highLoadRegions)
		ret = append(ret, extra...)

		balanceRatio0, balanceRatio1 := calcMaxMeanRatio(storeInfos)
		log.Info("greedyBalance: adjust",
			zap.Int("migrationCost", len(ret)),
			zap.Float64("balanceRatio0", balanceRatio0),
			zap.Float64("balanceRatio1", balanceRatio1),
		)
	}

	return ret
}
