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
}

func (infl Influence) add(rhs *Influence, w float64) Influence {
	infl.ByteRate += rhs.ByteRate * w
	infl.KeyRate += rhs.KeyRate * w
	infl.Count += rhs.Count * w
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

	// Exp means Expectation, it is calculated from the average value from summary of byte/key rate, count.
	ExpByteRate float64
	ExpKeyRate  float64
	ExpCount    float64
}

func (load *storeLoad) ToLoadPred(infl Influence) *storeLoadPred {
	future := *load
	future.ByteRate += infl.ByteRate
	future.KeyRate += infl.KeyRate
	future.Count += infl.Count
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
	}
}

func maxLoad(a, b *storeLoad) *storeLoad {
	return &storeLoad{
		ByteRate: math.Max(a.ByteRate, b.ByteRate),
		KeyRate:  math.Max(a.KeyRate, b.KeyRate),
		Count:    math.Max(a.Count, b.Count),
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
		Count:          len(li.HotPeers),
		Stats:          peers,
	}
}

// DimensionCount defines the number of dimensions
const DimensionCount = 2

type regionInfo struct {
	id         uint64
	srcStoreID uint64
	dstStoreID uint64
	loads      [DimensionCount]float64
	diffLoad   float64
	splitRatio float64
	splitDimID uint64
}

func newRegionInfo(id uint64, srcStoreID uint64, load1 float64, load2 float64) *regionInfo {
	return &regionInfo{
		id:         id,
		srcStoreID: srcStoreID,
		loads:      [DimensionCount]float64{load1, load2},
		diffLoad:   math.Abs(load1 - load2),
	}
}

type regionInfoByDiff []*regionInfo

func (s regionInfoByDiff) Len() int           { return len(s) }
func (s regionInfoByDiff) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s regionInfoByDiff) Less(i, j int) bool { return s[i].diffLoad < s[j].diffLoad }

type regionContainer struct {
	emphRegions     [DimensionCount]map[uint64][]*regionInfo
	migratedRegions map[uint64]*regionInfo
	count           [DimensionCount]uint64
}

func newRegionContainer() *regionContainer {
	ret := &regionContainer{
		migratedRegions: make(map[uint64]*regionInfo, 0),
	}
	for i := range ret.emphRegions {
		ret.emphRegions[i] = make(map[uint64][]*regionInfo, 0)
	}
	return ret
}

func (rc *regionContainer) push(dimID uint64, region *regionInfo) {
	if _, ok := rc.emphRegions[dimID][region.srcStoreID]; !ok {
		rc.emphRegions[dimID][region.srcStoreID] = make([]*regionInfo, 0)
	}
	rc.emphRegions[dimID][region.srcStoreID] = append(rc.emphRegions[dimID][region.srcStoreID], region)

	rc.migratedRegions[region.id] = region
	rc.count[dimID]++
}

func (rc *regionContainer) pop(dimID uint64, candidateStoreID uint64) *regionInfo {
	sid := candidateStoreID
	if _, ok := rc.emphRegions[dimID][sid]; !ok { // should pick region from remote node
		for sid1 := range rc.emphRegions[dimID] {
			sid = sid1
			break
		}
	}

	var region *regionInfo
	{
		length := len(rc.emphRegions[dimID][sid])
		region = rc.emphRegions[dimID][sid][length-1]
		region.dstStoreID = candidateStoreID
		rc.emphRegions[dimID][sid] = rc.emphRegions[dimID][sid][:length-1]
	}

	if len(rc.emphRegions[dimID][sid]) == 0 {
		delete(rc.emphRegions[dimID], sid)
	}
	rc.count[dimID]--

	return region
}

func (rc *regionContainer) getMigratedRegions() []*regionInfo {
	ret := make([]*regionInfo, 0)
	for _, region := range rc.migratedRegions {
		if region.srcStoreID != region.dstStoreID {
			ret = append(ret, region)
		}
	}
	return ret
}

func (rc *regionContainer) empty(dimID uint64) bool {
	return rc.count[dimID] == 0
}

type storeInfo struct {
	id          uint64
	loads       []float64
	regions     map[uint64]*regionInfo
	emphRegions [][]*regionInfo
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
		id:          id,
		loads:       loads,
		regions:     regions,
		emphRegions: make([][]*regionInfo, DimensionCount),
	}
	for i := range ret.emphRegions {
		ret.emphRegions[i] = make([]*regionInfo, 0)
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
		if region.loads[0] >= region.loads[1] {
			si.emphRegions[0] = append(si.emphRegions[0], region)
		} else {
			si.emphRegions[1] = append(si.emphRegions[1], region)
		}
	}
	sort.Sort(regionInfoByDiff(si.emphRegions[0]))
	sort.Sort(regionInfoByDiff(si.emphRegions[1]))
}

func calcMaxMeanRatio(storeInfos []*storeInfo) float64 {
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
	return math.Max(ratios[0], ratios[1])
}

func greedyBalance(storeInfos []*storeInfo, ratio float64) (ret []*regionInfo, needSplit bool) {
	type balanceType int

	const (
		above balanceType = iota
		cross
		under
	)

	storeBalanceType := func(store *storeInfo) balanceType {
		if store.loads[0] > 1.0 && store.loads[1] > 1.0 {
			return above
		} else if (store.loads[0]-1.0)*(store.loads[1]-1.0) < 0 {
			return cross
		} else {
			return under
		}
	}

	pickHigher := func(store *storeInfo) (dimID uint64, region *regionInfo) {
		if store.loads[0] < store.loads[1] {
			dimID = 1
		}
		{
			length := len(store.emphRegions[dimID])
			if length == 0 {
				log.Warn("can not choose region from pickHigher",
					zap.Uint64("storeID", store.id),
					zap.Int("emphRegionLen0", len(store.emphRegions[0])),
					zap.Int("emphRegionLen1", len(store.emphRegions[1])),
					zap.Float64("storeLoadRatio0", store.loads[0]),
					zap.Float64("storeLoadRatio1", store.loads[1]),
				)
				return
			}
			region = store.emphRegions[dimID][length-1]
			store.emphRegions[dimID] = store.emphRegions[dimID][:length-1]
		}
		return
	}

	checkOrder := func(store *storeInfo) (higher uint64, lower uint64) {
		if store.loads[0] < store.loads[1] {
			higher = 1
		}
		lower = 1 - higher
		return
	}

	checkRegionSplit := func(region *regionInfo) bool {
		var splitDimID uint64
		if region.loads[0] < region.loads[1] {
			splitDimID = 1
		}
		region.splitRatio = ratio / region.loads[splitDimID]
		region.splitDimID = splitDimID

		if region.splitRatio > 0.9 && region.loads[splitDimID]*(1-region.splitRatio) < ratio/2 { // avoid split small region
			// log.Warn("greedyBalance: unpredictable region split ratio",
			// 	zap.Uint64("regionID", region.id),
			// )
			region.splitRatio = 0.0
			return false
		}
		log.Info("greedyBalance: split region info",
			zap.Uint64("regionID", region.id),
			zap.Float64("load0", region.loads[0]),
			zap.Float64("load1", region.loads[1]),
			zap.Float64("splitRatio", region.splitRatio),
			zap.Uint64("splitDimID", splitDimID),
		)
		return true
	}

	container := newRegionContainer()
	splitRegions := make([]*regionInfo, 0)

	// step 1: remove regions to make the difference of two dimensions' loads not over `ratio`,
	//         identify regions that need to be split
	for _, store := range storeInfos {
		store.classifyRegion()
		pickCount := 0
		for math.Abs(store.loads[0]-store.loads[1]) > ratio || storeBalanceType(store) == above {
			preHigher, _ := checkOrder(store)
			dimID, region := pickHigher(store)
			if region == nil { // statistics error or no hotspots were recognized
				// return nil, false
				break
			}

			// check region whether needs to be split
			if checkRegionSplit(region) {
				splitRegions = append(splitRegions, region)
			}

			store.remove(region)
			curHigher, _ := checkOrder(store)
			container.push(dimID, region)
			pickCount++

			if storeBalanceType(store) != under {
				continue
			} else if math.Abs(store.loads[0]-store.loads[1]) <= ratio ||
				preHigher != curHigher {
				break
			}
		}

		log.Info("greedyBalance: step 1",
			zap.Int("# of picked regions", pickCount),
			zap.Uint64("storeID", store.id),
		)
	}

	if len(splitRegions) != 0 {
		ret = splitRegions
		needSplit = true
		return
	}

	log.Info("greedyBalance: step2")
	// step 2: carefully fill regions into stores, make sure each store will not overflow too much
	sort.Sort(storeInfoByLoad0(storeInfos))
	for _, store := range storeInfos {
		for storeBalanceType(store) != above {
			var dimID uint64
			if store.loads[0] > store.loads[1] {
				dimID = 1
			}

			if !container.empty(dimID) {
				region := container.pop(dimID, store.id)
				store.add(region)
			} else {
				break
			}
		}
	}
	log.Info("greedyBalance: step3")
	// step 3: fill remaining regions
	for dimID := uint64(0); dimID < DimensionCount; dimID++ {
		if dimID == 0 {
			sort.Sort(storeInfoByLoad0(storeInfos))
		} else {
			sort.Sort(storeInfoByLoad1(storeInfos))
		}
		for _, store := range storeInfos {
			for store.loads[dimID] < 1 && !container.empty(dimID) {
				region := container.pop(dimID, store.id)
				store.add(region)
			}
		}
	}

	for _, store := range storeInfos {
		log.Info("store load", zap.Uint64("storeID", store.id), zap.Float64("dim0", store.loads[0]), zap.Float64("dim1", store.loads[1]))
	}

	ret = container.getMigratedRegions()
	log.Info("greedyBalance: summary",
		zap.Int("migrationCost", len(ret)),
		zap.Float64("balanceRatio", calcMaxMeanRatio(storeInfos)),
	)
	return
}
