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

// loadInfluence records operator influence.
type loadInfluence struct {
	loads [DimensionCount]float64
}

func (infl loadInfluence) add(rhs *loadInfluence, w float64) loadInfluence {
	for i := range infl.loads {
		infl.loads[i] += rhs.loads[i] * w
	}
	return infl
}

// TODO: merge it into OperatorInfluence.
type pendingLoadInfluence struct {
	op       *operator.Operator
	from, to uint64
	origin   loadInfluence
}

func newPendingLoadInfluence(op *operator.Operator, from, to uint64, infl loadInfluence) *pendingLoadInfluence {
	return &pendingLoadInfluence{
		op:     op,
		from:   from,
		to:     to,
		origin: infl,
	}
}

// summaryPendingLoadInfluence calculate the summary pending loadInfluence for each store and return storeID -> loadInfluence
// It makes each key/byte rate or count become (1+w) times to the origin value while f is the function to provide w(weight)
func summaryPendingLoadInfluence(pendings map[*pendingLoadInfluence]struct{}, f func(*operator.Operator) float64) map[uint64]loadInfluence {
	ret := map[uint64]loadInfluence{}
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

type dimType int

const (
	readBytesDim dimType = iota
	readKeysDim
	readOpsDim
	writeBytesLeaderDim
	writeKeysLeaderDim
	writeOpsLeaderDim
	writeBytesPeerDim
	writeKeysPeerDim
	writeOpsPeerDim

	// DimensionCount defines the number of dimensions
	DimensionCount
)

func dimNeedSched(dimID dimType) bool {
	switch dimID {
	//jk :case readBytesDim, readKeysDim, writeKeysLeaderDim, writeKeysPeerDim, writeBytesPeerDim:
	case readBytesDim, readKeysDim, writeKeysLeaderDim, writeBytesPeerDim:
		return true
	default:
		return false
	}
}

func dimForLeader(dimID dimType) bool {
	if dimID < writeBytesPeerDim {
		return true
	}
	return false
}

func dimForRead(dimID dimType) bool {
	if dimID < writeBytesLeaderDim {
		return true
	}
	return false
}

func dimForBytesDim(dimID dimType) bool {
	switch dimID {
	case readBytesDim, writeBytesLeaderDim, writeBytesPeerDim:
		return true
	default:
		return false
	}
}

func dimForKeysDim(dimID dimType) bool {
	switch dimID {
	case readKeysDim, writeKeysLeaderDim, writeKeysPeerDim:
		return true
	default:
		return false
	}
}

type peerInfo struct {
	regionID   uint64
	srcStoreID uint64
	dstStoreID uint64
	loads      [DimensionCount]float64
	peerStat   *statistics.HotPeerStat
	isLeader   bool

	//jk:
	keyload  float64
	byteload float64
	combload float64
}

func newPeerInfo(regionID uint64, srcStoreID uint64, peerStat *statistics.HotPeerStat, isLeader bool) *peerInfo {
	return &peerInfo{
		regionID:   regionID,
		srcStoreID: srcStoreID,
		dstStoreID: srcStoreID,
		peerStat:   peerStat,
		isLeader:   isLeader,
	}
}

type storeInfo struct {
	id    uint64
	loads [DimensionCount]float64
	peers map[uint64]*peerInfo

	//jk:
	keyload    float64
	byteload   float64
	combload   float64
	loadsratio [DimensionCount]float64
}

func newStoreInfo(id uint64, peers map[uint64]*peerInfo) *storeInfo {
	ret := &storeInfo{
		id:    id,
		peers: peers,
	}
	return ret
}

func (store *storeInfo) getMaxLoadInfo(allowedDimensions []uint64) (id uint64, load float64) {
	for _, dimID := range allowedDimensions {
		if store.loads[dimID] > load {
			load = store.loads[dimID]
			id = dimID
		}
	}
	return
}

func loadToStr(loads [DimensionCount]float64, allowedDimensions []uint64) string {
	ret := ""
	for _, i := range allowedDimensions {
		ret += fmt.Sprintf("%v:%v,", i, loads[i])
	}
	return ret
}

func migratePeer(srcStore, dstStore *storeInfo, srcPeer *peerInfo, opTy opType) {
	if opTy == transferLeader {
		if _, ok := dstStore.peers[srcPeer.regionID]; !ok {
			log.Info("try to transfer to null peer",
				zap.Uint64("srcStoreID", srcStore.id),
				zap.Uint64("dstStoreID", dstStore.id),
				zap.Uint64("regionID", srcPeer.regionID),
			)
			return
		}
		dstPeer := dstStore.peers[srcPeer.regionID]

		if _, ok := srcStore.peers[srcPeer.regionID]; ok {
			for i := range srcPeer.loads {
				if !dimForLeader(dimType(i)) { // skip transfer leader to write dimension
					continue
				}
				srcStore.loads[i] -= srcPeer.loads[i]
				//jk
				srcStore.combload -= (srcPeer.loads[i] * srcStore.loadsratio[i])

			}
			srcStore.peers[srcPeer.regionID] = dstPeer
		}
		if _, ok := dstStore.peers[srcPeer.regionID]; ok {
			for i := range srcPeer.loads {
				if !dimForLeader(dimType(i)) { // skip transfer leader to write dimension
					continue
				}
				dstStore.loads[i] += srcPeer.loads[i]
				//jk
				dstStore.combload += (srcPeer.loads[i] * dstStore.loadsratio[i])

			}
			dstStore.peers[srcPeer.regionID] = srcPeer
		}
	} else {
		if _, ok := srcStore.peers[srcPeer.regionID]; ok {
			for i := range srcPeer.loads {
				srcStore.loads[i] -= srcPeer.loads[i]
			}
			//jk
			srcStore.combload -= srcPeer.combload
			//srcStore.keyload -= srcPeer.keyload
			//srcStore.byteload -= srcPeer.byteload

			delete(srcStore.peers, srcPeer.regionID)
		}
		if _, ok := dstStore.peers[srcPeer.regionID]; !ok {
			dstStore.peers[srcPeer.regionID] = srcPeer
			for i := range srcPeer.loads {
				dstStore.loads[i] += srcPeer.loads[i]
			}
			//jk
			//dstStore.keyload += srcPeer.keyload
			//dstStore.byteload += srcPeer.byteload
			dstStore.combload += srcPeer.combload
		}
	}

}

func normalizeStoreLoads(sis []*storeInfo) {
	storeLen := len(sis)
	if storeLen == 0 {
		return
	}
	expLoads := make([]float64, len(sis[0].loads))
	for _, si := range sis {
		for i, load := range si.loads {
			expLoads[i] += load
		}
	}
	for i := range expLoads {
		expLoads[i] /= float64(storeLen)
	}

	for _, si := range sis {
		for i := range si.loads {
			if expLoads[i] > 0 {
				si.loads[i] /= expLoads[i]
			}
		}
		for _, peer := range si.peers {
			for i := range peer.loads {
				if expLoads[i] > 0 {
					peer.loads[i] /= expLoads[i]
				}
			}
		}
	}
}

func IsContainInt(items []uint64, item int) bool {
	for _, eachItem := range items {
		if eachItem == uint64(item) {
			return true
		}
	}
	return false
}

//jk
func normalizeStorecombLoads(sis []*storeInfo, b *multiBalancer) {
	storeLen := len(sis)
	if storeLen == 0 {
		return
	}

	expLoads := make([]float64, len(sis[0].loads))
	keyexpLoads := 0.0
	byteexpLoads := 0.0

	for _, si := range sis {
		si.byteload = 0.0
		si.keyload = 0.0
		for i, load := range si.loads {
			expLoads[i] += load

			if dimForBytesDim(dimType(i)) && IsContainInt(b.allowedDimensions, i) {
				byteexpLoads += load
				si.byteload += load
			}
			if dimForKeysDim(dimType(i)) && IsContainInt(b.allowedDimensions, i) {
				si.keyload += load
				keyexpLoads += load
			}
		}
	}
	for _, si := range sis {
		for i := range expLoads {
			si.loadsratio[i] = 0
			if dimForBytesDim(dimType(i)) && byteexpLoads != 0 {
				si.loadsratio[i] = expLoads[i] / byteexpLoads
			}
			if dimForKeysDim(dimType(i)) && keyexpLoads != 0 {
				si.loadsratio[i] = expLoads[i] / keyexpLoads
			}
		}
	}

	for i := range expLoads {
		expLoads[i] /= float64(storeLen)
	}
	byteexpLoads /= float64(storeLen)
	keyexpLoads /= float64(storeLen)

	for _, si := range sis {
		for i := range si.loads {
			if expLoads[i] > 0 {
				si.loads[i] /= expLoads[i]
			}
		}

		si.keyload /= keyexpLoads
		si.byteload /= byteexpLoads
		// jk
		//si.combload = 0.5*si.keyload + 0.5*si.byteload

		//load ratio will be referenced so update according to combload ratio
		//for _, si := range sis {

		for i := range si.loadsratio {
			if dimForKeysDim(dimType(i)) {
				si.loadsratio[i] *= 0
			}
			if dimForBytesDim(dimType(i)) {
				si.loadsratio[i] *= 1
			}

		}
		//}

		// jk
		si.combload = si.byteload
		//si.combload = si.keyload

		for _, peer := range si.peers {
			peer.byteload = 0.0
			peer.keyload = 0.0

			for i := range peer.loads {
				if dimForBytesDim(dimType(i)) && IsContainInt(b.allowedDimensions, i) {
					peer.byteload += peer.loads[i]
				}
				if dimForKeysDim(dimType(i)) && IsContainInt(b.allowedDimensions, i) {
					peer.keyload += peer.loads[i]
				}

				if expLoads[i] > 0 {
					peer.loads[i] /= expLoads[i]
				}
			}
			peer.keyload /= keyexpLoads
			peer.byteload /= byteexpLoads
			//peer.combload = 0.5*peer.keyload + 0.5*peer.byteload
			peer.combload = peer.byteload
			//peer.combload = peer.keyload
		}
	}

}

type sortedPeerInfos struct {
	sortedPeers []*peerInfo
	remainLoads float64
	dimID       uint64
}

func buildSortedPeers(s *storeInfo, dimID uint64) *sortedPeerInfos {
	sortedPeers := make([]*peerInfo, 0)
	remainLoads := 0.0
	for id, r := range s.peers {
		sortedPeers = append(sortedPeers, r)
		if r == nil {
			log.Info(
				"buildSortedPeers find nil peer",
				zap.Uint64("rid", id),
			)
		}
		remainLoads += r.loads[dimID]
	}
	sort.Slice(sortedPeers, func(i, j int) bool {
		return sortedPeers[i].loads[dimID] < sortedPeers[j].loads[dimID]
	})

	return &sortedPeerInfos{
		sortedPeers: sortedPeers,
		remainLoads: remainLoads,
		dimID:       dimID,
	}
}

//jk
func buildcombSortedPeers(s *storeInfo) *sortedPeerInfos {
	sortedPeers := make([]*peerInfo, 0)
	remainLoads := 0.0
	for id, r := range s.peers {
		sortedPeers = append(sortedPeers, r)
		if r == nil {
			log.Info(
				"buildSortedPeers find nil peer",
				zap.Uint64("rid", id),
			)
		}
		remainLoads += r.combload
	}
	sort.Slice(sortedPeers, func(i, j int) bool {
		return sortedPeers[i].combload < sortedPeers[j].combload
	})

	return &sortedPeerInfos{
		sortedPeers: sortedPeers,
		remainLoads: remainLoads,
		//dimID:       dimID,
	}
}

func (s *sortedPeerInfos) pop() *peerInfo {
	length := len(s.sortedPeers)
	if length == 0 {
		return nil
	}

	region := s.sortedPeers[length-1]
	s.sortedPeers = s.sortedPeers[:length-1]
	s.remainLoads -= region.loads[s.dimID]
	return region
}

//jk
func (s *sortedPeerInfos) combpop() *peerInfo {
	length := len(s.sortedPeers)
	if length == 0 {
		return nil
	}

	region := s.sortedPeers[length-1]
	s.sortedPeers = s.sortedPeers[:length-1]
	s.remainLoads -= region.combload
	return region
}

func calcBalanceRatio(storeInfos []*storeInfo, allowedDimensions []uint64) float64 {
	var maxRatio float64
	for _, dimID := range allowedDimensions {
		var maxLoad, avgLoad float64
		for _, store := range storeInfos {
			avgLoad += store.loads[dimID]
			maxLoad = math.Max(maxLoad, store.loads[dimID])
		}
		avgLoad /= float64(len(storeInfos))
		maxRatio = math.Max(maxRatio, maxLoad/avgLoad)
	}
	return maxRatio
}

//jk

func calccombBalanceRatio(storeInfos []*storeInfo, allowedDimensions []uint64) float64 {
	var maxRatio float64

	var maxLoad, avgLoad float64
	for _, store := range storeInfos {
		avgLoad += store.combload
		maxLoad = math.Max(maxLoad, store.combload)
	}
	avgLoad /= float64(len(storeInfos))
	maxRatio = math.Max(maxRatio, maxLoad/avgLoad)

	return maxRatio
}
