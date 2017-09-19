package schedulers

import (
	"math"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("hotReadRegion", func(opt schedule.Options, args []string) (schedule.Scheduler, error) {
		return newBalanceHotReadRegionsScheduler(opt), nil
	})
}

type balanceHotReadRegionsScheduler struct {
	sync.RWMutex
	opt   schedule.Options
	limit uint64

	// store id -> hot regions statistics as the role of leader
	statisticsAsLeader map[uint64]*core.HotRegionsStat
	r                  *rand.Rand
}

func newBalanceHotReadRegionsScheduler(opt schedule.Options) *balanceHotReadRegionsScheduler {
	return &balanceHotReadRegionsScheduler{
		opt:                opt,
		limit:              1,
		statisticsAsLeader: make(map[uint64]*core.HotRegionsStat),
		r:                  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (h *balanceHotReadRegionsScheduler) GetName() string {
	return "balance-hot-read-region-scheduler"
}

func (h *balanceHotReadRegionsScheduler) GetInterval() time.Duration {
	return schedule.MinSlowScheduleInterval
}

func (h *balanceHotReadRegionsScheduler) GetResourceKind() core.ResourceKind {
	return core.PriorityKind
}

func (h *balanceHotReadRegionsScheduler) GetResourceLimit() uint64 {
	return h.limit
}

func (h *balanceHotReadRegionsScheduler) Prepare(cluster schedule.Cluster) error { return nil }

func (h *balanceHotReadRegionsScheduler) Cleanup(cluster schedule.Cluster) {}

func (h *balanceHotReadRegionsScheduler) Schedule(cluster schedule.Cluster) *schedule.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	h.calcScore(cluster)

	// balance by leader
	srcRegion, newLeader := h.balanceByLeader(cluster)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_leader").Inc()
		step := schedule.TransferLeader{FromStore: srcRegion.Leader.GetStoreId(), ToStore: newLeader.GetStoreId()}
		return schedule.NewOperator("transferHotReadLeader", srcRegion.GetId(), core.PriorityKind, step)
	}

	// balance by peer
	srcRegion, srcPeer, destPeer := h.balanceByPeer(cluster)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_peer").Inc()
		return schedule.CreateMovePeerOperator("moveHotReadRegion", srcRegion, core.PriorityKind, srcPeer.GetStoreId(), destPeer.GetStoreId(), destPeer.GetId())
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func (h *balanceHotReadRegionsScheduler) calcScore(cluster schedule.Cluster) {
	h.Lock()
	defer h.Unlock()

	h.statisticsAsLeader = make(map[uint64]*core.HotRegionsStat)
	items := cluster.RegionReadStats()
	for _, r := range items {
		if r.HotDegree < h.opt.GetHotRegionLowThreshold() {
			continue
		}

		regionInfo := cluster.GetRegion(r.RegionID)
		leaderStoreID := regionInfo.Leader.GetStoreId()
		leaderStat, ok := h.statisticsAsLeader[leaderStoreID]
		if !ok {
			leaderStat = &core.HotRegionsStat{
				RegionsStat: make(core.RegionsStat, 0, storeHotRegionsDefaultLen),
			}
			h.statisticsAsLeader[leaderStoreID] = leaderStat
		}

		stat := core.RegionStat{
			RegionID:       r.RegionID,
			FlowBytes:      r.FlowBytes,
			HotDegree:      r.HotDegree,
			LastUpdateTime: r.LastUpdateTime,
			StoreID:        leaderStoreID,
			AntiCount:      r.AntiCount,
			Version:        r.Version,
		}
		leaderStat.TotalFlowBytes += r.FlowBytes
		leaderStat.RegionsCount++
		leaderStat.RegionsStat = append(leaderStat.RegionsStat, stat)
	}
}

func (h *balanceHotReadRegionsScheduler) balanceByPeer(cluster schedule.Cluster) (*core.RegionInfo, *metapb.Peer, *metapb.Peer) {
	var (
		maxReadBytes           uint64
		srcStoreID             uint64
		maxHotStoreRegionCount int
	)

	// get the srcStoreId
	for storeID, statistics := range h.statisticsAsLeader {
		count, readBytes := statistics.RegionsStat.Len(), statistics.TotalFlowBytes
		if count >= 2 && (count > maxHotStoreRegionCount || (count == maxHotStoreRegionCount && readBytes > maxReadBytes)) {
			maxHotStoreRegionCount = count
			maxReadBytes = readBytes
			srcStoreID = storeID
		}
	}
	if srcStoreID == 0 {
		return nil, nil, nil
	}

	stores := cluster.GetStores()
	var destStoreID uint64
	for _, i := range h.r.Perm(h.statisticsAsLeader[srcStoreID].RegionsStat.Len()) {
		rs := h.statisticsAsLeader[srcStoreID].RegionsStat[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		filters := []schedule.Filter{
			schedule.NewExcludedFilter(srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			schedule.NewDistinctScoreFilter(h.opt.GetLocationLabels(), stores, cluster.GetLeaderStore(srcRegion)),
			schedule.NewStateFilter(h.opt),
			schedule.NewStorageThresholdFilter(h.opt),
		}
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if schedule.FilterTarget(store, filters) {
				continue
			}
			destStoreIDs = append(destStoreIDs, store.GetId())
		}

		destStoreID = h.selectDestStoreByPeer(destStoreIDs, srcRegion, srcStoreID)
		if destStoreID != 0 {
			srcRegion.ReadBytes = rs.FlowBytes
			h.adjustBalanceLimit(srcStoreID, byLeader)

			var srcPeer *metapb.Peer
			for _, peer := range srcRegion.GetPeers() {
				if peer.GetStoreId() == srcStoreID {
					srcPeer = peer
					break
				}
			}

			if srcPeer == nil {
				return nil, nil, nil
			}

			destPeer, err := cluster.AllocPeer(destStoreID)
			if err != nil {
				log.Errorf("failed to allocate peer: %v", err)
				return nil, nil, nil
			}

			return srcRegion, srcPeer, destPeer
		}
	}

	return nil, nil, nil
}

func (h *balanceHotReadRegionsScheduler) selectDestStoreByPeer(candidateStoreIDs []uint64, srcRegion *core.RegionInfo, srcStoreID uint64) uint64 {
	sr := h.statisticsAsLeader[srcStoreID]
	srcReadBytes := sr.TotalFlowBytes
	srcHotRegionsCount := sr.RegionsStat.Len()

	var (
		destStoreID  uint64
		minReadBytes uint64 = math.MaxUint64
	)
	minRegionsCount := int(math.MaxInt32)
	for _, storeID := range candidateStoreIDs {
		if s, ok := h.statisticsAsLeader[storeID]; ok {
			if srcHotRegionsCount-s.RegionsStat.Len() > 1 && minRegionsCount > s.RegionsStat.Len() {
				destStoreID = storeID
				minReadBytes = s.TotalFlowBytes
				minRegionsCount = s.RegionsStat.Len()
				continue
			}
			if minRegionsCount == s.RegionsStat.Len() && minReadBytes > s.TotalFlowBytes &&
				uint64(float64(srcReadBytes)*hotRegionScheduleFactor) > s.TotalFlowBytes+2*srcRegion.ReadBytes {
				minReadBytes = s.TotalFlowBytes
				destStoreID = storeID
			}
		} else {
			destStoreID = storeID
			break
		}
	}
	return destStoreID
}

func (h *balanceHotReadRegionsScheduler) adjustBalanceLimit(storeID uint64, t BalanceType) {
	srcStatistics := h.statisticsAsLeader[storeID]
	allStatistics := h.statisticsAsLeader

	var hotRegionTotalCount float64
	for _, m := range allStatistics {
		hotRegionTotalCount += float64(m.RegionsStat.Len())
	}

	avgRegionCount := hotRegionTotalCount / float64(len(allStatistics))
	// Multiplied by hotRegionLimitFactor to avoid transfer back and forth
	limit := uint64((float64(srcStatistics.RegionsStat.Len()) - avgRegionCount) * hotRegionLimitFactor)
	h.limit = maxUint64(1, limit)
}

func (h *balanceHotReadRegionsScheduler) balanceByLeader(cluster schedule.Cluster) (*core.RegionInfo, *metapb.Peer) {
	var (
		maxReadBytes           uint64
		srcStoreID             uint64
		maxHotStoreRegionCount int
	)

	// select srcStoreId by leader
	for storeID, statistics := range h.statisticsAsLeader {
		if statistics.RegionsStat.Len() < 2 {
			continue
		}

		if maxHotStoreRegionCount < statistics.RegionsStat.Len() {
			maxHotStoreRegionCount = statistics.RegionsStat.Len()
			maxReadBytes = statistics.TotalFlowBytes
			srcStoreID = storeID
			continue
		}

		if maxHotStoreRegionCount == statistics.RegionsStat.Len() && maxReadBytes < statistics.TotalFlowBytes {
			maxReadBytes = statistics.TotalFlowBytes
			srcStoreID = storeID
		}
	}
	if srcStoreID == 0 {
		return nil, nil
	}

	// select destPeer
	for _, i := range h.r.Perm(h.statisticsAsLeader[srcStoreID].RegionsStat.Len()) {
		rs := h.statisticsAsLeader[srcStoreID].RegionsStat[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		destPeer := h.selectDestStoreByLeader(srcRegion)
		if destPeer != nil {
			h.adjustBalanceLimit(srcStoreID, byLeader)
			return srcRegion, destPeer
		}
	}
	return nil, nil
}

func (h *balanceHotReadRegionsScheduler) selectDestStoreByLeader(srcRegion *core.RegionInfo) *metapb.Peer {
	sr := h.statisticsAsLeader[srcRegion.Leader.GetStoreId()]
	srcReadBytes := sr.TotalFlowBytes
	srcHotRegionsCount := sr.RegionsStat.Len()

	var (
		destPeer     *metapb.Peer
		minReadBytes uint64 = math.MaxUint64
	)
	minRegionsCount := int(math.MaxInt32)
	for storeID, peer := range srcRegion.GetFollowers() {
		if s, ok := h.statisticsAsLeader[storeID]; ok {
			if srcHotRegionsCount-s.RegionsStat.Len() > 1 && minRegionsCount > s.RegionsStat.Len() {
				destPeer = peer
				minReadBytes = s.TotalFlowBytes
				minRegionsCount = s.RegionsStat.Len()
				continue
			}
			if minRegionsCount == s.RegionsStat.Len() && minReadBytes > s.TotalFlowBytes &&
				uint64(float64(srcReadBytes)*hotRegionScheduleFactor) > s.TotalFlowBytes+2*srcRegion.ReadBytes {
				minReadBytes = s.TotalFlowBytes
				destPeer = peer
			}
		} else {
			destPeer = peer
			break
		}
	}
	return destPeer
}

func (h *balanceHotReadRegionsScheduler) GetStatus() *core.StoreHotRegionInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(map[uint64]*core.HotRegionsStat, len(h.statisticsAsLeader))
	for id, stat := range h.statisticsAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	return &core.StoreHotRegionInfos{
		AsLeader: asLeader,
	}
}
