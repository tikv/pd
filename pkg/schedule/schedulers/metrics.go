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
	"math"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tikv/pd/pkg/core"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const hotRegionTopN = 10

var (
	topHotRegionMetrics = newHotRegionCollector()

	schedulerStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "status",
			Help:      "Status of the scheduler.",
		}, []string{"kind", "type"})

	schedulerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "event_count",
			Help:      "Counter of scheduler events.",
		}, []string{"type", "name"})

	// TODO: pre-allocate gauge metrics
	opInfluenceStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "op_influence",
			Help:      "Store status for schedule",
		}, []string{"scheduler", "store", "type"})

	// TODO: pre-allocate gauge metrics
	tolerantResourceStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "tolerant_resource",
			Help:      "Store status for schedule",
		}, []string{"scheduler"})

	balanceWitnessCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "balance_witness",
			Help:      "Counter of balance witness scheduler.",
		}, []string{"type", "store"})

	// TODO: pre-allocate gauge metrics
	hotSchedulerResultCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_region",
			Help:      "Counter of hot region scheduler.",
		}, []string{"type", "store"})

	balanceDirectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "balance_direction",
			Help:      "Counter of direction of balance related schedulers.",
		}, []string{"type", "store", "direction"})

	// TODO: pre-allocate gauge metrics
	hotDirectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_region_direction",
			Help:      "Counter of hot region scheduler.",
		}, []string{"type", "rw", "store", "direction", "dim"})

	hotPeerHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_peer",
			Help:      "Bucketed histogram of the scheduling hot peer.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30),
		}, []string{"type", "rw", "dim"})

	evictedSlowStoreStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "evicted_slow_store_status",
			Help:      "Store evicted status due to slow",
		}, []string{"store", "slow_type"})

	evictedStoppingStoreStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "evicted_stopping_store_status",
			Help:      "Store evicted status due to stopping",
		}, []string{"store"})

	slowStoreTriggerLimitGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "slow_store_trigger_limit",
			Help:      "slow store trigger limit",
		}, []string{"store", "slow_type"})

	storeSlowTrendEvictedStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "store_slow_trend_evicted_status",
			Help:      "Store evicted by slow trend status for schedule",
		}, []string{"address", "store"})

	storeSlowTrendActionStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "store_slow_trend_action_status",
			Help:      "Store trend scheduler calculating actions",
		}, []string{"type", "status"})

	storeSlowTrendMiscGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "store_slow_trend_misc",
			Help:      "Store trend internal uncatalogued values",
		}, []string{"type", "dim"})

	// HotPendingSum is the sum of pending influence in hot region scheduler.
	HotPendingSum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_pending_sum",
			Help:      "Pending influence sum of store in hot region scheduler.",
		}, []string{"store", "rw", "dim"})

	ruleStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "rule_manager",
			Name:      "status",
			Help:      "Status of the rule.",
		}, []string{"type"})

	regionLabelStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "region_label",
			Name:      "status",
			Help:      "Status of the region labeler.",
		}, []string{"type"})

	balanceRangeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "balance_range",
			Name:      "store",
			Help:      "Store status for balance range schedule",
		}, []string{"store", "type"})
	balanceRangeJobGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "balance_range",
			Name:      "job",
			Help:      "job status for balance range schedule",
		}, []string{})
)

func init() {
	prometheus.MustRegister(schedulerStatusGauge)
	prometheus.MustRegister(ruleStatusGauge)
	prometheus.MustRegister(regionLabelStatusGauge)
	prometheus.MustRegister(schedulerCounter)
	prometheus.MustRegister(balanceWitnessCounter)
	prometheus.MustRegister(hotSchedulerResultCounter)
	prometheus.MustRegister(hotDirectionCounter)
	prometheus.MustRegister(balanceDirectionCounter)
	prometheus.MustRegister(opInfluenceStatus)
	prometheus.MustRegister(tolerantResourceStatus)
	prometheus.MustRegister(hotPeerHist)
	prometheus.MustRegister(evictedSlowStoreStatusGauge)
	prometheus.MustRegister(evictedStoppingStoreStatusGauge)
	prometheus.MustRegister(slowStoreTriggerLimitGauge)
	prometheus.MustRegister(storeSlowTrendEvictedStatusGauge)
	prometheus.MustRegister(storeSlowTrendActionStatusGauge)
	prometheus.MustRegister(storeSlowTrendMiscGauge)
	prometheus.MustRegister(HotPendingSum)
	prometheus.MustRegister(balanceRangeGauge)
	prometheus.MustRegister(balanceRangeJobGauge)
	prometheus.MustRegister(topHotRegionMetrics)
}

// DeleteStoreMetrics deletes the per-store scheduler metrics of a store.
func DeleteStoreMetrics(storeID string) {
	opInfluenceStatus.DeletePartialMatch(prometheus.Labels{"store": storeID})
	balanceWitnessCounter.DeleteLabelValues("move-witness", storeID+"-out")
	balanceWitnessCounter.DeleteLabelValues("move-witness", storeID+"-in")
	hotSchedulerResultCounter.DeletePartialMatch(prometheus.Labels{"store": storeID})
	balanceDirectionCounter.DeletePartialMatch(prometheus.Labels{"store": storeID})
	hotDirectionCounter.DeletePartialMatch(prometheus.Labels{"store": storeID})
	evictedSlowStoreStatusGauge.DeletePartialMatch(prometheus.Labels{"store": storeID})
	evictedStoppingStoreStatusGauge.DeleteLabelValues(storeID)
	slowStoreTriggerLimitGauge.DeletePartialMatch(prometheus.Labels{"store": storeID})
	storeSlowTrendEvictedStatusGauge.DeletePartialMatch(prometheus.Labels{"store": storeID})
	balanceRangeGauge.DeletePartialMatch(prometheus.Labels{"store": storeID})
	HotPendingSum.DeletePartialMatch(prometheus.Labels{"store": storeID})
}

func balanceLeaderCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.BalanceLeaderScheduler.String(), event)
}

func balanceRegionCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.BalanceRegionScheduler.String(), event)
}

func evictLeaderCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.EvictLeaderScheduler.String(), event)
}

func grantHotRegionCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.GrantHotRegionScheduler.String(), event)
}

func grantLeaderCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.GrantHotRegionScheduler.String(), event)
}

func hotRegionCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.BalanceHotRegionScheduler.String(), event)
}

func labelCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.LabelScheduler.String(), event)
}

func randomMergeCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.RandomMergeScheduler.String(), event)
}

func scatterRangeCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.ScatterRangeScheduler.String(), event)
}

func shuffleHotRegionCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.ShuffleHotRegionScheduler.String(), event)
}

func shuffleLeaderCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.ShuffleLeaderScheduler.String(), event)
}

func shuffleRegionCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.ShuffleRegionScheduler.String(), event)
}

func transferWitnessLeaderCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.TransferWitnessLeaderScheduler.String(), event)
}

func balanceRangeCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.BalanceRangeScheduler.String(), event)
}

// WithLabelValues is a heavy operation, define variable to avoid call it every time.
var (
	balanceLeaderScheduleCounter         = balanceLeaderCounterWithEvent("schedule")
	balanceLeaderNoLeaderRegionCounter   = balanceLeaderCounterWithEvent("no-leader-region")
	balanceLeaderRegionHotCounter        = balanceLeaderCounterWithEvent("region-hot")
	balanceLeaderNoSourceStoreCounter    = balanceLeaderCounterWithEvent("no-source-store")
	balanceLeaderNoTargetStoreCounter    = balanceLeaderCounterWithEvent("no-target-store")
	balanceLeaderNoFollowerRegionCounter = balanceLeaderCounterWithEvent("no-follower-region")
	balanceLeaderSkipCounter             = balanceLeaderCounterWithEvent("skip")
	balanceLeaderNewOpCounter            = balanceLeaderCounterWithEvent("new-operator")

	balanceRegionScheduleCounter      = balanceRegionCounterWithEvent("schedule")
	balanceRegionNoRegionCounter      = balanceRegionCounterWithEvent("no-region")
	balanceRegionHotCounter           = balanceRegionCounterWithEvent("region-hot")
	balanceRegionNoLeaderCounter      = balanceRegionCounterWithEvent("no-leader")
	balanceRegionNewOpCounter         = balanceRegionCounterWithEvent("new-operator")
	balanceRegionSkipCounter          = balanceRegionCounterWithEvent("skip")
	balanceRegionCreateOpFailCounter  = balanceRegionCounterWithEvent("create-operator-fail")
	balanceRegionNoReplacementCounter = balanceRegionCounterWithEvent("no-replacement")

	evictLeaderCounter              = evictLeaderCounterWithEvent("schedule")
	evictLeaderNoLeaderCounter      = evictLeaderCounterWithEvent("no-leader")
	evictLeaderPickUnhealthyCounter = evictLeaderCounterWithEvent("pick-unhealthy-region")
	evictLeaderNoTargetStoreCounter = evictLeaderCounterWithEvent("no-target-store")
	evictLeaderNewOperatorCounter   = evictLeaderCounterWithEvent("new-operator")

	evictSlowStoreCounter     = schedulerCounter.WithLabelValues(types.EvictSlowStoreScheduler.String(), "schedule")
	evictStoppingStoreCounter = schedulerCounter.WithLabelValues(types.EvictStoppingStoreScheduler.String(), "schedule")

	grantHotRegionCounter     = grantHotRegionCounterWithEvent("schedule")
	grantHotRegionSkipCounter = grantHotRegionCounterWithEvent("skip")

	grantLeaderCounter            = grantLeaderCounterWithEvent("schedule")
	grantLeaderNoFollowerCounter  = grantLeaderCounterWithEvent("no-follower")
	grantLeaderNewOperatorCounter = grantLeaderCounterWithEvent("new-operator")

	// counter related with the hot region
	hotSchedulerCounter                     = hotRegionCounterWithEvent("schedule")
	hotSchedulerSkipCounter                 = hotRegionCounterWithEvent("skip")
	hotSchedulerSearchRevertRegionsCounter  = hotRegionCounterWithEvent("search_revert_regions")
	hotSchedulerNotSameEngineCounter        = hotRegionCounterWithEvent("not_same_engine")
	hotSchedulerNoRegionCounter             = hotRegionCounterWithEvent("no_region")
	hotSchedulerUnhealthyReplicaCounter     = hotRegionCounterWithEvent("unhealthy_replica")
	hotSchedulerAbnormalReplicaCounter      = hotRegionCounterWithEvent("abnormal_replica")
	hotSchedulerIgnoredAffinity             = hotRegionCounterWithEvent("ignored_affinity")
	hotSchedulerCreateOperatorFailedCounter = hotRegionCounterWithEvent("create_operator_failed")
	hotSchedulerNewOperatorCounter          = hotRegionCounterWithEvent("new_operator")
	hotSchedulerSnapshotSenderLimitCounter  = hotRegionCounterWithEvent("snapshot_sender_limit")
	// Fallback to engine-wide expectation when a scoped load snapshot is stale.
	hotSchedulerPlacementScopeFallbackCounter = hotRegionCounterWithEvent("placement_scope_fallback")
	// hot region counter related with the split region
	hotSchedulerNotFoundSplitKeysCounter          = hotRegionCounterWithEvent("not_found_split_keys")
	hotSchedulerRegionBucketsNotHotCounter        = hotRegionCounterWithEvent("region_buckets_not_hot")
	hotSchedulerOnlyOneBucketsHotCounter          = hotRegionCounterWithEvent("only_one_buckets_hot")
	hotSchedulerHotBucketNotValidCounter          = hotRegionCounterWithEvent("hot_buckets_not_valid")
	hotSchedulerRegionBucketsSingleHotSpotCounter = hotRegionCounterWithEvent("region_buckets_single_hot_spot")
	hotSchedulerSplitSuccessCounter               = hotRegionCounterWithEvent("split_success")
	hotSchedulerNeedSplitBeforeScheduleCounter    = hotRegionCounterWithEvent("need_split_before_move_peer")
	hotSchedulerRegionTooHotNeedSplitCounter      = hotRegionCounterWithEvent("region_is_too_hot_need_split")
	// hot region counter related with the move peer
	hotSchedulerMoveLeaderCounter     = hotRegionCounterWithEvent(moveLeader.String())
	hotSchedulerMovePeerCounter       = hotRegionCounterWithEvent(movePeer.String())
	hotSchedulerTransferLeaderCounter = hotRegionCounterWithEvent(transferLeader.String())
	// hot region counter related with reading and writing
	readSkipAllDimUniformStoreCounter    = hotRegionCounterWithEvent("read-skip-all-dim-uniform-store")
	writeSkipAllDimUniformStoreCounter   = hotRegionCounterWithEvent("write-skip-all-dim-uniform-store")
	readSkipByteDimUniformStoreCounter   = hotRegionCounterWithEvent("read-skip-byte-uniform-store")
	writeSkipByteDimUniformStoreCounter  = hotRegionCounterWithEvent("write-skip-byte-uniform-store")
	readSkipKeyDimUniformStoreCounter    = hotRegionCounterWithEvent("read-skip-key-uniform-store")
	writeSkipKeyDimUniformStoreCounter   = hotRegionCounterWithEvent("write-skip-key-uniform-store")
	readSkipQueryDimUniformStoreCounter  = hotRegionCounterWithEvent("read-skip-query-uniform-store")
	writeSkipQueryDimUniformStoreCounter = hotRegionCounterWithEvent("write-skip-query-uniform-store")
	readSkipCPUDimUniformStoreCounter    = hotRegionCounterWithEvent("read-skip-cpu-uniform-store")
	pendingOpFailsStoreCounter           = hotRegionCounterWithEvent("pending-op-fails")

	labelCounter            = labelCounterWithEvent("schedule")
	labelNewOperatorCounter = labelCounterWithEvent("new-operator")
	labelNoTargetCounter    = labelCounterWithEvent("no-target")
	labelSkipCounter        = labelCounterWithEvent("skip")
	labelNoRegionCounter    = labelCounterWithEvent("no-region")

	randomMergeCounter              = randomMergeCounterWithEvent("schedule")
	randomMergeNewOperatorCounter   = randomMergeCounterWithEvent("new-operator")
	randomMergeNoSourceStoreCounter = randomMergeCounterWithEvent("no-source-store")
	randomMergeNoRegionCounter      = randomMergeCounterWithEvent("no-region")
	randomMergeNoTargetStoreCounter = randomMergeCounterWithEvent("no-target-store")
	randomMergeNotAllowedCounter    = randomMergeCounterWithEvent("not-allowed")

	scatterRangeCounter                    = scatterRangeCounterWithEvent("schedule")
	scatterRangeNewOperatorCounter         = scatterRangeCounterWithEvent("new-operator")
	scatterRangeNewLeaderOperatorCounter   = scatterRangeCounterWithEvent("new-leader-operator")
	scatterRangeNewRegionOperatorCounter   = scatterRangeCounterWithEvent("new-region-operator")
	scatterRangeNoNeedBalanceRegionCounter = scatterRangeCounterWithEvent("no-need-balance-region")
	scatterRangeNoNeedBalanceLeaderCounter = scatterRangeCounterWithEvent("no-need-balance-leader")

	shuffleHotRegionCounter            = shuffleHotRegionCounterWithEvent("schedule")
	shuffleHotRegionNewOperatorCounter = shuffleHotRegionCounterWithEvent("new-operator")
	shuffleHotRegionSkipCounter        = shuffleHotRegionCounterWithEvent("skip")

	shuffleLeaderCounter              = shuffleLeaderCounterWithEvent("schedule")
	shuffleLeaderNewOperatorCounter   = shuffleLeaderCounterWithEvent("new-operator")
	shuffleLeaderNoTargetStoreCounter = shuffleLeaderCounterWithEvent("no-target-store")
	shuffleLeaderNoFollowerCounter    = shuffleLeaderCounterWithEvent("no-follower")

	shuffleRegionCounter                   = shuffleRegionCounterWithEvent("schedule")
	shuffleRegionNewOperatorCounter        = shuffleRegionCounterWithEvent("new-operator")
	shuffleRegionNoRegionCounter           = shuffleRegionCounterWithEvent("no-region")
	shuffleRegionNoNewPeerCounter          = shuffleRegionCounterWithEvent("no-new-peer")
	shuffleRegionCreateOperatorFailCounter = shuffleRegionCounterWithEvent("create-operator-fail")
	shuffleRegionNoSourceStoreCounter      = shuffleRegionCounterWithEvent("no-source-store")

	transferWitnessLeaderCounter              = transferWitnessLeaderCounterWithEvent("schedule")
	transferWitnessLeaderNewOperatorCounter   = transferWitnessLeaderCounterWithEvent("new-operator")
	transferWitnessLeaderNoTargetStoreCounter = transferWitnessLeaderCounterWithEvent("no-target-store")

	balanceRangeCounter              = balanceRangeCounterWithEvent("schedule")
	balanceRangeNewOperatorCounter   = balanceRangeCounterWithEvent("new-operator")
	balanceRangeExpiredCounter       = balanceRangeCounterWithEvent("expired")
	balanceRangeNoRegionCounter      = balanceRangeCounterWithEvent("no-region")
	balanceRangeHotCounter           = balanceRangeCounterWithEvent("region-hot")
	balanceRangeNoLeaderCounter      = balanceRangeCounterWithEvent("no-leader")
	balanceRangeCreateOpFailCounter  = balanceRangeCounterWithEvent("create-operator-fail")
	balanceRangeNoReplacementCounter = balanceRangeCounterWithEvent("no-replacement")
	balanceRangeNoJobCounter         = balanceRangeCounterWithEvent("no-job")
	balanceRangeBalancedCounter      = balanceRangeCounterWithEvent("balanced")
	balancePersistFailedCounter      = balanceRangeCounterWithEvent("persist-failed")
)

// Keep immutable metric snapshots, rather than a GaugeVec retaining labels for
// every region ever seen. Scrapes read at most 60 metrics and never scan regions.
type hotRegionCollector struct {
	syncutil.RWMutex
	loadDesc       *prometheus.Desc
	storeRatioDesc *prometheus.Desc
	metrics        [utils.RWTypeLen][]prometheus.Metric
}

func newHotRegionCollector() *hotRegionCollector {
	labels := []string{"type", "dim", "region", "store"}
	return &hotRegionCollector{
		loadDesc: prometheus.NewDesc(
			"pd_hotspot_top_region_load",
			"Smoothed load of the top 10 distinct sustained hot regions per scheduling type; dim specifies the unit (byte/s, key/s, query/s, or native CPU usage). Replicated peer loads are represented by the maximum, not summed.",
			labels, nil),
		storeRatioDesc: prometheus.NewDesc(
			"pd_hotspot_top_region_store_load_ratio",
			"Smoothed hot Region load divided by the Store's rolling total load for the same scheduling type and dimension; a fraction that can exceed 1 when the Region and Store smoothing windows differ.",
			labels, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *hotRegionCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.loadDesc
	ch <- c.storeRatioDesc
}

// Collect implements prometheus.Collector.
func (c *hotRegionCollector) Collect(ch chan<- prometheus.Metric) {
	// The slices and metrics are immutable after publication. Release the lock
	// before sending so a slow scrape cannot block the periodic producer.
	c.RLock()
	snapshot := c.metrics
	c.RUnlock()
	for _, metrics := range snapshot {
		for _, metric := range metrics {
			ch <- metric
		}
	}
}

func (c *hotRegionCollector) set(rw utils.RWType, metrics []prometheus.Metric) {
	c.Lock()
	c.metrics[rw] = metrics
	c.Unlock()
}

// ResetHotRegionMetrics removes all Top N samples, including on leadership loss.
func ResetHotRegionMetrics() {
	topHotRegionMetrics.Lock()
	topHotRegionMetrics.metrics = [utils.RWTypeLen][]prometheus.Metric{}
	topHotRegionMetrics.Unlock()
}

func storeLoadKind(ty resourceType, dim int, store *core.StoreInfo) (utils.StoreLoadKind, bool) {
	isRead := ty == readLeader || ty == readPeer
	switch dim {
	case utils.ByteDim:
		if isRead {
			return utils.StoreReadBytes, true
		}
		if store != nil && store.IsTiFlashWrite() {
			return utils.StoreRegionsWriteBytes, true
		}
		return utils.StoreWriteBytes, true
	case utils.KeyDim:
		if isRead {
			return utils.StoreReadKeys, true
		}
		if store != nil && store.IsTiFlashWrite() {
			return utils.StoreRegionsWriteKeys, true
		}
		return utils.StoreWriteKeys, true
	case utils.QueryDim:
		if isRead {
			return utils.StoreReadQuery, true
		}
		return utils.StoreWriteQuery, true
	case utils.CPUDim:
		if isRead {
			return utils.StoreReadCPU, true
		}
	}
	return 0, false
}

type hotRegionEntry struct {
	regionID uint64
	storeID  uint64
	load     float64
}

// hotter gives deterministic ordering, including replicas with equal load.
func (e hotRegionEntry) hotter(other hotRegionEntry) bool {
	if e.load != other.load {
		return e.load > other.load
	}
	if e.regionID != other.regionID {
		return e.regionID < other.regionID
	}
	return e.storeID < other.storeID
}

type hotRegionTop struct {
	entries [hotRegionTopN]hotRegionEntry
	count   int
}

// add keeps only ten entries, ordered hottest first. Deduplication also uses
// only these ten entries: a discarded replica cannot exceed the current cutoff,
// so retaining a cluster-sized region map is unnecessary. The cutoff rejects
// most candidates with a single comparison; insertion costs at most ten moves.
func (t *hotRegionTop) add(e hotRegionEntry) {
	if e.load <= 0 || math.IsNaN(e.load) || math.IsInf(e.load, 0) {
		return
	}
	if t.count == hotRegionTopN && !e.hotter(t.entries[t.count-1]) {
		return
	}
	for i := 0; i < t.count; i++ {
		if t.entries[i].regionID == e.regionID {
			if !e.hotter(t.entries[i]) {
				return
			}
			copy(t.entries[i:], t.entries[i+1:t.count])
			t.count--
			break
		}
	}
	pos := 0
	for pos < t.count && !e.hotter(t.entries[pos]) {
		pos++
	}
	if t.count < hotRegionTopN {
		t.count++
	}
	copy(t.entries[pos+1:t.count], t.entries[pos:t.count-1])
	t.entries[pos] = e
}

// selectHotRegions returns the top N hot regions and a snapshot of the valid
// stores considered, so callers can look up a region's store without issuing a
// separate lock-protected GetStore call per entry.
func selectHotRegions(cluster sche.SchedulerCluster, stats map[uint64][]*statistics.HotPeerStat, dim int, leaderOnly bool) (hotRegionTop, map[uint64]*core.StoreInfo) {
	var top hotRegionTop
	stores := make(map[uint64]*core.StoreInfo)
	for storeID, peers := range stats {
		store := cluster.GetStore(storeID)
		if store == nil || store.IsRemoved() || store.IsUnhealthy() {
			continue
		}
		stores[storeID] = store
		for _, peer := range peers {
			if leaderOnly && !peer.IsLeader() {
				continue
			}
			top.add(hotRegionEntry{regionID: peer.RegionID, storeID: storeID, load: peer.GetLoad(dim)})
		}
	}
	return top, stores
}

// CollectHotRegionMetrics reuses the already filtered, denoised hot-cache
// snapshot used by hotspot monitoring. It is independent of scheduling being
// paused, hitting an operator limit, or producing an operator.
func (c *Controller) CollectHotRegionMetrics(rw utils.RWType, stats map[uint64][]*statistics.HotPeerStat, storeLoads map[uint64]statistics.StoreKindLoads) {
	c.RLock()
	defer c.RUnlock()
	var scheduler *hotScheduler
	if controller := c.schedulers[types.BalanceHotRegionScheduler.String()]; controller != nil {
		scheduler, _ = controller.Scheduler.(*hotScheduler)
	} else {
		scheduler, _ = c.schedulerHandlers[types.BalanceHotRegionScheduler.String()].(*hotScheduler)
	}
	if scheduler == nil {
		topHotRegionMetrics.set(rw, nil)
		return
	}
	metrics := make([]prometheus.Metric, 0, 4*hotRegionTopN)
	appendTop := func(ty resourceType, leaderOnly bool) {
		priorities := scheduler.getPriorities(c.cluster, ty)
		if len(priorities) == 0 {
			return
		}
		dim := utils.StringToDim(priorities[0])
		top, stores := selectHotRegions(c.cluster, stats, dim, leaderOnly)
		for _, entry := range top.entries[:top.count] {
			labels := []string{ty.String(), utils.DimToString(dim), strconv.FormatUint(entry.regionID, 10), strconv.FormatUint(entry.storeID, 10)}
			metrics = append(metrics, prometheus.MustNewConstMetric(topHotRegionMetrics.loadDesc, prometheus.GaugeValue, entry.load,
				labels...))
			store := stores[entry.storeID]
			if store != nil {
				kind, ok := storeLoadKind(ty, dim, store)
				if !ok {
					continue
				}
				if storeLoad, exists := storeLoads[entry.storeID]; exists {
					total := storeLoad[kind]
					if total > 0 && !math.IsNaN(total) && !math.IsInf(total, 0) {
						ratio := entry.load / total
						if !math.IsNaN(ratio) && !math.IsInf(ratio, 0) {
							metrics = append(metrics, prometheus.MustNewConstMetric(topHotRegionMetrics.storeRatioDesc, prometheus.GaugeValue, ratio, labels...))
						}
					}
				}
			}
		}
	}
	if rw == utils.Read {
		appendTop(readPeer, false)
	} else {
		appendTop(writeLeader, true)
		appendTop(writePeer, false)
	}
	topHotRegionMetrics.set(rw, metrics)
}
