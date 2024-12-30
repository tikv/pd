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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/tikv/pd/pkg/schedule/types"
)

var (
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
		}, []string{"type", "source", "target"})

	// TODO: pre-allocate gauge metrics
	hotDirectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_region_direction",
			Help:      "Counter of hot region scheduler.",
		}, []string{"type", "rw", "store", "direction", "dim"})

	hotPendingStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_pending",
			Help:      "Pending influence status in hot region scheduler.",
		}, []string{"type", "source", "target"})

	hotPeerHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "scheduler",
			Name:      "hot_peer",
			Help:      "Bucketed histogram of the scheduling hot peer.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30),
		}, []string{"type", "rw", "dim"})

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
)

func init() {
	prometheus.MustRegister(schedulerStatusGauge)
	prometheus.MustRegister(ruleStatusGauge)
	prometheus.MustRegister(schedulerCounter)
	prometheus.MustRegister(balanceWitnessCounter)
	prometheus.MustRegister(hotSchedulerResultCounter)
	prometheus.MustRegister(hotDirectionCounter)
	prometheus.MustRegister(balanceDirectionCounter)
	prometheus.MustRegister(opInfluenceStatus)
	prometheus.MustRegister(tolerantResourceStatus)
	prometheus.MustRegister(hotPendingStatus)
	prometheus.MustRegister(hotPeerHist)
	prometheus.MustRegister(storeSlowTrendEvictedStatusGauge)
	prometheus.MustRegister(storeSlowTrendActionStatusGauge)
	prometheus.MustRegister(storeSlowTrendMiscGauge)
	prometheus.MustRegister(HotPendingSum)
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

func splitBucketCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.SplitBucketScheduler.String(), event)
}

func transferWitnessLeaderCounterWithEvent(event string) prometheus.Counter {
	return schedulerCounter.WithLabelValues(types.TransferWitnessLeaderScheduler.String(), event)
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

	evictSlowStoreCounter = schedulerCounter.WithLabelValues(types.EvictSlowStoreScheduler.String(), "schedule")

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
	hotSchedulerCreateOperatorFailedCounter = hotRegionCounterWithEvent("create_operator_failed")
	hotSchedulerNewOperatorCounter          = hotRegionCounterWithEvent("new_operator")
	hotSchedulerSnapshotSenderLimitCounter  = hotRegionCounterWithEvent("snapshot_sender_limit")
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

	splitBucketDisableCounter            = splitBucketCounterWithEvent("bucket-disable")
	splitBuckerSplitLimitCounter         = splitBucketCounterWithEvent("split-limit")
	splitBucketScheduleCounter           = splitBucketCounterWithEvent("schedule")
	splitBucketNoRegionCounter           = splitBucketCounterWithEvent("no-region")
	splitBucketRegionTooSmallCounter     = splitBucketCounterWithEvent("region-too-small")
	splitBucketOperatorExistCounter      = splitBucketCounterWithEvent("operator-exist")
	splitBucketKeyRangeNotMatchCounter   = splitBucketCounterWithEvent("key-range-not-match")
	splitBucketNoSplitKeysCounter        = splitBucketCounterWithEvent("no-split-keys")
	splitBucketCreateOperatorFailCounter = splitBucketCounterWithEvent("create-operator-fail")
	splitBucketNewOperatorCounter        = splitBucketCounterWithEvent("new-operator")

	transferWitnessLeaderCounter              = transferWitnessLeaderCounterWithEvent("schedule")
	transferWitnessLeaderNewOperatorCounter   = transferWitnessLeaderCounterWithEvent("new-operator")
	transferWitnessLeaderNoTargetStoreCounter = transferWitnessLeaderCounterWithEvent("no-target-store")
)
