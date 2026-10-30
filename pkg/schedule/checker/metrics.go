// Copyright 2019 TiKV Project Authors.
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

package checker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
)

var (
	checkerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "event_count",
			Help:      "Counter of checker events.",
		}, []string{"type", "name"})
	regionListGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "region_list",
			Help:      "Number of region about different type.",
		}, []string{"type"})

	patrolCheckRegionsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "patrol_regions_time",
			Help:      "Time spent of patrol checks region.",
		})

	patrolPhaseDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "patrol_phase_duration_seconds",
			Help:      "Bucketed histogram of duration for patrol phases.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"phase"})

	checkRegionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "check_region_duration_seconds",
			Help:      "Bucketed histogram of duration for checking region in checkers.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"checker"})

	patrolRegionChannelSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "patrol_region_channel_size",
			Help:      "Size of patrol region channel.",
		})
	splitScatterPendingExpiredCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "split_scatter_pending_expired_total",
			Help:      "Counter of expired split-scatter pending entries by whether they were selected for dispatch.",
		}, []string{"attempted"})
	splitScatterPendingDroppedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "split_scatter_pending_dropped_total",
			Help:      "Counter of split-scatter pending entries dropped by the capacity limit.",
		})
	splitScatterPendingGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "split_scatter_pending",
			Help:      "Number of split-scatter pending entries.",
		})
	affinityBalancePlanCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "affinity_balance_plan_total",
			Help:      "Counter of affinity table balance steps by result, or disabled ticks with registered tables.",
		}, []string{"result"})
	affinityBalanceDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "affinity_balance_duration_seconds",
			Help:      "Duration of executed affinity balance phases per table step, summed across validation rounds.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 16),
		}, []string{"phase"})
	affinityBalanceTargetsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "affinity_balance_targets_total",
			Help:      "Counter of affinity balance targets entering commit or actually written.",
		}, []string{"type"})
	affinityBalancePlannerStopCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "affinity_balance_planner_stop_total",
			Help:      "Counter of affinity balance planner calls by stop reason.",
		}, []string{"reason"})
	affinityBalanceTablesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pd",
			Subsystem: "checker",
			Name:      "affinity_balance_tables",
			Help:      "Number of registered affinity balance tables or tables with unexpired idle records at the end of each tick.",
		}, []string{"state"})
)

const (
	affinityBalanceWritten       = "written"
	affinityBalanceNoImprovement = "no_improvement"
	affinityBalanceBlocked       = "blocked"
	affinityBalanceRejected      = "rejected"
	affinityBalanceCreating      = "creating"
	affinityBalanceError         = "error"
	affinityBalanceDisabled      = "disabled"
)

// WithLabelValues is a heavy operation, pre-cache the label handles.
var (
	splitScatterPendingExpiredAttemptedCounter   = splitScatterPendingExpiredCounter.WithLabelValues("true")
	splitScatterPendingExpiredUnattemptedCounter = splitScatterPendingExpiredCounter.WithLabelValues("false")
	affinityBalancePlanCounters                  = map[string]prometheus.Counter{
		affinityBalanceWritten:       affinityBalancePlanCounter.WithLabelValues(affinityBalanceWritten),
		affinityBalanceNoImprovement: affinityBalancePlanCounter.WithLabelValues(affinityBalanceNoImprovement),
		affinityBalanceBlocked:       affinityBalancePlanCounter.WithLabelValues(affinityBalanceBlocked),
		affinityBalanceRejected:      affinityBalancePlanCounter.WithLabelValues(affinityBalanceRejected),
		affinityBalanceCreating:      affinityBalancePlanCounter.WithLabelValues(affinityBalanceCreating),
		affinityBalanceError:         affinityBalancePlanCounter.WithLabelValues(affinityBalanceError),
		affinityBalanceDisabled:      affinityBalancePlanCounter.WithLabelValues(affinityBalanceDisabled),
	}
	affinityBalanceStepDuration     = affinityBalanceDuration.WithLabelValues("step")
	affinityBalancePlanDuration     = affinityBalanceDuration.WithLabelValues("plan")
	affinityBalanceValidateDuration = affinityBalanceDuration.WithLabelValues("validate")
	affinityBalanceCommitDuration   = affinityBalanceDuration.WithLabelValues("commit")
	affinityBalanceTargetsPlanned   = affinityBalanceTargetsCounter.WithLabelValues("planned")
	affinityBalanceTargetsWritten   = affinityBalanceTargetsCounter.WithLabelValues("written")
	affinityBalancePlannerStops     = map[balanceplan.StopReason]prometheus.Counter{
		balanceplan.StopNoGroups: affinityBalancePlannerStopCounter.WithLabelValues(string(balanceplan.StopNoGroups)),
		balanceplan.StopBound:    affinityBalancePlannerStopCounter.WithLabelValues(string(balanceplan.StopBound)),
		balanceplan.StopAttempts: affinityBalancePlannerStopCounter.WithLabelValues(string(balanceplan.StopAttempts)),
		balanceplan.StopBudget:   affinityBalancePlannerStopCounter.WithLabelValues(string(balanceplan.StopBudget)),
		balanceplan.StopCanceled: affinityBalancePlannerStopCounter.WithLabelValues(string(balanceplan.StopCanceled)),
	}
	affinityBalanceTablesRegistered = affinityBalanceTablesGauge.WithLabelValues("registered")
	affinityBalanceTablesIdle       = affinityBalanceTablesGauge.WithLabelValues("idle")
)

func init() {
	prometheus.MustRegister(checkerCounter)
	prometheus.MustRegister(regionListGauge)
	prometheus.MustRegister(patrolCheckRegionsGauge)
	prometheus.MustRegister(patrolPhaseDuration)
	prometheus.MustRegister(checkRegionDuration)
	prometheus.MustRegister(patrolRegionChannelSize)
	prometheus.MustRegister(splitScatterPendingExpiredCounter)
	prometheus.MustRegister(splitScatterPendingDroppedCounter)
	prometheus.MustRegister(splitScatterPendingGauge)
	prometheus.MustRegister(affinityBalancePlanCounter)
	prometheus.MustRegister(affinityBalanceDuration)
	prometheus.MustRegister(affinityBalanceTargetsCounter)
	prometheus.MustRegister(affinityBalancePlannerStopCounter)
	prometheus.MustRegister(affinityBalanceTablesGauge)
}

func (b *affinityBalancer) collectTableMetrics(disabled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if disabled && len(b.tables) > 0 {
		affinityBalancePlanCounters[affinityBalanceDisabled].Inc()
	}
	now, idle := time.Now(), 0
	for _, record := range b.idle {
		if now.Before(record.until) {
			idle++
		}
	}
	affinityBalanceTablesRegistered.Set(float64(len(b.tables)))
	affinityBalanceTablesIdle.Set(float64(idle))
}

func recordAffinityBalanceStepMetrics(result string, res *balancePlanResult, duration, commitDuration time.Duration, written int) {
	// Tables that disappeared or became ineligible before planning have no plan result.
	if counter := affinityBalancePlanCounters[result]; counter != nil {
		counter.Inc()
	}
	affinityBalanceStepDuration.Observe(duration.Seconds())
	if len(res.stops) > 0 {
		affinityBalancePlanDuration.Observe(res.planDuration.Seconds())
		for _, stop := range res.stops {
			if counter := affinityBalancePlannerStops[stop]; counter != nil {
				counter.Inc()
			}
		}
	}
	if res.validateDuration > 0 {
		affinityBalanceValidateDuration.Observe(res.validateDuration.Seconds())
	}
	if commitDuration > 0 {
		affinityBalanceCommitDuration.Observe(commitDuration.Seconds())
		affinityBalanceTargetsPlanned.Add(float64(len(res.targets)))
	}
	if written > 0 {
		affinityBalanceTargetsWritten.Add(float64(written))
	}
}

const (
	// NOTE: these types are different from pkg/schedule/config/type.go,
	// they are only used for prometheus metrics to keep the compatibility.
	ruleChecker         = "rule_checker"
	jointStateChecker   = "joint_state_checker"
	learnerChecker      = "learner_checker"
	mergeChecker        = "merge_checker"
	replicaChecker      = "replica_checker"
	splitChecker        = "split_checker"
	splitScatterChecker = "split_scatter_checker"
	affinityChecker     = "affinity_checker"
)

const (
	// patrol phases
	phaseWaitForChannel       = "wait_for_channel"
	phaseCheckPriority        = "check_priority"
	phaseCheckPending         = "check_pending"
	phaseDispatchSplitScatter = "dispatch_split_scatter"
	phaseScanRegions          = "scan_regions"
	phaseUpdateLabel          = "update_label_stats"
)

// checkerControllerMetrics contains pre-created Prometheus metrics for the checker controller.
// It follows the best practice of pre-compiling metrics with labels to avoid runtime overhead.
type checkerControllerMetrics struct {
	// Pre-created histograms for patrol phases. Key is the phase name.
	patrolPhaseHistograms map[string]prometheus.Observer
	// Pre-created histograms for checkers. Key is the checker name.
	checkRegionHistograms map[string]prometheus.Observer
	// Gauge for patrol region channel size.
	patrolRegionChannelSize prometheus.Gauge
}

// newCheckerControllerMetrics creates and initializes a new checkerControllerMetrics instance.
func newCheckerControllerMetrics() *checkerControllerMetrics {
	patrolPhases := []string{
		phaseWaitForChannel,
		phaseCheckPriority,
		phaseCheckPending,
		phaseDispatchSplitScatter,
		phaseScanRegions,
		phaseUpdateLabel,
	}

	// when adding a new checker, remember to add it here for metrics
	checkerTypes := []string{
		jointStateChecker,
		splitChecker,
		ruleChecker,
		learnerChecker,
		replicaChecker,
		mergeChecker,
		affinityChecker,
	}

	m := &checkerControllerMetrics{
		patrolPhaseHistograms:   make(map[string]prometheus.Observer, len(patrolPhases)),
		checkRegionHistograms:   make(map[string]prometheus.Observer, len(checkerTypes)),
		patrolRegionChannelSize: patrolRegionChannelSize, // Use the gauge defined at package-level
	}

	for _, phase := range patrolPhases {
		m.patrolPhaseHistograms[phase] = patrolPhaseDuration.WithLabelValues(phase)
	}

	for _, checker := range checkerTypes {
		m.checkRegionHistograms[checker] = checkRegionDuration.WithLabelValues(checker)
	}
	return m
}

func ruleCheckerCounterWithEvent(event string) prometheus.Counter {
	return checkerCounter.WithLabelValues(ruleChecker, event)
}

func jointStateCheckerCounterWithEvent(event string) prometheus.Counter {
	return checkerCounter.WithLabelValues(jointStateChecker, event)
}

func mergeCheckerCounterWithEvent(event string) prometheus.Counter {
	return checkerCounter.WithLabelValues(mergeChecker, event)
}

func replicaCheckerCounterWithEvent(event string) prometheus.Counter {
	return checkerCounter.WithLabelValues(replicaChecker, event)
}

func splitScatterCheckerCounterWithEvent(event string) prometheus.Counter {
	return checkerCounter.WithLabelValues(splitScatterChecker, event)
}

// WithLabelValues is a heavy operation, define variable to avoid call it every time.
var (
	ruleCheckerCounter                            = ruleCheckerCounterWithEvent("check")
	ruleCheckerPausedCounter                      = ruleCheckerCounterWithEvent("paused")
	ruleCheckerRegionNoLeaderCounter              = ruleCheckerCounterWithEvent("region-no-leader")
	ruleCheckerGetCacheCounter                    = ruleCheckerCounterWithEvent("get-cache")
	ruleCheckerNeedSplitCounter                   = ruleCheckerCounterWithEvent("need-split")
	ruleCheckerSetCacheCounter                    = ruleCheckerCounterWithEvent("set-cache")
	ruleCheckerReplaceDownCounter                 = ruleCheckerCounterWithEvent("replace-down")
	ruleCheckerPromoteWitnessCounter              = ruleCheckerCounterWithEvent("promote-witness")
	ruleCheckerReplaceOfflineCounter              = ruleCheckerCounterWithEvent("replace-offline")
	ruleCheckerAddRulePeerCounter                 = ruleCheckerCounterWithEvent("add-rule-peer")
	ruleCheckerAddTiFlashLearnerCounter           = ruleCheckerCounterWithEvent("add-tiflash-learner")
	ruleCheckerNoStoreAddCounter                  = ruleCheckerCounterWithEvent("no-store-add")
	ruleCheckerNoStoreThenTryReplace              = ruleCheckerCounterWithEvent("no-store-then-try-replace")
	ruleCheckerNoStoreReplaceCounter              = ruleCheckerCounterWithEvent("no-store-replace")
	ruleCheckerFixPeerRoleCounter                 = ruleCheckerCounterWithEvent("fix-peer-role")
	ruleCheckerFixLeaderRoleCounter               = ruleCheckerCounterWithEvent("fix-leader-role")
	ruleCheckerNotAllowLeaderCounter              = ruleCheckerCounterWithEvent("not-allow-leader")
	ruleCheckerFixFollowerRoleCounter             = ruleCheckerCounterWithEvent("fix-follower-role")
	ruleCheckerNoNewLeaderCounter                 = ruleCheckerCounterWithEvent("no-new-leader")
	ruleCheckerDemoteVoterRoleCounter             = ruleCheckerCounterWithEvent("demote-voter-role")
	ruleCheckerRecentlyPromoteToNonWitnessCounter = ruleCheckerCounterWithEvent("recently-promote-to-non-witness")
	ruleCheckerCancelSwitchToWitnessCounter       = ruleCheckerCounterWithEvent("cancel-switch-to-witness")
	ruleCheckerSetVoterWitnessCounter             = ruleCheckerCounterWithEvent("set-voter-witness")
	ruleCheckerSetLearnerWitnessCounter           = ruleCheckerCounterWithEvent("set-learner-witness")
	ruleCheckerSetVoterNonWitnessCounter          = ruleCheckerCounterWithEvent("set-voter-non-witness")
	ruleCheckerSetLearnerNonWitnessCounter        = ruleCheckerCounterWithEvent("set-learner-non-witness")
	ruleCheckerMoveToBetterLocationCounter        = ruleCheckerCounterWithEvent("move-to-better-location")
	ruleCheckerSkipRemoveOrphanPeerCounter        = ruleCheckerCounterWithEvent("skip-remove-orphan-peer")
	ruleCheckerRemoveOrphanPeerCounter            = ruleCheckerCounterWithEvent("remove-orphan-peer")
	ruleCheckerReplaceOrphanPeerCounter           = ruleCheckerCounterWithEvent("replace-orphan-peer")
	ruleCheckerReplaceOrphanPeerNoFitCounter      = ruleCheckerCounterWithEvent("replace-orphan-peer-no-fit")

	jointCheckCounter                 = jointStateCheckerCounterWithEvent("check")
	jointCheckerPausedCounter         = jointStateCheckerCounterWithEvent("paused")
	jointCheckerFailedCounter         = jointStateCheckerCounterWithEvent("create-operator-fail")
	jointCheckerNewOpCounter          = jointStateCheckerCounterWithEvent("new-operator")
	jointCheckerTransferLeaderCounter = jointStateCheckerCounterWithEvent("transfer-leader")

	learnerCheckerPausedCounter = checkerCounter.WithLabelValues(learnerChecker, "paused")

	mergeCheckerCounter                     = mergeCheckerCounterWithEvent("check")
	mergeCheckerPausedCounter               = mergeCheckerCounterWithEvent("paused")
	mergeCheckerRecentlySplitCounter        = mergeCheckerCounterWithEvent("recently-split")
	mergeCheckerRecentlyStartCounter        = mergeCheckerCounterWithEvent("recently-start")
	mergeCheckerNoLeaderCounter             = mergeCheckerCounterWithEvent("no-leader")
	mergeCheckerNoNeedCounter               = mergeCheckerCounterWithEvent("no-need")
	mergeCheckerUnhealthyRegionCounter      = mergeCheckerCounterWithEvent("unhealthy-region")
	mergeCheckerAbnormalReplicaCounter      = mergeCheckerCounterWithEvent("abnormal-replica")
	mergeCheckerHotRegionCounter            = mergeCheckerCounterWithEvent("hot-region")
	mergeCheckerNoTargetCounter             = mergeCheckerCounterWithEvent("no-target")
	mergeCheckerTargetTooLargeCounter       = mergeCheckerCounterWithEvent("target-too-large")
	mergeCheckerSplitSizeAfterMergeCounter  = mergeCheckerCounterWithEvent("split-size-after-merge")
	mergeCheckerSplitKeysAfterMergeCounter  = mergeCheckerCounterWithEvent("split-keys-after-merge")
	mergeCheckerNewOpCounter                = mergeCheckerCounterWithEvent("new-operator")
	mergeCheckerLargerSourceCounter         = mergeCheckerCounterWithEvent("larger-source")
	mergeCheckerAdjNotExistCounter          = mergeCheckerCounterWithEvent("adj-not-exist")
	mergeCheckerAdjRecentlySplitCounter     = mergeCheckerCounterWithEvent("adj-recently-split")
	mergeCheckerAdjRegionHotCounter         = mergeCheckerCounterWithEvent("adj-region-hot")
	mergeCheckerAdjDisallowMergeCounter     = mergeCheckerCounterWithEvent("adj-disallow-merge")
	mergeCheckerAdjAbnormalPeerStoreCounter = mergeCheckerCounterWithEvent("adj-abnormal-peerstore")
	mergeCheckerAdjSpecialPeerCounter       = mergeCheckerCounterWithEvent("adj-special-peer")
	mergeCheckerAdjAbnormalReplicaCounter   = mergeCheckerCounterWithEvent("adj-abnormal-replica")

	replicaCheckerCounter                         = replicaCheckerCounterWithEvent("check")
	replicaCheckerPausedCounter                   = replicaCheckerCounterWithEvent("paused")
	replicaCheckerNewOpCounter                    = replicaCheckerCounterWithEvent("new-operator")
	replicaCheckerNoTargetStoreCounter            = replicaCheckerCounterWithEvent("no-target-store")
	replicaCheckerNoWorstPeerCounter              = replicaCheckerCounterWithEvent("no-worst-peer")
	replicaCheckerCreateOpFailedCounter           = replicaCheckerCounterWithEvent("create-operator-failed")
	replicaCheckerAllRightCounter                 = replicaCheckerCounterWithEvent("all-right")
	replicaCheckerNotBetterCounter                = replicaCheckerCounterWithEvent("not-better")
	replicaCheckerRemoveExtraOfflineFailedCounter = replicaCheckerCounterWithEvent("remove-extra-offline-replica-failed")
	replicaCheckerRemoveExtraDownFailedCounter    = replicaCheckerCounterWithEvent("remove-extra-down-replica-failed")
	replicaCheckerNoStoreOfflineCounter           = replicaCheckerCounterWithEvent("no-store-offline")
	replicaCheckerNoStoreDownCounter              = replicaCheckerCounterWithEvent("no-store-down")
	replicaCheckerReplaceOfflineFailedCounter     = replicaCheckerCounterWithEvent("replace-offline-replica-failed")
	replicaCheckerReplaceDownFailedCounter        = replicaCheckerCounterWithEvent("replace-down-replica-failed")

	splitCheckerCounter       = checkerCounter.WithLabelValues(splitChecker, "check")
	splitCheckerPausedCounter = checkerCounter.WithLabelValues(splitChecker, "paused")

	splitScatterDispatchDisabledCounter          = splitScatterCheckerCounterWithEvent("dispatch-disabled")
	splitScatterDispatchScheduleLimitCounter     = splitScatterCheckerCounterWithEvent("dispatch-schedule-limit")
	splitScatterDispatchRegionMissingCounter     = splitScatterCheckerCounterWithEvent("dispatch-region-missing")
	splitScatterDispatchScheduleDisabledCounter  = splitScatterCheckerCounterWithEvent("dispatch-schedule-disabled")
	splitScatterDispatchNotReplicatedCounter     = splitScatterCheckerCounterWithEvent("dispatch-not-fully-replicated")
	splitScatterDispatchScatterFailedCounter     = splitScatterCheckerCounterWithEvent("dispatch-scatter-failed")
	splitScatterDispatchHotRegionCounter         = splitScatterCheckerCounterWithEvent("dispatch-hot-region")
	splitScatterDispatchBalancedReadCPUCounter   = splitScatterCheckerCounterWithEvent("dispatch-balanced-read-cpu")
	splitScatterDispatchStoreLimitCounter        = splitScatterCheckerCounterWithEvent("dispatch-store-limit")
	splitScatterDispatchAddOperatorFailedCounter = splitScatterCheckerCounterWithEvent("dispatch-add-operator-failed")
	splitScatterDispatchOperatorCreatedCounter   = splitScatterCheckerCounterWithEvent("dispatch-operator-created")
	splitScatterDispatchNoOperatorNeededCounter  = splitScatterCheckerCounterWithEvent("dispatch-no-operator-needed")

	affinityCheckerCounter                        = checkerCounter.WithLabelValues(affinityChecker, "check")
	affinityCheckerPausedCounter                  = checkerCounter.WithLabelValues(affinityChecker, "paused")
	affinityCheckerPlacementRulesDisabledCounter  = checkerCounter.WithLabelValues(affinityChecker, "placement-rules-disabled")
	affinityCheckerRegionNoLeaderCounter          = checkerCounter.WithLabelValues(affinityChecker, "region-no-leader")
	affinityCheckerGroupSchedulingDisabledCounter = checkerCounter.WithLabelValues(affinityChecker, "group-scheduling-disabled")
	affinityCheckerNewOpCounter                   = checkerCounter.WithLabelValues(affinityChecker, "new-operator")
	affinityCheckerCreateOpFailedCounter          = checkerCounter.WithLabelValues(affinityChecker, "create-operator-failed")
	affinityCheckerUnhealthyRegionCounter         = checkerCounter.WithLabelValues(affinityChecker, "unhealthy-region")
	affinityCheckerAbnormalReplicaCounter         = checkerCounter.WithLabelValues(affinityChecker, "abnormal-replica")
	affinityCheckerLearnerPeerConflictCounter     = checkerCounter.WithLabelValues(affinityChecker, "learner-peer-conflict")
	affinityCheckerTargetStoreRejectedCounter     = checkerCounter.WithLabelValues(affinityChecker, "target-store-rejected")
	affinityCheckerTargetStoreBlockedCounter      = checkerCounter.WithLabelValues(affinityChecker, "target-store-blocked")

	// Affinity merge checker metrics
	affinityMergeCheckerCounter                     = checkerCounter.WithLabelValues(affinityChecker, "merge-check")
	affinityMergeCheckerDisabledCounter             = checkerCounter.WithLabelValues(affinityChecker, "merge-disabled")
	affinityMergeCheckerGlobalDisabledCounter       = checkerCounter.WithLabelValues(affinityChecker, "merge-global-disabled")
	affinityMergeCheckerSkipStartupCounter          = checkerCounter.WithLabelValues(affinityChecker, "merge-skip-startup")
	affinityMergeCheckerSkipCachedCounter           = checkerCounter.WithLabelValues(affinityChecker, "merge-skip-cached")
	affinityMergeCheckerNoNeedCounter               = checkerCounter.WithLabelValues(affinityChecker, "merge-no-need")
	affinityMergeCheckerNoTargetCounter             = checkerCounter.WithLabelValues(affinityChecker, "merge-no-target")
	affinityMergeCheckerTargetTooBigCounter         = checkerCounter.WithLabelValues(affinityChecker, "merge-target-too-big")
	affinityMergeCheckerNewOpCounter                = checkerCounter.WithLabelValues(affinityChecker, "merge-new-operator")
	affinityMergeCheckerAdjNotExistCounter          = checkerCounter.WithLabelValues(affinityChecker, "merge-adj-not-exist")
	affinityMergeCheckerAdjDifferentGroupCounter    = checkerCounter.WithLabelValues(affinityChecker, "merge-adj-different-group")
	affinityMergeCheckerAdjNotAffinityCounter       = checkerCounter.WithLabelValues(affinityChecker, "merge-adj-not-affinity")
	affinityMergeCheckerAdjDisallowMergeCounter     = checkerCounter.WithLabelValues(affinityChecker, "merge-adj-disallow-merge")
	affinityMergeCheckerAdjAbnormalPeerStoreCounter = checkerCounter.WithLabelValues(affinityChecker, "merge-adj-abnormal-peerstore")
	affinityMergeCheckerAdjUnhealthyCounter         = checkerCounter.WithLabelValues(affinityChecker, "merge-adj-unhealthy")
	affinityMergeCheckerAdjAbnormalReplicaCounter   = checkerCounter.WithLabelValues(affinityChecker, "merge-adj-abnormal-replica")
)
