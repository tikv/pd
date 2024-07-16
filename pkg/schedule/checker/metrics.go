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

import "github.com/prometheus/client_golang/prometheus"

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
)

func init() {
	prometheus.MustRegister(checkerCounter)
	prometheus.MustRegister(regionListGauge)
	prometheus.MustRegister(patrolCheckRegionsGauge)
}

const (
	// NOTE: these types are different from pkg/schedule/config/type.go,
	// they are only used for prometheus metrics to keep the compatibility.
	ruleChecker       = "rule_checker"
	jointStateChecker = "joint_state_checker"
	learnerChecker    = "learner_checker"
	mergeChecker      = "merge_checker"
	replicaChecker    = "replica_checker"
	splitChecker      = "split_checker"
)

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
)
