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
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/checker"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
)

// DefaultCacheSize is the default length of waiting list.
const DefaultCacheSize = 1000

// CheckerController is used to manage all checkers.
type CheckerController struct {
	cluster           opt.Cluster
	opts              *config.PersistOptions
	opController      *OperatorController
	learnerChecker    *checker.LearnerChecker
	replicaChecker    *checker.ReplicaChecker
	ruleChecker       *checker.RuleChecker
	mergeChecker      *checker.MergeChecker
	jointStateChecker *checker.JointStateChecker
	regionWaitingList cache.Cache
}

// NewCheckerController create a new CheckerController.
// TODO: isSupportMerge should be removed.
func NewCheckerController(ctx context.Context, cluster opt.Cluster, ruleManager *placement.RuleManager, opController *OperatorController) *CheckerController {
	regionWaitingList := cache.NewDefaultCache(DefaultCacheSize)
	return &CheckerController{
		cluster:           cluster,
		opts:              cluster.GetOpts(),
		opController:      opController,
		learnerChecker:    checker.NewLearnerChecker(cluster),
		replicaChecker:    checker.NewReplicaChecker(cluster, regionWaitingList),
		ruleChecker:       checker.NewRuleChecker(cluster, ruleManager, regionWaitingList),
		mergeChecker:      checker.NewMergeChecker(ctx, cluster),
		jointStateChecker: checker.NewJointStateChecker(cluster),
		regionWaitingList: regionWaitingList,
	}
}

// CheckRegion will check the region and add a new operator if needed.
func (c *CheckerController) CheckRegion(region *core.RegionInfo) []*operator.Operator {
	// If PD has restarted, it need to check learners added before and promote them.
	// Don't check isRaftLearnerEnabled cause it maybe disable learner feature but there are still some learners to promote.
	opController := c.opController

	if op := c.jointStateChecker.Check(region); op != nil {
		return []*operator.Operator{op}
	}

	if c.opts.IsPlacementRulesEnabled() {
		if op := c.ruleChecker.Check(region); op != nil {
			if opController.OperatorCount(operator.OpReplica) < c.opts.GetReplicaScheduleLimit() {
				return []*operator.Operator{op}
			}
			operator.OperatorLimitCounter.WithLabelValues(c.ruleChecker.GetType(), operator.OpReplica.String()).Inc()
			c.regionWaitingList.Put(region.GetID(), nil)
		}
	} else {
		if op := c.learnerChecker.Check(region); op != nil {
			return []*operator.Operator{op}
		}
		if op := c.replicaChecker.Check(region); op != nil {
			if opController.OperatorCount(operator.OpReplica) < c.opts.GetReplicaScheduleLimit() {
				return []*operator.Operator{op}
			}
			operator.OperatorLimitCounter.WithLabelValues(c.replicaChecker.GetType(), operator.OpReplica.String()).Inc()
			c.regionWaitingList.Put(region.GetID(), nil)
		}
	}

	if c.mergeChecker != nil {
		allowed := opController.OperatorCount(operator.OpMerge) < c.opts.GetMergeScheduleLimit()
		if !allowed {
			operator.OperatorLimitCounter.WithLabelValues(c.mergeChecker.GetType(), operator.OpMerge.String()).Inc()
		} else {
			if ops := c.mergeChecker.Check(region); ops != nil {
				// It makes sure that two operators can be added successfully altogether.
				return ops
			}
		}
	}
	return nil
}

// GetMergeChecker returns the merge checker.
func (c *CheckerController) GetMergeChecker() *checker.MergeChecker {
	return c.mergeChecker
}

// GetRuleChecker return the rule checker
func (c *CheckerController) GetRuleChecker() *checker.RuleChecker {
	return c.ruleChecker
}

// SortRegionInfoByMissPeers regions should be sorted by miss peers
func (c *CheckerController) SortRegionInfoByMissPeers(regionIds []*core.RegionInfo) []uint64 {
	ids := make([]uint64, len(regionIds))
	for i, v := range regionIds {
		ids[i] = v.GetID()
	}
	return c.SortRegionIdByMissPeers(ids)
}

// SortMissRegion return regions order by miss count,it will only scan
func (c *CheckerController) SortRegionIdByMissPeers(regionIds []uint64) []uint64 {
	log.Info("aaa")
	levels := c.ruleChecker.GetMaxMissPeer() + 1
	buckets := make([][]uint64, levels)
	for i, _ := range buckets {
		buckets[i] = make([]uint64, 0)
	}
	result := make([]uint64, len(regionIds))

	for _, id := range regionIds {
		region := c.cluster.GetRegion(id)
		if region == nil {
			buckets[0] = append(buckets[0], id)
			continue
		}
		missPeer := c.ruleChecker.GetMissPeer(region)
		buckets[missPeer] = append(buckets[missPeer], id)
	}
	idx := 0
	for i := range buckets {
		for j := range buckets[i] {
			result[idx] = buckets[levels-i][j]
			idx = idx + 1
		}
	}
	return result
}

// GetWaitingRegions returns the regions in the waiting list.
func (c *CheckerController) GetWaitingRegions() []*cache.Item {
	items := c.regionWaitingList.Elems()
	return items
}

// AddWaitingRegion returns the regions in the waiting list.
func (c *CheckerController) AddWaitingRegion(region *core.RegionInfo) {
	c.regionWaitingList.Put(region.GetID(), nil)
}

// RemoveWaitingRegion removes the region from the waiting list.
func (c *CheckerController) RemoveWaitingRegion(id uint64) {
	c.regionWaitingList.Remove(id)
}
