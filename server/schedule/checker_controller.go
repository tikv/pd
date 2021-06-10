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

// DefaultMissPeerSize is the default length of priority queue list.
const DefaultMissPeerSize = 4096

// CheckerController is used to manage all checkers.
type CheckerController struct {
	cluster             opt.Cluster
	opts                *config.PersistOptions
	opController        *OperatorController
	learnerChecker      *checker.LearnerChecker
	replicaChecker      *checker.ReplicaChecker
	ruleChecker         *checker.RuleChecker
	mergeChecker        *checker.MergeChecker
	jointStateChecker   *checker.JointStateChecker
	regionWaitingList   cache.Cache
	regionPriorityQueue *cache.PriorityQueue
}

// NewCheckerController create a new CheckerController.
// TODO: isSupportMerge should be removed.
func NewCheckerController(ctx context.Context, cluster opt.Cluster, ruleManager *placement.RuleManager, opController *OperatorController) *CheckerController {
	regionWaitingList := cache.NewDefaultCache(DefaultCacheSize)
	regionPriorityQueue := cache.NewPriorityQueue(DefaultMissPeerSize)
	return &CheckerController{
		cluster:             cluster,
		opts:                cluster.GetOpts(),
		opController:        opController,
		learnerChecker:      checker.NewLearnerChecker(cluster),
		replicaChecker:      checker.NewReplicaChecker(cluster, regionWaitingList, regionPriorityQueue),
		ruleChecker:         checker.NewRuleChecker(cluster, ruleManager, regionWaitingList, regionPriorityQueue),
		mergeChecker:        checker.NewMergeChecker(ctx, cluster),
		jointStateChecker:   checker.NewJointStateChecker(cluster),
		regionWaitingList:   regionWaitingList,
		regionPriorityQueue: regionPriorityQueue,
	}
}

// CheckRegion will check the region and add a new operator if needed.
func (c *CheckerController) CheckRegion(region *core.RegionInfo) (ops []*operator.Operator) {
	// If PD has restarted, it need to check learners added before and promote them.
	// Don't check isRaftLearnerEnabled cause it maybe disable learner feature but there are still some learners to promote.
	opController := c.opController

	if op := c.jointStateChecker.Check(region); op != nil {
		return []*operator.Operator{op}
	}

	if c.opts.IsPlacementRulesEnabled() {
		if op := c.ruleChecker.Check(region); op != nil {
			ops = []*operator.Operator{op}
		}
	} else {
		if op := c.learnerChecker.Check(region); op != nil {
			return []*operator.Operator{op}
		}
		if op := c.replicaChecker.Check(region); op != nil {
			ops = []*operator.Operator{op}
		}
	}
	if len(ops) > 0 {
		if opController.OperatorCount(operator.OpReplica) < c.opts.GetReplicaScheduleLimit() {
			return ops
		}
		operator.OperatorLimitCounter.WithLabelValues(c.ruleChecker.GetType(), operator.OpReplica.String()).Inc()
		c.regionWaitingList.Put(region.GetID(), nil)
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

// GetMissRegions return miss regions that it's peers less than majority
func (c *CheckerController) GetMissRegions() []*cache.Entry {
	return c.regionPriorityQueue.GetAll()
}

// RemoveMissRegions remove the regions from priority queue
func (c *CheckerController) RemoveMissRegions(ids []uint64) {
	s := make([]interface{}, len(ids))
	for i, v := range ids {
		s[i] = v
	}
	c.regionPriorityQueue.RemoveValues(s)
}

// UpdateMissPeer update region priority
func (c *CheckerController) UpdateMissPeer(entry *cache.Entry) {
	c.regionPriorityQueue.Update(entry, entry.Priority)
}

// GetWaitingRegions returns the regions in the waiting list.
func (c *CheckerController) GetWaitingRegions() []*cache.Item {
	return c.regionWaitingList.Elems()
}

// AddWaitingRegion returns the regions in the waiting list.
func (c *CheckerController) AddWaitingRegion(region *core.RegionInfo) {
	c.regionWaitingList.Put(region.GetID(), nil)
}

// RemoveWaitingRegion removes the region from the waiting list.
func (c *CheckerController) RemoveWaitingRegion(id uint64) {
	c.regionWaitingList.Remove(id)
}
