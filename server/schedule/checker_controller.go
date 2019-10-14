// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule/checker"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
)

// CheckerController is used to manage all checkers.
type CheckerController struct {
	cluster          opt.Cluster
	opController     *OperatorController
	learnerChecker   *checker.LearnerChecker
	replicaChecker   *checker.ReplicaChecker
	namespaceChecker *checker.NamespaceChecker
	mergeChecker     *checker.MergeChecker
}

// NewCheckerController create a new CheckerController.
// TODO: isSupportMerge should be removed.
func NewCheckerController(cluster opt.Cluster, classifier namespace.Classifier, opController *OperatorController) *CheckerController {
	return &CheckerController{
		cluster:          cluster,
		opController:     opController,
		learnerChecker:   checker.NewLearnerChecker(),
		replicaChecker:   checker.NewReplicaChecker(cluster, classifier),
		namespaceChecker: checker.NewNamespaceChecker(cluster, classifier),
		mergeChecker:     checker.NewMergeChecker(cluster, classifier),
	}
}

// CheckRegion will check the region and add a new operator if needed.
func (c *CheckerController) CheckRegion(region *core.RegionInfo) bool {
	// If PD has restarted, it need to check learners added before and promote them.
	// Don't check isRaftLearnerEnabled cause it maybe disable learner feature but there are still some learners to promote.
	opController := c.opController

	if op := c.learnerChecker.Check(region); op != nil {
		if opController.AddOperator(op) {
			return true
		}
	}

	if opController.OperatorCount(operator.OpLeader) < c.cluster.GetLeaderScheduleLimit() &&
		opController.OperatorCount(operator.OpRegion) < c.cluster.GetRegionScheduleLimit() &&
		opController.OperatorCount(operator.OpReplica) < c.cluster.GetReplicaScheduleLimit() {
		if op := c.namespaceChecker.Check(region); op != nil {
			if opController.AddWaitingOperator(op) {
				return true
			}
		}
	}

	if opController.OperatorCount(operator.OpReplica) < c.cluster.GetReplicaScheduleLimit() {
		if op := c.replicaChecker.Check(region); op != nil {
			if opController.AddWaitingOperator(op) {
				return true
			}
		}
	}
	if c.mergeChecker != nil && opController.OperatorCount(operator.OpMerge) < c.cluster.GetMergeScheduleLimit() {
		if ops := c.mergeChecker.Check(region); ops != nil {
			// It makes sure that two operators can be added successfully altogether.
			if opController.AddWaitingOperator(ops...) {
				return true
			}
		}
	}
	return false
}

// GetMergeChecker returns the merge checker.
func (c *CheckerController) GetMergeChecker() *checker.MergeChecker {
	return c.mergeChecker
}
