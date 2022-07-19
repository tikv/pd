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
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/plan"
)

var (
	statusOK                   = plan.NewStatus(plan.StatusOK)
	statusPaused               = plan.NewStatus(plan.StatusPaused, "checker paused")
	statusNotInJointState      = plan.NewStatus(plan.StatusNoNeed, "no peer in JointState")
	statusCreateOperatorFailed = plan.NewStatus(plan.StatusCreateOperatorFailed)
)

type checkNode struct {
	checker  string
	regionID uint64
	// srcStoreID  uint64
	// destStoreID uint64
	status plan.Status
	ops    []*operator.Operator
}

type checkPlan struct {
	cacheNode bool
	region    *core.RegionInfo
	nodes     []*checkNode
}

func newNode(checker string, regionID uint64) *checkNode {
	return &checkNode{
		checker:  checker,
		regionID: regionID,
		status:   statusOK,
	}
}

// StopWith finshed the check plan with status, it returns true if the status is ok.
func (node *checkNode) StopWith(status plan.Status) bool {
	node.status = status
	return node.status.IsOK()
}

// FinishWithOps finished the plan with ops.
func (node *checkNode) FinishWithOps(ops ...*operator.Operator) {
	//node.ops := make([]*operator.Operator,len(ops))
	node.ops = ops
}

func newPlan(region *core.RegionInfo, cacheNode bool) *checkPlan {
	return &checkPlan{
		region:    region,
		nodes:     []*checkNode{},
		cacheNode: cacheNode,
	}
}

func (p *checkPlan) newCheckNode(checker string) *checkNode {
	node := newNode(checker, p.region.GetID())
	if !p.cacheNode {
		p.nodes = []*checkNode{node}
	} else {
		p.nodes = append(p.nodes, node)
	}
	return node
}

func (p *checkPlan) Region() *core.RegionInfo {
	return p.region
}

func (p *checkPlan) Operators() []*operator.Operator {
	if len(p.nodes) == 0 {
		return nil
	}
	return p.nodes[len(p.nodes)-1].ops
}

// GetPlans returns the plans list for current check.
func (p *checkPlan) GetPlans() []plan.Plan {
	plans := make([]plan.Plan, len(p.nodes))
	for id, plan := range p.nodes {
		plans[id] = plan
	}
	return plans
}
