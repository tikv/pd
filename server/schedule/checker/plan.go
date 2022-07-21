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
	statusNoStoreToAdd         = plan.NewStatus(plan.StatusStoreDraining)
)

type checkNode struct {
	checker string
	region  *core.RegionInfo
	// srcStoreID  uint64
	// destStoreID uint64
	step      string
	status    plan.Status
	children  []*checkNode
	cacheNode bool
}

func newNode(checker string, region *core.RegionInfo, cache bool) *checkNode {
	return &checkNode{
		checker:   checker,
		region:    region,
		status:    statusOK,
		children:  []*checkNode{},
		cacheNode: cache,
	}
}

// StopWith finshed the check plan with status, allways return nil.
func (node *checkNode) StopWith(status plan.Status) []*operator.Operator {
	node.status = status
	return nil
}

func (node *checkNode) StopByPaused() []*operator.Operator {
	return node.StopWith(statusPaused)
}

// FinishWithOps finished the plan with ops.
func (node *checkNode) FinishWithOps(ops ...*operator.Operator) []*operator.Operator {
	return ops
}

func (node *checkNode) faildWithCreateOperator(err error) []*operator.Operator {
	node.step = "CreateOperator"
	node.status = plan.NewStatus(plan.StatusCreateOperatorFailed, err.Error())
	return nil
}

func (node *checkNode) StopAtStep(step string, status plan.Status) []*operator.Operator {
	node.step = step
	node.status = status
	return nil
}

func (node *checkNode) StopAtCreateOps(err error, ops ...*operator.Operator) []*operator.Operator {
	if err != nil {
		return node.faildWithCreateOperator(err)
	}
	return node.FinishWithOps(ops...)
}

// always return false
func (node *checkNode) noNeed(step, reason string) []*operator.Operator {
	status := plan.NewStatus(plan.StatusNoNeed, reason)
	return node.StopAtStep(step, status)
}

func (node *checkNode) newSubCheck(checker string) *checkNode {
	child := newNode(checker, node.region, node.cacheNode)
	if !node.cacheNode {
		node.children = []*checkNode{child}
	} else {
		node.children = append(node.children, child)
	}
	return child
}

// GetPlans returns the plans list for current check.
func (p *checkNode) GetPlans() []plan.Plan {
	plans := make([]plan.Plan, len(p.children))
	for id, plan := range p.children {
		plans[id] = plan
	}
	return plans
}

func (p *checkNode) lastChild() *checkNode {
	len := len(p.children)
	if len > 0 {
		return p.children[len-1]
	}
	return nil
}
