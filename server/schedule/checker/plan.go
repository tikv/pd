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
	"fmt"

	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/plan"
)

var (
	statusOK              = plan.NewStatus(plan.StatusOK)
	statusPaused          = plan.NewStatus(plan.StatusPaused, "checker paused")
	statusNotInJointState = plan.NewStatus(plan.StatusNoNeed, "no peer in JointState")
)

type checkPlanNode struct {
	checker     string
	region      *core.RegionInfo
	srcStoreID  uint64
	destStoreID uint64
	step        string
	status      plan.Status
	children    []*checkPlanNode
	cache       bool
}

func newCheckPlan(checker string, region *core.RegionInfo, cache bool) *checkPlanNode {
	return &checkPlanNode{
		checker:  checker,
		region:   region,
		status:   statusOK,
		children: []*checkPlanNode{},
		cache:    cache,
	}
}

func (node *checkPlanNode) stopByPaused() []*operator.Operator {
	node.status = statusPaused
	return nil
}

func (node *checkPlanNode) stopAt(step string, status plan.Status) []*operator.Operator {
	node.step = step
	node.status = status
	return nil
}

func (node *checkPlanNode) stopAtCreateOps(err error, ops ...*operator.Operator) []*operator.Operator {
	if last := node.lastChild(); last != nil {
		node.step = last.checker
	} else {
		node.step = "CreateOperator"
	}
	if err != nil {
		node.status = plan.NewStatus(plan.StatusCreateOperatorFailed, err.Error())
		return nil
	}
	node.status = statusOK
	return ops
}

// always return false
func (node *checkPlanNode) noNeed(step string, reason ...string) []*operator.Operator {
	status := plan.NewStatus(plan.StatusNoNeed, reason...)
	return node.stopAt(step, status)
}

func (node *checkPlanNode) newSubCheck(checker string) *checkPlanNode {
	if !node.cache {
		return node
	}
	child := newCheckPlan(checker, node.region, node.cache)
	child.srcStoreID = node.srcStoreID
	child.destStoreID = node.destStoreID
	node.children = append(node.children, child)
	return child
}

func (node *checkPlanNode) lastChild() *checkPlanNode {
	len := len(node.children)
	if len > 0 {
		return node.children[len-1]
	}
	return nil
}

// GetPlans returns the plans list for current check.
func (node *checkPlanNode) GetPlans() plan.Plan {
	return node
}

func (node *checkPlanNode) ToString() []string {
	prefix := node.checker
	plans := make([]string, 0)
	if len(node.children) == 0 {
		value := fmt.Sprintf("%s\nStep:%s\nStatus:%s\nRegion:%d,SrcStore:%d,DescStore:%d", prefix, node.step, node.status.String(), node.region.GetID(), node.srcStoreID, node.destStoreID)
		plans = append(plans, value)
		return plans
	}
	for _, child := range node.children {
		for _, subPlan := range child.ToString() {
			plans = append(plans, fmt.Sprintf("%s->%s", prefix, subPlan))
		}
	}
	return plans
}
