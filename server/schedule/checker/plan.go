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
	statusOK              = plan.NewStatus(plan.StatusOK)
	statusPaused          = plan.NewStatus(plan.StatusPaused, "checker paused")
	statusNotInJointState = plan.NewStatus(plan.StatusNoNeed, "no peer in JointState")
)

type checkPlan struct {
	checker string
	region  *core.RegionInfo
	// srcStoreID  uint64
	// destStoreID uint64
	step      string
	status    plan.Status
	children  []*checkPlan
	cacheNode bool
}

func newCheckPlan(checker string, region *core.RegionInfo, cache bool) *checkPlan {
	return &checkPlan{
		checker:   checker,
		region:    region,
		status:    statusOK,
		children:  []*checkPlan{},
		cacheNode: cache,
	}
}

func (node *checkPlan) stopByPaused() []*operator.Operator {
	node.status = statusPaused
	return nil
}

func (node *checkPlan) stopAt(step string, status plan.Status) []*operator.Operator {
	node.step = step
	node.status = status
	return nil
}

func (node *checkPlan) stopAtCreateOps(err error, ops ...*operator.Operator) []*operator.Operator {
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
func (node *checkPlan) noNeed(step string, reason ...string) []*operator.Operator {
	status := plan.NewStatus(plan.StatusNoNeed, reason...)
	return node.stopAt(step, status)
}

func (node *checkPlan) newSubCheck(checker string) *checkPlan {
	if !node.cacheNode {
		return node
	}
	child := newCheckPlan(checker, node.region, node.cacheNode)
	node.children = append(node.children, child)
	return child
}

func (p *checkPlan) lastChild() *checkPlan {
	len := len(p.children)
	if len > 0 {
		return p.children[len-1]
	}
	return nil
}

// GetPlans returns the plans list for current check.
func (p *checkPlan) GetPlans() plan.Plan {
	return p
}
