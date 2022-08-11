// Copyright 2022 TiKV Project Authors.
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
	"fmt"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/plan"
)

type balanceStep int

func (b *balanceStep) Desc() string {
	switch *b {
	case 0:
		return "select source store"
	case 1:
		return "select region"
	case 2:
		return "select target store"
	case 3:
		return "verify the plan"
	case 4:
		return "create operator"
	}
	return ""
}

func (b *balanceStep) Number() int {
	return int(*b)
}

func (b *balanceStep) Add() {
	*b++
}

func (b *balanceStep) Sub() {
	*b--
}

type balanceSchedulerPlan struct {
	source *core.StoreInfo
	target *core.StoreInfo
	region *core.RegionInfo
	status plan.Status
	step   *balanceStep
}

// NewBalanceSchedulerPlan returns a new balanceSchedulerBasePlan
func NewBalanceSchedulerPlan() *balanceSchedulerPlan {
	step := balanceStep(0)
	basePlan := &balanceSchedulerPlan{
		step: &step,
	}
	return basePlan
}

func (p *balanceSchedulerPlan) GetStep() plan.Step {
	return p.step
}

func (p *balanceSchedulerPlan) GenerateCoreResource(resource interface{}) {
	switch *p.step {
	case 0:
		p.source = resource.(*core.StoreInfo)
	case 1:
		p.region = resource.(*core.RegionInfo)
	case 2:
		p.target = resource.(*core.StoreInfo)
	}
}

func (p *balanceSchedulerPlan) GetCoreResource(step plan.Step) *plan.CoreResource {
	switch step.Number() {
	case 0:
		if *p.step < 0 {
			return nil
		}
		return plan.NewStoreResource(p.source.GetID())
	case 1:
		if *p.step < 1 {
			return nil
		}
		return plan.NewRegionResource(p.region.GetID())
	case 2:
		if *p.step < 2 {
			return nil
		}
		return plan.NewStoreResource(p.target.GetID())
	}
	return plan.NewEmptyResource()
}

func (p *balanceSchedulerPlan) GetMaxSelectStep() int {
	return 3
}

func (p *balanceSchedulerPlan) GetStatus() plan.Status {
	return p.status
}

func (p *balanceSchedulerPlan) SetStatus(status plan.Status) {
	p.status = status
}

func (p *balanceSchedulerPlan) Clone(opts ...plan.Option) plan.Plan {
	plan := &balanceSchedulerPlan{
		status: p.status,
	}
	step := *p.step
	plan.step = &step
	if *p.step > 0 {
		plan.source = p.source
	}
	if *p.step > 1 {
		plan.region = p.region
	}
	if *p.step > 2 {
		plan.target = p.target
	}
	for _, opt := range opts {
		opt(plan)
	}
	return plan
}

type BalanceSchedulerPlanAnalyzer struct {
}

func (a *BalanceSchedulerPlanAnalyzer) Summary(plans interface{}) (string, error) {
	ps, ok := plans.([]*balanceSchedulerPlan)
	if !ok {
		return "", errs.ErrDiagnoseLoadPlanError
	}
	secondGroup := make(map[plan.Status]uint64)
	var firstGroup map[uint64]map[plan.Status]int
	maxStep := -1
	for _, p := range ps {
		step := p.GetStep().Number()
		if step > maxStep {
			firstGroup = make(map[uint64]map[plan.Status]int)
			maxStep = p.GetStep().Number()
		} else if step < maxStep {
			continue
		}
		var store uint64
		if step == 1 {
			store = p.source.GetID()
		} else {
			store = p.GetCoreResource(p.GetStep()).ID
		}
		if _, ok := firstGroup[store]; !ok {
			firstGroup[store] = make(map[plan.Status]int)
		}
		firstGroup[store][p.status]++
	}

	for _, status := range firstGroup {
		max := 0
		curstat := plan.NewStatus(plan.StatusOK)
		for stat, c := range status {
			if stat.Priority() > curstat.Priority() || (stat.Priority() == curstat.Priority() && c >= max) {
				max = c
				curstat = stat
			}
		}
		secondGroup[curstat] += 1
	}
	var resstr string
	for k, v := range secondGroup {
		resstr += fmt.Sprintf("%d stores are filtered by %s; ", v, k.String())
	}
	return resstr, nil
}
