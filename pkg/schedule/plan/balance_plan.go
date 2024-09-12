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

package plan

import (
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
)

const (
	pickSource = iota
	pickRegion
	pickTarget
	// We can think of shouldBalance as a filtering step for target, except that the current implementation is separate.
	// shouldBalance
	// createOperator
)

// BalanceSchedulerPlan is a plan for balance scheduler
type BalanceSchedulerPlan struct {
	Source *core.StoreInfo
	Target *core.StoreInfo
	Region *core.RegionInfo
	Status *Status
	Step   int
}

// NewBalanceSchedulerPlan returns a new balanceSchedulerBasePlan
func NewBalanceSchedulerPlan() *BalanceSchedulerPlan {
	basePlan := &BalanceSchedulerPlan{
		Status: NewStatus(StatusOK),
	}
	return basePlan
}

// GetStep is used to get current step of plan.
func (p *BalanceSchedulerPlan) GetStep() int {
	return p.Step
}

// SetResource is used to set resource for current step.
func (p *BalanceSchedulerPlan) SetResource(resource any) {
	switch p.Step {
	// for balance-region/leader scheduler, the first step is selecting stores as source candidates.
	case pickSource:
		p.Source = resource.(*core.StoreInfo)
	// the second step is selecting region from source store.
	case pickRegion:
		p.Region = resource.(*core.RegionInfo)
	// the third step is selecting stores as target candidates.
	case pickTarget:
		p.Target = resource.(*core.StoreInfo)
	}
}

// SetResourceWithStep is used to set resource for specific step.
func (p *BalanceSchedulerPlan) SetResourceWithStep(resource any, step int) {
	p.Step = step
	p.SetResource(resource)
}

// GetResource is used to get resource for specific step.
func (p *BalanceSchedulerPlan) GetResource(step int) uint64 {
	if p.Step < step {
		return 0
	}
	// Please use with care. Add a nil check if need in the future
	switch step {
	case pickSource:
		return p.Source.GetID()
	case pickRegion:
		return p.Region.GetID()
	case pickTarget:
		return p.Target.GetID()
	}
	return 0
}

// GetStatus is used to get status of plan.
func (p *BalanceSchedulerPlan) GetStatus() *Status {
	return p.Status
}

// SetStatus is used to set status of plan.
func (p *BalanceSchedulerPlan) SetStatus(status *Status) {
	p.Status = status
}

// Clone is used to clone a new plan.
func (p *BalanceSchedulerPlan) Clone(opts ...Option) Plan {
	plan := &BalanceSchedulerPlan{
		Status: p.Status,
	}
	plan.Step = p.Step
	if p.Step > pickSource {
		plan.Source = p.Source
	}
	if p.Step > pickRegion {
		plan.Region = p.Region
	}
	if p.Step > pickTarget {
		plan.Target = p.Target
	}
	for _, opt := range opts {
		opt(plan)
	}
	return plan
}

// BalancePlanSummary is used to summarize for BalancePlan
func BalancePlanSummary(plans []Plan) (map[uint64]Status, bool, error) {
	// storeStatusCounter is used to count the number of various statuses of each store
	storeStatusCounter := make(map[uint64]map[Status]int)
	// statusCounter is used to count the number of status which is regarded as best status of each store
	statusCounter := make(map[uint64]Status)
	storeMaxStep := make(map[uint64]int)
	normal := true
	for _, pi := range plans {
		p, ok := pi.(*BalanceSchedulerPlan)
		if !ok {
			return nil, false, errs.ErrDiagnosticLoadPlan
		}
		step := p.GetStep()
		// We can simply think of createOperator as a filtering step for target in BalancePlanSummary.
		if step > pickTarget {
			step = pickTarget
		}
		var store uint64
		// `step == pickRegion` is a special processing in summary, because we want to exclude the factor of region
		// and consider the failure as the status of source store.
		if step == pickRegion {
			store = p.Source.GetID()
		} else {
			store = p.GetResource(step)
		}
		maxStep, ok := storeMaxStep[store]
		if !ok {
			maxStep = -1
		}
		if step > maxStep {
			storeStatusCounter[store] = make(map[Status]int)
			storeMaxStep[store] = step
		} else if step < maxStep {
			continue
		}
		if !p.Status.IsNormal() {
			normal = false
		}
		storeStatusCounter[store][*p.Status]++
	}

	for id, store := range storeStatusCounter {
		max := 0
		curStat := *NewStatus(StatusOK)
		for stat, c := range store {
			if balancePlanStatusComparer(max, curStat, c, stat) {
				max = c
				curStat = stat
			}
		}
		statusCounter[id] = curStat
	}
	return statusCounter, normal, nil
}

// balancePlanStatusComparer returns true if new status is better than old one.
func balancePlanStatusComparer(oldStatusCount int, oldStatus Status, newStatusCount int, newStatus Status) bool {
	if newStatus.Priority() != oldStatus.Priority() {
		return newStatus.Priority() > oldStatus.Priority()
	}
	if newStatusCount != oldStatusCount {
		return newStatusCount > oldStatusCount
	}
	return newStatus.StatusCode < oldStatus.StatusCode
}
