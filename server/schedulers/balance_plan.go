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
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/plan"
)

const (
	pickSource = iota
	pickRegion
	pickTarget
	// We can think of shouldBalance as a filtering step for target, except that the current implementation is separate.
	// shouldBalance
	// createOperator
)

type balanceSchedulerPlan struct {
	source *core.StoreInfo
	target *core.StoreInfo
	region *core.RegionInfo
	status *plan.Status
	step   int
}

// NewBalanceSchedulerPlan returns a new balanceSchedulerBasePlan
func NewBalanceSchedulerPlan() *balanceSchedulerPlan {
	basePlan := &balanceSchedulerPlan{
		status: plan.NewStatus(plan.StatusOK),
	}
	return basePlan
}

func (p *balanceSchedulerPlan) GetStep() int {
	return p.step
}

func (p *balanceSchedulerPlan) SetResource(resource interface{}) {
	switch p.step {
	// for balance-region/leader scheduler, the first step is selecting stores as source candidates.
	case pickSource:
		p.source = resource.(*core.StoreInfo)
	// the second step is selecting region from source store.
	case pickRegion:
		p.region = resource.(*core.RegionInfo)
	// the third step is selecting stores as target candidates.
	case pickTarget:
		p.target = resource.(*core.StoreInfo)
	}
}

func (p *balanceSchedulerPlan) SetResourceWithStep(resource interface{}, step int) {
	p.step = step
	p.SetResource(resource)
}

func (p *balanceSchedulerPlan) GetResource(step int) uint64 {
	if p.step < step {
		return 0
	}
	switch step {
	case pickSource:
		if p.source != nil {
			return p.source.GetID()
		} else {
			return 0
		}
	case pickRegion:
		if p.region != nil {
			return p.region.GetID()
		} else {
			return 0
		}
	case pickTarget:
		if p.target != nil {
			return p.target.GetID()
		} else {
			return 0
		}
	}
	return 0
}

func (p *balanceSchedulerPlan) GetStatus() *plan.Status {
	return p.status
}

func (p *balanceSchedulerPlan) SetStatus(status *plan.Status) {
	p.status = status
}

func (p *balanceSchedulerPlan) Clone(opts ...plan.Option) plan.Plan {
	plan := &balanceSchedulerPlan{
		status: p.status,
	}
	plan.step = p.step
	if p.step > pickSource {
		plan.source = p.source
	}
	if p.step > pickRegion {
		plan.region = p.region
	}
	if p.step > pickTarget {
		plan.target = p.target
	}
	for _, opt := range opts {
		opt(plan)
	}
	return plan
}

// BalancePlanSummary is used to summarize for BalancePlan
func BalancePlanSummary(plans []plan.Plan) (map[uint64]plan.Status, bool, error) {
	// storeStatusCounter is used to count the number of various statuses of each store
	var storeStatusCounter map[uint64]map[plan.Status]int
	// statusCounter is used to count the number of status which is regarded as best status of each store
	statusCounter := make(map[uint64]plan.Status)
	storeMaxStep := make(map[uint64]int)
	normal := true
	for _, pi := range plans {
		p, ok := pi.(*balanceSchedulerPlan)
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
			store = p.source.GetID()
		} else {
			store = p.GetResource(step)
		}
		maxStep := storeMaxStep[store]
		if step > maxStep {
			storeStatusCounter = make(map[uint64]map[plan.Status]int)
			storeMaxStep[store] = step
		} else if step < maxStep {
			continue
		}
		if !p.status.IsNormal() {
			normal = false
		}
		if _, ok := storeStatusCounter[store]; !ok {
			storeStatusCounter[store] = make(map[plan.Status]int)
		}
		storeStatusCounter[store][*p.status]++
	}

	for id, store := range storeStatusCounter {
		max := 0
		curStat := *plan.NewStatus(plan.StatusOK)
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
func balancePlanStatusComparer(oldStatusCount int, oldStatus plan.Status, newStatusCount int, newStatus plan.Status) bool {
	if newStatus.Priority() != oldStatus.Priority() {
		return newStatus.Priority() > oldStatus.Priority()
	}
	if newStatusCount != oldStatusCount {
		return newStatusCount > oldStatusCount
	}
	return newStatus.StatusCode < oldStatus.StatusCode
}
