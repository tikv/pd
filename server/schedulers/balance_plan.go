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
	case 0:
		p.source = resource.(*core.StoreInfo)
	// the second step is selecting region from source store.
	case 1:
		p.region = resource.(*core.RegionInfo)
	// the third step is selecting stores as target candidates.
	case 2:
		p.target = resource.(*core.StoreInfo)
	}
}

func (p *balanceSchedulerPlan) GetResource(step int) uint64 {
	switch step {
	case 0:
		if p.step < step {
			return 0
		}
		return p.source.GetID()
	case 1:
		if p.step < step {
			return 0
		}
		return p.region.GetID()
	case 2:
		if p.step < step {
			return 0
		}
		return p.target.GetID()
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
	if p.step > 0 {
		plan.source = p.source
	}
	if p.step > 1 {
		plan.region = p.region
	}
	if p.step > 2 {
		plan.target = p.target
	}
	for _, opt := range opts {
		opt(plan)
	}
	return plan
}

func BalancePlanSummary(plans []plan.Plan) (string, error) {
	// storeStatusCounter is used to
	var storeStatusCounter map[uint64]map[plan.Status]int
	statusCounter := make(map[plan.Status]uint64)
	maxStep := -1
	for _, pi := range plans {
		p, ok := pi.(*balanceSchedulerPlan)
		if !ok {
			return "", errs.ErrDiagnoseLoadPlanError
		}
		step := p.GetStep()
		// we don't consider the situation for verification step
		if step > 2 {
			continue
		}
		if step > maxStep {
			storeStatusCounter = make(map[uint64]map[plan.Status]int)
			maxStep = p.GetStep()
		} else if step < maxStep {
			continue
		}
		var store uint64
		if step == 1 {
			store = p.source.GetID()
		} else {
			store = p.GetResource(p.GetStep())
		}
		if _, ok := storeStatusCounter[store]; !ok {
			storeStatusCounter[store] = make(map[plan.Status]int)
		}
		storeStatusCounter[store][*p.status]++
	}

	for _, store := range storeStatusCounter {
		max := 0
		curstat := *plan.NewStatus(plan.StatusOK)
		for stat, c := range store {
			if balancePlanStatusComparer(max, curstat, c, stat) {
				max = c
				curstat = stat
			}
		}
		statusCounter[curstat] += 1
	}
	var resstr string
	for k, v := range statusCounter {
		resstr += fmt.Sprintf("%d stores are filtered by %s; ", v, k.String())
	}
	return resstr, nil
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
