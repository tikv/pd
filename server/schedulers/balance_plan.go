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
	case 0:
		p.source = resource.(*core.StoreInfo)
	case 1:
		p.region = resource.(*core.RegionInfo)
	case 2:
		p.target = resource.(*core.StoreInfo)
	}
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
