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

type balanceSchedulerBasePlan struct {
	source *core.StoreInfo
	target *core.StoreInfo
	region *core.RegionInfo
	status plan.Status
	step   int
}

func NewBalanceSchedulerBasePlan() *balanceSchedulerBasePlan {
	basePlan := &balanceSchedulerBasePlan{}
	return basePlan
}

func (p *balanceSchedulerBasePlan) Step() int {
	return p.step
}

func (p *balanceSchedulerBasePlan) GetStatus() plan.Status {
	return p.status
}

func (p *balanceSchedulerBasePlan) GetSourceStore() *core.StoreInfo {
	return p.source
}

func (p *balanceSchedulerBasePlan) GetTargetStore() *core.StoreInfo {
	return p.target
}

func (p *balanceSchedulerBasePlan) GetRegion() *core.RegionInfo {
	return p.region
}

func (p *balanceSchedulerBasePlan) SetSourceStore(store *core.StoreInfo) {
	p.source = store
}

func (p *balanceSchedulerBasePlan) SetTargetStore(store *core.StoreInfo) {
	p.target = store
}

func (p *balanceSchedulerBasePlan) SetRegion(region *core.RegionInfo) {
	p.region = region
}

func (p *balanceSchedulerBasePlan) SetStatus(status plan.Status) {
	p.status = status
}

func (p *balanceSchedulerBasePlan) Clone(opts ...plan.PlanOption) plan.Plan {
	plan := &balanceSchedulerBasePlan{
		region: p.region,
		source: p.source,
		target: p.target,
		status: p.status,
		step:   p.step,
	}
	for _, opt := range opts {
		opt(plan)
	}
	return plan
}
