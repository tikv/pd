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
	"github.com/tikv/pd/server/core"
)

// Plan is the basic unit for both scheduling and diagnosis.
// TODO: for each scheduler/checker, we can have an individual definition but need to implement the common interfaces.
type Plan interface {
	GetSourceStore() *core.StoreInfo
	GetRegion() *core.RegionInfo
	GetTargetStore() *core.StoreInfo
	SetSourceStore(store *core.StoreInfo)
	SetRegion(region *core.RegionInfo)
	SetTargetStore(store *core.StoreInfo)
	SetStatus(status Status)
	GetStatus() Status
	Step() int
	Clone(...PlanOption) Plan
}

type PlanCollector struct {
	basePlan           Plan
	unschedulablePlans []Plan
	schedulablePlans   []Plan
	enable             bool
}

func NewPlanCollector(enable bool, plan Plan) *PlanCollector {
	return &PlanCollector{
		basePlan:           plan,
		unschedulablePlans: make([]Plan, 0),
		schedulablePlans:   make([]Plan, 0),
		enable:             enable,
	}
}

func (c *PlanCollector) Collect(opts ...PlanOption) {
	if c.enable {
		plan := c.basePlan.Clone(opts...)
		if plan.GetStatus().IsOK() {
			c.schedulablePlans = append(c.schedulablePlans, plan)
		} else {
			c.unschedulablePlans = append(c.unschedulablePlans, plan)
		}
	}
}

func (c *PlanCollector) GetPlans() []Plan {
	return append(c.schedulablePlans, c.unschedulablePlans...)
}

type PlanOption func(plan Plan)

func SetStatus(status Status) PlanOption {
	return func(plan Plan) {
		plan.SetStatus(status)
	}
}

func SetSourceStore(store *core.StoreInfo) PlanOption {
	return func(plan Plan) {
		plan.SetSourceStore(store)
	}
}

func SetRegion(region *core.RegionInfo) PlanOption {
	return func(plan Plan) {
		plan.SetRegion(region)
	}
}

func SetTargetStore(store *core.StoreInfo) PlanOption {
	return func(plan Plan) {
		plan.SetTargetStore(store)
	}
}
