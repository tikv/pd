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

// Plan is the basic unit for both scheduling and diagnosis.
// TODO: for each scheduler/checker, we can have an individual definition but need to implement the common interfaces.
type Plan interface {
	GetStep() Step
	GetStatus() Status
	GetCoreResource(step Step) *CoreResource

	Clone(ops ...Option) Plan // generate plan for clone option
	GenerateCoreResource(interface{})
	SetStatus(Status)
}

type Analyzer interface {
	Summary(interface{}) (string, error)
}

type Step interface {
	Number() int
	Desc() string
}

// Collector is a plan collector
type Collector struct {
	basePlan           Plan
	unschedulablePlans []Plan
	schedulablePlans   []Plan
	enable             bool
}

// NewCollector returns a new Collector
func NewCollector(enable bool, plan Plan) *Collector {
	return &Collector{
		basePlan:           plan,
		unschedulablePlans: make([]Plan, 0),
		schedulablePlans:   make([]Plan, 0),
		enable:             enable,
	}
}

// Collect is used to collect a new Plan and save it into PlanCollector
func (c *Collector) Collect(opts ...Option) {
	if c.enable {
		plan := c.basePlan.Clone(opts...)
		if plan.GetStatus().IsOK() {
			c.schedulablePlans = append(c.schedulablePlans, plan)
		} else {
			c.unschedulablePlans = append(c.unschedulablePlans, plan)
		}
	}
}

// GetPlans returns all plans and the first part plans are schedulable
func (c *Collector) GetPlans() []Plan {
	return append(c.schedulablePlans, c.unschedulablePlans...)
}

// Option is to do some action for plan
type Option func(plan Plan)

// SetStatus is used to set status for plan
func SetStatus(status Status) Option {
	return func(plan Plan) {
		plan.SetStatus(status)
	}
}

// GenerateCoreResource is used to generate Resource for plan
func GenerateCoreResource(resource interface{}) Option {
	return func(plan Plan) {
		plan.GenerateCoreResource(resource)
	}
}
