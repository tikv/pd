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
	GetStep() int
	GetStatus() *Status
	GetResource(int) uint64

	Clone(ops ...Option) Plan // generate plan for clone option
	SetResource(any)
	// SetResourceWithStep is used to set resource for specific step.
	// The meaning of step is different for different plans.
	// Such as balancePlan, pickSource = 0, pickRegion = 1, pickTarget = 2
	SetResourceWithStep(resource any, step int)
	SetStatus(*Status)
}

// Summary is used to analyze plan simply.
// It will return the status of store.
type Summary func([]Plan) (map[uint64]Status, bool, error)

// Collector is a plan collector.
type Collector struct {
	basePlan           Plan
	unschedulablePlans []Plan
	schedulablePlans   []Plan
}

// NewCollector returns a new Collector.
func NewCollector(plan Plan) *Collector {
	return &Collector{
		basePlan:           plan,
		unschedulablePlans: make([]Plan, 0),
		schedulablePlans:   make([]Plan, 0),
	}
}

// Collect is used to collect a new Plan and save it into PlanCollector.
func (c *Collector) Collect(opts ...Option) {
	if c == nil {
		return
	}
	plan := c.basePlan.Clone(opts...)
	if plan.GetStatus().IsOK() {
		c.schedulablePlans = append(c.schedulablePlans, plan)
	} else {
		c.unschedulablePlans = append(c.unschedulablePlans, plan)
	}
}

// GetPlans returns all plans and the first part plans are schedulable.
func (c *Collector) GetPlans() []Plan {
	if c == nil {
		return nil
	}
	return append(c.schedulablePlans, c.unschedulablePlans...)
}

// Option is to do some action for plan.
type Option func(plan Plan)

// SetStatus is used to set status for plan.
func SetStatus(status *Status) Option {
	return func(plan Plan) {
		plan.SetStatus(status)
	}
}

// SetResource is used to generate Resource for plan.
func SetResource(resource any) Option {
	return func(plan Plan) {
		plan.SetResource(resource)
	}
}

// SetResourceWithStep is used to generate Resource for plan.
func SetResourceWithStep(resource any, step int) Option {
	return func(plan Plan) {
		plan.SetResourceWithStep(resource, step)
	}
}
