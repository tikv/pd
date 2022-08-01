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
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
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

type balanceSchedulerBasePlan struct {
	source *core.StoreInfo
	target *core.StoreInfo
	region *core.RegionInfo
	status plan.Status
	step   *balanceStep
}

// NewBalanceSchedulerBasePlan returns a new balanceSchedulerBasePlan
func NewBalanceSchedulerBasePlan() *balanceSchedulerBasePlan {
	step := balanceStep(0)
	basePlan := &balanceSchedulerBasePlan{
		step: &step,
	}
	return basePlan
}

func (p *balanceSchedulerBasePlan) GetStep() plan.Step {
	return p.step
}

func (p *balanceSchedulerBasePlan) GenerateCoreResource(id uint64) {
	switch *p.step {
	case 0:
		p.source = core.NewStoreInfo(&metapb.Store{Id: id})
	case 1:
		p.region = core.NewRegionInfo(&metapb.Region{Id: id}, nil)
	case 2:
		p.target = core.NewStoreInfo(&metapb.Store{Id: id})
	}
}

func (p *balanceSchedulerBasePlan) GetPhases() []map[string]string {
	ret := make([]map[string]string, 0)
	if *p.step >= 0 {
		ret = append(ret, map[string]string{
			"select source store": strconv.FormatUint(p.source.GetID(), 10)})
	}
	if *p.step >= 1 {
		ret = append(ret, map[string]string{
			"select region": strconv.FormatUint(p.region.GetID(), 10)})
	}
	if *p.step >= 2 {
		ret = append(ret, map[string]string{
			"select target store": strconv.FormatUint(p.target.GetID(), 10)})
	}
	if *p.step >= 3 {
		ret = append(ret, map[string]string{
			"verify the plan": ""})
	}
	if *p.step >= 4 {
		ret = append(ret, map[string]string{
			"create operator": ""})
	}
	return ret
}

func (p *balanceSchedulerBasePlan) GetCoreResource(step plan.Step) *plan.CoreResource {
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

func (p *balanceSchedulerBasePlan) GetMaxSelectStep() int {
	return 3
}

func (p *balanceSchedulerBasePlan) GetStatus() plan.Status {
	return p.status
}

func (p *balanceSchedulerBasePlan) SetStatus(status plan.Status) {
	p.status = status
}

func (p *balanceSchedulerBasePlan) Desc() string {
	ret := ""
	if *p.step < 0 {
		return ret + fmt.Sprintf(" status %s step %d", p.status.String(), p.step)
	}
	if p.source != nil {
		ret += fmt.Sprintf("source store %d", p.source.GetID())
	}
	if *p.step < 1 {
		return ret + fmt.Sprintf(" status %s step %d", p.status.String(), p.step)
	}
	if p.region != nil {
		ret += fmt.Sprintf(" region %d", p.region.GetID())
	}
	if *p.step < 2 {
		return ret + fmt.Sprintf(" status %s step %d", p.status.String(), p.step)
	}
	if p.target != nil {
		ret += fmt.Sprintf(" target store %d", p.target.GetID())
	}
	return ret + fmt.Sprintf(" status %s step %d", p.status.String(), p.step)
}

func (p *balanceSchedulerBasePlan) Clone(opts ...plan.Option) plan.Plan {
	plan := &balanceSchedulerBasePlan{
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
