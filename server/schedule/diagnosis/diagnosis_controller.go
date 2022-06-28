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

package diagnosis

import (
	"fmt"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

/**
concise:
	xx can't create schedule operator from store-xx in (source-filter/region-filter/target-filter/approve) step.
			step 1 : store-xx is filtered because of xxx
			step 2 : most region in store-xx are filtered because of xxx
			step 3 : most target store are filtered because of xxx
			step 4 : schedule plan can't be promoted because of xxx

detailed:
	xx can't create schedule operator from store-xx in (source-filter/region-filter/target-filter/approve) step.
		if failed in step 1:
			store-xx is filtered because of xxx
		else:
			The diagram below shows the reasons for failure in the attempts to build a scheduling plan for store-xx.
			|	Step						|		Failure Reason			|		Ratio		|	Sample Object	|
			|	Step 2: search region		|		Pending Peer			|		10%			|		region-1	|
			|	Step 2: search region		|		Downing Peer			|		30%			|		region-2	|
			|	Step 2: search region		|		not fit replicated		|		30%			|		region-3	|
**/

type SampleObjects []uint64

func (o SampleObjects) Object(step ScheduleStep) string {
	// str := ""
	// if len(o) < 1 {
	// 	return str
	// }
	// for idx, i := range o {
	// 	if idx > 0 {
	// 		str += ","
	// 	}
	// 	str += strconv.FormatUint(i, 10)
	// }
	switch step {
	case 0:
		return fmt.Sprintf("store-%d", o)
	case 1:
		return fmt.Sprintf("region-%d", o)
	case 2:
		return fmt.Sprintf("store-%d", o)
	case 3:
		return fmt.Sprintf("store-%d", o)
	}
	return ""
}

type SampleObject uint64

func (o SampleObject) Object(step ScheduleStep) string {
	switch step {
	case 0:
		return fmt.Sprintf("store-%d", o)
	case 1:
		return fmt.Sprintf("region-%d", o)
	case 2:
		return fmt.Sprintf("store-%d", o)
	case 3:
		return fmt.Sprintf("store-%d", o)
	}
	return ""
}

type ScheduleStep int

func (s ScheduleStep) Name() string {
	switch s {
	case 0:
		return "search-source-store step"
	case 1:
		return "search-regoin step"
	case 2:
		return "search-target-store step"
	case 3:
		return "aprove-plan step"
	}
	return ""
}

func (s ScheduleStep) Description() string {
	switch s {
	case 0:
		return "Step 1: search source store"
	case 1:
		return "Step 2: search regoin"
	case 2:
		return "Step 3: search target store"
	case 3:
		return "Step 4: aprove plan"
	}
	return ""
}

func (s ScheduleStep) Reason(reason string, objectID uint64) string {
	switch s {
	case 0:
		return fmt.Sprintf("store-%d is filtered as source because of "+reason+".", objectID)
	case 1:
		return fmt.Sprintf("Most regions in store-%d are filtered because of "+reason+".", objectID)
	case 2:
		return "Most targets store are filtered because of " + reason + "."
	case 3:
		return "Scheduling plan can't be promoted because of " + reason + "."
	}
	return ""
}

type MatrixDiagnosisResult struct {
	SchedulerName string                        `json:"scheduler"`
	StoreID       []*MatrixStoreDiagnosisResult `json:"stores"`
}

type MatrixStoreDiagnosisResult struct {
	StoreID string                         `json:"store"`
	Targets []*MatrixTargetDiagnosisResult `json:"targets"`
}

type MatrixTargetDiagnosisResult struct {
	TargetID string           `json:"target"`
	Detailed []*ReasonMetrics `json:"detailed"`
}

type StepDiagnosisResult struct {
	SchedulerName string           `json:"scheduler"`
	StoreID       uint64           `json:"store"`
	Schedulable   bool             `json:"Schedulable"`
	Description   string           `json:"description"`
	Reason        string           `json:"Reason"`
	Detailed      []*ReasonMetrics `json:"detailed"`
}

type DiagnosisController struct {
	dryRun bool
	//mu syncutil.RWMutex
	//enable    bool
	//cache        *cache.TTLUint64
	// scope         string
	// storeReaders  map[uint64]*DiagnosisAnalyzer
	// historyRecord map[uint64]*DiagnosisAnalyzer
	// currentReader  *DiagnosisStoreRecoder
	currentStep     ScheduleStep
	currentSource   uint64
	currentRegion   uint64
	currentTarget   uint64
	plans           []*SchedulePlan
	schedulablePlan *SchedulePlan
}

func NewDiagnosisController(dryRun bool) *DiagnosisController {

	return &DiagnosisController{
		dryRun: dryRun,
		plans:  make([]*SchedulePlan, 0),
		//enable:    false,
		//cache:        cache.NewIDTTL(ctx, time.Minute, 5*time.Minute),
	}
}

func (c *DiagnosisController) Debug() {
	log.Info("DiagnosisController Debug", zap.Int("currentStep", int(c.currentStep)), zap.Uint64("currentSource", c.currentSource), zap.Uint64("currentRegion", c.currentRegion), zap.Uint64("currentTarget", c.currentTarget))
}

func (c *DiagnosisController) GetSourceStore() uint64 {
	return c.currentSource
}

func (c *DiagnosisController) GetTargetStore() uint64 {
	return c.currentTarget
}

func (c *DiagnosisController) GetRegion() uint64 {
	return c.currentRegion
}

func (c *DiagnosisController) SetSelectedObject(objectID uint64) {
	switch c.currentStep {
	case 1:
		c.currentSource = objectID
	case 2:
		c.currentRegion = objectID
	case 3:
		c.currentTarget = objectID
	}
}

func (c *DiagnosisController) setObject(objectID uint64) {
	switch c.currentStep {
	case 0:
		c.currentSource = objectID
	case 1:
		c.currentRegion = objectID
	case 2:
		c.currentTarget = objectID
	}
}

func (c *DiagnosisController) NextStep() {
	c.currentStep++
}

func (c *DiagnosisController) LastStep() {
	c.currentStep--
}

func (c *DiagnosisController) GetPlans() []*SchedulePlan {
	return c.plans
}

func (c *DiagnosisController) Diagnose(objectID uint64, reason string) {
	if !c.dryRun {
		return
	}
	c.setObject(objectID)
	c.recordNotSchedulablePlan(reason)
}

func (c *DiagnosisController) RecordSchedulablePlan(objectID uint64) {
	if !c.dryRun {
		return
	}
	c.setObject(objectID)
	c.recordSchedulablePlan()
}

func (c *DiagnosisController) recordNotSchedulablePlan(reason string) {
	plan := &SchedulePlan{
		Step:        c.currentStep,
		Source:      c.currentSource,
		Target:      c.currentTarget,
		Region:      c.currentRegion,
		reason:      reason,
		schedulable: false,
	}
	if c.currentStep < 2 {
		plan.Target = 0
	}
	if c.currentStep < 1 {
		plan.Region = 0
	}
	c.plans = append(c.plans, plan)
}

func (c *DiagnosisController) recordSchedulablePlan() {
	c.schedulablePlan = &SchedulePlan{
		Step:        c.currentStep,
		Source:      c.currentSource,
		Target:      c.currentTarget,
		Region:      c.currentRegion,
		schedulable: true,
	}
}

type ReasonMetrics struct {
	Step         string
	Reason       string
	Ratio        string
	SampleObject string
}

type SchedulePlan struct {
	Step        ScheduleStep
	Source      uint64
	Target      uint64
	Region      uint64
	reason      string
	schedulable bool
}

func (s *SchedulePlan) GetSourceStore() uint64 {
	return s.Source
}
func (s *SchedulePlan) GetRegion() uint64 {
	return s.Region
}
func (s *SchedulePlan) GetTargetStore() uint64 {
	return s.Target
}
func (s *SchedulePlan) GetStep() ScheduleStep {
	return s.Step
}
func (s *SchedulePlan) GetReason() string {
	return s.reason
}

func (s *SchedulePlan) IsSchedulable() bool {
	return s.schedulable
}

func (s *SchedulePlan) GetFailObject() uint64 {
	switch s.Step {
	case 0:
		return s.Source
	case 1:
		return s.Region
	case 2:
		return s.Target
	case 3:
		return s.Target
	default:
		return 0
	}
}
