// Copyright 2023 TiKV Project Authors.
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
	"time"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/movingaverage"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
)

const (
	maxDiagnosticResultNum = 10
)

const (
	// Disabled means the current scheduler is unavailable or removed
	Disabled = "disabled"
	// Paused means the current scheduler is paused
	Paused = "paused"
	// Halted means the current scheduler is halted
	Halted = "halted"
	// Scheduling means the current scheduler is generating.
	Scheduling = "scheduling"
	// Pending means the current scheduler cannot generate scheduling operator
	Pending = "pending"
	// Normal means that there is no need to create operators since everything is fine.
	Normal = "normal"
)

// DiagnosableSummaryFunc includes all implementations of plan.Summary.
// And it also includes all schedulers which pd support to diagnose.
var DiagnosableSummaryFunc = map[types.CheckerSchedulerType]plan.Summary{
	types.BalanceRegionScheduler: plan.BalancePlanSummary,
	types.BalanceLeaderScheduler: plan.BalancePlanSummary,
}

// DiagnosticRecorder is used to manage diagnostic for one scheduler.
type DiagnosticRecorder struct {
	schedulerType types.CheckerSchedulerType
	config        sc.SchedulerConfigProvider
	summaryFunc   plan.Summary
	results       *cache.FIFO
}

// NewDiagnosticRecorder creates a new DiagnosticRecorder.
func NewDiagnosticRecorder(tp types.CheckerSchedulerType, config sc.SchedulerConfigProvider) *DiagnosticRecorder {
	summaryFunc, ok := DiagnosableSummaryFunc[tp]
	if !ok {
		return nil
	}
	return &DiagnosticRecorder{
		schedulerType: tp,
		config:        config,
		summaryFunc:   summaryFunc,
		results:       cache.NewFIFO(maxDiagnosticResultNum),
	}
}

// IsAllowed is used to check whether the diagnostic is allowed.
func (d *DiagnosticRecorder) IsAllowed() bool {
	if d == nil {
		return false
	}
	return d.config.IsDiagnosticAllowed()
}

// GetLastResult is used to get the last diagnostic result.
func (d *DiagnosticRecorder) GetLastResult() *DiagnosticResult {
	if d.results.Len() == 0 {
		return nil
	}
	items := d.results.FromLastSameElems(func(i any) (bool, string) {
		result, ok := i.(*DiagnosticResult)
		if result == nil {
			return ok, ""
		}
		return ok, result.Status
	})
	length := len(items)
	if length == 0 {
		return nil
	}

	var resStr string
	firstStatus := items[0].Value.(*DiagnosticResult).Status
	if firstStatus == Pending || firstStatus == Normal {
		wa := movingaverage.NewWeightAllocator(length, 3)
		counter := make(map[uint64]map[plan.Status]float64)
		for i := range length {
			item := items[i].Value.(*DiagnosticResult)
			for storeID, status := range item.StoreStatus {
				if _, ok := counter[storeID]; !ok {
					counter[storeID] = make(map[plan.Status]float64)
				}
				statusCounter := counter[storeID]
				statusCounter[status] += wa.Get(i)
			}
		}
		statusCounter := make(map[plan.Status]uint64)
		for _, store := range counter {
			max := 0.
			curStat := *plan.NewStatus(plan.StatusOK)
			for stat, c := range store {
				if c > max {
					max = c
					curStat = stat
				}
			}
			statusCounter[curStat] += 1
		}
		if len(statusCounter) > 0 {
			for k, v := range statusCounter {
				resStr += fmt.Sprintf("%d store(s) %s; ", v, k.String())
			}
		} else if firstStatus == Pending {
			// This is used to handle pending status because of reach limit in `IsScheduleAllowed`
			resStr = fmt.Sprintf("%v reach limit", d.schedulerType)
		}
	}
	return &DiagnosticResult{
		Name:      d.schedulerType.String(),
		Status:    firstStatus,
		Summary:   resStr,
		Timestamp: uint64(time.Now().Unix()),
	}
}

// SetResultFromStatus is used to set result from status.
func (d *DiagnosticRecorder) SetResultFromStatus(status string) {
	if d == nil {
		return
	}
	result := &DiagnosticResult{
		Name:      d.schedulerType.String(),
		Timestamp: uint64(time.Now().Unix()),
		Status:    status,
	}
	d.results.Put(result.Timestamp, result)
}

// SetResultFromPlans is used to set result from plans.
func (d *DiagnosticRecorder) SetResultFromPlans(ops []*operator.Operator, plans []plan.Plan) {
	if d == nil {
		return
	}
	result := d.analyze(ops, plans, uint64(time.Now().Unix()))
	d.results.Put(result.Timestamp, result)
}

func (d *DiagnosticRecorder) analyze(ops []*operator.Operator, plans []plan.Plan, ts uint64) *DiagnosticResult {
	res := &DiagnosticResult{
		Name:      d.schedulerType.String(),
		Timestamp: ts,
		Status:    Normal,
	}
	// TODO: support more schedulers and checkers
	switch d.schedulerType {
	case types.BalanceRegionScheduler, types.BalanceLeaderScheduler:
		if len(ops) != 0 {
			res.Status = Scheduling
			return res
		}
		res.Status = Pending
		if d.summaryFunc != nil {
			isAllNormal := false
			res.StoreStatus, isAllNormal, _ = d.summaryFunc(plans)
			if isAllNormal {
				res.Status = Normal
			}
		}
		return res
	default:
	}
	// TODO: save plan into result
	return res
}

// DiagnosticResult is used to save diagnostic result and is also used to output.
type DiagnosticResult struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Summary   string `json:"summary"`
	Timestamp uint64 `json:"timestamp"`

	StoreStatus map[uint64]plan.Status `json:"-"`
}
