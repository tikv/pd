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

package schedule

import (
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/movingaverage"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"go.uber.org/zap"
)

const (
	// disabled means the current scheduler is unavailable or removed
	disabled = "disabled"
	// paused means the current scheduler is paused
	paused = "paused"
	// scheduling means the current scheduler is generating.
	scheduling = "scheduling"
	// pending means the current scheduler cannot generate scheduling operator
	pending = "pending"
	// normal means that there is no need to create operators since everything is fine.
	normal = "normal"
)

const (
	maxDiagnosticResultNum = 10
)

// DiagnosableSummaryFunc includes all implementations of plan.Summary.
// And it also includes all schedulers which pd support to diagnose.
var DiagnosableSummaryFunc = map[string]plan.Summary{
	schedulers.BalanceRegionName: schedulers.BalancePlanSummary,
	schedulers.BalanceLeaderName: schedulers.BalancePlanSummary,
}

type diagnosticManager struct {
	cluster   sche.ClusterInformer
	recorders map[string]*diagnosticRecorder
}

func newDiagnosticManager(cluster sche.ClusterInformer) *diagnosticManager {
	recorders := make(map[string]*diagnosticRecorder)
	for name := range DiagnosableSummaryFunc {
		recorders[name] = newDiagnosticRecorder(name, cluster)
	}
	return &diagnosticManager{
		cluster:   cluster,
		recorders: recorders,
	}
}

func (d *diagnosticManager) getDiagnosticResult(name string) (*DiagnosticResult, error) {
	if !d.cluster.GetOpts().IsDiagnosticAllowed() {
		return nil, errs.ErrDiagnosticDisabled
	}

	isSchedulerExisted, _ := d.cluster.IsSchedulerExisted(name)
	isDisabled, _ := d.cluster.IsSchedulerDisabled(name)
	if !isSchedulerExisted || isDisabled {
		ts := uint64(time.Now().Unix())
		res := &DiagnosticResult{Name: name, Timestamp: ts, Status: disabled}
		return res, nil
	}

	recorder := d.getRecorder(name)
	if recorder == nil {
		return nil, errs.ErrSchedulerUndiagnosable.FastGenByArgs(name)
	}
	result := recorder.getLastResult()
	if result == nil {
		return nil, errs.ErrNoDiagnosticResult.FastGenByArgs(name)
	}
	return result, nil
}

func (d *diagnosticManager) getRecorder(name string) *diagnosticRecorder {
	return d.recorders[name]
}

// diagnosticRecorder is used to manage diagnostic for one scheduler.
type diagnosticRecorder struct {
	schedulerName string
	cluster       sche.ClusterInformer
	summaryFunc   plan.Summary
	results       *cache.FIFO
}

func newDiagnosticRecorder(name string, cluster sche.ClusterInformer) *diagnosticRecorder {
	summaryFunc, ok := DiagnosableSummaryFunc[name]
	if !ok {
		log.Error("can't find summary function", zap.String("scheduler-name", name))
		return nil
	}
	return &diagnosticRecorder{
		cluster:       cluster,
		schedulerName: name,
		summaryFunc:   summaryFunc,
		results:       cache.NewFIFO(maxDiagnosticResultNum),
	}
}

func (d *diagnosticRecorder) isAllowed() bool {
	if d == nil {
		return false
	}
	return d.cluster.GetOpts().IsDiagnosticAllowed()
}

func (d *diagnosticRecorder) getLastResult() *DiagnosticResult {
	if d.results.Len() == 0 {
		return nil
	}
	items := d.results.FromLastSameElems(func(i interface{}) (bool, string) {
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
	if firstStatus == pending || firstStatus == normal {
		wa := movingaverage.NewWeightAllocator(length, 3)
		counter := make(map[uint64]map[plan.Status]float64)
		for i := 0; i < length; i++ {
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
		} else if firstStatus == pending {
			// This is used to handle pending status because of reach limit in `IsScheduleAllowed`
			resStr = fmt.Sprintf("%s reach limit", d.schedulerName)
		}
	}
	return &DiagnosticResult{
		Name:      d.schedulerName,
		Status:    firstStatus,
		Summary:   resStr,
		Timestamp: uint64(time.Now().Unix()),
	}
}

func (d *diagnosticRecorder) setResultFromStatus(status string) {
	if d == nil {
		return
	}
	result := &DiagnosticResult{Name: d.schedulerName, Timestamp: uint64(time.Now().Unix()), Status: status}
	d.results.Put(result.Timestamp, result)
}

func (d *diagnosticRecorder) setResultFromPlans(ops []*operator.Operator, plans []plan.Plan) {
	if d == nil {
		return
	}
	result := d.analyze(ops, plans, uint64(time.Now().Unix()))
	d.results.Put(result.Timestamp, result)
}

func (d *diagnosticRecorder) analyze(ops []*operator.Operator, plans []plan.Plan, ts uint64) *DiagnosticResult {
	res := &DiagnosticResult{Name: d.schedulerName, Timestamp: ts, Status: normal}
	name := d.schedulerName
	// TODO: support more schedulers and checkers
	switch name {
	case schedulers.BalanceRegionName, schedulers.BalanceLeaderName:
		if len(ops) != 0 {
			res.Status = scheduling
			return res
		}
		res.Status = pending
		if d.summaryFunc != nil {
			isAllNormal := false
			res.StoreStatus, isAllNormal, _ = d.summaryFunc(plans)
			if isAllNormal {
				res.Status = normal
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
