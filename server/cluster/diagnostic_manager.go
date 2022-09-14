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

package cluster

import (
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/plan"
	"github.com/tikv/pd/server/schedulers"
	"go.uber.org/zap"
)

const (
	// disabled means the current scheduler is unavailable or removed
	disabled = "disabled"
	// paused means the current scheduler is paused
	paused = "paused"
	// scheduling means the current scheduler is generating or executing scheduling operator
	scheduling = "scheduling"
	// pending means the current scheduler cannot generate scheduling operator
	pending = "pending"
	// normal means that there is no need to create operators since everything is fine.
	normal = "normal"
)

const (
	maxDiagnosticResultNum = 10
)

// SummaryFuncs includes all implementations of plan.Summary.
var SummaryFuncs = map[string]plan.Summary{
	schedulers.BalanceRegionName: schedulers.BalancePlanSummary,
}

type diagnosticManager struct {
	syncutil.RWMutex
	cluster *RaftCluster
	workers map[string]*diagnosticWorker
}

func newDiagnosticManager(cluster *RaftCluster) *diagnosticManager {
	workers := make(map[string]*diagnosticWorker)
	for name := range DiagnosableSchedulers {
		workers[name] = newDiagnosticWorker(name, cluster)
	}
	return &diagnosticManager{
		cluster: cluster,
		workers: workers,
	}
}

func (d *diagnosticManager) getDiagnosticResult(name string) (*DiagnosticResult, error) {
	isSchedulerExisted, _ := d.cluster.IsSchedulerExisted(name)
	isDisabled, _ := d.cluster.IsSchedulerDisabled(name)
	if !isSchedulerExisted || isDisabled {
		ts := uint64(time.Now().Unix())
		res := &DiagnosticResult{Name: name, Timestamp: ts, Status: disabled}
		return res, nil
	}
	worker := d.getWorker(name)
	if worker == nil {
		return nil, errs.ErrSchedulerUndiagnosable.FastGenByArgs(name)
	}
	result := worker.getLastResult()
	if result == nil {
		return nil, errs.ErrSchedulerDiagnosisNotRunning.FastGenByArgs(name)
	}
	return result, nil
}

func (d *diagnosticManager) getWorker(name string) *diagnosticWorker {
	return d.workers[name]
}

// diagnosticWorker is used to manage diagnose mechanism
type diagnosticWorker struct {
	schedulerName string
	cluster       *RaftCluster
	summaryFunc   plan.Summary
	results       *resultCache
}

func newDiagnosticWorker(name string, cluster *RaftCluster) *diagnosticWorker {
	summaryFunc, ok := SummaryFuncs[name]
	if !ok {
		log.Error("can't find summary function", zap.String("scheduler-name", name))
		return nil
	}
	return &diagnosticWorker{
		cluster:       cluster,
		schedulerName: name,
		summaryFunc:   summaryFunc,
		results:       newResultMemorizer(maxDiagnosticResultNum),
	}
}

func (d *diagnosticWorker) isAllowed() bool {
	if d == nil {
		return false
	}
	return d.cluster.opt.IsDiagnosisAllowed()
}

func (d *diagnosticWorker) getLastResult() *DiagnosticResult {
	if d.results.Len() == 0 {
		return nil
	}
	return d.results.generateResult()
}

func (d *diagnosticWorker) setResultFromStatus(status string) {
	if d == nil {
		return
	}
	result := &DiagnosticResult{Name: d.schedulerName, Timestamp: uint64(time.Now().Unix()), Status: status}
	d.results.put(result)
}

func (d *diagnosticWorker) setResultFromPlans(ops []*operator.Operator, plans []plan.Plan) {
	if d == nil {
		return
	}
	result := d.analyze(ops, plans, uint64(time.Now().Unix()))
	d.results.put(result)
}

func (d *diagnosticWorker) analyze(ops []*operator.Operator, plans []plan.Plan, ts uint64) *DiagnosticResult {
	res := &DiagnosticResult{Name: schedulers.BalanceRegionName, Timestamp: ts, Status: normal}
	name := d.schedulerName
	// TODO: support more schedulers and checkers
	switch name {
	case schedulers.BalanceRegionName:
		runningNum := d.cluster.GetOperatorController().OperatorCount(operator.OpRegion)
		if runningNum != 0 || len(ops) != 0 {
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

	StoreStatus        map[uint64]plan.Status `json:"-"`
	SchedulablePlans   []plan.Plan            `json:"-"`
	UnschedulablePlans []plan.Plan            `json:"-"`
}

// resultCache is an encapsulation for cache.FIFO.
type resultCache struct {
	*cache.FIFO
}

func newResultMemorizer(maxCount int) *resultCache {
	return &resultCache{FIFO: cache.NewFIFO(maxCount)}
}

func (m *resultCache) put(result *DiagnosticResult) {
	m.Put(result.Timestamp, result)
}

// generateResult firstly selects the continuous items which have the same Status with the the lastest one.
// And then dynamically assign weights to these results. More recent results will be assigned more weight.
func (m *resultCache) generateResult() *DiagnosticResult {
	items := m.FIFO.FromLastSameElems(func(i interface{}) (bool, string) {
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

	wa := newWeightAllocator(length, 3)

	counter := make(map[uint64]map[plan.Status]float64)
	for i := 0; i < length; i++ {
		item := items[i].Value.(*DiagnosticResult)
		for storeID, status := range item.StoreStatus {
			if _, ok := counter[storeID]; !ok {
				counter[storeID] = make(map[plan.Status]float64)
			}
			statusCounter := counter[storeID]
			statusCounter[status] += wa.getWeight(i)
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
	var resStr string
	for k, v := range statusCounter {
		resStr += fmt.Sprintf("%d store(s) %s; ", v, k.String())
	}
	return &DiagnosticResult{
		Name:      items[0].Value.(*DiagnosticResult).Name,
		Status:    items[0].Value.(*DiagnosticResult).Status,
		Summary:   resStr,
		Timestamp: uint64(time.Now().Unix()),
	}
}

type weightAllocator struct {
	segIndexs []int
	wights    []float64
}

func newWeightAllocator(length, segNum int) *weightAllocator {
	segLength := length / segNum
	segIndexs := make([]int, 0, segNum)
	wights := make([]float64, 0, length)

	unitCount := 0
	segIndexs = append(segIndexs, 0)
	for i := 0; i < segNum-1; i++ {
		next := segLength
		if (length % segNum) > i {
			next++
		}
		unitCount += (segNum - i) * next
		segIndexs = append(segIndexs, next)
	}
	unitCount += segLength

	// If there are 10 results, the weight is [0.143,0.143,0.143,0.143,0.095,0.095,0.095,0.047,0.047,0.047],
	// and segIndexs is
	// If there are 3 results, the weight is [0.5,0.33,0.17]
	unitWeight := 1.0 / float64(unitCount)
	for i := 0; i < segNum; i++ {
		for j := 0; j < segIndexs[i]; j++ {
			wights = append(wights, unitWeight*float64(segNum-i))
		}
	}
	return &weightAllocator{
		segIndexs: segIndexs,
		wights:    wights,
	}
}

func (a *weightAllocator) getWeight(i int) float64 {
	if i >= len(a.wights) {
		return 0
	}
	return a.wights[i]
}
