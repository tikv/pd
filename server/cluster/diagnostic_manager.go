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
	"sync/atomic"
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
	disabled   = "disabled"
	paused     = "paused"
	scheduling = "scheduling"
	pending    = "pending"
	normal     = "normal"
)

const (
	maxDiagnosticResultNum = 10
)

var SummaryFuncs = map[string]plan.Summary{}

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
	if isDisabled, _ := d.cluster.IsSchedulerDisabled(name); isDisabled {
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
	schedulerName   string
	cluster         *RaftCluster
	summaryFunc     plan.Summary
	result          *ResultMemorizer
	samplingCounter uint64
}

func newDiagnosticWorker(name string, cluster *RaftCluster) *diagnosticWorker {
	summaryFunc, ok := SummaryFuncs[name]
	if !ok {
		log.Error("can't find summary function", zap.String("scheduler-name", name))
	}
	return &diagnosticWorker{
		cluster:       cluster,
		schedulerName: name,
		summaryFunc:   summaryFunc,
		result:        NewResultMemorizer(maxDiagnosticResultNum),
	}
}

func (d *diagnosticWorker) isAllowed() bool {
	if d == nil {
		return false
	}
	if !d.cluster.opt.IsDiagnosisAllowed() {
		return false
	}
	currentCount := atomic.LoadUint64(&d.samplingCounter) + 1
	if currentCount == d.cluster.opt.GetDiagnosticSamplingRate() {
		atomic.StoreUint64(&d.samplingCounter, 0)
		return true
	}
	atomic.StoreUint64(&d.samplingCounter, currentCount)
	return false
}

func (d *diagnosticWorker) getLastResult() *DiagnosticResult {
	if d.result.Len() == 0 {
		return nil
	}
	return d.result.GenerateResult()
}

func (d *diagnosticWorker) setResultFromStatus(status string) {
	if d == nil {
		return
	}
	result := &DiagnosticResult{Name: d.schedulerName, Timestamp: uint64(time.Now().Unix()), Status: status}
	d.result.Put(result)
}

func (d *diagnosticWorker) setResultFromPlans(ops []*operator.Operator, plans []plan.Plan) {
	if d == nil {
		return
	}
	result := d.analyze(ops, plans, uint64(time.Now().Unix()))
	d.result.Put(result)
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

type DiagnosticResult struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Summary   string `json:"summary"`
	Timestamp uint64 `json:"timestamp"`

	StoreStatus        map[uint64]plan.Status `json:"-"`
	SchedulablePlans   []plan.Plan            `json:"-"`
	UnschedulablePlans []plan.Plan            `json:"-"`
}

type ResultMemorizer struct {
	*cache.FIFO
}

func NewResultMemorizer(maxCount int) *ResultMemorizer {
	return &ResultMemorizer{FIFO: cache.NewFIFO(maxCount)}
}

func (m *ResultMemorizer) Put(result *DiagnosticResult) {
	m.FIFO.Put(result.Timestamp, result)
}

func (m *ResultMemorizer) GenerateResult() *DiagnosticResult {
	items := m.FIFO.FromLastSameElems(func(i interface{}) bool {
		_, ok := i.(*DiagnosticResult)
		return ok
	}, func(i interface{}) string {
		result, _ := i.(*DiagnosticResult)
		if result == nil {
			return ""
		}
		return result.Status
	})
	length := len(items)
	if length == 0 {
		return nil
	}
	x1, x2, x3 := length/3, length/3, length/3
	if (length % 3) > 0 {
		x1++
	}
	if (length % 3) > 1 {
		x2++
	}
	pi := 1.0 / float64(3*x1+2*x2+x3)
	// If there are 10 results, the weight is [0.143,0.143,0.143,0.143,0.095,0.095,0.095,0.047,0.047,0.047]
	// If there are 3 results, the weight is [0.5,0.33,0.17]
	counter := make(map[uint64]map[plan.Status]float64)
	for i := 0; i < x1; i++ {
		item := items[i].Value.(*DiagnosticResult)
		for storeID, status := range item.StoreStatus {
			if _, ok := counter[storeID]; !ok {
				counter[storeID] = make(map[plan.Status]float64)
			}
			statusCounter := counter[storeID]
			statusCounter[status] += pi * 3
		}
	}
	for i := x1; i < x1+x2; i++ {
		item := items[i].Value.(*DiagnosticResult)
		for storeID, status := range item.StoreStatus {
			if _, ok := counter[storeID]; !ok {
				counter[storeID] = make(map[plan.Status]float64)
			}
			statusCounter := counter[storeID]
			statusCounter[status] += pi * 2
		}
	}
	for i := x1 + x2; i < x1+x2+x3; i++ {
		item := items[i].Value.(*DiagnosticResult)
		for storeID, status := range item.StoreStatus {
			if _, ok := counter[storeID]; !ok {
				counter[storeID] = make(map[plan.Status]float64)
			}
			statusCounter := counter[storeID]
			statusCounter[status] += pi * 1
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
