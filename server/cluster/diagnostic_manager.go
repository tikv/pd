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
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/plan"
	"github.com/tikv/pd/server/schedulers"
)

const (
	disabled   = "disabled"
	paused     = "paused"
	scheduling = "scheduling"
	pending    = "pending"
	// TODO: find a better name
	normal = "normal"
)

const maxDiagnosticResultNum = 10

// diagnosticManager is used to manage diagnose mechanism which shares the actual scheduler with coordinator
type diagnosticManager struct {
	cluster       *RaftCluster
	schedulers    map[string]*scheduleController
	dryRunResults map[string]*cache.FIFO
}

func newDiagnosticManager(cluster *RaftCluster, schedulerControllers map[string]*scheduleController) *diagnosticManager {
	return &diagnosticManager{
		cluster:       cluster,
		schedulers:    schedulerControllers,
		dryRunResults: make(map[string]*cache.FIFO),
	}
}

func (d *diagnosticManager) dryRun(name string, ts uint64) (*DiagnosticResult, error) {
	scheduler, ok := d.schedulers[name]
	if !ok {
		return nil, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	// TODO: support running multiple times if there is no performance issue.
	ops, plans := scheduler.DryRun()
	result := d.analyze(name, ops, plans, ts)
	if _, ok := d.dryRunResults[name]; !ok {
		d.dryRunResults[name] = cache.NewFIFO(maxDiagnosticResultNum)
	}
	queue := d.dryRunResults[name]
	queue.Put(result.Timestamp, result)
	return result, nil
}

// TODO: implement this function for each scheduler maybe.
func (d *diagnosticManager) analyze(name string, ops []*operator.Operator, plans []plan.Plan, ts uint64) *DiagnosticResult {
	res := &DiagnosticResult{Name: schedulers.BalanceRegionName, Timestamp: ts, Status: normal}
	if isPaused, _ := d.cluster.IsSchedulerPaused(name); isPaused {
		res.Status = paused
		return res
	}
	if isDisabled, _ := d.cluster.IsSchedulerDisabled(name); isDisabled {
		res.Status = disabled
		return res
	}

	// TODO: support more schedulers and checkers
	switch name {
	case schedulers.BalanceRegionName:
		runningNum := d.cluster.GetOperatorController().OperatorCount(operator.OpRegion)
		if runningNum != 0 || len(ops) != 0 {
			res.Status = scheduling
		}
		// TODO: handle plans to get a summary
	default:
	}
	index := len(ops)
	res.UnschedulablePlans = plans[index:]
	res.SchedulablePlans = plans[:index]
	return res
}

// TODO: support get dry run result for a given ts.
// func (d *diagnosticManager) getDryRunResult(name string, ts uint64) *DiagnosticResult {
// 	queue := d.dryRunResults[name]
// 	// find the first result equal or larger than the given ts
// 	res := queue.FromElems(ts - 1)
// 	if len(res) != 0 {
// 		return res[0].Value.(*DiagnosticResult)
// 	}
// 	return nil
// }

type DiagnosticResult struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Summary   string `json:"summary"`
	Timestamp uint64 `json:"timestamp"`

	SchedulablePlans   []plan.Plan
	UnschedulablePlans []plan.Plan
}
