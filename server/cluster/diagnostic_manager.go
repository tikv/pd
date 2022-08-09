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
	"time"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/plan"
)

const maxDiagnosticsResultNum = 10

// diagnosticsManager is used to manage diagnose mechanism which shares the actual scheduler with coordinator
type diagnosticsManager struct {
	cluster      *RaftCluster
	schedulers   map[string]*scheduleController
	dryRunResult map[string]*cache.FIFO
}

func newDiagnosticsManager(cluster *RaftCluster, schedulerControllers map[string]*scheduleController) *diagnosticsManager {
	return &diagnosticsManager{
		cluster:      cluster,
		schedulers:   schedulerControllers,
		dryRunResult: make(map[string]*cache.FIFO),
	}
}

func (d *diagnosticsManager) dryRun(name string) error {
	if _, ok := d.schedulers[name]; !ok {
		return errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	// TODO: support running multiple times if there is no performance issue.
	ops, plans := d.schedulers[name].DryRun()
	result := newDryRunResult(ops, plans)
	if _, ok := d.dryRunResult[name]; !ok {
		d.dryRunResult[name] = cache.NewFIFO(maxDiagnosticsResultNum)
	}
	queue := d.dryRunResult[name]
	queue.Put(result.timestamp, result)
	return nil
}

// TODO: implement this function for each scheduler maybe.
func (d *diagnosticsManager) analyze(name string) (*DiagnosticsResult, error) {
	return &DiagnosticsResult{}, nil
}

type dryRunResult struct {
	timestamp          uint64
	unschedulablePlans []plan.Plan
	schedulablePlans   []plan.Plan
}

func newDryRunResult(ops []*operator.Operator, result []plan.Plan) *dryRunResult {
	index := len(ops)
	if len(ops) > 0 {
		if ops[0].Kind()&operator.OpMerge != 0 {
			index /= 2
		}
	}
	if index > len(result) {
		return nil
	}
	return &dryRunResult{
		timestamp:          uint64(time.Now().Unix()),
		unschedulablePlans: result[index:],
		schedulablePlans:   result[:index],
	}
}

type DiagnosticsResult struct {
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	Summary   string    `json:"summary"`
	Timestamp time.Time `json:"timestamp"`
}
