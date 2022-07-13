package cluster

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/server/schedule/checker"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/plan"
)

const maxDiagnosisResultNum = 6

// diagnosisManager is used to manage diagnose mechanism which shares the actual scheduler with coordinator
type diagnosisManager struct {
	cluster      *RaftCluster
	schedulers   map[string]*scheduleController
	checkers     *checker.Controller
	dryRunResult map[string]*cache.FIFO
}

func newDiagnosisManager(cluster *RaftCluster, schedulerControllers map[string]*scheduleController, checkers *checker.Controller) *diagnosisManager {
	return &diagnosisManager{
		cluster:      cluster,
		schedulers:   schedulerControllers,
		checkers:     checkers,
		dryRunResult: make(map[string]*cache.FIFO),
	}
}

func (d *diagnosisManager) diagnosisDryRun(name string) error {
	var result *diagnosisResult
	// if it's a scheduler.
	_, ok := d.schedulers[name]
	if ok {
		result = newDiagnosisResult(d.schedulers[name].DiagnoseDryRun())
	}
	// if it's a checker.
	switch name {
	case "merge-checker":
		// TODO: dry run merge checker
	case "rule-checker":
		// TODO: dry run rule checker
	default:
	}

	if result == nil {
		return errors.Errorf("cannot found the specified scheduler or checker for a given name: %s", name)
	}

	if _, ok := d.dryRunResult[name]; !ok {
		d.dryRunResult[name] = cache.NewFIFO(maxDiagnosisResultNum)
	}
	queue := d.dryRunResult[name]
	queue.Put(result.timestamp, result)
	return nil
}

type diagnosisResult struct {
	timestamp          uint64
	unschedulablePlans []plan.Plan
	schedulablePlans   []plan.Plan
}

func newDiagnosisResult(ops []*operator.Operator, result []plan.Plan) *diagnosisResult {
	index := len(ops)
	if len(ops) > 0 {
		if ops[0].Kind()&operator.OpMerge != 0 {
			index /= 2
		}
	}
	if index > len(result) {
		return nil
	}
	return &diagnosisResult{
		timestamp:          uint64(time.Now().Unix()),
		unschedulablePlans: result[index:],
		schedulablePlans:   result[:index],
	}
}
