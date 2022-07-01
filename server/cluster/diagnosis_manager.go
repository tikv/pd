package cluster

import (
	"context"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/diagnosis"
	"github.com/tikv/pd/server/schedule/hbstream"
	"github.com/tikv/pd/server/schedule/operator"
	"go.uber.org/zap"
)

type diagnosisManager struct {
	syncutil.RWMutex
	//interval 		time.Duration
	schedulers map[string]*diagnosisSchedulerManager

	// wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *RaftCluster
	// prepareChecker  *prepareChecker
	// checkers        *checker.Controller
	// regionScatterer *schedule.RegionScatterer
	// regionSplitter  *schedule.RegionSplitter
	opController *schedule.OperatorController
}

// newDiagnosisManager creates a new coordinator.
func newDiagnosisManager(ctx context.Context, cluster *RaftCluster, hbStreams *hbstream.HeartbeatStreams) *diagnosisManager {
	ctx, cancel := context.WithCancel(ctx)
	opController := schedule.NewOperatorController(ctx, cluster, hbStreams)
	return &diagnosisManager{
		ctx:          ctx,
		cancel:       cancel,
		cluster:      cluster,
		schedulers:   make(map[string]*diagnosisSchedulerManager),
		opController: opController,
	}
}

func (d *diagnosisManager) isExistSchedulerDiagnosis(name string) bool {
	_, ok := d.schedulers[name]
	return ok
}

func (d *diagnosisManager) addSchedulerDiagnosis(scheduler schedule.Scheduler, args ...string) error {
	d.Lock()
	defer d.Unlock()

	if _, ok := d.schedulers[scheduler.GetName()]; ok {
		return errs.ErrSchedulerExisted.FastGenByArgs()
	}

	ds := newDiagnosisSchedulerManager(d, scheduler)
	if err := ds.Scheduler.Prepare(d.cluster); err != nil {
		return err
	}
	d.schedulers[scheduler.GetName()] = ds
	return nil
}

func (d *diagnosisManager) GetSchedulerDiagnosisResult(name string) *diagnosis.MatrixDiagnosisResult {
	if scheduler, ok := d.schedulers[name]; ok {
		return scheduler.GetSchedulerDiagnosisResult()
	}
	return nil
}

func (d *diagnosisManager) GetSchedulerStoreDiagnosisResult(name string, store uint64) *diagnosis.StepDiagnosisResult {
	return nil
}

func (d *diagnosisManager) runSchedulerDiagnosis(name string) []schedule.Plan {
	if scheduler, ok := d.schedulers[name]; ok {
		scheduler.runDiagnosis()
		plans := scheduler.result
		return plans
	}
	return nil
}

type diagnosisSchedulerManager struct {
	cluster   *RaftCluster
	Scheduler *scheduleController
	result    []schedule.Plan
	ops       []*operator.Operator
}

// newDiagnosisSchedulerManager creates a new scheduleController.
func newDiagnosisSchedulerManager(m *diagnosisManager, s schedule.Scheduler) *diagnosisSchedulerManager {
	return &diagnosisSchedulerManager{
		Scheduler: newScheduleController(m.ctx, m.cluster, m.opController, s),
		cluster:   m.cluster,
	}
}

func (d *diagnosisSchedulerManager) runDiagnosis() {
	d.ops, d.result = d.Scheduler.Schedule(true)
	log.Info("Qebug", zap.Int("len ops", len(d.ops)), zap.Int("len plan", len(d.result)))
}

func (d *diagnosisSchedulerManager) GetSchedulerDiagnosisResult() *diagnosis.MatrixDiagnosisResult {
	log.Info("Diagnosis", zap.Bool("SchedulerDiagnosis is nil", d == nil))
	d.runDiagnosis()
	analyzer := schedule.NewMatrixPlanAnalyzer(d.Scheduler.GetName(), d.cluster)
	for _, plan := range d.result {
		analyzer.PutPlan(plan)
	}
	return analyzer.AnalysisResult()
}

func (d *diagnosisSchedulerManager) GetSchedulerStoreDiagnosisResult(store uint64) *diagnosis.StepDiagnosisResult {
	d.runDiagnosis()
	analyzer := schedule.NewPeriodicalPlanAnalyzer(d.Scheduler.GetName(), store)
	for _, plan := range d.result {
		analyzer.PutPlan(plan)
	}
	return analyzer.AnalysisResult()
}

type SchedulerDiagnoseRecord struct{}
