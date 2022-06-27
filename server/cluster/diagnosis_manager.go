package cluster

import (
	"context"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/diagnosis"
	"github.com/tikv/pd/server/schedule/hbstream"
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

func (d *diagnosisManager) GetSchedulerDiagnosisResult(name string) *diagnosis.DiagnosisResult {
	return nil
}

func (d *diagnosisManager) GetSchedulerStoreDiagnosisResult(name string, store uint64) *diagnosis.DiagnosisResult {
	return nil
}

func (d *diagnosisManager) runSchedulerDiagnosis(name string) []schedule.Plan {
	if scheduler, ok := d.schedulers[name]; ok {
		plans := scheduler.runDiagnosis()
		return plans
	}
	return nil
}

type diagnosisSchedulerManager struct {
	Scheduler *scheduleController
	result    map[uint64]*SchedulerDiagnoseRecord
}

// newDiagnosisSchedulerManager creates a new scheduleController.
func newDiagnosisSchedulerManager(m *diagnosisManager, s schedule.Scheduler) *diagnosisSchedulerManager {
	return &diagnosisSchedulerManager{
		Scheduler: newScheduleController(m.ctx, m.cluster, m.opController, s),
		result:    make(map[uint64]*SchedulerDiagnoseRecord),
	}
}

func (d *diagnosisSchedulerManager) runDiagnosis() []schedule.Plan {
	_, plans := d.Scheduler.Schedule()
	return plans
}

type SchedulerDiagnoseRecord struct{}
