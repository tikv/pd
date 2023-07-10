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
	"context"
	"sync/atomic"
	"time"

	"github.com/tikv/pd/pkg/core"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
)

const maxScheduleRetries = 10

var denySchedulersByLabelerCounter = labeler.LabelerEventCounter.WithLabelValues("schedulers", "deny")

// ScheduleController is used to manage a scheduler to
type ScheduleController struct {
	Scheduler
	cluster            sche.ScheduleCluster
	opController       *operator.Controller
	nextInterval       time.Duration
	ctx                context.Context
	cancel             context.CancelFunc
	delayAt            int64
	delayUntil         int64
	diagnosticRecorder *DiagnosticRecorder
}

// NewScheduleController creates a new ScheduleController.
func NewScheduleController(ctx context.Context, cluster sche.ScheduleCluster, opController *operator.Controller, s Scheduler) *ScheduleController {
	ctx, cancel := context.WithCancel(ctx)
	return &ScheduleController{
		Scheduler:          s,
		cluster:            cluster,
		opController:       opController,
		nextInterval:       s.GetMinInterval(),
		ctx:                ctx,
		cancel:             cancel,
		diagnosticRecorder: NewDiagnosticRecorder(s.GetName(), cluster.GetOpts()),
	}
}

// Ctx returns the context of ScheduleController
func (s *ScheduleController) Ctx() context.Context {
	return s.ctx
}

// Stop stops the ScheduleController
func (s *ScheduleController) Stop() {
	s.cancel()
}

// Schedule tries to create some operators.
func (s *ScheduleController) Schedule(diagnosable bool) []*operator.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// no need to retry if schedule should stop to speed exit
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}
		cacheCluster := newCacheCluster(s.cluster)
		// we need only process diagnostic once in the retry loop
		diagnosable = diagnosable && i == 0
		ops, plans := s.Scheduler.Schedule(cacheCluster, diagnosable)
		if diagnosable {
			s.diagnosticRecorder.SetResultFromPlans(ops, plans)
		}
		foundDisabled := false
		for _, op := range ops {
			if labelMgr := s.cluster.GetRegionLabeler(); labelMgr != nil {
				region := s.cluster.GetRegion(op.RegionID())
				if region == nil {
					continue
				}
				if labelMgr.ScheduleDisabled(region) {
					denySchedulersByLabelerCounter.Inc()
					foundDisabled = true
					break
				}
			}
		}
		if len(ops) > 0 {
			// If we have schedule, reset interval to the minimal interval.
			s.nextInterval = s.Scheduler.GetMinInterval()
			// try regenerating operators
			if foundDisabled {
				continue
			}
			return ops
		}
	}
	s.nextInterval = s.Scheduler.GetNextInterval(s.nextInterval)
	return nil
}

// DiagnoseDryRun returns the operators and plans of a scheduler.
func (s *ScheduleController) DiagnoseDryRun() ([]*operator.Operator, []plan.Plan) {
	cacheCluster := newCacheCluster(s.cluster)
	return s.Scheduler.Schedule(cacheCluster, true)
}

// GetInterval returns the interval of scheduling for a scheduler.
func (s *ScheduleController) GetInterval() time.Duration {
	return s.nextInterval
}

// SetInterval sets the interval of scheduling for a scheduler. for test purpose.
func (s *ScheduleController) SetInterval(interval time.Duration) {
	s.nextInterval = interval
}

// AllowSchedule returns if a scheduler is allowed to
func (s *ScheduleController) AllowSchedule(diagnosable bool) bool {
	if !s.Scheduler.IsScheduleAllowed(s.cluster) {
		if diagnosable {
			s.diagnosticRecorder.SetResultFromStatus(Pending)
		}
		return false
	}
	if s.isSchedulingHalted() {
		if diagnosable {
			s.diagnosticRecorder.SetResultFromStatus(Halted)
		}
		return false
	}
	if s.IsPaused() {
		if diagnosable {
			s.diagnosticRecorder.SetResultFromStatus(Paused)
		}
		return false
	}
	return true
}

func (s *ScheduleController) isSchedulingHalted() bool {
	return s.cluster.GetOpts().IsSchedulingHalted()
}

// IsPaused returns if a scheduler is paused.
func (s *ScheduleController) IsPaused() bool {
	delayUntil := atomic.LoadInt64(&s.delayUntil)
	return time.Now().Unix() < delayUntil
}

// GetDelayAt returns paused timestamp of a paused scheduler
func (s *ScheduleController) GetDelayAt() int64 {
	if s.IsPaused() {
		return atomic.LoadInt64(&s.delayAt)
	}
	return 0
}

// GetDelayUntil returns resume timestamp of a paused scheduler
func (s *ScheduleController) GetDelayUntil() int64 {
	if s.IsPaused() {
		return atomic.LoadInt64(&s.delayUntil)
	}
	return 0
}

// SetDelay sets the delay of a scheduler.
func (s *ScheduleController) SetDelay(delayAt, delayUntil int64) {
	atomic.StoreInt64(&s.delayAt, delayAt)
	atomic.StoreInt64(&s.delayUntil, delayUntil)
}

// GetDiagnosticRecorder returns the diagnostic recorder of a scheduler.
func (s *ScheduleController) GetDiagnosticRecorder() *DiagnosticRecorder {
	return s.diagnosticRecorder
}

// IsDiagnosticAllowed returns if a scheduler is allowed to do diagnostic.
func (s *ScheduleController) IsDiagnosticAllowed() bool {
	return s.diagnosticRecorder.IsAllowed()
}

// cacheCluster include cache info to improve the performance.
type cacheCluster struct {
	sche.ScheduleCluster
	stores []*core.StoreInfo
}

// GetStores returns store infos from cache
func (c *cacheCluster) GetStores() []*core.StoreInfo {
	return c.stores
}

// newCacheCluster constructor for cache
func newCacheCluster(c sche.ScheduleCluster) *cacheCluster {
	return &cacheCluster{
		ScheduleCluster: c,
		stores:          c.GetStores(),
	}
}
