// Copyright 2024 TiKV Project Authors.
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

package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
)

// RegionHeartbeatStageName is the name of the stage of the region heartbeat.
const (
	HandleStatsAsync        = "HandleStatsAsync"
	ObserveRegionStatsAsync = "ObserveRegionStatsAsync"
	UpdateSubTree           = "UpdateSubTree"
	HandleOverlaps          = "HandleOverlaps"
	CollectRegionStatsAsync = "CollectRegionStatsAsync"
	SaveRegionToKV          = "SaveRegionToKV"
	SyncRegionToFollower    = "SyncRegionToFollower"
)

const (
	initialCapacity = 10000
	// maxPendingTaskNum bounds the closures and their captured objects retained
	// by a runner that cannot keep up with incoming work.
	maxPendingTaskNum = 20000000
)

// Runner is the interface for running tasks.
type Runner interface {
	RunTask(id uint64, name string, f func(context.Context), opts ...TaskOption) error
	Start(ctx context.Context)
	Stop()
}

// Task is a task to be run.
type Task struct {
	id          uint64
	submittedAt time.Time
	f           func(context.Context)
	name        string
	// retained indicates whether the task should be dropped if the task queue exceeds maxPendingDuration.
	retained bool
}

type taskID struct {
	id   uint64
	name string
}

// ConcurrentRunner is a task runner that limits the number of concurrent tasks.
type ConcurrentRunner struct {
	ctx                context.Context
	cancel             context.CancelFunc
	name               string
	limiter            *ConcurrencyLimiter
	maxPendingDuration time.Duration
	maxPendingTaskNum  int
	taskChan           chan *Task
	pendingMu          sync.Mutex
	wg                 sync.WaitGroup
	pendingTaskCount   map[string]int
	pendingTasks       []*Task
	pendingHead        int
	pendingLen         int
	existTasks         map[taskID]*Task
	maxWaitingDuration prometheus.Gauge
}

// NewConcurrentRunner creates a new ConcurrentRunner.
func NewConcurrentRunner(name string, limiter *ConcurrencyLimiter, maxPendingDuration time.Duration) *ConcurrentRunner {
	s := &ConcurrentRunner{
		name:               name,
		limiter:            limiter,
		maxPendingDuration: maxPendingDuration,
		maxPendingTaskNum:  maxPendingTaskNum,
		taskChan:           make(chan *Task, 1),
		pendingTasks:       make([]*Task, 0, initialCapacity),
		pendingTaskCount:   make(map[string]int),
		existTasks:         make(map[taskID]*Task),
		maxWaitingDuration: runnerTaskMaxWaitingDuration.WithLabelValues(name),
	}
	return s
}

// TaskOption configures TaskOp
type TaskOption func(opts *Task)

// WithRetained sets whether the task should be retained.
func WithRetained(retained bool) TaskOption {
	return func(opts *Task) { opts.retained = retained }
}

// Start starts the runner.
func (cr *ConcurrentRunner) Start(ctx context.Context) {
	cr.ctx, cr.cancel = context.WithCancel(ctx)
	cr.wg.Add(1)
	go func() {
		defer cr.wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case task := <-cr.taskChan:
				if cr.limiter != nil {
					token, err := cr.limiter.AcquireToken(cr.ctx)
					if err != nil {
						continue
					}
					go cr.run(cr.ctx, task, token)
				} else {
					go cr.run(cr.ctx, task, nil)
				}
			case <-cr.ctx.Done():
				cr.pendingMu.Lock()
				cr.resetPendingTasks(0)
				cr.pendingMu.Unlock()
				log.Info("stopping async task runner", zap.String("name", cr.name))
				return
			case <-ticker.C:
				maxDuration := time.Duration(0)
				cr.pendingMu.Lock()
				if cr.pendingTaskNum() > 0 {
					maxDuration = time.Since(cr.pendingTasks[cr.pendingHead].submittedAt)
				}
				for taskName, cnt := range cr.pendingTaskCount {
					runnerPendingTasks.WithLabelValues(cr.name, taskName).Set(float64(cnt))
				}
				cr.pendingMu.Unlock()
				cr.maxWaitingDuration.Set(maxDuration.Seconds())
			}
		}
	}()
}

func (cr *ConcurrentRunner) run(ctx context.Context, task *Task, token *TaskToken) {
	start := time.Now()
	select {
	case <-ctx.Done():
		return
	default:
	}
	task.f(ctx)
	if token != nil {
		cr.limiter.ReleaseToken(token)
		cr.processPendingTasks()
	}
	runnerTaskExecutionDuration.WithLabelValues(cr.name, task.name).Observe(time.Since(start).Seconds())
	runnerSucceededTasks.WithLabelValues(cr.name, task.name).Inc()
}

func (cr *ConcurrentRunner) processPendingTasks() {
	cr.pendingMu.Lock()
	defer cr.pendingMu.Unlock()
	if cr.pendingTaskNum() > 0 {
		task := cr.pendingTasks[cr.pendingHead]
		select {
		case cr.taskChan <- task:
			cr.pendingTaskCount[task.name]--
			delete(cr.existTasks, taskID{id: task.id, name: task.name})
			cr.popPendingTask()
		default:
		}
		return
	}
}

func (cr *ConcurrentRunner) pendingTaskNum() int {
	return cr.pendingLen
}

// pendingTaskAt returns a pending task by its FIFO offset. It must be called
// with pendingMu held.
func (cr *ConcurrentRunner) pendingTaskAt(offset int) *Task {
	index := cr.pendingHead + offset
	if index >= cap(cr.pendingTasks) {
		index -= cap(cr.pendingTasks)
	}
	return cr.pendingTasks[index]
}

// pushPendingTask appends a task to the pending ring. It must be called with
// pendingMu held.
func (cr *ConcurrentRunner) pushPendingTask(task *Task) {
	if cr.pendingLen == 0 {
		cr.pendingTasks = append(cr.pendingTasks, task)
		cr.pendingHead = 0
		cr.pendingLen = 1
		return
	}
	if cr.pendingLen == cap(cr.pendingTasks) {
		capacity := max(initialCapacity, cr.pendingLen*2)
		pendingTasks := make([]*Task, cr.pendingLen, capacity)
		for i := range cr.pendingLen {
			pendingTasks[i] = cr.pendingTaskAt(i)
		}
		cr.pendingTasks = pendingTasks
		cr.pendingHead = 0
	}

	index := cr.pendingHead + cr.pendingLen
	if index >= cap(cr.pendingTasks) {
		index -= cap(cr.pendingTasks)
	}
	if index == len(cr.pendingTasks) {
		cr.pendingTasks = append(cr.pendingTasks, task)
	} else {
		cr.pendingTasks[index] = task
	}
	cr.pendingLen++
}

// popPendingTask removes the FIFO head. It must be called with pendingMu held.
func (cr *ConcurrentRunner) popPendingTask() {
	cr.pendingTasks[cr.pendingHead] = nil
	if cr.pendingLen == 1 {
		cr.pendingLen = 0
		if len(cr.pendingTasks) >= initialCapacity {
			cr.resetPendingTasks(initialCapacity)
		} else {
			cr.pendingTasks = cr.pendingTasks[:0]
			cr.pendingHead = 0
		}
		return
	}
	cr.pendingHead++
	if cr.pendingHead == cap(cr.pendingTasks) {
		cr.pendingHead = 0
	}
	cr.pendingLen--
}

// resetPendingTasks releases all pending task storage. It must be called with
// pendingMu held. capacity is kept small during normal operation and zero when
// the runner stops.
func (cr *ConcurrentRunner) resetPendingTasks(capacity int) {
	cr.pendingTasks = make([]*Task, 0, capacity)
	cr.pendingHead = 0
	cr.pendingLen = 0
	cr.existTasks = make(map[taskID]*Task)
	for taskName := range cr.pendingTaskCount {
		cr.pendingTaskCount[taskName] = 0
	}
}

// Stop stops the runner.
func (cr *ConcurrentRunner) Stop() {
	cr.cancel()
	cr.wg.Wait()
}

// RunTask runs the task asynchronously.
func (cr *ConcurrentRunner) RunTask(id uint64, name string, f func(context.Context), opts ...TaskOption) error {
	cr.pendingMu.Lock()
	defer func() {
		cr.pendingMu.Unlock()
		cr.processPendingTasks()
	}()

	pendingTaskNum := cr.pendingTaskNum()
	tid := taskID{id: id, name: name}
	if pendingTaskNum > 0 {
		// Here we use a map to find the task with the same ID.
		// Then replace the old task with the new one.
		if t, ok := cr.existTasks[tid]; ok {
			t.f = f
			return nil
		}
	}
	task := &Task{
		id:          id,
		name:        name,
		f:           f,
		submittedAt: time.Now(),
	}
	for _, opt := range opts {
		opt(task)
	}
	if pendingTaskNum > 0 {
		if !task.retained {
			maxWait := time.Since(cr.pendingTasks[cr.pendingHead].submittedAt)
			if maxWait > cr.maxPendingDuration {
				runnerFailedTasks.WithLabelValues(cr.name, name).Inc()
				return errs.ErrMaxWaitingTasksExceeded
			}
		}
		if pendingTaskNum >= cr.maxPendingTaskNum {
			runnerFailedTasks.WithLabelValues(cr.name, name).Inc()
			return errs.ErrMaxWaitingTasksExceeded
		}
	}
	cr.pushPendingTask(task)
	cr.existTasks[tid] = task
	cr.pendingTaskCount[name]++
	return nil
}

// SyncRunner is a simple task runner that limits the number of concurrent tasks.
type SyncRunner struct{}

// NewSyncRunner creates a new SyncRunner.
func NewSyncRunner() *SyncRunner {
	return &SyncRunner{}
}

// RunTask runs the task synchronously.
func (*SyncRunner) RunTask(_ uint64, _ string, f func(context.Context), _ ...TaskOption) error {
	f(context.Background())
	return nil
}

// Start starts the runner.
func (*SyncRunner) Start(context.Context) {}

// Stop stops the runner.
func (*SyncRunner) Stop() {}
