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
	"errors"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// RegionHeartbeatStageName is the name of the stage of the region heartbeat.
const (
	HandleStatsAsync        = "HandleStatsAsync"
	ObserveRegionStatsAsync = "ObserveRegionStatsAsync"
	UpdateSubTree           = "UpdateSubTree"
	HandleOverlaps          = "HandleOverlaps"
	CollectRegionStatsAsync = "CollectRegionStatsAsync"
	SaveRegionToKV          = "SaveRegionToKV"
)

const initialCapacity = 100

// Runner is the interface for running tasks.
type Runner interface {
	RunTask(ctx context.Context, name string, f func(context.Context), opts ...TaskOption) error
	Start()
	Stop()
}

// Task is a task to be run.
type Task struct {
	ctx         context.Context
	submittedAt time.Time
	opts        *TaskOpts
	f           func(context.Context)
	name        string
}

// ErrMaxWaitingTasksExceeded is returned when the number of waiting tasks exceeds the maximum.
var ErrMaxWaitingTasksExceeded = errors.New("max waiting tasks exceeded")

// ConcurrentRunner is a simple task runner that limits the number of concurrent tasks.
type ConcurrentRunner struct {
	name               string
	limiter            *ConcurrencyLimiter
	maxPendingDuration time.Duration
	taskChan           chan *Task
	pendingTasks       []*Task
	pendingMu          sync.Mutex
	stopChan           chan struct{}
	wg                 sync.WaitGroup
	pendingTaskCount   map[string]int64
	failedTaskCount    prometheus.Counter
	maxWaitingDuration prometheus.Gauge
}

// NewConcurrentRunner creates a new ConcurrentRunner.
func NewConcurrentRunner(name string, limiter *ConcurrencyLimiter, maxPendingDuration time.Duration) *ConcurrentRunner {
	s := &ConcurrentRunner{
		name:               name,
		limiter:            limiter,
		maxPendingDuration: maxPendingDuration,
		taskChan:           make(chan *Task),
		pendingTasks:       make([]*Task, 0, initialCapacity),
		failedTaskCount:    RunnerTaskFailedTasks.WithLabelValues(name),
		pendingTaskCount:   make(map[string]int64),
		maxWaitingDuration: RunnerTaskMaxWaitingDuration.WithLabelValues(name),
	}
	return s
}

// TaskOpts is the options for RunTask.
type TaskOpts struct{}

// TaskOption configures TaskOp
type TaskOption func(opts *TaskOpts)

// Start starts the runner.
func (cr *ConcurrentRunner) Start() {
	cr.stopChan = make(chan struct{})
	cr.wg.Add(1)
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		defer cr.wg.Done()
		for {
			select {
			case task := <-cr.taskChan:
				if cr.limiter != nil {
					token, err := cr.limiter.AcquireToken(context.Background())
					if err != nil {
						continue
					}
					go cr.run(task, token)
				} else {
					go cr.run(task, nil)
				}
			case <-cr.stopChan:
				cr.pendingMu.Lock()
				cr.pendingTasks = make([]*Task, 0, initialCapacity)
				cr.pendingMu.Unlock()
				log.Info("stopping async task runner", zap.String("name", cr.name))
				return
			case <-ticker.C:
				maxDuration := time.Duration(0)
				cr.pendingMu.Lock()
				if len(cr.pendingTasks) > 0 {
					maxDuration = time.Since(cr.pendingTasks[0].submittedAt)
				}
				for name, cnt := range cr.pendingTaskCount {
					RunnerTaskPendingTasks.WithLabelValues(cr.name, name).Set(float64(cnt))
				}
				cr.pendingMu.Unlock()
				cr.maxWaitingDuration.Set(maxDuration.Seconds())
			}
		}
	}()
}

func (cr *ConcurrentRunner) run(task *Task, token *TaskToken) {
	task.f(task.ctx)
	if token != nil {
		cr.limiter.ReleaseToken(token)
		cr.processPendingTasks()
	}
}

func (cr *ConcurrentRunner) processPendingTasks() {
	cr.pendingMu.Lock()
	defer cr.pendingMu.Unlock()
	for len(cr.pendingTasks) > 0 {
		task := cr.pendingTasks[0]
		select {
		case cr.taskChan <- task:
			cr.pendingTasks = cr.pendingTasks[1:]
			cr.pendingTaskCount[task.name]--
			return
		default:
			return
		}
	}
}

// Stop stops the runner.
func (cr *ConcurrentRunner) Stop() {
	close(cr.stopChan)
	cr.wg.Wait()
}

// RunTask runs the task asynchronously.
func (cr *ConcurrentRunner) RunTask(ctx context.Context, name string, f func(context.Context), opts ...TaskOption) error {
	taskOpts := &TaskOpts{}
	for _, opt := range opts {
		opt(taskOpts)
	}
	task := &Task{
		ctx:  ctx,
		name: name,
		f:    f,
		opts: taskOpts,
	}

	cr.processPendingTasks()
	select {
	case cr.taskChan <- task:
	default:
		cr.pendingMu.Lock()
		defer cr.pendingMu.Unlock()
		if len(cr.pendingTasks) > 0 {
			maxWait := time.Since(cr.pendingTasks[0].submittedAt)
			if maxWait > cr.maxPendingDuration {
				cr.failedTaskCount.Inc()
				return ErrMaxWaitingTasksExceeded
			}
		}
		task.submittedAt = time.Now()
		cr.pendingTasks = append(cr.pendingTasks, task)
		cr.pendingTaskCount[task.name]++
	}
	return nil
}

// SyncRunner is a simple task runner that limits the number of concurrent tasks.
type SyncRunner struct{}

// NewSyncRunner creates a new SyncRunner.
func NewSyncRunner() *SyncRunner {
	return &SyncRunner{}
}

// RunTask runs the task synchronously.
func (*SyncRunner) RunTask(ctx context.Context, _ string, f func(context.Context), _ ...TaskOption) error {
	f(ctx)
	return nil
}

// Start starts the runner.
func (*SyncRunner) Start() {}

// Stop stops the runner.
func (*SyncRunner) Stop() {}
