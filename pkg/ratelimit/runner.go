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
	"container/list"
	"context"
	"errors"
	"fmt"
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

const (
	initialCapacity   = 10000
	maxPendingTaskNum = 20000000
)

// Runner is the interface for running tasks.
type Runner interface {
	RunTask(regionID uint64, name string, f func(), opts ...TaskOption) error
	Start()
	Stop()
}

// Task is a task to be run.
type Task struct {
	regionID    uint64
	submittedAt time.Time
	f           func()
	name        string
	// retained indicates whether the task should be dropped if the task queue exceeds maxPendingDuration.
	retained bool
}

// ErrMaxWaitingTasksExceeded is returned when the number of waiting tasks exceeds the maximum.
var ErrMaxWaitingTasksExceeded = errors.New("max waiting tasks exceeded")

// ConcurrentRunner is a simple task runner that limits the number of concurrent tasks.
type ConcurrentRunner struct {
	name               string
	limiter            *ConcurrencyLimiter
	maxPendingDuration time.Duration
	taskChan           chan *Task
	pendingMu          sync.Mutex
	stopChan           chan struct{}
	wg                 sync.WaitGroup
	pendingTaskCount   map[string]int
	pendingTasks       *list.List
	pendingRegionTasks map[string]*list.Element
	maxWaitingDuration prometheus.Gauge
}

// NewConcurrentRunner creates a new ConcurrentRunner.
func NewConcurrentRunner(name string, limiter *ConcurrencyLimiter, maxPendingDuration time.Duration) *ConcurrentRunner {
	s := &ConcurrentRunner{
		name:               name,
		limiter:            limiter,
		maxPendingDuration: maxPendingDuration,
		taskChan:           make(chan *Task),
		pendingTasks:       list.New(),
		pendingTaskCount:   make(map[string]int),
		pendingRegionTasks: make(map[string]*list.Element),
		maxWaitingDuration: RunnerTaskMaxWaitingDuration.WithLabelValues(name),
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
func (cr *ConcurrentRunner) Start() {
	cr.stopChan = make(chan struct{})
	cr.wg.Add(1)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
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
				cr.pendingTasks = list.New()
				cr.pendingMu.Unlock()
				log.Info("stopping async task runner", zap.String("name", cr.name))
				return
			case <-ticker.C:
				maxDuration := time.Duration(0)
				cr.pendingMu.Lock()
				if cr.pendingTasks.Len() > 0 {
					maxDuration = time.Since(cr.pendingTasks.Front().Value.(*Task).submittedAt)
				}
				for taskName, cnt := range cr.pendingTaskCount {
					RunnerPendingTasks.WithLabelValues(cr.name, taskName).Set(float64(cnt))
				}
				cr.pendingMu.Unlock()
				cr.maxWaitingDuration.Set(maxDuration.Seconds())
			}
		}
	}()
}

func (cr *ConcurrentRunner) run(task *Task, token *TaskToken) {
	start := time.Now()
	task.f()
	if token != nil {
		cr.limiter.ReleaseToken(token)
		cr.processPendingTasks()
	}
	RunnerTaskExecutionDuration.WithLabelValues(cr.name, task.name).Observe(time.Since(start).Seconds())
	RunnerSucceededTasks.WithLabelValues(cr.name, task.name).Inc()
}

func (cr *ConcurrentRunner) processPendingTasks() {
	cr.pendingMu.Lock()
	defer cr.pendingMu.Unlock()
	if cr.pendingTasks.Len() > 0 {
		task := cr.pendingTasks.Front().Value.(*Task)
		select {
		case cr.taskChan <- task:
			cr.pendingTasks.Remove(cr.pendingTasks.Front())
			cr.pendingTaskCount[task.name]--
			delete(cr.pendingRegionTasks, fmt.Sprintf("%d-%s", task.regionID, task.name))
		default:
		}
		return
	}
}

// Stop stops the runner.
func (cr *ConcurrentRunner) Stop() {
	close(cr.stopChan)
	cr.wg.Wait()
}

// RunTask runs the task asynchronously.
func (cr *ConcurrentRunner) RunTask(regionID uint64, name string, f func(), opts ...TaskOption) error {
	task := &Task{
		regionID:    regionID,
		name:        name,
		f:           f,
		submittedAt: time.Now(),
	}
	for _, opt := range opts {
		opt(task)
	}
	cr.processPendingTasks()
	cr.pendingMu.Lock()
	defer func() {
		cr.pendingMu.Unlock()
		cr.processPendingTasks()
	}()

	pendingTaskNum := cr.pendingTasks.Len()
	taskID := fmt.Sprintf("%d-%s", regionID, name)
	if pendingTaskNum > 0 {
		if element, ok := cr.pendingRegionTasks[taskID]; ok {
			// Update the task in pendingTasks
			element.Value = task
			// Update the task in pendingRegionTasks
			cr.pendingRegionTasks[taskID] = element
			return nil
		}
		if !task.retained {
			maxWait := time.Since(cr.pendingTasks.Front().Value.(*Task).submittedAt)
			if maxWait > cr.maxPendingDuration {
				RunnerFailedTasks.WithLabelValues(cr.name, task.name).Inc()
				return ErrMaxWaitingTasksExceeded
			}
		}
		// We use the max task number to limit the memory usage.
		// It occupies around 1.5GB memory when there is 20000000 pending task.
		if pendingTaskNum > maxPendingTaskNum {
			RunnerFailedTasks.WithLabelValues(cr.name, task.name).Inc()
			return ErrMaxWaitingTasksExceeded
		}
	}
	element := cr.pendingTasks.PushBack(task)
	cr.pendingRegionTasks[taskID] = element
	cr.pendingTaskCount[task.name]++
	return nil
}

// SyncRunner is a simple task runner that limits the number of concurrent tasks.
type SyncRunner struct{}

// NewSyncRunner creates a new SyncRunner.
func NewSyncRunner() *SyncRunner {
	return &SyncRunner{}
}

// RunTask runs the task synchronously.
func (*SyncRunner) RunTask(_ uint64, _ string, f func(), _ ...TaskOption) error {
	f()
	return nil
}

// Start starts the runner.
func (*SyncRunner) Start() {}

// Stop stops the runner.
func (*SyncRunner) Stop() {}
