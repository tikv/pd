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
	"go.uber.org/zap"
)

const initialCapacity = 100

// Runner is the interface for running tasks.
type Runner interface {
	RunTask(ctx context.Context, opt TaskOpts, f func(context.Context)) error
}

// Task is a task to be run.
type Task struct {
	Ctx         context.Context
	Opts        TaskOpts
	f           func(context.Context)
	submittedAt time.Time
}

// ErrMaxWaitingTasksExceeded is returned when the number of waiting tasks exceeds the maximum.
var ErrMaxWaitingTasksExceeded = errors.New("max waiting tasks exceeded")

// AsyncRunner is a simple task runner that limits the number of concurrent tasks.
type AsyncRunner struct {
	name               string
	maxPendingDuration time.Duration
	taskChan           chan *Task
	pendingTasks       []*Task
	pendingMu          sync.Mutex
	stopChan           chan struct{}
	wg                 sync.WaitGroup
}

// NewAsyncRunner creates a new AsyncRunner.
func NewAsyncRunner(name string, maxPendingDuration time.Duration) *AsyncRunner {
	s := &AsyncRunner{
		name:               name,
		maxPendingDuration: maxPendingDuration,
		taskChan:           make(chan *Task),
		pendingTasks:       make([]*Task, 0, initialCapacity),
		stopChan:           make(chan struct{}),
	}
	s.Start()
	return s
}

// TaskOpts is the options for RunTask.
type TaskOpts struct {
	// TaskName is a human-readable name for the operation. TODO: metrics by name.
	TaskName string
	Limit    *ConcurrencyLimiter
}

// Start starts the runner.
func (s *AsyncRunner) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case task := <-s.taskChan:
				if task.Opts.Limit != nil {
					token, err := task.Opts.Limit.Acquire(context.Background())
					if err != nil {
						log.Error("failed to acquire semaphore", zap.String("task-name", task.Opts.TaskName), zap.Error(err))
						continue
					}
					go s.run(task.Ctx, task.f, token)
				} else {
					go s.run(task.Ctx, task.f, nil)
				}
			case <-s.stopChan:
				log.Info("stopping async task runner", zap.String("name", s.name))
				return
			}
		}
	}()
}

func (s *AsyncRunner) run(ctx context.Context, task func(context.Context), token *TaskToken) {
	task(ctx)
	if token != nil {
		token.Release()
		s.processPendingTasks()
	}
}

func (s *AsyncRunner) processPendingTasks() {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for len(s.pendingTasks) > 0 {
		task := s.pendingTasks[0]
		select {
		case s.taskChan <- task:
			s.pendingTasks = s.pendingTasks[1:]
			return
		default:
			return
		}
	}
}

// Stop stops the runner.
func (s *AsyncRunner) Stop() {
	close(s.stopChan)
	s.wg.Wait()
}

// RunTask runs the task asynchronously.
func (s *AsyncRunner) RunTask(ctx context.Context, opt TaskOpts, f func(context.Context)) error {
	task := &Task{
		Ctx:  ctx,
		Opts: opt,
		f:    f,
	}
	s.processPendingTasks()
	select {
	case s.taskChan <- task:
	default:
		s.pendingMu.Lock()
		defer s.pendingMu.Unlock()
		if len(s.pendingTasks) > 0 {
			maxWait := time.Since(s.pendingTasks[0].submittedAt)
			if maxWait > s.maxPendingDuration {
				return ErrMaxWaitingTasksExceeded
			}
		}
		task.submittedAt = time.Now()
		s.pendingTasks = append(s.pendingTasks, task)
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
func (*SyncRunner) RunTask(ctx context.Context, _ TaskOpts, f func(context.Context)) error {
	f(ctx)
	return nil
}
