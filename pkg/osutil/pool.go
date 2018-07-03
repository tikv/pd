// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"sync"
	"time"

	"github.com/juju/errors"
)

// Task is a task handle by worker.
type Task func()

// Worker serving services in a separate goroutine.
type Worker struct {
	lastUseTime time.Time
	ch          chan Task
}

// Pool serves incoming task via schdule the task to one worker in pool,
// in LIFO order. the most recently stopped worker will serve the next
// incoming task.
type Pool struct {
	sync.RWMutex
	MaxWorkersCount int

	workersCount int
	// the queue is in LIFO order
	readyQueue            []*Worker
	stopCh                chan struct{}
	workerPool            sync.Pool
	maxIdleWorkerDuration time.Duration
}

// NewPool creates a pool.
func NewPool(size int) *Pool {
	return &Pool{
		MaxWorkersCount: size,
		readyQueue:      make([]*Worker, 0, size),
	}
}

// SetMaxIdleWorkerDuration set the maximum idle worker duration.
// If this time is exceeded, the worker will be GC.
func (p *Pool) SetMaxIdleWorkerDuration(d time.Duration) {
	p.Lock()
	defer p.Unlock()
	p.maxIdleWorkerDuration = d
}

// WorkersCount returns the
func (p *Pool) WorkersCount() int {
	p.RLock()
	defer p.RUnlock()
	return p.workersCount
}

// Start starts the pool.
func (p *Pool) Start() {
	p.stopCh = make(chan struct{})
	go func() {
		for {
			select {
			case <-p.stopCh:
				return
			default:
			}
			p.clean()
			time.Sleep(p.getMaxIdleWokerDuration())
		}
	}()
}

// Stop stops the pool.
func (p *Pool) Stop() {
	close(p.stopCh)
	p.Lock()
	defer p.Unlock()
	for i, worker := range p.readyQueue {
		close(worker.ch)
		p.readyQueue[i] = nil
	}
	p.readyQueue = p.readyQueue[:0]
}

var defaultIdleDuration = 10 * time.Second

func (p *Pool) getMaxIdleWokerDuration() time.Duration {
	if p.maxIdleWorkerDuration <= 0 {
		return defaultIdleDuration
	}
	return p.maxIdleWorkerDuration
}

func (p *Pool) clean() {
	now := time.Now()
	var expired []*Worker
	p.Lock()
	ready := p.readyQueue
	n := len(ready)
	i := 0
	for ; i < n && now.Sub(ready[i].lastUseTime) > p.getMaxIdleWokerDuration(); i++ {
		expired = append(expired, ready[i])
	}
	p.readyQueue = ready[i:]
	p.Unlock()
	for _, worker := range expired {
		worker.ch <- nil
	}
}

// Schedule schdule the task to worker.
func (p *Pool) Schedule(task Task) error {
	var worker *Worker
	createWorker := false
	p.Lock()
	defer p.Unlock()
	n := len(p.readyQueue) - 1
	if n < 0 {
		if p.workersCount < p.MaxWorkersCount {
			createWorker = true
			p.workersCount++
		}
	} else {
		worker = p.readyQueue[n]
		p.readyQueue[n] = nil
		p.readyQueue = p.readyQueue[:n]
	}
	if worker == nil && createWorker {
		newWorker := p.workerPool.Get()
		if newWorker == nil {
			newWorker = &Worker{
				ch: make(chan Task, 1),
			}
		}
		worker = newWorker.(*Worker)
		go func() {
			p.poll(worker)
			p.workerPool.Put(newWorker)
		}()
	}
	if worker == nil {
		return errors.New("pool is busy now")
	}
	worker.ch <- task
	return nil
}

func (p *Pool) poll(worker *Worker) {
LOOP:
	for {
		select {
		case task := <-worker.ch:
			if task == nil {
				break LOOP
			}
			task()
			p.release(worker)
		case <-p.stopCh:
			break LOOP
		}
	}
	p.Lock()
	p.workersCount--
	p.Unlock()
}

func (p *Pool) release(worker *Worker) {
	worker.lastUseTime = time.Now()
	p.Lock()
	defer p.Unlock()
	p.readyQueue = append(p.readyQueue, worker)
}
