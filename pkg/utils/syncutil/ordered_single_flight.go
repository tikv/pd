// Copyright 2025 TiKV Project Authors.
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

package syncutil

import (
	"context"
	"sync/atomic"
)

type task[TResult any] struct {
	finishCh chan struct{}
	result   TResult
	err      error

	// The refState is a combination of the total number of callers waiting for the result of this task, and a flag
	// indicating whether the task is sealed, i.e., no more callers are allowed to share this task.
	// When a waiter is canceled, the caller count is decreased by one. When it reaches zero and the task is sealed,
	// the execution will be canceled by calling `execCancel`.
	refState   atomic.Int64
	execCtx    context.Context
	execCancel func()
}

const sealedFlag int64 = 1 << 62

func newTask[TResult any]() *task[TResult] {
	ctx, cancel := context.WithCancel(context.Background())
	t := &task[TResult]{
		finishCh:   make(chan struct{}, 1),
		execCtx:    ctx,
		execCancel: cancel,
	}
	return t
}

// cancelOne cancels one of the caller waiting for the task. Assumes it currently has at least one.
func (t *task[TResult]) cancelOne() {
	newValue := t.refState.Add(-1)
	sealed, remainingCallers := (newValue&sealedFlag) != 0, newValue&^sealedFlag
	if sealed && remainingCallers == 0 {
		t.execCancel()
	}
}

func (t *task[TResult]) seal() {
	var lastValue int64
	for {
		value := t.refState.Load()
		if value&sealedFlag != 0 {
			lastValue = value
			break
		}
		newValue := value | sealedFlag
		if t.refState.CompareAndSwap(value, newValue) {
			lastValue = newValue
			break
		}
	}
	remainingCallers := lastValue &^ sealedFlag
	if remainingCallers == 0 {
		t.execCancel()
	}
}

func (t *task[TResult]) finish() {
	close(t.finishCh)
	t.execCancel()
}

// OrderedSingleFlight is a utility to make concurrent calls to a function internally only one execution, and shares the
// execution result to the outer callers. This is similar to the normal singleflight.Group from
// golang.org/x/sync/singleflight package, but with the following differences:
//
//   - Key is not supported. An OrderedSingleFlight instance runs only one function.
//   - The invocation and inner execution is *strongly ordered*, which means, a call to Do gets the result of an
//     execution that is guaranteed to started after the beginning of the invocation, and an earlier execution that
//     started before the current invocation to Do won't be reused for this call. If there's already an execution running
//     in progress and a call to Do happens, it enters the next batch instead of sharing the result of the already-started
//     execution.
//   - The return type is generic, instead of interface{}.
type OrderedSingleFlight[TResult any] struct {
	tokenCh chan struct{}

	mu          Mutex
	pendingTask *task[TResult]

	// A counter calculating the accumulated number of executions.
	execCounter atomic.Int64
}

// NewOrderedSingleFlight creates an instance of OrderedSingleFlight.
func NewOrderedSingleFlight[TResult any]() *OrderedSingleFlight[TResult] {
	res := &OrderedSingleFlight[TResult]{
		tokenCh: make(chan struct{}, 1),
	}
	res.tokenCh <- struct{}{}
	return res
}

// Do tries to execute the function `f`, or if possible, wait and reuse the result of an execution triggered by another
// goroutine. See comments of OrderedSingleFlight for details.
func (s *OrderedSingleFlight[TResult]) Do(ctx context.Context, f func(context.Context) (TResult, error)) (TResult, error) {
	return s.doImpl(ctx, f, nil, nil)
}

func (s *OrderedSingleFlight[TResult]) doImpl(
	ctx context.Context,
	f func(context.Context) (TResult, error),
	onExecutionStart func(),
	onExecutionFinish func(),
) (TResult, error) {
	var currentTask *task[TResult]

	s.mu.Lock()
	if s.pendingTask == nil {
		s.pendingTask = newTask[TResult]()
	}
	currentTask = s.pendingTask
	currentTask.refState.Add(1)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		currentTask.cancelOne()
		var res TResult
		return res, ctx.Err()
	case <-currentTask.finishCh:
		// Executed by other goroutines. Reuse the result.
		return currentTask.result, currentTask.err
	case <-s.tokenCh:
	}

	// Token is acquired. Before starting the goroutine for executing the function f, any early return should be done
	// after returning back the token to tokenCh.

	// If another goroutine closes `finishCh` and releases the token at almost the same time,
	// it's possible that the token arrives earlier than the `finishCh` is detected to be closed. Check the result
	// again. If the result is already ready, return it directly.
	select {
	case <-currentTask.finishCh:
		// Release the token back.
		s.tokenCh <- struct{}{}
		return currentTask.result, currentTask.err
	default:
	}

	// The following code executes exactly once for each instance of the `task`.

	// Unset the pendingTask, so further calls introduces the next pending task
	s.mu.Lock()
	s.pendingTask = nil
	// Once a caller executes to this point, it disallows further callers to share the task. The task is marked sealed,
	// which means since now, once the ref count decreases to 0, the task will be canceled.
	currentTask.seal()
	s.mu.Unlock()

	if onExecutionStart != nil {
		onExecutionStart()
	}

	// Execute the function and distribute the result. Spawn it into a separate goroutine to make the current call able
	// to be canceled by the context without interrupting other callers.
	go func() {
		defer func() {
			if onExecutionFinish != nil {
				onExecutionFinish()
			}
			s.tokenCh <- struct{}{}
		}()

		res, err := f(currentTask.execCtx)
		currentTask.result = res
		currentTask.err = err
		s.execCounter.Add(1)
		currentTask.finish()
	}()

	select {
	case <-ctx.Done():
		currentTask.cancelOne()
		var res TResult
		return res, ctx.Err()
	case <-currentTask.finishCh:
		return currentTask.result, currentTask.err
	}
}

// ExecCount returns the accumulated number of executions of the inner function.
func (s *OrderedSingleFlight[TResult]) ExecCount() int64 {
	return s.execCounter.Load()
}

type orderedSingleFlightGroupEntry[TResult any] struct {
	singleFlight *OrderedSingleFlight[TResult]
	callers      int
	executions   int
}

// OrderedSingleFlightGroup is a keyed wrapper around OrderedSingleFlight.
type OrderedSingleFlightGroup[TKey comparable, TResult any] struct {
	mu      Mutex
	entries map[TKey]*orderedSingleFlightGroupEntry[TResult]
}

// NewOrderedSingleFlightGroup creates an instance of OrderedSingleFlightGroup.
func NewOrderedSingleFlightGroup[TKey comparable, TResult any]() *OrderedSingleFlightGroup[TKey, TResult] {
	return &OrderedSingleFlightGroup[TKey, TResult]{
		entries: make(map[TKey]*orderedSingleFlightGroupEntry[TResult]),
	}
}

// Do tries to execute the function `f` for the given key, or if possible, wait and reuse the result of an execution
// triggered by another goroutine with the same key.
func (g *OrderedSingleFlightGroup[TKey, TResult]) Do(ctx context.Context, key TKey, f func(context.Context) (TResult, error)) (TResult, error) {
	entry := g.acquireEntry(key)
	defer g.releaseCaller(key, entry)

	return entry.singleFlight.doImpl(
		ctx,
		f,
		func() {
			g.startExecution(entry)
		},
		func() {
			g.finishExecution(key, entry)
		},
	)
}

func (g *OrderedSingleFlightGroup[TKey, TResult]) acquireEntry(key TKey) *orderedSingleFlightGroupEntry[TResult] {
	g.mu.Lock()
	defer g.mu.Unlock()

	entry, ok := g.entries[key]
	if !ok {
		entry = &orderedSingleFlightGroupEntry[TResult]{
			singleFlight: NewOrderedSingleFlight[TResult](),
		}
		g.entries[key] = entry
	}
	entry.callers++
	return entry
}

func (g *OrderedSingleFlightGroup[TKey, TResult]) releaseCaller(key TKey, entry *orderedSingleFlightGroupEntry[TResult]) {
	g.mu.Lock()
	defer g.mu.Unlock()

	entry.callers--
	g.tryDeleteLocked(key, entry)
}

func (g *OrderedSingleFlightGroup[TKey, TResult]) startExecution(entry *orderedSingleFlightGroupEntry[TResult]) {
	g.mu.Lock()
	defer g.mu.Unlock()

	entry.executions++
}

func (g *OrderedSingleFlightGroup[TKey, TResult]) finishExecution(key TKey, entry *orderedSingleFlightGroupEntry[TResult]) {
	g.mu.Lock()
	defer g.mu.Unlock()

	entry.executions--
	g.tryDeleteLocked(key, entry)
}

func (g *OrderedSingleFlightGroup[TKey, TResult]) tryDeleteLocked(key TKey, entry *orderedSingleFlightGroupEntry[TResult]) {
	if entry.callers == 0 && entry.executions == 0 && g.entries[key] == entry {
		delete(g.entries, key)
	}
}
