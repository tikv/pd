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
)

type task[TResult any] struct {
	finishCh chan struct{}
	result   TResult
	err      error
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
func (s *OrderedSingleFlight[TResult]) Do(ctx context.Context, f func() (TResult, error)) (TResult, error) {
	var currentTask *task[TResult]

	s.mu.Lock()
	if s.pendingTask == nil {
		s.pendingTask = &task[TResult]{finishCh: make(chan struct{}, 1)}
	}
	currentTask = s.pendingTask
	s.mu.Unlock()

	select {
	case <-ctx.Done():
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
	s.mu.Unlock()

	// Execute the function and distribute the result. Spawn it into a separate goroutine to make the current call able
	// to be canceled by the context without interrupting other callers.
	go func() {
		defer func() {
			s.tokenCh <- struct{}{}
		}()

		res, err := f()
		currentTask.result = res
		currentTask.err = err
		close(currentTask.finishCh)
	}()

	select {
	case <-ctx.Done():
		var res TResult
		return res, ctx.Err()
	case <-currentTask.finishCh:
		return currentTask.result, currentTask.err
	}
}
