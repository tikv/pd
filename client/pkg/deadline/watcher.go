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

package deadline

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/client/pkg/utils/timerutil"
)

// The `cancel` function will be invoked once the specified `timeout` elapses without receiving a `done` signal.
type deadline struct {
	timer  *time.Timer
	done   chan struct{}
	cancel context.CancelFunc
}

// Watcher is used to watch and manage the deadlines.
type Watcher struct {
	ctx    context.Context
	source string
	Ch     chan *deadline
}

// NewWatcher is used to create a new deadline watcher.
func NewWatcher(ctx context.Context, capacity int, source string) *Watcher {
	watcher := &Watcher{
		ctx:    ctx,
		source: source,
		Ch:     make(chan *deadline, capacity),
	}
	go watcher.Watch()
	return watcher
}

// Watch is used to watch the deadlines and invoke the `cancel` function when the deadline is reached.
// The `err` will be returned if the deadline is reached.
func (w *Watcher) Watch() {
	log.Info("[pd] start the deadline watcher", zap.String("source", w.source))
	defer log.Info("[pd] exit the deadline watcher", zap.String("source", w.source))
	for {
		select {
		case d := <-w.Ch:
			select {
			case <-d.timer.C:
				log.Error("[pd] the deadline is reached", zap.String("source", w.source))
				d.cancel()
				timerutil.GlobalTimerPool.Put(d.timer)
			case <-d.done:
				timerutil.GlobalTimerPool.Put(d.timer)
			case <-w.ctx.Done():
				timerutil.GlobalTimerPool.Put(d.timer)
				return
			}
		case <-w.ctx.Done():
			return
		}
	}
}

// Start is used to start a deadline. It returns a channel which will be closed when the deadline is reached.
// Returns nil if the deadline is not started.
func (w *Watcher) Start(
	ctx context.Context,
	timeout time.Duration,
	cancel context.CancelFunc,
) chan struct{} {
	// Check if the watcher is already canceled.
	select {
	case <-w.ctx.Done():
		return nil
	case <-ctx.Done():
		return nil
	default:
	}
	// Initialize the deadline.
	timer := timerutil.GlobalTimerPool.Get(timeout)
	d := &deadline{
		timer:  timer,
		done:   make(chan struct{}),
		cancel: cancel,
	}
	// Send the deadline to the watcher.
	select {
	case <-w.ctx.Done():
		timerutil.GlobalTimerPool.Put(timer)
		return nil
	case <-ctx.Done():
		timerutil.GlobalTimerPool.Put(timer)
		return nil
	case w.Ch <- d:
		return d.done
	}
}
