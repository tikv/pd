// Copyright 2026 TiKV Project Authors.
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

package gc

import (
	"container/heap"
	"context"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	gcStateWarmupPageQueue     = 16
	gcStateWarmupPollInterval  = 100 * time.Millisecond
	gcStateWarmupRetryDelay    = time.Second
	gcStateWarmupMaxRetryDelay = 30 * time.Second
)

// A scope stays completed even if its cached value is later invalidated.
// joined is polled by the fixed worker pool, never by per-scope goroutines.
type gcStateWarmupScope struct {
	completed  bool
	queued     bool
	running    bool
	joined     *gcStateLoadFlight
	retryDelay time.Duration
}

// gcStateWarmup owns only the initial discovery campaign. inputMu protects
// short, nonblocking callback admission, independently of stateMu and I/O.
// stateMu protects scope scheduling and retry/completion state. Candidate
// callbacks consume only delivered pages; full snapshot reconciliation runs in
// the worker loop. Workers never hold stateMu while entering the loader or
// waiting for manager/generation locks or storage I/O.
type gcStateWarmup struct {
	manager     *GCStateManager
	generation  *gcStateGeneration
	index       *enabledKeyspaceIndex
	concurrency int

	inputMu   syncutil.Mutex
	accepting bool
	pages     chan []enabledKeyspace
	snapshot  chan []enabledKeyspace
	wake      chan struct{}
	done      chan struct{}

	stateMu syncutil.Mutex
	scopes  map[uint32]*gcStateWarmupScope
	// A pending scope is queued, running, joined, or in retries, exclusively.
	// Only joined flights need periodic inspection, shared by all workers.
	joined                    map[uint32]*gcStateWarmupScope
	nextJoinedCheck           time.Time
	retries                   gcStateWarmupRetries
	remaining                 int
	candidates                []uint32
	initialSnapshotReconciled bool
	nullPending               bool
	finished                  chan struct{}
}

func newGCStateWarmup(m *GCStateManager, generation *gcStateGeneration, index *enabledKeyspaceIndex) *gcStateWarmup {
	concurrency := m.getConfig().GCStateCacheWarmupConcurrency
	return &gcStateWarmup{
		manager:     m,
		generation:  generation,
		index:       index,
		concurrency: concurrency,
		accepting:   true,
		pages:       make(chan []enabledKeyspace, gcStateWarmupPageQueue),
		snapshot:    make(chan []enabledKeyspace, 1),
		wake:        make(chan struct{}, concurrency),
		done:        make(chan struct{}),
		finished:    make(chan struct{}),
		nullPending: true,
		scopes:      map[uint32]*gcStateWarmupScope{constant.NullKeyspaceID: {}},
		joined:      make(map[uint32]*gcStateWarmupScope),
		remaining:   1,
	}
}

func (w *gcStateWarmup) notify() {
	for range w.concurrency {
		select {
		case w.wake <- struct{}{}:
		default:
			return
		}
	}
}

func (w *gcStateWarmup) onPage(entries []enabledKeyspace) {
	w.inputMu.Lock()
	defer w.inputMu.Unlock()
	if !w.accepting {
		return
	}
	select {
	case w.pages <- entries:
	default:
	}
	// Missing hints are recovered from the complete initial snapshot.
	w.notify()
}

func (w *gcStateWarmup) onInitialSnapshot(entries []enabledKeyspace) {
	w.inputMu.Lock()
	defer w.inputMu.Unlock()
	if !w.accepting {
		return
	}
	select {
	case w.snapshot <- entries:
	default:
	}
	w.notify()
}

func (w *gcStateWarmup) run(ctx context.Context) {
	defer close(w.done)
	started := time.Now()
	log.Info("start warming GC state cache", zap.Int("concurrency", w.concurrency),
		zap.Int("batch-size", endpoint.MaxGCSafePointBatchSize))
	defer func() {
		message := "warming GC state cache canceled"
		select {
		case <-w.finished:
			message = "warming GC state cache completed"
		default:
		}
		log.Info(message, zap.Int("concurrency", w.concurrency), zap.Duration("cost", time.Since(started)))
	}()
	workers := new(sync.WaitGroup)
	workers.Add(w.concurrency)
	for range w.concurrency {
		go func() {
			defer workers.Done()
			w.work(ctx)
		}()
	}
	workers.Wait()
	// Callbacks never acquire stateMu. Closing admission before dropping the
	// mailboxes also releases queued pages when the campaign is cancelled.
	w.inputMu.Lock()
	w.accepting = false
	w.pages = nil
	w.snapshot = nil
	w.inputMu.Unlock()
	w.scopes = nil
	w.joined = nil
	w.retries = nil
	w.candidates = nil
}

func (w *gcStateWarmup) work(ctx context.Context) {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for ctx.Err() == nil {
		failpoint.InjectCall("beforeGCStateWarmupSchedule")
		w.stateMu.Lock()
		if !w.nullPending {
			w.ingestPages()
			w.reconcileInitialSnapshot()
			w.refresh(time.Now())
		}
		if w.tryFinish() {
			w.stateMu.Unlock()
			return
		}
		if w.nullPending || len(w.candidates) > 0 {
			next := w.next
			if w.nullPending {
				// Reserve the first background slot for an independent null
				// singleton, even when metadata pages are already available.
				w.nullPending = false
				w.scopes[constant.NullKeyspaceID].running = true
				pending := true
				next = func() (uint32, bool) {
					if !pending {
						return 0, false
					}
					pending = false
					return constant.NullKeyspaceID, true
				}
			}
			w.stateMu.Unlock()
			result, err := w.manager.loadGCStateBatch(ctx, w.generation, next)
			if err != nil {
				return
			}
			w.stateMu.Lock()
			w.recordBatchResult(result, time.Now())
			w.stateMu.Unlock()
			w.notify()
			continue
		}
		deadline := w.nextDeadline()
		w.stateMu.Unlock()
		failpoint.InjectCall("beforeGCStateWarmupWait")
		var timerC <-chan time.Time
		if !deadline.IsZero() {
			if timer == nil {
				timer = time.NewTimer(time.Until(deadline))
			} else {
				timer.Reset(time.Until(deadline))
			}
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			return
		case <-w.generation.done:
			return
		case <-w.finished:
			return
		case <-w.wake:
		case <-timerC:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// ingestPages only receives already available pages without waiting for producers.
func (w *gcStateWarmup) ingestPages() {
	for !w.initialSnapshotReconciled {
		select {
		case entries := <-w.pages:
			for _, entry := range entries {
				if entry.gcManagementType == keyspace.KeyspaceLevelGC {
					w.add(entry.id)
				}
			}
		default:
			return
		}
	}
}

// reconcileInitialSnapshot repairs dropped page hints and excludes targets from
// failed metadata attempts. It runs only in the worker loop, outside loader locks.
func (w *gcStateWarmup) reconcileInitialSnapshot() {
	if w.initialSnapshotReconciled {
		return
	}
	select {
	case entries := <-w.snapshot:
		targets := make(map[uint32]*gcStateWarmupScope, len(entries)+1)
		targets[constant.NullKeyspaceID] = w.scopes[constant.NullKeyspaceID]
		for _, entry := range entries {
			if entry.gcManagementType != keyspace.KeyspaceLevelGC {
				continue
			}
			w.add(entry.id)
			targets[entry.id] = w.scopes[entry.id]
		}
		// Hints from failed metadata attempts may not belong to the first
		// complete snapshot. Their successful cache fills remain valid, but
		// they are no longer part of this campaign's retry set.
		w.scopes = targets
		w.remaining = 0
		for _, scope := range targets {
			if !scope.completed {
				w.remaining++
			}
		}
		for id := range w.joined {
			if targets[id] == nil {
				delete(w.joined, id)
			}
		}
		if len(w.joined) == 0 {
			w.nextJoinedCheck = time.Time{}
		}
		w.retries = slices.DeleteFunc(w.retries, func(retry gcStateWarmupRetry) bool { return targets[retry.id] == nil })
		heap.Init(&w.retries)
		w.candidates = slices.DeleteFunc(w.candidates, func(id uint32) bool { return targets[id] == nil })
		w.initialSnapshotReconciled = true
	default:
	}
}

func (w *gcStateWarmup) add(id uint32) {
	if _, exists := w.scopes[id]; exists {
		return
	}
	w.scopes[id] = &gcStateWarmupScope{queued: true}
	w.remaining++
	w.candidates = append(w.candidates, id)
}

func (w *gcStateWarmup) next() (uint32, bool) {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	for {
		if len(w.candidates) == 0 {
			w.ingestPages()
			if len(w.candidates) == 0 {
				return 0, false
			}
		}
		id := w.candidates[0]
		w.candidates = w.candidates[1:]
		scope := w.scopes[id]
		if scope == nil || !scope.queued {
			continue
		}
		scope.queued = false
		scope.running = true
		return id, true
	}
}

func (w *gcStateWarmup) refresh(now time.Time) {
	if len(w.joined) > 0 && !now.Before(w.nextJoinedCheck) {
		// Workers share this deadline under stateMu, so wake notifications and
		// concurrent timers cannot repeat the same joined scan every turn.
		w.nextJoinedCheck = now.Add(gcStateWarmupPollInterval)
		for id, scope := range w.joined {
			select {
			case <-scope.joined.done:
				err := scope.joined.err
				scope.joined = nil
				delete(w.joined, id)
				w.recordScopeResult(id, scope, err, now)
			default:
			}
		}
		if len(w.joined) == 0 {
			w.nextJoinedCheck = time.Time{}
		}
	}
	// Even after a large outage, one scheduling turn only removes a batch of
	// due retries. Future retries require only a look at the heap's root.
	for range endpoint.MaxGCSafePointBatchSize {
		if len(w.retries) == 0 || now.Before(w.retries[0].retryAt) {
			break
		}
		retry := heap.Pop(&w.retries).(gcStateWarmupRetry)
		id := retry.id
		scope := w.scopes[id]
		// Only retries consult the live index. They may retire an obsolete
		// original target, but live membership never adds a new target.
		if w.initialSnapshotReconciled && id != constant.NullKeyspaceID && w.index != nil {
			if w.index.canSkipGCStateWarmup(id) {
				w.recordScopeResult(id, scope, nil, now)
				continue
			}
		}
		scope.queued = true
		w.candidates = append(w.candidates, id)
	}
}

// nextDeadline is called under stateMu before waiting. Buffered wake
// notifications preserve any earlier deadline or new input arriving after it.
func (w *gcStateWarmup) nextDeadline() time.Time {
	deadline := w.nextJoinedCheck
	if len(w.retries) > 0 && (deadline.IsZero() || w.retries[0].retryAt.Before(deadline)) {
		deadline = w.retries[0].retryAt
	}
	return deadline
}

func (w *gcStateWarmup) recordBatchResult(result gcStateLoadResult, now time.Time) {
	for _, id := range result.completed {
		if scope := w.scopes[id]; scope != nil {
			w.recordScopeResult(id, scope, nil, now)
		}
	}
	for id, err := range result.failed {
		if scope := w.scopes[id]; scope != nil {
			w.recordScopeResult(id, scope, err, now)
		}
	}
	for id, flight := range result.joined {
		if scope := w.scopes[id]; scope != nil {
			if len(w.joined) == 0 {
				w.nextJoinedCheck = now.Add(gcStateWarmupPollInterval)
			}
			scope.running = false
			scope.joined = flight
			w.joined[id] = scope
		}
	}
}

func (w *gcStateWarmup) recordScopeResult(id uint32, scope *gcStateWarmupScope, err error, now time.Time) {
	scope.running = false
	if err == nil {
		if !scope.completed {
			w.remaining--
		}
		scope.completed = true
		return
	}
	if scope.retryDelay == 0 {
		scope.retryDelay = gcStateWarmupRetryDelay
	} else {
		scope.retryDelay = min(scope.retryDelay*2, gcStateWarmupMaxRetryDelay)
	}
	heap.Push(&w.retries, gcStateWarmupRetry{id: id, retryAt: now.Add(scope.retryDelay)})
}

func (w *gcStateWarmup) tryFinish() bool {
	select {
	case <-w.finished:
		return true
	default:
	}
	if !w.initialSnapshotReconciled || w.remaining != 0 {
		return false
	}
	close(w.finished)
	return true
}
