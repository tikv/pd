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
	"context"
	"sync"
	"time"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	gcStateWarmupWorkers       = 4
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
	retryAt    time.Time
	retryDelay time.Duration
}

// gcStateWarmup owns only the initial discovery campaign. inputMu protects
// short, nonblocking callback admission, independently of assembly and I/O.
// assembly serializes candidate consumption through the common loader's single
// cache/flight check. Workers release it before executing their own batch.
type gcStateWarmup struct {
	manager    *GCStateManager
	generation *gcStateGeneration
	index      *enabledKeyspaceCache

	inputMu   syncutil.Mutex
	accepting bool
	pages     chan []enabledKeyspace
	snapshot  chan []enabledKeyspace
	wake      chan struct{}
	done      chan struct{}

	assembly syncutil.Mutex
	scopes   map[uint32]*gcStateWarmupScope
	// Only joined flights and failed scopes need periodic inspection.
	waiting     map[uint32]*gcStateWarmupScope
	remaining   int
	candidates  []uint32
	initial     bool
	nullPending bool
	finished    chan struct{}
}

func newGCStateWarmup(m *GCStateManager, generation *gcStateGeneration, index *enabledKeyspaceCache) *gcStateWarmup {
	return &gcStateWarmup{
		manager:     m,
		generation:  generation,
		index:       index,
		accepting:   true,
		pages:       make(chan []enabledKeyspace, gcStateWarmupPageQueue),
		snapshot:    make(chan []enabledKeyspace, 1),
		wake:        make(chan struct{}, gcStateWarmupWorkers),
		done:        make(chan struct{}),
		finished:    make(chan struct{}),
		nullPending: true,
		scopes:      map[uint32]*gcStateWarmupScope{constant.NullKeyspaceID: {}},
		waiting:     make(map[uint32]*gcStateWarmupScope),
		remaining:   1,
	}
}

func (w *gcStateWarmup) notify() {
	for range gcStateWarmupWorkers {
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
	workers := new(sync.WaitGroup)
	workers.Add(gcStateWarmupWorkers)
	for range gcStateWarmupWorkers {
		go func() {
			defer workers.Done()
			w.work(ctx)
		}()
	}
	workers.Wait()
	// Callbacks never acquire assembly. Closing admission before dropping the
	// mailboxes also releases queued pages when the campaign is cancelled.
	w.inputMu.Lock()
	w.accepting = false
	w.pages = nil
	w.snapshot = nil
	w.inputMu.Unlock()
	w.scopes = nil
	w.waiting = nil
	w.candidates = nil
}

func (w *gcStateWarmup) work(ctx context.Context) {
	ticker := time.NewTicker(gcStateWarmupPollInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		w.assembly.Lock()
		if !w.nullPending {
			w.ingest()
			w.refresh(time.Now())
		}
		if w.complete() {
			w.assembly.Unlock()
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
			batch, err := w.manager.prepareGCStateLoadBatch(ctx, w.generation, next)
			w.assembly.Unlock()
			if err != nil {
				return
			}
			result := w.manager.executeGCStateLoadBatch(ctx, batch)
			w.assembly.Lock()
			w.record(result, time.Now())
			w.assembly.Unlock()
			w.notify()
			continue
		}
		w.assembly.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-w.generation.done:
			return
		case <-w.finished:
			return
		case <-w.wake:
		case <-ticker.C:
		}
	}
}

// ingest only receives already available pages. No manager lock is needed for
// target reconciliation, and no producer can block on this assembly section.
func (w *gcStateWarmup) ingest() {
	for !w.initial {
		select {
		case entries := <-w.pages:
			for _, entry := range entries {
				if entry.gcManagementType == keyspace.KeyspaceLevelGC {
					w.add(entry.id)
				}
			}
		default:
			goto snapshot
		}
	}
snapshot:
	if w.initial {
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
		for id := range w.waiting {
			if targets[id] == nil {
				delete(w.waiting, id)
			}
		}
		w.initial = true
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
	for {
		if len(w.candidates) == 0 {
			w.ingest()
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
	for id, scope := range w.waiting {
		if scope.joined != nil {
			select {
			case <-scope.joined.done:
				err := scope.joined.err
				scope.joined = nil
				w.finish(id, scope, err, now)
			default:
			}
		}
		if scope.completed || scope.queued || scope.running || scope.joined != nil || scope.retryAt.IsZero() || now.Before(scope.retryAt) {
			continue
		}
		// Only retries consult the live index. They may retire an obsolete
		// original target, but live membership never adds a new target.
		if w.initial && id != constant.NullKeyspaceID && w.index != nil {
			w.index.mu.Lock()
			entry, exists := w.index.entries[id]
			needed := !w.index.ready || (exists && entry.gcManagementType == keyspace.KeyspaceLevelGC)
			w.index.mu.Unlock()
			if !needed {
				w.finish(id, scope, nil, now)
				continue
			}
		}
		delete(w.waiting, id)
		scope.queued = true
		w.candidates = append(w.candidates, id)
	}
}

func (w *gcStateWarmup) record(result gcStateLoadResult, now time.Time) {
	for _, id := range result.completed {
		if scope := w.scopes[id]; scope != nil {
			w.finish(id, scope, nil, now)
		}
	}
	for id, err := range result.failed {
		if scope := w.scopes[id]; scope != nil {
			w.finish(id, scope, err, now)
		}
	}
	for id, flight := range result.joined {
		if scope := w.scopes[id]; scope != nil {
			scope.running = false
			scope.joined = flight
			w.waiting[id] = scope
		}
	}
}

func (w *gcStateWarmup) finish(id uint32, scope *gcStateWarmupScope, err error, now time.Time) {
	scope.running = false
	if err == nil {
		if !scope.completed {
			w.remaining--
		}
		scope.completed = true
		delete(w.waiting, id)
		return
	}
	w.waiting[id] = scope
	if scope.retryDelay == 0 {
		scope.retryDelay = gcStateWarmupRetryDelay
	} else {
		scope.retryDelay = min(scope.retryDelay*2, gcStateWarmupMaxRetryDelay)
	}
	scope.retryAt = now.Add(scope.retryDelay)
}

func (w *gcStateWarmup) complete() bool {
	select {
	case <-w.finished:
		return true
	default:
	}
	if !w.initial || w.remaining != 0 {
		return false
	}
	close(w.finished)
	return true
}
