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

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
)

type gcStateChangeKind uint8

const (
	gcStateChangeUnknown gcStateChangeKind = iota
	gcStateChangeUpsert
	gcStateChangeRemoved
)

// GCStateChange describes one effective GC state change for a keyspace scope.
// nolint:revive // Keep GC in the name to match the established GCState domain API.
type GCStateChange struct {
	kind              gcStateChangeKind
	upsert            GCState
	removedKeyspaceID uint32
}

// NewGCStateUpsert creates a change containing the complete effective GC state.
func NewGCStateUpsert(state GCState) GCStateChange {
	state.GCBarriers = nil
	return GCStateChange{kind: gcStateChangeUpsert, upsert: state}
}

// NewGCStateRemoved creates a change that removes a keyspace scope.
func NewGCStateRemoved(keyspaceID uint32) GCStateChange {
	return GCStateChange{kind: gcStateChangeRemoved, removedKeyspaceID: keyspaceID}
}

// Upsert returns the effective GC state when the change is an upsert.
func (c GCStateChange) Upsert() (GCState, bool) {
	return c.upsert, c.kind == gcStateChangeUpsert
}

// RemovedKeyspaceID returns the removed keyspace ID when the change is a removal.
func (c GCStateChange) RemovedKeyspaceID() (uint32, bool) {
	return c.removedKeyspaceID, c.kind == gcStateChangeRemoved
}

// KeyspaceID returns the keyspace scope changed by this value.
func (c GCStateChange) KeyspaceID() (uint32, bool) {
	if state, ok := c.Upsert(); ok {
		return state.KeyspaceID, true
	}
	return c.RemovedKeyspaceID()
}

const (
	defaultGCStateWatchInitialBatchSize    = 1024
	defaultGCStateWatchInitChannelCapacity = 1
	defaultGCStateWatchLiveChannelCapacity = 1024
)

type gcStateWatchConfig struct {
	initialBatchSize    int
	initChannelCapacity int
	liveChannelCapacity int
}

type gcStateWatcherTerminationReason string

const (
	watcherTerminationClientCancel gcStateWatcherTerminationReason = "client_cancel"
	watcherTerminationLeaderLost   gcStateWatcherTerminationReason = "leader_lost"
	watcherTerminationSlowConsumer gcStateWatcherTerminationReason = "slow_consumer"
	watcherTerminationInitError    gcStateWatcherTerminationReason = "init_error"
)

// GCStateWatcher receives a consistent initial view followed by live GC state changes.
//
// A watcher supports one receiving goroutine. Close may be called concurrently with
// receiving and with manager-owned lifecycle operations.
// nolint:revive // Keep GC in the name to match the established GCState domain API.
type GCStateWatcher struct {
	ctx             context.Context
	cancel          context.CancelCauseFunc
	manager         *GCStateManager
	id              uint64
	initCh          chan []GCStateChange
	liveCh          chan GCStateChange
	initDone        bool
	pendingInit     []GCStateChange
	dirtyDuringInit map[uint32]struct{}
}

func newGCStateWatcher(parent context.Context, cfg gcStateWatchConfig, skipLoadingInitial bool) *GCStateWatcher {
	ctx, cancel := context.WithCancelCause(parent)
	watcher := &GCStateWatcher{
		ctx:      ctx,
		cancel:   cancel,
		initCh:   make(chan []GCStateChange, cfg.initChannelCapacity),
		liveCh:   make(chan GCStateChange, cfg.liveChannelCapacity),
		initDone: skipLoadingInitial,
	}
	if !skipLoadingInitial {
		watcher.dirtyDuringInit = make(map[uint32]struct{})
	}
	return watcher
}

func (w *GCStateWatcher) receiveOne(block bool) (GCStateChange, bool, error) {
	for {
		if err := w.Err(); err != nil {
			return GCStateChange{}, false, err
		}

		for len(w.pendingInit) > 0 {
			change := w.pendingInit[0]
			w.pendingInit = w.pendingInit[1:]
			keyspaceID, ok := change.KeyspaceID()
			if ok {
				if _, dirty := w.dirtyDuringInit[keyspaceID]; dirty {
					// For each scope, consumers observe either initial v1 followed by live v2,
					// or live v2 with the later-arriving initial v1 suppressed.
					continue
				}
			}
			return change, true, nil
		}
		w.pendingInit = nil

		if w.initDone {
			if block {
				select {
				case <-w.ctx.Done():
					return GCStateChange{}, false, w.Err()
				case change := <-w.liveCh:
					return change, true, nil
				}
			}
			select {
			case <-w.ctx.Done():
				return GCStateChange{}, false, w.Err()
			case change := <-w.liveCh:
				return change, true, nil
			default:
				return GCStateChange{}, false, nil
			}
		}

		var (
			change GCStateChange
			batch  []GCStateChange
			ok     bool
		)
		if block {
			select {
			case <-w.ctx.Done():
				return GCStateChange{}, false, w.Err()
			case change = <-w.liveCh:
				if keyspaceID, valid := change.KeyspaceID(); valid {
					// Marking live scopes dirty preserves the alternate v2-only order when
					// initial v1 has not yet been delivered.
					w.dirtyDuringInit[keyspaceID] = struct{}{}
				}
				return change, true, nil
			case batch, ok = <-w.initCh:
			}
		} else {
			select {
			case <-w.ctx.Done():
				return GCStateChange{}, false, w.Err()
			case change = <-w.liveCh:
				if keyspaceID, valid := change.KeyspaceID(); valid {
					w.dirtyDuringInit[keyspaceID] = struct{}{}
				}
				return change, true, nil
			case batch, ok = <-w.initCh:
			default:
				return GCStateChange{}, false, nil
			}
		}

		if !ok {
			w.initCh = nil
			w.initDone = true
			w.dirtyDuringInit = nil
			continue
		}
		w.pendingInit = append([]GCStateChange(nil), batch...)
	}
}

// Err returns the first cause that terminated the watcher.
func (w *GCStateWatcher) Err() error {
	return context.Cause(w.ctx)
}

// RecvBatch waits for one visible change and opportunistically collects up to maxChanges.
func (w *GCStateWatcher) RecvBatch(maxChanges int) ([]GCStateChange, error) {
	if maxChanges <= 0 {
		panic("GCStateWatcher.RecvBatch requires a positive maximum")
	}
	first, ok, err := w.receiveOne(true)
	if err != nil {
		return nil, err
	}
	if !ok {
		panic("blocking watcher receive returned no result")
	}
	result := []GCStateChange{first}
	for len(result) < maxChanges {
		change, ok, err := w.receiveOne(false)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		result = append(result, change)
	}
	if err := w.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Close stops the watcher and removes it from its manager.
func (w *GCStateWatcher) Close() {
	if w.manager == nil {
		w.cancel(context.Canceled)
		return
	}
	w.manager.terminateGCStateWatcher(w, context.Canceled, watcherTerminationClientCancel)
}

// WatchGCStates registers a watcher in the current local leadership generation.
func (m *GCStateManager) WatchGCStates(ctx context.Context, skipLoadingInitial bool) (*GCStateWatcher, error) {
	return m.registerGCStateWatcher(ctx, skipLoadingInitial, gcStateWatchConfig{
		initialBatchSize:    defaultGCStateWatchInitialBatchSize,
		initChannelCapacity: defaultGCStateWatchInitChannelCapacity,
		liveChannelCapacity: defaultGCStateWatchLiveChannelCapacity,
	})
}

func (m *GCStateManager) registerGCStateWatcher(
	ctx context.Context,
	skipLoadingInitial bool,
	cfg gcStateWatchConfig,
) (*GCStateWatcher, error) {
	watcher := newGCStateWatcher(ctx, cfg, skipLoadingInitial)

	m.mu.Lock()
	if m.activeLeadershipGeneration.Load() == 0 {
		m.mu.Unlock()
		watcher.cancel(errs.ErrNotLeader)
		return nil, errs.ErrNotLeader
	}
	m.nextWatcherID++
	watcher.manager = m
	watcher.id = m.nextWatcherID
	m.watchers[watcher.id] = watcher
	gcStateWatcherGauge.Inc()
	m.mu.Unlock()

	failpoint.InjectCall("watchGCStatesRegistered")
	if !skipLoadingInitial {
		go m.loadInitialGCStates(watcher, cfg.initialBatchSize)
	}
	return watcher, nil
}

func (m *GCStateManager) loadInitialGCStates(watcher *GCStateWatcher, batchSize int) {
	batch := make([]GCStateChange, 0, batchSize)
	stopped := false
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		ready := batch
		batch = make([]GCStateChange, 0, batchSize)
		select {
		case watcher.initCh <- ready:
			return true
		case <-watcher.ctx.Done():
			return false
		}
	}

	err := m.iterateAllKeyspacesGCStates(
		watcher.ctx,
		true,
		func(uint32) bool { return true },
		func(state GCState) {
			if stopped {
				return
			}
			failpoint.InjectCall("watchGCStatesInitialStateLoaded", state.KeyspaceID)
			if watcher.Err() != nil {
				stopped = true
				return
			}
			batch = append(batch, NewGCStateUpsert(state))
			if len(batch) == batchSize {
				stopped = !flush()
			}
		},
		nil,
	)

	if stopped || watcher.Err() != nil {
		return
	}
	if err != nil {
		m.terminateGCStateWatcher(watcher, errors.Annotate(err, "load initial GC states"), watcherTerminationInitError)
		return
	}
	if !flush() {
		return
	}
	close(watcher.initCh)
}

func (m *GCStateManager) terminateGCStateWatcher(
	watcher *GCStateWatcher,
	cause error,
	reason gcStateWatcherTerminationReason,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terminateGCStateWatcherLocked(watcher, cause, reason)
}

func (m *GCStateManager) publishGCStateChangeLocked(change GCStateChange) {
	for _, watcher := range m.watchers {
		select {
		case watcher.liveCh <- change:
		default:
			log.Warn("GC state watcher is too slow",
				zap.Uint64("watcher-id", watcher.id),
				zap.Int("capacity", cap(watcher.liveCh)),
				zap.Int("queue-length", len(watcher.liveCh)))
			m.terminateGCStateWatcherLocked(watcher, errs.ErrGCStateWatcherSlowConsumer, watcherTerminationSlowConsumer)
		}
	}
	// TODO: Publish keyspace metadata upserts and removals through this same serialized path when an authoritative GC-leader-owned lifecycle hook exists.
}

func (m *GCStateManager) terminateGCStateWatcherLocked(
	watcher *GCStateWatcher,
	cause error,
	reason gcStateWatcherTerminationReason,
) {
	registered, ok := m.watchers[watcher.id]
	if !ok || registered != watcher {
		return
	}
	delete(m.watchers, watcher.id)
	gcStateWatcherGauge.Dec()
	recordGCStateWatcherTerminationMetrics(reason)
	watcher.cancel(cause)
}

func recordGCStateWatcherTerminationMetrics(reason gcStateWatcherTerminationReason) {
	switch reason {
	case watcherTerminationClientCancel:
		gcStateWatcherTerminationClientCancelCounter.Inc()
	case watcherTerminationLeaderLost:
		gcStateWatcherTerminationLeaderLostCounter.Inc()
	case watcherTerminationSlowConsumer:
		gcStateWatcherTerminationSlowConsumerCounter.Inc()
	case watcherTerminationInitError:
		gcStateWatcherTerminationInitErrorCounter.Inc()
	default:
		panic("unknown GC state watcher termination reason")
	}
}

func (m *GCStateManager) terminateAllGCStateWatchersLocked(
	cause error,
	reason gcStateWatcherTerminationReason,
) {
	for _, watcher := range m.watchers {
		m.terminateGCStateWatcherLocked(watcher, cause, reason)
	}
}
