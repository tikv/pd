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
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/keypath"
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
	gcStateWatchMetadataWaitTimeout        = 5 * time.Minute
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

// GCStateWatcher merges ordered initial batches and live GC state changes for one stream.
// The initial scan is not globally atomic and may interleave with live delivery. For each
// keyspace, the merge prevents initial and live delivery from regressing to an older state.
//
// A watcher supports one receiving goroutine. Close may be called concurrently with
// receiving and with manager-owned lifecycle operations.
// nolint:revive // Keep GC in the name to match the established GCState domain API.
type GCStateWatcher struct {
	ctx              context.Context
	cancel           context.CancelCauseFunc
	manager          *GCStateManager
	id               uint64
	initCh           chan []GCStateChange
	liveCh           chan GCStateChange
	initDone         bool
	pendingInit      []GCStateChange
	pendingLiveCount int
	dirtyDuringInit  map[uint32]struct{}
	enabledKeyspaces *enabledKeyspaceCache
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

		if w.pendingLiveCount > 0 {
			// This watcher has one receiver, so every change counted when the
			// initial batch was acquired is still queued until we consume it.
			change := <-w.liveCh
			w.pendingLiveCount--
			if keyspaceID, valid := change.KeyspaceID(); valid {
				w.dirtyDuringInit[keyspaceID] = struct{}{}
			}
			return change, true, nil
		}

		for len(w.pendingInit) > 0 {
			change := w.pendingInit[0]
			w.pendingInit = w.pendingInit[1:]
			keyspaceID, ok := change.KeyspaceID()
			if ok {
				if _, dirty := w.dirtyDuringInit[keyspaceID]; dirty {
					// Registration precedes the initial scan, so a post-registration live v2 may
					// race with initial v1. If v1 is consumed first, delivery is v1 then v2; if
					// v2 is consumed first, this later v1 is suppressed and delivery is v2 only.
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
		failpoint.InjectCall("watchGCStatesInitialBatchReceived")
		w.pendingInit = batch
		// Snapshot only after acquiring the initial batch. Registration precedes
		// its scan, and mutations publish under the manager mutex before the next
		// mutation can update the cache. Thus any live state older than this batch's
		// initial state is already queued or consumed. The initial state's own live
		// publication may still follow its cache store, but cannot cause regression.
		// Drain this FIFO prefix first and suppress initial scopes it makes dirty.
		// Keep the remaining count across RecvBatch calls; later arrivals must not
		// extend the prefix and indefinitely postpone unrelated initial states.
		w.pendingLiveCount = len(w.liveCh)
	}
}

// Done returns a channel that is closed when the watcher terminates.
func (w *GCStateWatcher) Done() <-chan struct{} {
	return w.ctx.Done()
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
	var cache *enabledKeyspaceCache
	var generation uint64
	if !skipLoadingInitial {
		m.mu.RLock()
		generation = m.activeLeadershipGeneration.Load()
		cache = m.enabledKeyspaces
		m.mu.RUnlock()
		if generation == 0 {
			watcher.cancel(errs.ErrNotLeader)
			return nil, errs.ErrNotLeader
		}
		if cache != nil {
			failpoint.InjectCall("watchGCStatesBeforeCacheReady")
			waitCtx, cancel := context.WithTimeout(ctx, gcStateWatchMetadataWaitTimeout)
			err := cache.waitReady(waitCtx)
			cancel()
			if err != nil {
				if ctx.Err() == nil && cache.termCtx.Err() != nil {
					err = errs.ErrNotLeader
				}
				watcher.cancel(err)
				return nil, err
			}
		}
	}

	m.mu.Lock()
	if m.activeLeadershipGeneration.Load() == 0 || (generation != 0 && m.activeLeadershipGeneration.Load() != generation) {
		m.mu.Unlock()
		watcher.cancel(errs.ErrNotLeader)
		return nil, errs.ErrNotLeader
	}
	m.nextWatcherID++
	watcher.manager = m
	watcher.id = m.nextWatcherID
	watcher.enabledKeyspaces = cache
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
	if watcher.Err() != nil {
		return
	}

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

	addState := func(state GCState) {
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
	}
	var err error
	if watcher.enabledKeyspaces != nil {
		err = m.iterateEnabledKeyspacesGCStates(watcher.ctx, watcher.enabledKeyspaces, addState)
	} else {
		err = m.iterateAllKeyspacesGCStates(watcher.ctx, true, func(uint32) bool { return true }, addState, nil)
	}

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

func (m *GCStateManager) iterateEnabledKeyspacesGCStates(
	ctx context.Context,
	cache *enabledKeyspaceCache,
	cb func(GCState),
) error {
	// The default Get is linearizable. This exact key supplies only the global
	// revision; metadata membership comes from the shared index.
	probeCtx, cancel := context.WithTimeout(ctx, enabledKeyspaceRequestTimeout)
	resp, err := cache.client.Get(probeCtx, keypath.KeyspaceMetaPrefix())
	cancel()
	if err != nil {
		return fmt.Errorf("probe keyspace metadata revision: %w", err)
	}
	failpoint.InjectCall("watchGCStatesTargetRevisionProbed", resp.Header.Revision)
	waitCtx, cancel := context.WithTimeout(ctx, gcStateWatchMetadataWaitTimeout)
	entries, _, err := cache.snapshotAtLeast(waitCtx, resp.Header.Revision)
	cancel()
	if err != nil {
		return fmt.Errorf("wait for keyspace metadata revision %d: %w", resp.Header.Revision, err)
	}

	nullState, err := m.getGCStateImpl(constant.NullKeyspaceID, true)
	if err != nil {
		return err
	}
	cb(nullState)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.gcManagementType != keyspace.KeyspaceLevelGC {
			cb(GCState{KeyspaceID: entry.id, IsKeyspaceLevel: false})
			continue
		}
		state, err := m.getGCStateImpl(entry.id, true)
		if err != nil {
			return err
		}
		cb(state)
	}
	return nil
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
