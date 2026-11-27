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
	"errors"
	"fmt"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

// keyspaceGCStateLoadError marks failures confined to a keyspace or its
// migration source. Watch initialization and warmup can skip affected keyspaces.
type keyspaceGCStateLoadError struct{ cause error }

func (e *keyspaceGCStateLoadError) Error() string { return e.cause.Error() }
func (e *keyspaceGCStateLoadError) Unwrap() error { return e.cause }

// Cause exposes the original error to github.com/pingcap/errors.
func (e *keyspaceGCStateLoadError) Cause() error { return e.cause }

func canSkipKeyspaceGCState(id uint32, err error) bool {
	if id == constant.NullKeyspaceID {
		return false
	}
	_, ok := errors.AsType[*keyspaceGCStateLoadError](err)
	return ok
}

// loadLocalSafePointPairs marks per-scope decode errors for watch and warmup.
// Storage and response errors affect the whole batch.
func (m *GCStateManager) loadLocalSafePointPairs(ctx context.Context, ids []uint32) ([]endpoint.GCSafePointReadResult, error) {
	pairs, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, ids)
	for i := range pairs {
		if decodeErr := pairs[i].Err; decodeErr != nil {
			pairs[i].Err = &keyspaceGCStateLoadError{decodeErr}
		}
	}
	return pairs, err
}

// err becomes immutable when done closes. Success describes this load, even if
// its cached values have been invalidated before a waiter observes completion.
type gcStateLoadFlight struct {
	done chan struct{}
	err  error
}

func (f *gcStateLoadFlight) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.done:
		return f.err
	}
}

type gcStateLoadResult struct {
	completed []uint32
	failed    map[uint32]error
	joined    map[uint32]*gcStateLoadFlight
}

type gcStateLoadBatch struct {
	keyspaceIDs []uint32
	flights     []*gcStateLoadFlight
	result      gcStateLoadResult
}

func (g *gcStateGeneration) loadContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.ctx, cancel)
	// AfterFunc dispatches asynchronously when already canceled. Observe that
	// state synchronously so no new load starts in a canceled generation.
	if g.ctx.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}

// loadGCStateBatch consumes currently available candidates until it owns 60
// missing scopes or next reports exhaustion, then reads and publishes them.
// Cached completions and joined flights do not consume read slots. next consumes
// only available candidates and must not acquire the manager or generation lock.
//
// Each local read holds the manager read lock through publication, excluding
// safe-point writers. Pending migrations retain their flights while doing
// external I/O outside the lock. Joined flights never block the batch.
func (m *GCStateManager) loadGCStateBatch(ctx context.Context, generation *gcStateGeneration, next func() (uint32, bool)) (gcStateLoadResult, error) {
	if err := ctx.Err(); err != nil {
		return gcStateLoadResult{}, err
	}
	if generation == nil || m.activeGeneration.Load() != generation {
		return gcStateLoadResult{}, errs.ErrNotLeader
	}
	ctx, cancel := generation.loadContext(ctx)
	defer cancel()
	batch := &gcStateLoadBatch{result: gcStateLoadResult{failed: make(map[uint32]error), joined: make(map[uint32]*gcStateLoadFlight)}}
	pending, err := m.loadLocalGCStateBatch(ctx, generation, next, batch)
	if err != nil {
		return gcStateLoadResult{}, err
	}
	if len(pending) != 0 {
		m.migrateStarterBatch(ctx, generation, pending, 0, func(items []*migrationItem) {
			m.publishStarterGroup(ctx, generation, batch, items)
		})
	}
	return batch.result, nil
}

// Hold the read lock from the group's final local read through publication.
// An ordinary GC write may already have advanced beyond the frozen T.
func (m *GCStateManager) publishStarterGroup(ctx context.Context, generation *gcStateGeneration, batch *gcStateLoadBatch, items []*migrationItem) {
	ids := make([]uint32, 0, len(items))
	for _, item := range items {
		if item.err == nil {
			ids = append(ids, item.localRead.KeyspaceID)
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	pairs, err := m.loadLocalSafePointPairs(ctx, ids)
	generation.mu.Lock()
	defer generation.mu.Unlock()
	for _, item := range items {
		pair := endpoint.GCSafePointReadResult{Err: item.err}
		if pair.Err == nil {
			pair.Err = err
			if err == nil {
				pair, pairs = pairs[0], pairs[1:]
				if pair.Err == nil {
					pair.Err = validateStarterGCState(pair)
				}
			}
		}
		m.finishGCStateFlight(ctx, generation, batch, item.index, pair)
	}
}

func (m *GCStateManager) loadLocalGCStateBatch(ctx context.Context, generation *gcStateGeneration, next func() (uint32, bool), batch *gcStateLoadBatch) ([]*migrationItem, error) {
	failpoint.InjectCall("beforeGCStateLoadManagerLock")
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.activeGeneration.Load() != generation {
		return nil, errs.ErrNotLeader
	}
	// Keep flight claims and batch assembly in one critical section.
	// Once a flight is claimed, every exit goes through publication below.
	err := func() error {
		generation.mu.Lock()
		defer generation.mu.Unlock()
		if m.activeGeneration.Load() != generation {
			return errs.ErrNotLeader
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		seen := make(map[uint32]struct{})
		for len(batch.keyspaceIDs) < endpoint.MaxGCSafePointBatchSize {
			if ctx.Err() != nil || m.activeGeneration.Load() != generation {
				break
			}
			id, ok := next()
			if !ok {
				break
			}
			if _, duplicate := seen[id]; duplicate {
				continue
			}
			seen[id] = struct{}{}
			if _, cached := m.gcStateCache.load(id); cached {
				batch.result.completed = append(batch.result.completed, id)
				failpoint.InjectCall("getGCStateCacheAccess", "slow_hit")
				gcStateCacheAccessSlowHitCounter.Inc()
			} else {
				// A join is still a cache miss for this caller, even though it does
				// not issue another storage read.
				failpoint.InjectCall("getGCStateCacheAccess", "miss")
				gcStateCacheAccessMissCounter.Inc()
				if flight, loading := generation.flights[id]; loading {
					batch.result.joined[id] = flight
					failpoint.InjectCall("onGCStateLoadJoined", id)
				} else {
					flight := &gcStateLoadFlight{done: make(chan struct{})}
					generation.flights[id] = flight
					batch.keyspaceIDs = append(batch.keyspaceIDs, id)
					batch.flights = append(batch.flights, flight)
				}
			}
		}
		return nil
	}()
	if err != nil || len(batch.keyspaceIDs) == 0 {
		return nil, err
	}
	var values []endpoint.GCSafePointReadResult
	if m.activeGeneration.Load() != generation {
		err = errs.ErrNotLeader
	} else {
		values, err = m.loadLocalSafePointPairs(ctx, batch.keyspaceIDs)
		failpoint.InjectCall("afterGCStateLoadBatchRead", ctx, m, batch.keyspaceIDs)
	}
	generation.mu.Lock()
	defer generation.mu.Unlock()
	var pending []*migrationItem
	for i := range batch.keyspaceIDs {
		pair := endpoint.GCSafePointReadResult{Err: err}
		if err == nil {
			pair = values[i]
			var needsMigration bool
			needsMigration, pair.Err = needsStarterMigration(pair, m.migrationCutoff)
			if needsMigration {
				pending = append(pending, &migrationItem{index: i, localRead: pair})
				continue
			}
		}
		m.finishGCStateFlight(ctx, generation, batch, i, pair)
	}
	return pending, nil
}

// Both the manager read lock and generation.mu are held.
func (m *GCStateManager) finishGCStateFlight(ctx context.Context, generation *gcStateGeneration, batch *gcStateLoadBatch, i int, pair endpoint.GCSafePointReadResult) {
	id, flight := batch.keyspaceIDs[i], batch.flights[i]
	if generation.flights[id] != flight {
		// Retirement completed this flight. It must not retire a replacement.
		pair.Err = flight.err
	} else {
		if m.activeGeneration.Load() != generation {
			pair.Err = errs.ErrNotLeader
		} else if ctx.Err() != nil {
			pair.Err = ctx.Err()
		}
		if pair.Err == nil {
			m.gcStateCache.store(id, gcStateCacheEntry{TxnSafePoint: pair.TxnSafePoint, GCSafePoint: pair.GCSafePoint})
			failpoint.InjectCall("onGCStateLoadPublished", m, id)
		}
		flight.err = pair.Err
		close(flight.done)
		delete(generation.flights, id)
	}
	if pair.Err == nil {
		batch.result.completed = append(batch.result.completed, id)
	} else {
		batch.result.failed[id] = pair.Err
	}
}

func validateStarterGCState(pair endpoint.GCSafePointReadResult) error {
	if pair.TxnSafePointEmpty {
		return &keyspaceGCStateLoadError{fmt.Errorf("empty transaction safe point for keyspace %d", pair.KeyspaceID)}
	}
	if pair.GCSafePoint > pair.TxnSafePoint {
		return &keyspaceGCStateLoadError{fmt.Errorf("keyspace %d GC safe point %d exceeds transaction safe point %d", pair.KeyspaceID, pair.GCSafePoint, pair.TxnSafePoint)}
	}
	return nil
}

// loadGCState gives each foreground request independent cancellation. The owner
// goroutine owns the entire loader call. Joined requests only wait after it
// has released the manager lock.
func (m *GCStateManager) loadGCState(ctx context.Context, generation *gcStateGeneration, id uint32) (GCState, error) {
	// Successful safe-point writes refresh the cache, but failed writes invalidate
	// it. Repeated failures can remove successful fills before this waiter rechecks
	// the cache. Allow one shared retry, then return a direct atomic storage snapshot
	// without requiring that its cached values survive an intervening writer.
	for range 2 {
		ready := make(chan gcStateLoadResult, 1)
		go func() {
			pending := true
			result, err := m.loadGCStateBatch(context.WithoutCancel(ctx), generation, func() (uint32, bool) {
				if !pending {
					return 0, false
				}
				pending = false
				return id, true
			})
			if err != nil {
				ready <- gcStateLoadResult{failed: map[uint32]error{id: err}}
				return
			}
			ready <- result
		}()
		var result gcStateLoadResult
		select {
		case <-ctx.Done():
			return GCState{}, ctx.Err()
		case result = <-ready:
		}
		if err := result.failed[id]; err != nil {
			return GCState{}, err
		}
		if flight := result.joined[id]; flight != nil {
			if err := flight.wait(ctx); err != nil {
				return GCState{}, err
			}
			failpoint.InjectCall("afterGCStateLoadJoinedWait")
		}
		failpoint.InjectCall("afterGCStateLoadCompleted")
		// Foreground waiters never return a flight's old payload. Recheck generation
		// around the cache read, and retry if an intervening writer invalidated it.
		if m.activeGeneration.Load() != generation {
			return GCState{}, errs.ErrNotLeader
		}
		cached, ok := m.gcStateCache.load(id)
		if m.activeGeneration.Load() != generation {
			return GCState{}, errs.ErrNotLeader
		}
		if ok {
			return GCState{KeyspaceID: id, IsKeyspaceLevel: id != constant.NullKeyspaceID, TxnSafePoint: cached.TxnSafePoint, GCSafePoint: cached.GCSafePoint}, nil
		}
	}
	// Keep request cancellation responsive even if the direct reader is waiting
	// for the manager lock. Unlike a shared load, this read belongs to this caller.
	ready := make(chan endpoint.GCSafePointReadResult, 1)
	go func() { ready <- m.readGCStateDirect(ctx, generation, id) }()
	select {
	case <-ctx.Done():
		return GCState{}, ctx.Err()
	case <-generation.done:
		return GCState{}, errs.ErrNotLeader
	case result := <-ready:
		failpoint.InjectCall("beforeGCStateLoadDirectReturn")
		// Retirement or request cancellation may happen after the reader releases
		// its lock, even when the ready branch wins the select above.
		if m.activeGeneration.Load() != generation {
			return GCState{}, errs.ErrNotLeader
		}
		if err := ctx.Err(); err != nil {
			return GCState{}, err
		}
		if result.Err != nil {
			return GCState{}, result.Err
		}
		return GCState{KeyspaceID: id, IsKeyspaceLevel: id != constant.NullKeyspaceID, TxnSafePoint: result.TxnSafePoint, GCSafePoint: result.GCSafePoint}, nil
	}
}

func (m *GCStateManager) readGCStateDirect(ctx context.Context, generation *gcStateGeneration, id uint32) endpoint.GCSafePointReadResult {
	if err := m.ensureMigratedTxnSafePoint(ctx, generation, id); err != nil {
		return endpoint.GCSafePointReadResult{Err: err}
	}
	failpoint.InjectCall("beforeGCStateLoadManagerLock")
	m.mu.RLock()
	defer m.mu.RUnlock()
	if generation == nil || m.activeGeneration.Load() != generation {
		return endpoint.GCSafePointReadResult{Err: errs.ErrNotLeader}
	}
	ctx, cancel := generation.loadContext(ctx)
	defer cancel()
	failpoint.InjectCall("getGCStateCacheAccess", "miss")
	gcStateCacheAccessMissCounter.Inc()
	if err := ctx.Err(); err != nil {
		return endpoint.GCSafePointReadResult{Err: err}
	}
	values, err := m.loadLocalSafePointPairs(ctx, []uint32{id})
	if m.activeGeneration.Load() != generation {
		err = errs.ErrNotLeader
	} else if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		return endpoint.GCSafePointReadResult{Err: err}
	}
	result := values[0]
	if m.migrationCutoff > 0 && id != constant.NullKeyspaceID && result.Err == nil {
		result.Err = validateStarterGCState(result)
	}
	return result
}
