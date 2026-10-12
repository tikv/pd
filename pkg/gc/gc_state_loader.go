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

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

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
// This call owns the manager read lock through publication, excluding safe-point
// writers. Assembly is serialized by generation.mu, so concurrent workers cannot
// split a partial batch. The lock order is manager, generation, then any short
// scheduling lock acquired by next; generation.mu is released before storage I/O.
// ctx must be independent of individual foreground waiters. Generation retirement
// cancels the I/O, and joined flights are returned without waiting for them.
func (m *GCStateManager) loadGCStateBatch(ctx context.Context, generation *gcStateGeneration, next func() (uint32, bool)) (gcStateLoadResult, error) {
	if err := ctx.Err(); err != nil {
		return gcStateLoadResult{}, err
	}
	failpoint.InjectCall("beforeGCStateLoadManagerLock")
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return gcStateLoadResult{}, err
	}
	if generation == nil || m.activeGeneration.Load() != generation {
		return gcStateLoadResult{}, errs.ErrNotLeader
	}
	ctx, cancel := generation.loadContext(ctx)
	defer cancel()
	batch := &gcStateLoadBatch{result: gcStateLoadResult{failed: make(map[uint32]error), joined: make(map[uint32]*gcStateLoadFlight)}}
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
		// Leadership may have been gated while next was assembling the batch,
		// before retirement can acquire generation.mu and cancel its context.
		if m.activeGeneration.Load() != generation {
			cancel()
		}
		return nil
	}()
	if err != nil {
		return gcStateLoadResult{}, err
	}
	if len(batch.keyspaceIDs) == 0 {
		return batch.result, nil
	}
	values, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, batch.keyspaceIDs)
	failpoint.InjectCall("afterGCStateLoadBatchRead")
	generation.mu.Lock()
	defer generation.mu.Unlock()
	if m.activeGeneration.Load() != generation {
		err = errs.ErrNotLeader
	} else if ctx.Err() != nil {
		err = ctx.Err()
	}
	for i, id := range batch.keyspaceIDs {
		flight := batch.flights[i]
		scopeErr := err
		if scopeErr == nil {
			scopeErr = values[i].Err
		}
		if generation.flights[id] != flight {
			// Retirement completed this flight. It must not retire a replacement.
			scopeErr = flight.err
		} else {
			if scopeErr == nil {
				m.gcStateCache.store(id, gcStateCacheEntry{TxnSafePoint: values[i].TxnSafePoint, GCSafePoint: values[i].GCSafePoint})
			}
			flight.err = scopeErr
			close(flight.done)
			delete(generation.flights, id)
		}
		if scopeErr == nil {
			batch.result.completed = append(batch.result.completed, id)
		} else {
			batch.result.failed[id] = scopeErr
		}
	}
	return batch.result, nil
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
	failpoint.InjectCall("beforeGCStateLoadManagerLock")
	m.mu.RLock()
	defer m.mu.RUnlock()
	if generation == nil || m.activeGeneration.Load() != generation {
		return endpoint.GCSafePointReadResult{Err: errs.ErrNotLeader}
	}
	ctx, cancel := generation.loadContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return endpoint.GCSafePointReadResult{Err: err}
	}
	failpoint.InjectCall("getGCStateCacheAccess", "miss")
	gcStateCacheAccessMissCounter.Inc()
	values, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, []uint32{id})
	if m.activeGeneration.Load() != generation {
		err = errs.ErrNotLeader
	} else if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		return endpoint.GCSafePointReadResult{Err: err}
	}
	// Return the pair from this transaction directly; do not publish it to the
	// cache or re-enter the shared-load retry loop.
	return values[0]
}
