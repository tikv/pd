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
	generation  *gcStateGeneration
	keyspaceIDs []uint32
	flights     []*gcStateLoadFlight
	result      gcStateLoadResult
}

// prepareGCStateLoadBatch consumes currently available candidates until it owns
// 60 missing scopes or next reports exhaustion. next must never block. Cache
// checking and claiming a flight share a critical section with publication.
//
// A nonempty batch retains m.mu.RLock. Its owner MUST call execute on the SAME
// goroutine, after releasing any assembly lock and before waiting on anything.
// Empty batches retain no lock, but execute still returns their result. A nil
// batch on error owns no lock. Cached completions and joined flights are available
// in batch.result immediately; the owner must execute before waiting on flights.
func (m *GCStateManager) prepareGCStateLoadBatch(ctx context.Context, generation *gcStateGeneration, next func() (uint32, bool)) (*gcStateLoadBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	failpoint.InjectCall("beforeGCStateLoadManagerLock")
	m.mu.RLock()
	if err := ctx.Err(); err != nil {
		m.mu.RUnlock()
		return nil, err
	}
	if generation == nil || m.activeGeneration.Load() != generation {
		m.mu.RUnlock()
		return nil, errs.ErrNotLeader
	}
	batch := &gcStateLoadBatch{generation: generation, result: gcStateLoadResult{failed: make(map[uint32]error), joined: make(map[uint32]*gcStateLoadFlight)}}
	seen := make(map[uint32]struct{})
	for len(batch.keyspaceIDs) < endpoint.MaxGCSafePointBatchSize {
		id, ok := next()
		if !ok {
			break
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		generation.mu.Lock()
		if m.activeGeneration.Load() != generation {
			generation.mu.Unlock()
			// Already claimed flights must still be executed to release the read lock.
			if len(batch.keyspaceIDs) == 0 {
				m.mu.RUnlock()
				return nil, errs.ErrNotLeader
			}
			break
		}
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
		generation.mu.Unlock()
	}
	if len(batch.keyspaceIDs) == 0 {
		m.mu.RUnlock()
	}
	return batch, nil
}

// executeGCStateLoadBatch reads and publishes an owned batch, then releases its
// manager read lock. It never waits for joined flights. ctx must be independent
// of individual foreground waiters; generation retirement cancels the I/O.
// Call exactly once for each prepared batch, including cancellation/error paths.
func (m *GCStateManager) executeGCStateLoadBatch(ctx context.Context, batch *gcStateLoadBatch) gcStateLoadResult {
	if len(batch.keyspaceIDs) == 0 {
		return batch.result
	}
	defer m.mu.RUnlock()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	generation := batch.generation
	generation.mu.Lock()
	if m.activeGeneration.Load() != generation {
		cancel()
	} else {
		generation.cancels[batch] = cancel
	}
	generation.mu.Unlock()
	values, err := m.gcMetaStorage.LoadGCSafePointPairs(ctx, batch.keyspaceIDs)
	failpoint.InjectCall("afterGCStateLoadBatchRead")
	generation.mu.Lock()
	defer generation.mu.Unlock()
	delete(generation.cancels, batch)
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
	return batch.result
}

// loadGCState gives each foreground request independent cancellation. The owner
// goroutine performs both prepare and execute, so no manager lock is transferred
// between goroutines. Joined requests only wait after preparation released it.
func (m *GCStateManager) loadGCState(ctx context.Context, generation *gcStateGeneration, id uint32) (GCState, error) {
	for {
		ready := make(chan gcStateLoadResult, 1)
		go func() {
			pending := true
			batch, err := m.prepareGCStateLoadBatch(context.WithoutCancel(ctx), generation, func() (uint32, bool) {
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
			ready <- m.executeGCStateLoadBatch(context.WithoutCancel(ctx), batch)
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
		}
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
}
