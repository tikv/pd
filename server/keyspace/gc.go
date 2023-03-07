// Copyright 2023 TiKV Project Authors.
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

package keyspace

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/server/config"
	"go.uber.org/zap"
)

// Some common per-loop config.
const (
	// gcPerLoopTimeout specifies timeout for a single loop.
	gcPerLoopTimeout = 30 * time.Second
	// gcKeyspaceBatch specifies batch size when fetching keyspaces.
	gcKeyspaceBatch = 100
)

// gcWorker is used to clean up keyspace related information.
type gcWorker struct {
	sync.RWMutex
	manager *Manager
	member  *member.EmbeddedEtcdMember

	nextKeyspaceID uint32
	ticker         *time.Ticker
	// gcLifeTime specifies how long should we keep archived keyspace.
	gcLifeTime time.Duration
}

// newGCWorker returns a newGCWorker.
func (manager *Manager) newGCWorker() *gcWorker {
	dummyTicker := time.NewTicker(time.Hour)
	dummyTicker.Stop()

	worker := &gcWorker{
		manager:    manager,
		member:     manager.member,
		ticker:     dummyTicker, // A dummy ticker, real ticker will be setup by reload.
		gcLifeTime: manager.config.GCLifeTime.Duration,
	}
	return worker
}

// run starts the main loop of the gc worker.
func (worker *gcWorker) run() {
	go func() {
		for {
			select {
			// If manager's context done, stop the loop completely.
			case <-worker.manager.ctx.Done():
				worker.ticker.Stop()
				log.Info("[keyspace] gc loop stopped due to context cancel")
				return
			case now := <-worker.ticker.C:
				if !worker.member.IsLeader() {
					// If server currently not leader, stop the ticker and don't do gc.
					worker.ticker.Stop()
					log.Info("[keyspace] gc loop stopped, server is not leader")
					continue
				}
				// If a keyspace archived before safePoint, we should clean up the keyspace.
				worker.RLock()
				safePoint := now.Add(-worker.gcLifeTime).Unix()
				failpoint.Inject("safePoint", func(val failpoint.Value) {
					// Adds an option to overwrite safePoint for easier testing.
					injectedSafePoint, ok := val.(int64)
					if ok {
						safePoint = injectedSafePoint
					}
				})
				worker.RUnlock()
				log.Info("[keyspace] starting gc")
				worker.nextKeyspaceID = worker.scanKeyspacesAndDoGC(worker.manager.ctx, safePoint)
			}
		}
	}()
}

func (worker *gcWorker) reload(cfg *config.KeyspaceConfig) {
	worker.ticker.Stop()
	worker.Lock()
	defer worker.Unlock()

	if !cfg.GCEnable {
		log.Info("[keyspace] gc disabled")
		return
	}
	// Set the worker's gc lifetime and run duration
	worker.gcLifeTime = cfg.GCLifeTime.Duration
	worker.ticker.Reset(cfg.GCRunInterval.Duration)
	log.Info("[keyspace] gc config reloaded",
		zap.Duration("gc lifetime", worker.gcLifeTime),
		zap.Duration("run interval", cfg.GCRunInterval.Duration),
	)
}

// scanKeyspacesAndDoGC scans current keyspace and attempts to do one round of gc within gcPerLoopTimeout.
// It starts with nextKeyspaceID, and return the last garbage collected keyspace's id + 1 as the starting id
// for next round.
func (worker *gcWorker) scanKeyspacesAndDoGC(ctx context.Context, safePoint int64) (nextID uint32) {
	nextID = worker.nextKeyspaceID
	// Set up per loop timeout and corresponding cancel function.
	ctx, cancel := context.WithTimeout(ctx, gcPerLoopTimeout)
	defer cancel()

	manager := worker.manager
	var (
		batch []*keyspacepb.KeyspaceMeta
		err   error
	)
	// Scan all keyspaces.
	for {
		select {
		case <-ctx.Done():
			log.Info("[keyspace] stopping gc loop due to context cancel")
			return
		default:
		}

		// Loads one batch of keyspaces.
		if batch, err = manager.store.LoadRangeKeyspace(nextID, gcKeyspaceBatch); err != nil {
			log.Error("[keyspace] stopping gc loop, failed to fetch keyspace meta", zap.Error(err))
			return
		}
		// Batch length of 0 means that we finished scanning one round of all keyspaces.
		if len(batch) == 0 {
			// start next batch should be from the beginning.
			nextID = 0
			continue
		}
		for _, meta := range batch {
			if canGC(meta, safePoint) {
				worker.gcKeyspace(ctx, meta)
			}
		}
		nextID += uint32(len(batch))
	}
}

// gcKeyspace gc one keyspace related information.
// It will be tried again in the next scan if it fails.
func (worker *gcWorker) gcKeyspace(ctx context.Context, meta *keyspacepb.KeyspaceMeta) {
	select {
	case <-ctx.Done():
		log.Info("[keyspace] skipping gc due to context cancel",
			zap.Uint32("ID", meta.GetId()), zap.String("name", meta.GetName()))
		return
	default:
	}

	log.Info("[keyspace] start cleaning keyspace meta data",
		zap.String("name", meta.GetName()),
		zap.Uint32("ID", meta.GetId()),
		zap.String("state", meta.GetState().String()),
		zap.Int64("last state change", meta.GetStateChangedAt()),
	)

	// Following section should be idempotent:
	// TODO: Clean TiKV range.
	// TODO: Clean TiFlash placement rules.
	// TODO: Clean Region Label rules.
	// TODO: Clean keyspace related etcd paths.
	// And only when all of the above succeeded:
	// TODO: Set keyspace state to TOMBSTONE
}
