package keyspace

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/config"
	"go.uber.org/zap"
)

// Some common per-loop config.
const (
	// gcPerLoopTimeout specifies timeout for a single loop.
	gcPerLoopTimeout = 30 * time.Second
	// gcKeyspaceBatch specifies batch size when fetching keyspaces.
	gcKeyspaceBatch = 100
	// controlLoopInterval specifies the interval between each etcd leader check.
	controlLoopInterval = 30 * time.Second
)

// runSig is used to control the running of the gc loop.
type runSig int

// Signals for gcWorker.
const (
	sigStart runSig = iota
	sigStop
	sigReloadConfig
)

// gcWorker is used to clean up keyspace related information.
type gcWorker struct {
	running atomic.Bool

	ctx     context.Context
	cancel  context.CancelFunc
	manager *Manager
	member  *member.EmbeddedEtcdMember

	// sigCh is used to signal the gcWorker to start/stop.
	sigCh          chan runSig
	ticker         *time.Ticker
	nextKeyspaceID uint32

	// Below are the configurations for gcWorker,
	// modification requires calling reloadConfig afterward to take effect.

	enable atomic.Bool
	// configMu protects gcRunInterval and gcLifeTime.
	configMu syncutil.RWMutex
	// gcRunInterval specifies how often should gc loop be run.
	gcRunInterval time.Duration
	// gcLifeTime specifies how long should we keep archived keyspace.
	gcLifeTime time.Duration
}

// newGCWorker returns a newGCWorker.
func (manager *Manager) newGCWorker() *gcWorker {
	return &gcWorker{
		manager:       manager,
		member:        manager.member,
		sigCh:         make(chan runSig),
		gcRunInterval: manager.config.GCRunInterval.Duration,
		gcLifeTime:    manager.config.GCLifeTime.Duration,
	}
}

// run starts the main loop of the gc worker.
// To avoid locking, it should be the only routine with access to worker's ticker, ctx and cancel.
func (worker *gcWorker) run() {
	worker.running.Store(true)
	worker.ticker = time.NewTicker(worker.gcRunInterval)
	// setting up gc worker's context to be child of the manager.
	worker.ctx, worker.cancel = context.WithCancel(worker.manager.ctx)

	// Load configuration here to make sure changes only takes effect after reload.
	worker.configMu.RLock()
	gcLifeTime := worker.gcLifeTime
	gcRunInterval := worker.gcRunInterval
	worker.configMu.RUnlock()

	// start control loop.
	go worker.controlLoop()

	for {
		select {
		// If manager's context done, stop the loop completely.
		case <-worker.manager.ctx.Done():
			return
		case now := <-worker.ticker.C:
			// Sanity check: if server currently not leader, don't do gc.
			if !worker.member.IsLeader() {
				worker.running.Store(false)
				worker.ticker.Stop()
				worker.cancel()
				// continue here will make worker wait for the next signal from sigCh.
				continue
			}
			// If a keyspace archived before safePoint, we should clean up the keyspace.
			safePoint := now.Add(-gcLifeTime).Unix()
			worker.nextKeyspaceID = worker.scanKeyspacesAndDoGC(safePoint)
		case sig := <-worker.sigCh:
			// Control signal received, act accordingly.
			switch sig {
			case sigStart:
				worker.ctx, worker.cancel = context.WithCancel(worker.manager.ctx)
				worker.ticker = time.NewTicker(gcRunInterval)
				log.Info("[keyspace] gc loop started")
			case sigStop:
				worker.ticker.Stop()
				worker.cancel()
				log.Info("[keyspace] gc loop stopped")
			case sigReloadConfig:
				worker.configMu.RLock()
				gcLifeTime = worker.gcLifeTime
				gcRunInterval = worker.gcRunInterval
				worker.configMu.RUnlock()
			}
		}
	}
}

func (worker *gcWorker) reloadConfig(cfg *config.KeyspaceConfig) {
	worker.enable.Store(cfg.GCEnable)
	worker.configMu.Lock()
	worker.gcLifeTime = cfg.GCLifeTime.Duration
	worker.gcRunInterval = cfg.GCRunInterval.Duration
	worker.configMu.Unlock()
	worker.sigCh <- sigReloadConfig
}

func (worker *gcWorker) controlLoop() {
	ticker := time.NewTicker(controlLoopInterval)
	for {
		select {
		case <-worker.manager.ctx.Done():
			// If manager's ctx done, return.
			return
		case <-ticker.C:
			if worker.member.IsLeader() && worker.enable.Load() {
				// Make sure the gc loop is running if it's not.
				// Check if running first before CAS to avoid unnecessary operation.
				if !worker.running.Load() && worker.running.CompareAndSwap(false, true) {
					worker.sigCh <- sigStart
				}
			} else if worker.running.Load() && worker.running.CompareAndSwap(true, false) {
				worker.sigCh <- sigStop
			}
		}
	}
}

// scanKeyspacesAndDoGC scans current keyspace and attempts to do one round of gc within gcPerLoopTimeout.
// It starts with nextKeyspaceID, and return the last garbage collected keyspace's id + 1 as the starting id
// for next round.
func (worker *gcWorker) scanKeyspacesAndDoGC(safePoint int64) (nextID uint32) {
	nextID = worker.nextKeyspaceID
	// Set up per loop timeout and corresponding cancel function.
	ctx, cancel := context.WithTimeout(worker.ctx, gcPerLoopTimeout)
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
		nextID = nextID + uint32(len(batch))
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
	return
}
