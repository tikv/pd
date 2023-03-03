package keyspace

import (
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	gcPerLoopTimeout = 30 * time.Second
	gcKeyspaceBatch  = 100
)

// keyspaceGCLoop starts the gc loop.
func (manager *Manager) keyspaceGCLoop() {
	ctx, cancel := context.WithCancel(manager.ctx)
	defer cancel()

	var (
		gcRunInterval         = manager.config.GCRunInterval.Duration
		gcLifeTime            = manager.config.GCLifeTime.Duration
		nextKeyspaceID uint32 = 0
	)

	ticker := time.NewTicker(gcRunInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			// If gc not enabled or if server currently not leader, skip.
			if !(manager.config.GCEnable && manager.member.IsLeader()) {
				continue
			}
			gcSafePoint := now.Add(-gcLifeTime).Unix()
			nextKeyspaceID = manager.scanKeyspacesAndDoGC(ctx, gcSafePoint, nextKeyspaceID)
		}
	}
}

// scanKeyspacesAndDoGC scans current keyspace and attempts to do one round of gc within gcPerLoopTimeout.
// It starts with nextKeyspaceID, and return the last garbage collected keyspace's id + 1 as the starting id
// for next round.
func (manager *Manager) scanKeyspacesAndDoGC(ctx context.Context, gcSafePoint int64, startID uint32) (nextID uint32) {
	nextID = startID
	// Set up per loop timeout and corresponding cancel function.
	ctx, cancel := context.WithTimeout(ctx, gcPerLoopTimeout)
	defer cancel()

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
		if batch, err = manager.store.LoadRangeKeyspace(nextID, gcKeyspaceBatch); err != nil {
			log.Info("[keyspace] stopping gc loop, failed to fetch keyspace meta")
			return
		}
		if len(batch) == 0 {
			// finished scanning one round of all keyspaces, now we start from beginning.
			nextID = 0
			continue
		}
		for _, meta := range batch {
			if canGC(meta, gcSafePoint) {
				manager.gcKeyspace(ctx, meta)
			}
		}
		nextID = nextID + uint32(len(batch))
	}
}

func canGC(meta *keyspacepb.KeyspaceMeta, gcSafePoint int64) bool {
	if meta.GetState() != keyspacepb.KeyspaceState_ARCHIVED {
		return false
	}
	return meta.GetStateChangedAt() < gcSafePoint
}

// gcKeyspace gc one keyspace related information.
// It will be tried again in the next scan if it fails.
func (manager *Manager) gcKeyspace(ctx context.Context, meta *keyspacepb.KeyspaceMeta) {
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
