// Copyright 2016 TiKV Project Authors.
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

package tso

import (
	"context"
	"fmt"
	"runtime/trace"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const (
	// UpdateTimestampGuard is the min timestamp interval.
	UpdateTimestampGuard = time.Millisecond
	// maxLogical is the max upper limit for logical time.
	// When a TSO's logical time reaches this limit,
	// the physical time will be forced to increase.
	maxLogical = int64(1 << 18)
	// jetLagWarningThreshold is the warning threshold of jetLag in `timestampOracle.UpdateTimestamp`.
	// In case of small `updatePhysicalInterval`, the `3 * updatePhysicalInterval` would also is small,
	// and trigger unnecessary warnings about clock offset.
	// It's an empirical value.
	jetLagWarningThreshold = 150 * time.Millisecond
)

// tsoObject is used to store the current TSO in memory with a RWMutex lock.
type tsoObject struct {
	syncutil.RWMutex
	physical time.Time
	logical  int64
}

// timestampOracle is used to maintain the logic of TSO.
type timestampOracle struct {
	client          *clientv3.Client
	keyspaceGroupID uint32
	// When tsPath is empty, it means that it is a global timestampOracle.
	tsPath  string
	storage endpoint.TSOStorage
	// TODO: remove saveInterval
	saveInterval           time.Duration
	updatePhysicalInterval time.Duration
	maxResetTSGap          func() time.Duration
	// tso info stored in the memory
	tsoMux *tsoObject
	// last timestamp window stored in etcd
	lastSavedTime atomic.Value // stored as time.Time

	// pre-initialized metrics
	metrics *tsoMetrics
}

func (t *timestampOracle) setTSOPhysical(next time.Time, force bool) {
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	// Do not update the zero physical time if the `force` flag is false.
	if t.tsoMux.physical == typeutil.ZeroTime && !force {
		return
	}
	// make sure the ts won't fall back
	if typeutil.SubTSOPhysicalByWallClock(next, t.tsoMux.physical) > 0 {
		t.tsoMux.physical = next
		t.tsoMux.logical = 0
	}
}

func (t *timestampOracle) getTSO() (time.Time, int64) {
	t.tsoMux.RLock()
	defer t.tsoMux.RUnlock()
	if t.tsoMux.physical == typeutil.ZeroTime {
		return typeutil.ZeroTime, 0
	}
	return t.tsoMux.physical, t.tsoMux.logical
}

// generateTSO will add the TSO's logical part with the given count and returns the new TSO result.
func (t *timestampOracle) generateTSO(ctx context.Context, count int64) (physical int64, logical int64) {
	defer trace.StartRegion(ctx, "timestampOracle.generateTSO").End()
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	if t.tsoMux.physical == typeutil.ZeroTime {
		return 0, 0
	}
	physical = t.tsoMux.physical.UnixNano() / int64(time.Millisecond)
	t.tsoMux.logical += count
	logical = t.tsoMux.logical
	return physical, logical
}

func (t *timestampOracle) getLastSavedTime() time.Time {
	last := t.lastSavedTime.Load()
	if last == nil {
		return typeutil.ZeroTime
	}
	return last.(time.Time)
}

// SyncTimestamp is used to synchronize the timestamp.
func (t *timestampOracle) SyncTimestamp() error {
	log.Info("start to sync timestamp", logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0))
	t.metrics.syncEvent.Inc()

	failpoint.Inject("delaySyncTimestamp", func() {
		time.Sleep(time.Second)
	})

	last, err := t.storage.LoadTimestamp(t.tsPath)
	if err != nil {
		return err
	}
	lastSavedTime := t.getLastSavedTime()
	// We could skip the synchronization if the following conditions are met:
	//   1. The timestamp in memory has been initialized.
	//   2. The last saved timestamp in etcd is not zero.
	//   3. The last saved timestamp in memory is not zero.
	//   4. The last saved timestamp in etcd is equal to the last saved timestamp in memory.
	// 1 is to ensure the timestamp in memory could always be initialized. 2-4 are to ensure
	// the synchronization could be skipped safely.
	if t.isInitialized() &&
		last != typeutil.ZeroTime &&
		lastSavedTime != typeutil.ZeroTime &&
		typeutil.SubRealTimeByWallClock(last, lastSavedTime) == 0 {
		log.Info("skip sync timestamp",
			logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0),
			zap.Time("last", last), zap.Time("last-saved", lastSavedTime))
		t.metrics.skipSyncEvent.Inc()
		return nil
	}

	next := time.Now()
	failpoint.Inject("fallBackSync", func() {
		next = next.Add(time.Hour)
	})
	failpoint.Inject("systemTimeSlow", func() {
		next = next.Add(-time.Hour)
	})
	// If the current system time minus the saved etcd timestamp is less than `UpdateTimestampGuard`,
	// the timestamp allocation will start from the saved etcd timestamp temporarily.
	if typeutil.SubRealTimeByWallClock(next, last) < UpdateTimestampGuard {
		log.Warn("system time may be incorrect",
			logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0),
			zap.Time("last", last), zap.Time("last-saved", lastSavedTime),
			zap.Time("next", next),
			errs.ZapError(errs.ErrIncorrectSystemTime))
		next = last.Add(UpdateTimestampGuard)
	}
	failpoint.Inject("failedToSaveTimestamp", func() {
		failpoint.Return(errs.ErrEtcdTxnInternal)
	})
	save := next.Add(t.saveInterval)
	start := time.Now()
	if err = t.storage.SaveTimestamp(keypath.TimestampPath(t.tsPath), save); err != nil {
		t.metrics.errSaveSyncTSEvent.Inc()
		return err
	}
	t.lastSavedTime.Store(save)
	t.metrics.syncSaveDuration.Observe(time.Since(start).Seconds())

	t.metrics.syncOKEvent.Inc()
	log.Info("sync and save timestamp",
		logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0),
		zap.Time("last", last), zap.Time("last-saved", lastSavedTime),
		zap.Time("save", save), zap.Time("next", next))
	// save into memory
	t.setTSOPhysical(next, true)
	return nil
}

// isInitialized is used to check whether the timestampOracle is initialized.
// There are two situations we have an uninitialized timestampOracle:
// 1. When the SyncTimestamp has not been called yet.
// 2. When the ResetUserTimestamp has been called already.
func (t *timestampOracle) isInitialized() bool {
	t.tsoMux.RLock()
	defer t.tsoMux.RUnlock()
	return t.tsoMux.physical != typeutil.ZeroTime
}

// resetUserTimestamp update the TSO in memory with specified TSO by an atomically way.
// When ignoreSmaller is true, resetUserTimestamp will ignore the smaller tso resetting error and do nothing.
// It's used to write MaxTS during the Global TSO synchronization without failing the writing as much as possible.
// cannot set timestamp to one which >= current + maxResetTSGap
func (t *timestampOracle) resetUserTimestamp(leadership *election.Leadership, tso uint64, ignoreSmaller, skipUpperBoundCheck bool) error {
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	if !leadership.Check() {
		t.metrics.errLeaseResetTSEvent.Inc()
		return errs.ErrResetUserTimestamp.FastGenByArgs("lease expired")
	}
	var (
		nextPhysical, nextLogical = tsoutil.ParseTS(tso)
		logicalDifference         = int64(nextLogical) - t.tsoMux.logical
		physicalDifference        = typeutil.SubTSOPhysicalByWallClock(nextPhysical, t.tsoMux.physical)
	)
	// do not update if next physical time is less/before than prev
	if physicalDifference < 0 {
		t.metrics.errResetSmallPhysicalTSEvent.Inc()
		if ignoreSmaller {
			return nil
		}
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified ts is smaller than now")
	}
	// do not update if next logical time is less/before/equal than prev
	if physicalDifference == 0 && logicalDifference <= 0 {
		t.metrics.errResetSmallLogicalTSEvent.Inc()
		if ignoreSmaller {
			return nil
		}
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified counter is smaller than now")
	}
	// do not update if physical time is too greater than prev
	if !skipUpperBoundCheck && physicalDifference >= t.maxResetTSGap().Milliseconds() {
		t.metrics.errResetLargeTSEvent.Inc()
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified ts is too larger than now")
	}
	// save into etcd only if nextPhysical is close to lastSavedTime
	if typeutil.SubRealTimeByWallClock(t.getLastSavedTime(), nextPhysical) <= UpdateTimestampGuard {
		save := nextPhysical.Add(t.saveInterval)
		start := time.Now()
		if err := t.storage.SaveTimestamp(keypath.TimestampPath(t.tsPath), save); err != nil {
			t.metrics.errSaveResetTSEvent.Inc()
			return err
		}
		t.lastSavedTime.Store(save)
		t.metrics.resetSaveDuration.Observe(time.Since(start).Seconds())
	}
	// save into memory only if nextPhysical or nextLogical is greater.
	t.tsoMux.physical = nextPhysical
	t.tsoMux.logical = int64(nextLogical)
	t.metrics.resetTSOOKEvent.Inc()
	return nil
}

// UpdateTimestamp is used to update the timestamp.
// This function will do two things:
//  1. When the logical time is going to be used up, increase the current physical time.
//  2. When the time window is not big enough, which means the saved etcd time minus the next physical time
//     will be less than or equal to `UpdateTimestampGuard`, then the time window needs to be updated and
//     we also need to save the next physical time plus `TSOSaveInterval` into etcd.
//
// Here is some constraints that this function must satisfy:
// 1. The saved time is monotonically increasing.
// 2. The physical time is monotonically increasing.
// 3. The physical time is always less than the saved timestamp.
//
// NOTICE: this function should be called after the TSO in memory has been initialized
// and should not be called when the TSO in memory has been reset anymore.
func (t *timestampOracle) UpdateTimestamp() error {
	if !t.isInitialized() {
		return errs.ErrUpdateTimestamp.FastGenByArgs("timestamp in memory has not been initialized")
	}
	prevPhysical, prevLogical := t.getTSO()
	t.metrics.tsoPhysicalGauge.Set(float64(prevPhysical.UnixNano() / int64(time.Millisecond)))
	t.metrics.tsoPhysicalGapGauge.Set(float64(time.Since(prevPhysical).Milliseconds()))

	now := time.Now()
	failpoint.Inject("fallBackUpdate", func() {
		now = now.Add(time.Hour)
	})
	failpoint.Inject("systemTimeSlow", func() {
		now = now.Add(-time.Hour)
	})

	t.metrics.saveEvent.Inc()

	jetLag := typeutil.SubRealTimeByWallClock(now, prevPhysical)
	if jetLag > 3*t.updatePhysicalInterval && jetLag > jetLagWarningThreshold {
		log.Warn("clock offset",
			logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0),
			zap.Duration("jet-lag", jetLag),
			zap.Time("prev-physical", prevPhysical),
			zap.Time("now", now),
			zap.Duration("update-physical-interval", t.updatePhysicalInterval))
		t.metrics.slowSaveEvent.Inc()
	}

	if jetLag < 0 {
		t.metrics.systemTimeSlowEvent.Inc()
	}

	var next time.Time
	// If the system time is greater, it will be synchronized with the system time.
	if jetLag > UpdateTimestampGuard {
		next = now
	} else if prevLogical > maxLogical/2 {
		// The reason choosing maxLogical/2 here is that it's big enough for common cases.
		// Because there is enough timestamp can be allocated before next update.
		log.Warn("the logical time may be not enough",
			logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0),
			zap.Int64("prev-logical", prevLogical))
		next = prevPhysical.Add(time.Millisecond)
	} else {
		// It will still use the previous physical time to alloc the timestamp.
		t.metrics.skipSaveEvent.Inc()
		return nil
	}

	// It is not safe to increase the physical time to `next`.
	// The time window needs to be updated and saved to etcd.
	if typeutil.SubRealTimeByWallClock(t.getLastSavedTime(), next) <= UpdateTimestampGuard {
		save := next.Add(t.saveInterval)
		start := time.Now()
		if err := t.storage.SaveTimestamp(keypath.TimestampPath(t.tsPath), save); err != nil {
			log.Warn("save timestamp failed",
				logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0),
				zap.String("timestamp-path", keypath.TimestampPath(t.tsPath)),
				zap.Error(err))
			t.metrics.errSaveUpdateTSEvent.Inc()
			return err
		}
		t.lastSavedTime.Store(save)
		t.metrics.updateSaveDuration.Observe(time.Since(start).Seconds())
	}
	// save into memory
	t.setTSOPhysical(next, false)

	return nil
}

var maxRetryCount = 10

// getTS is used to get a timestamp.
func (t *timestampOracle) getTS(ctx context.Context, leadership *election.Leadership, count uint32) (pdpb.Timestamp, error) {
	defer trace.StartRegion(ctx, "timestampOracle.getTS").End()
	var resp pdpb.Timestamp
	if count == 0 {
		return resp, errs.ErrGenerateTimestamp.FastGenByArgs("tso count should be positive")
	}
	for i := range maxRetryCount {
		currentPhysical, _ := t.getTSO()
		if currentPhysical == typeutil.ZeroTime {
			// If it's leader, maybe SyncTimestamp hasn't completed yet
			if leadership.Check() {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			t.metrics.notLeaderAnymoreEvent.Inc()
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
		}
		// Get a new TSO result with the given count
		resp.Physical, resp.Logical = t.generateTSO(ctx, int64(count))
		if resp.GetPhysical() == 0 {
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory has been reset")
		}
		if resp.GetLogical() >= maxLogical {
			log.Warn("logical part outside of max logical interval, please check ntp time, or adjust config item `tso-update-physical-interval`",
				logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0),
				zap.Reflect("response", resp),
				zap.Int("retry-count", i), errs.ZapError(errs.ErrLogicOverflow))
			t.metrics.logicalOverflowEvent.Inc()
			time.Sleep(t.updatePhysicalInterval)
			continue
		}
		// In case lease expired after the first check.
		if !leadership.Check() {
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs(fmt.Sprintf("requested %s anymore", errs.NotLeaderErr))
		}
		return resp, nil
	}
	t.metrics.exceededMaxRetryEvent.Inc()
	return resp, errs.ErrGenerateTimestamp.FastGenByArgs("generate global tso maximum number of retries exceeded")
}

// ResetTimestamp is used to reset the timestamp in memory.
func (t *timestampOracle) ResetTimestamp() {
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	log.Info("reset the timestamp in memory", logutil.CondUint32("keyspace-group-id", t.keyspaceGroupID, t.keyspaceGroupID > 0))
	t.tsoMux.physical = typeutil.ZeroTime
	t.tsoMux.logical = 0
	t.lastSavedTime.Store(typeutil.ZeroTime)
}
