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
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/election"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	timestampKey = "timestamp"
	// updateTimestampGuard is the min timestamp interval.
	updateTimestampGuard = time.Millisecond
	// maxLogical is the max upper limit for logical time.
	// When a TSO's logical time reaches this limit,
	// the physical time will be forced to increase.
	maxLogical = int64(1 << 18)
)

// tsoObject is used to store the current TSO in memory.
type tsoObject struct {
	physical time.Time
	logical  int64
}

// timestampOracle is used to maintain the logic of TSO.
type timestampOracle struct {
	client   *clientv3.Client
	rootPath string
	// TODO: remove saveInterval
	saveInterval           time.Duration
	updatePhysicalInterval time.Duration
	maxResetTSGap          func() time.Duration
	// tso info stored in the memory
	tsoMux struct {
		sync.RWMutex
		tso *tsoObject
	}
	// last timestamp window stored in etcd
	lastSavedTime atomic.Value // stored as time.Time
}

func (t *timestampOracle) setTSOPhysical(next time.Time) {
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	// make sure the ts won't fall back
	if t.tsoMux.tso == nil || typeutil.SubTimeByWallClock(next, t.tsoMux.tso.physical) > 0 {
		t.tsoMux.tso = &tsoObject{physical: next}
	}
}

func (t *timestampOracle) getTSO() (time.Time, int64) {
	t.tsoMux.RLock()
	defer t.tsoMux.RUnlock()
	if t.tsoMux.tso == nil {
		return typeutil.ZeroTime, 0
	}
	return t.tsoMux.tso.physical, t.tsoMux.tso.logical
}

// generateTSO will add the TSO's logical part with the given count and returns the new TSO result.
func (t *timestampOracle) generateTSO(count int64) (physical int64, logical int64) {
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	if t.tsoMux.tso == nil {
		return 0, 0
	}
	physical = t.tsoMux.tso.physical.UnixNano() / int64(time.Millisecond)
	t.tsoMux.tso.logical += count
	logical = t.tsoMux.tso.logical
	return physical, logical
}

func (t *timestampOracle) getTimestampPath() string {
	return path.Join(t.rootPath, timestampKey)
}

func (t *timestampOracle) loadTimestamp() (time.Time, error) {
	data, err := etcdutil.GetValue(t.client, t.getTimestampPath())
	if err != nil {
		return typeutil.ZeroTime, err
	}
	if len(data) == 0 {
		return typeutil.ZeroTime, nil
	}
	return typeutil.ParseTimestamp(data)
}

// save timestamp, if lastTs is 0, we think the timestamp doesn't exist, so create it,
// otherwise, update it.
func (t *timestampOracle) saveTimestamp(leadership *election.Leadership, ts time.Time) error {
	key := t.getTimestampPath()
	data := typeutil.Uint64ToBytes(uint64(ts.UnixNano()))
	resp, err := leadership.LeaderTxn().
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return errs.ErrEtcdKVPut.Wrap(err).GenWithStackByCause()
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxn.FastGenByArgs()
	}
	t.lastSavedTime.Store(ts)
	return nil
}

// SyncTimestamp is used to synchronize the timestamp.
func (t *timestampOracle) SyncTimestamp(leadership *election.Leadership) error {
	tsoCounter.WithLabelValues("sync").Inc()

	failpoint.Inject("delaySyncTimestamp", func() {
		time.Sleep(time.Second)
	})

	last, err := t.loadTimestamp()
	if err != nil {
		return err
	}

	next := time.Now()
	failpoint.Inject("fallBackSync", func() {
		next = next.Add(time.Hour)
	})

	// If the current system time minus the saved etcd timestamp is less than `updateTimestampGuard`,
	// the timestamp allocation will start from the saved etcd timestamp temporarily.
	if typeutil.SubTimeByWallClock(next, last) < updateTimestampGuard {
		log.Error("system time may be incorrect", zap.Time("last", last), zap.Time("next", next), errs.ZapError(errs.ErrIncorrectSystemTime))
		next = last.Add(updateTimestampGuard)
	}

	save := next.Add(t.saveInterval)
	if err = t.saveTimestamp(leadership, save); err != nil {
		tsoCounter.WithLabelValues("err_save_sync_ts").Inc()
		return err
	}

	tsoCounter.WithLabelValues("sync_ok").Inc()
	log.Info("sync and save timestamp", zap.Time("last", last), zap.Time("save", save), zap.Time("next", next))
	// save into memory
	t.setTSOPhysical(next)
	return nil
}

// isInitialized is used to check whether the timestampOracle is initialized.
// There are two situations we have an uninitialized timestampOracle:
// 1. When the SyncTimestamp has not been called yet.
// 2. When the ResetUserTimestamp has been called already.
func (t *timestampOracle) isInitialized() bool {
	t.tsoMux.RLock()
	defer t.tsoMux.RUnlock()
	return t.tsoMux.tso != nil
}

// resetUserTimestamp update the TSO in memory with specified TSO by an atomicly way.
func (t *timestampOracle) resetUserTimestamp(leadership *election.Leadership, tso uint64, ignoreSmaller bool) error {
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	if !leadership.Check() {
		tsoCounter.WithLabelValues("err_lease_reset_ts").Inc()
		return errs.ErrResetUserTimestamp.FastGenByArgs("lease expired")
	}
	nextPhysical, nextLogical := tsoutil.ParseTS(tso)
	nextPhysical = nextPhysical.Add(updateTimestampGuard)
	var err error
	// do not update if next logical time is less/before than prev
	if typeutil.SubTimeByWallClock(nextPhysical, t.tsoMux.tso.physical) == 0 && int64(nextLogical) <= t.tsoMux.tso.logical {
		tsoCounter.WithLabelValues("err_reset_small_counter").Inc()
		if !ignoreSmaller {
			err = errs.ErrResetUserTimestamp.FastGenByArgs("the specified counter is smaller than now")
		}
	}
	// do not update if next physical time is less/before than prev
	if typeutil.SubTimeByWallClock(nextPhysical, t.tsoMux.tso.physical) < 0 {
		tsoCounter.WithLabelValues("err_reset_small_ts").Inc()
		if !ignoreSmaller {
			err = errs.ErrResetUserTimestamp.FastGenByArgs("the specified ts is smaller than now")
		}
	}
	// do not update if physical time is too greater than prev
	if typeutil.SubTimeByWallClock(nextPhysical, t.tsoMux.tso.physical) >= t.maxResetTSGap() {
		tsoCounter.WithLabelValues("err_reset_large_ts").Inc()
		err = errs.ErrResetUserTimestamp.FastGenByArgs("the specified ts is too larger than now")
	}
	if err != nil {
		return err
	}
	// save into etcd only if the time difference is big enough
	if typeutil.SubTimeByWallClock(nextPhysical, t.tsoMux.tso.physical) > 3*updateTimestampGuard {
		save := nextPhysical.Add(t.saveInterval)
		if err = t.saveTimestamp(leadership, save); err != nil {
			tsoCounter.WithLabelValues("err_save_reset_ts").Inc()
			return err
		}
	}
	// save into memory and make sure the ts won't fall back
	if t.tsoMux.tso == nil {
		t.tsoMux.tso = &tsoObject{physical: nextPhysical, logical: int64(nextLogical)}
	}
	if typeutil.SubTimeByWallClock(nextPhysical, t.tsoMux.tso.physical) > 0 {
		t.tsoMux.tso = &tsoObject{physical: nextPhysical}
	}
	if typeutil.SubTimeByWallClock(nextPhysical, t.tsoMux.tso.physical) == 0 && int64(nextLogical) > t.tsoMux.tso.logical {
		t.tsoMux.tso = &tsoObject{physical: nextPhysical, logical: int64(nextLogical)}
	}
	tsoCounter.WithLabelValues("reset_tso_ok").Inc()
	return nil
}

// UpdateTimestamp is used to update the timestamp.
// This function will do two things:
// 1. When the logical time is going to be used up, increase the current physical time.
// 2. When the time window is not big enough, which means the saved etcd time minus the next physical time
//    will be less than or equal to `updateTimestampGuard`, then the time window needs to be updated and
//    we also need to save the next physical time plus `TSOSaveInterval` into etcd.
//
// Here is some constraints that this function must satisfy:
// 1. The saved time is monotonically increasing.
// 2. The physical time is monotonically increasing.
// 3. The physical time is always less than the saved timestamp.
func (t *timestampOracle) UpdateTimestamp(leadership *election.Leadership) error {
	prevPhysical, prevLogical := t.getTSO()
	now := time.Now()

	failpoint.Inject("fallBackUpdate", func() {
		now = now.Add(time.Hour)
	})

	tsoCounter.WithLabelValues("save").Inc()

	jetLag := typeutil.SubTimeByWallClock(now, prevPhysical)
	if jetLag > 3*t.updatePhysicalInterval {
		log.Warn("clock offset", zap.Duration("jet-lag", jetLag), zap.Time("prev-physical", prevPhysical), zap.Time("now", now))
		tsoCounter.WithLabelValues("slow_save").Inc()
	}

	if jetLag < 0 {
		tsoCounter.WithLabelValues("system_time_slow").Inc()
	}

	var next time.Time
	// If the system time is greater, it will be synchronized with the system time.
	if jetLag > updateTimestampGuard {
		next = now
	} else if prevLogical > maxLogical/2 {
		// The reason choosing maxLogical/2 here is that it's big enough for common cases.
		// Because there is enough timestamp can be allocated before next update.
		log.Warn("the logical time may be not enough", zap.Int64("prev-logical", prevLogical))
		next = prevPhysical.Add(time.Millisecond)
	} else {
		// It will still use the previous physical time to alloc the timestamp.
		tsoCounter.WithLabelValues("skip_save").Inc()
		return nil
	}

	// It is not safe to increase the physical time to `next`.
	// The time window needs to be updated and saved to etcd.
	if typeutil.SubTimeByWallClock(t.lastSavedTime.Load().(time.Time), next) <= updateTimestampGuard {
		save := next.Add(t.saveInterval)
		if err := t.saveTimestamp(leadership, save); err != nil {
			tsoCounter.WithLabelValues("err_save_update_ts").Inc()
			return err
		}
	}
	// save into memory
	t.setTSOPhysical(next)
	tsoGauge.WithLabelValues("tso").Set(float64(next.Unix()))

	return nil
}

// getTS is used to get a timestamp.
func (t *timestampOracle) getTS(leadership *election.Leadership, count uint32) (pdpb.Timestamp, error) {
	var resp pdpb.Timestamp

	if count == 0 {
		return resp, errs.ErrGenerateTimestamp.FastGenByArgs("tso count should be positive")
	}

	maxRetryCount := 10
	failpoint.Inject("skipRetryGetTS", func() {
		maxRetryCount = 1
	})

	for i := 0; i < maxRetryCount; i++ {
		currentPhysical, currentLogical := t.getTSO()
		if currentPhysical == typeutil.ZeroTime {
			// If it's leader, maybe SyncTimestamp hasn't completed yet
			if leadership.Check() {
				log.Info("sync hasn't completed yet, wait for a while")
				time.Sleep(200 * time.Millisecond)
				continue
			}
			log.Error("invalid timestamp",
				zap.Any("timestamp-physical", currentPhysical),
				zap.Any("timestamp-logical", currentLogical),
				errs.ZapError(errs.ErrInvalidTimestamp))
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
		}
		// Get a new TSO result with the given count
		resp.Physical, resp.Logical = t.generateTSO(int64(count))
		if resp.GetPhysical() == 0 {
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory has been reset")
		}
		if resp.GetLogical() >= maxLogical {
			log.Error("logical part outside of max logical interval, please check ntp time",
				zap.Reflect("response", resp),
				zap.Int("retry-count", i), errs.ZapError(errs.ErrLogicOverflow))
			tsoCounter.WithLabelValues("logical_overflow").Inc()
			time.Sleep(t.updatePhysicalInterval)
			continue
		}
		// In case lease expired after the first check.
		if !leadership.Check() {
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("not the pd or local tso allocator leader")
		}
		return resp, nil
	}
	return resp, errs.ErrGenerateTimestamp.FastGenByArgs("maximum number of retries exceeded")
}

// ResetTimestamp is used to reset the timestamp in memory.
func (t *timestampOracle) ResetTimestamp() {
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	log.Info("reset the timestamp in memory")
	t.tsoMux.tso = nil
}
