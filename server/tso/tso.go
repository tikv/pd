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
	// updateTimestampGuard is the min physical timestamp updating interval for the TSO.
	updateTimestampGuard = time.Millisecond
	// maxLogical is the max upper limit for logical time.
	// When a TSO's logical time reaches this limit,
	// the physical time will be forced to increase.
	maxLogical = int64(1 << 18)
	// MaxSuffixBits indicates the max number of suffix bits.
	MaxSuffixBits = 4
)

// tsoObject is used to store the current TSO in memory.
type tsoObject struct {
<<<<<<< HEAD
=======
	sync.RWMutex
	// time.Time has a minimum unit of nanoseconds,
	// however the minimum unit of the physical part of TSO is one millisecond, a.k.a 1ms,
	// so you MUST use millisecond as the minimum unit when comparing two TSOs' physical times,
	// and `typeutil.SubTSOPhysicalByWallClock` is used to do this SPECIFICALLY.
>>>>>>> f82cfbfcd (tso: fix the TSO fallback caused by resetUserTimestamp (#3628))
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
	suffix        int
	dcLocation    string
}

func (t *timestampOracle) setTSOPhysical(next time.Time) {
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	// make sure the ts won't fall back
<<<<<<< HEAD
	if t.tsoMux.tso == nil || typeutil.SubTimeByWallClock(next, t.tsoMux.tso.physical) > 0 {
		t.tsoMux.tso = &tsoObject{physical: next}
=======
	if typeutil.SubTSOPhysicalByWallClock(next, t.tsoMux.physical) > 0 {
		t.tsoMux.physical = next
		t.tsoMux.logical = 0
>>>>>>> f82cfbfcd (tso: fix the TSO fallback caused by resetUserTimestamp (#3628))
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
func (t *timestampOracle) generateTSO(count int64, suffixBits int) (physical int64, logical int64) {
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	if t.tsoMux.tso == nil {
		return 0, 0
	}
	physical = t.tsoMux.tso.physical.UnixNano() / int64(time.Millisecond)
	t.tsoMux.tso.logical += count
	logical = t.tsoMux.tso.logical
	if suffixBits > 0 && t.suffix >= 0 {
		logical = t.differentiateLogical(logical, suffixBits)
	}
	return physical, logical
}

// Because the Local TSO in each Local TSO Allocator is independent, so they are possible
// to be the same at sometimes, to avoid this case, we need to use the logical part of the
// Local TSO to do some differentiating work.
// For example, we have three DCs: dc-1, dc-2 and dc-3. The bits of suffix is defined by
// the const suffixBits. Then, for dc-2, the suffix may be 1 because it's persisted
// in etcd with the value of 1.
// Once we get a noramal TSO like this (18 bits): xxxxxxxxxxxxxxxxxx. We will make the TSO's
// low bits of logical part from each DC looks like:
//   global: xxxxxxxxxx00000000
//     dc-1: xxxxxxxxxx00000001
//     dc-2: xxxxxxxxxx00000010
//     dc-3: xxxxxxxxxx00000011
func (t *timestampOracle) differentiateLogical(rawLogical int64, suffixBits int) int64 {
	return rawLogical<<suffixBits + int64(t.suffix)
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
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	t.lastSavedTime.Store(ts)
	return nil
}

// SyncTimestamp is used to synchronize the timestamp.
func (t *timestampOracle) SyncTimestamp(leadership *election.Leadership) error {
	tsoCounter.WithLabelValues("sync", t.dcLocation).Inc()

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
	failpoint.Inject("systemTimeSlow", func() {
		next = next.Add(-time.Hour)
	})
	// If the current system time minus the saved etcd timestamp is less than `updateTimestampGuard`,
	// the timestamp allocation will start from the saved etcd timestamp temporarily.
	if typeutil.SubRealTimeByWallClock(next, last) < updateTimestampGuard {
		log.Error("system time may be incorrect", zap.Time("last", last), zap.Time("next", next), errs.ZapError(errs.ErrIncorrectSystemTime))
		next = last.Add(updateTimestampGuard)
	}

	save := next.Add(t.saveInterval)
	if err = t.saveTimestamp(leadership, save); err != nil {
		tsoCounter.WithLabelValues("err_save_sync_ts", t.dcLocation).Inc()
		return err
	}

	tsoCounter.WithLabelValues("sync_ok", t.dcLocation).Inc()
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

<<<<<<< HEAD
// resetUserTimestamp update the TSO in memory with specified TSO by an atomicly way.
=======
// resetUserTimestamp update the TSO in memory with specified TSO by an atomically way.
// When ignoreSmaller is true, resetUserTimestamp will ignore the smaller tso resetting error and do nothing.
// It's used to write MaxTS during the Global TSO synchronization whitout failing the writing as much as possible.
>>>>>>> f82cfbfcd (tso: fix the TSO fallback caused by resetUserTimestamp (#3628))
func (t *timestampOracle) resetUserTimestamp(leadership *election.Leadership, tso uint64, ignoreSmaller bool) error {
	t.tsoMux.Lock()
	defer t.tsoMux.Unlock()
	if !leadership.Check() {
		tsoCounter.WithLabelValues("err_lease_reset_ts", t.dcLocation).Inc()
		return errs.ErrResetUserTimestamp.FastGenByArgs("lease expired")
	}
<<<<<<< HEAD
	nextPhysical, nextLogical := tsoutil.ParseTS(tso)
	nextPhysical = nextPhysical.Add(updateTimestampGuard)
	var err error
	// do not update if next logical time is less/before than prev
	if typeutil.SubTimeByWallClock(nextPhysical, t.tsoMux.tso.physical) == 0 && int64(nextLogical) <= t.tsoMux.tso.logical {
		tsoCounter.WithLabelValues("err_reset_small_counter", t.dcLocation).Inc()
		if !ignoreSmaller {
			err = errs.ErrResetUserTimestamp.FastGenByArgs("the specified counter is smaller than now")
		}
	}
	// do not update if next physical time is less/before than prev
	if typeutil.SubTimeByWallClock(nextPhysical, t.tsoMux.tso.physical) < 0 {
=======
	var (
		nextPhysical, nextLogical = tsoutil.ParseTS(tso)
		logicalDifference         = int64(nextLogical) - t.tsoMux.logical
		physicalDifference        = typeutil.SubTSOPhysicalByWallClock(nextPhysical, t.tsoMux.physical)
	)
	// do not update if next physical time is less/before than prev
	if physicalDifference < 0 {
>>>>>>> f82cfbfcd (tso: fix the TSO fallback caused by resetUserTimestamp (#3628))
		tsoCounter.WithLabelValues("err_reset_small_ts", t.dcLocation).Inc()
		if ignoreSmaller {
			return nil
		}
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified ts is smaller than now")
	}
	// do not update if next logical time is less/before/equal than prev
	if physicalDifference == 0 && logicalDifference <= 0 {
		tsoCounter.WithLabelValues("err_reset_small_counter", t.dcLocation).Inc()
		if ignoreSmaller {
			return nil
		}
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified counter is smaller than now")
	}
	// do not update if physical time is too greater than prev
<<<<<<< HEAD
	if typeutil.SubTimeByWallClock(nextPhysical, t.tsoMux.tso.physical) >= t.maxResetTSGap() {
=======
	if physicalDifference >= t.maxResetTSGap().Milliseconds() {
>>>>>>> f82cfbfcd (tso: fix the TSO fallback caused by resetUserTimestamp (#3628))
		tsoCounter.WithLabelValues("err_reset_large_ts", t.dcLocation).Inc()
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified ts is too larger than now")
	}
	// save into etcd only if nextPhysical is close to lastSavedTime
	if typeutil.SubRealTimeByWallClock(t.lastSavedTime.Load().(time.Time), nextPhysical) <= updateTimestampGuard {
		save := nextPhysical.Add(t.saveInterval)
		if err := t.saveTimestamp(leadership, save); err != nil {
			tsoCounter.WithLabelValues("err_save_reset_ts", t.dcLocation).Inc()
			return err
		}
	}
<<<<<<< HEAD
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
=======
	// save into memory only if nextPhysical or nextLogical is greater.
	t.tsoMux.physical = nextPhysical
	t.tsoMux.logical = int64(nextLogical)
>>>>>>> f82cfbfcd (tso: fix the TSO fallback caused by resetUserTimestamp (#3628))
	tsoCounter.WithLabelValues("reset_tso_ok", t.dcLocation).Inc()
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
	tsoGauge.WithLabelValues("tso", t.dcLocation).Set(float64(prevPhysical.Unix()))

	now := time.Now()
	failpoint.Inject("fallBackUpdate", func() {
		now = now.Add(time.Hour)
	})
	failpoint.Inject("systemTimeSlow", func() {
		now = now.Add(-time.Hour)
	})

	tsoCounter.WithLabelValues("save", t.dcLocation).Inc()

	jetLag := typeutil.SubRealTimeByWallClock(now, prevPhysical)
	if jetLag > 3*t.updatePhysicalInterval {
		log.Warn("clock offset", zap.Duration("jet-lag", jetLag), zap.Time("prev-physical", prevPhysical), zap.Time("now", now), zap.Duration("update-physical-interval", t.updatePhysicalInterval))
		tsoCounter.WithLabelValues("slow_save", t.dcLocation).Inc()
	}

	if jetLag < 0 {
		tsoCounter.WithLabelValues("system_time_slow", t.dcLocation).Inc()
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
		tsoCounter.WithLabelValues("skip_save", t.dcLocation).Inc()
		return nil
	}

	// It is not safe to increase the physical time to `next`.
	// The time window needs to be updated and saved to etcd.
	if typeutil.SubRealTimeByWallClock(t.lastSavedTime.Load().(time.Time), next) <= updateTimestampGuard {
		save := next.Add(t.saveInterval)
		if err := t.saveTimestamp(leadership, save); err != nil {
			tsoCounter.WithLabelValues("err_save_update_ts", t.dcLocation).Inc()
			return err
		}
	}
	// save into memory
	t.setTSOPhysical(next)

	return nil
}

// getTS is used to get a timestamp.
func (t *timestampOracle) getTS(leadership *election.Leadership, count uint32, suffixBits int) (pdpb.Timestamp, error) {
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
		resp.Physical, resp.Logical = t.generateTSO(int64(count), suffixBits)
		if resp.GetPhysical() == 0 {
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory has been reset")
		}
		if resp.GetLogical() >= maxLogical {
			log.Error("logical part outside of max logical interval, please check ntp time",
				zap.Reflect("response", resp),
				zap.Int("retry-count", i), errs.ZapError(errs.ErrLogicOverflow))
			tsoCounter.WithLabelValues("logical_overflow", t.dcLocation).Inc()
			time.Sleep(t.updatePhysicalInterval)
			continue
		}
		// In case lease expired after the first check.
		if !leadership.Check() {
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("not the pd or local tso allocator leader")
		}
		resp.SuffixBits = uint32(suffixBits)
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
