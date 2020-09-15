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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/failpoint"
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
	// UpdateTimestampStep is used to update timestamp.
	UpdateTimestampStep = 50 * time.Millisecond
	// updateTimestampGuard is the min timestamp interval.
	updateTimestampGuard = time.Millisecond
	// maxLogical is the max upper limit for logical time.
	// When a TSO's logical time reaches this limit,
	// the physical time will be forced to increase.
	maxLogical = int64(1 << 18)
)

// atomicObject is used to store the current TSO in memory.
type atomicObject struct {
	physical time.Time
	logical  int64
}

// timestampOracle is used to maintain the logic of TSO.
type timestampOracle struct {
	client   *clientv3.Client
	rootPath string
	// TODO: remove saveInterval
	saveInterval  time.Duration
	maxResetTSGap func() time.Duration
	// For TSO, set after the PD becomes a leader.
	TSO           unsafe.Pointer
	lastSavedTime atomic.Value
}

func (t *timestampOracle) getTimestampPath() string {
	return path.Join(t.rootPath, "timestamp")
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

	current := &atomicObject{
		physical: next,
	}
	atomic.StorePointer(&t.TSO, unsafe.Pointer(current))

	return nil
}

// isInitialized is used to check whether the timestampOracle is initialized.
// There are two situations we have an uninitialized timestampOracle:
// 1. When the SyncTimestamp has not been called yet.
// 2. When the ResetUserTimestamp has been called already.
func (t *timestampOracle) isInitialized() bool {
	tsoNow := (*atomicObject)(atomic.LoadPointer(&t.TSO))
	if tsoNow == nil || tsoNow.physical == typeutil.ZeroTime {
		return false
	}
	return true
}

// ResetUserTimestamp update the physical part with specified TSO.
func (t *timestampOracle) ResetUserTimestamp(leadership *election.Leadership, tso uint64) error {
	if !leadership.Check() {
		tsoCounter.WithLabelValues("err_lease_reset_ts").Inc()
		return errs.ErrResetUserTimestamp.FastGenByArgs("lease expired")
	}
	physical, _ := tsoutil.ParseTS(tso)
	next := physical.Add(time.Millisecond)
	prev := (*atomicObject)(atomic.LoadPointer(&t.TSO))

	// do not update
	if typeutil.SubTimeByWallClock(next, prev.physical) <= 3*updateTimestampGuard {
		tsoCounter.WithLabelValues("err_reset_small_ts").Inc()
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified ts too small than now")
	}

	if typeutil.SubTimeByWallClock(next, prev.physical) >= t.maxResetTSGap() {
		tsoCounter.WithLabelValues("err_reset_large_ts").Inc()
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified ts too large than now")
	}

	save := next.Add(t.saveInterval)
	if err := t.saveTimestamp(leadership, save); err != nil {
		tsoCounter.WithLabelValues("err_save_reset_ts").Inc()
		return err
	}
	update := &atomicObject{
		physical: next,
	}
	atomic.CompareAndSwapPointer(&t.TSO, unsafe.Pointer(prev), unsafe.Pointer(update))
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
	prev := (*atomicObject)(atomic.LoadPointer(&t.TSO))
	now := time.Now()

	failpoint.Inject("fallBackUpdate", func() {
		now = now.Add(time.Hour)
	})

	tsoCounter.WithLabelValues("save").Inc()

	jetLag := typeutil.SubTimeByWallClock(now, prev.physical)
	if jetLag > 3*UpdateTimestampStep {
		log.Warn("clock offset", zap.Duration("jet-lag", jetLag), zap.Time("prev-physical", prev.physical), zap.Time("now", now))
		tsoCounter.WithLabelValues("slow_save").Inc()
	}

	if jetLag < 0 {
		tsoCounter.WithLabelValues("system_time_slow").Inc()
	}

	var next time.Time
	prevLogical := atomic.LoadInt64(&prev.logical)
	// If the system time is greater, it will be synchronized with the system time.
	if jetLag > updateTimestampGuard {
		next = now
	} else if prevLogical > maxLogical/2 {
		// The reason choosing maxLogical/2 here is that it's big enough for common cases.
		// Because there is enough timestamp can be allocated before next update.
		log.Warn("the logical time may be not enough", zap.Int64("prev-logical", prevLogical))
		next = prev.physical.Add(time.Millisecond)
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

	current := &atomicObject{
		physical: next,
		logical:  0,
	}

	atomic.StorePointer(&t.TSO, unsafe.Pointer(current))
	tsoGauge.WithLabelValues("tso").Set(float64(next.Unix()))

	return nil
}

// ResetTimestamp is used to reset the timestamp in memory.
func (t *timestampOracle) ResetTimestamp() {
	log.Info("reset the timestamp in memory")
	zero := &atomicObject{
		physical: typeutil.ZeroTime,
	}
	atomic.StorePointer(&t.TSO, unsafe.Pointer(zero))
}
