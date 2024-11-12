// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Note: This file is copied from https://go-review.googlesource.com/c/go/+/276133

package timerutil

import (
	"sync"
	"time"
)

// GlobalTimerPool is a global pool for reusing *time.Timer.
var GlobalTimerPool timerPool

// timerPool is a wrapper of sync.Pool which caches *time.Timer for reuse.
type timerPool struct {
	pool sync.Pool
}

// Get returns a timer with a given duration.
func (tp *timerPool) Get(d time.Duration) *time.Timer {
	if v := tp.pool.Get(); v != nil {
		timer := v.(*time.Timer)
		timer.Reset(d)
		return timer
	}
	return time.NewTimer(d)
}

// Put tries to call timer.Stop() before putting it back into pool,
// if the timer.Stop() returns false (it has either already expired or been stopped),
// have a shot at draining the channel with residual time if there is one.
func (tp *timerPool) Put(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	tp.pool.Put(timer)
}
