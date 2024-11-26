// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Note: This file is copied from https://go-review.googlesource.com/c/go/+/276133

package timerpool

import (
	"testing"
	"time"
)

func TestTimerPool(t *testing.T) {
	var tp TimerPool

	for range 100 {
		timer := tp.Get(20 * time.Millisecond)

		select {
		case <-timer.C:
			t.Errorf("timer expired too early")
			continue
		default:
		}

		select {
		case <-time.After(100 * time.Millisecond):
			t.Errorf("timer didn't expire on time")
		case <-timer.C:
		}

		tp.Put(timer)
	}
}

const timeout = 10 * time.Millisecond

func BenchmarkTimerUtilization(b *testing.B) {
	b.Run("TimerWithPool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t := GlobalTimerPool.Get(timeout)
			GlobalTimerPool.Put(t)
		}
	})
	b.Run("TimerWithoutPool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t := time.NewTimer(timeout)
			t.Stop()
		}
	})
}

func BenchmarkTimerPoolParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := GlobalTimerPool.Get(timeout)
			GlobalTimerPool.Put(t)
		}
	})
}

func BenchmarkTimerNativeParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := time.NewTimer(timeout)
			t.Stop()
		}
	})
}
