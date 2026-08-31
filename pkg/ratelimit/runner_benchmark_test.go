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

package ratelimit

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func BenchmarkConcurrentRunnerDuplicateTask(b *testing.B) {
	runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Minute)
	noop := func(context.Context) {}
	if err := runner.RunTask(0, "duplicate", noop, WithRetained(true)); err != nil {
		b.Fatal(err)
	}
	if err := runner.RunTask(1, "duplicate", noop, WithRetained(true)); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := runner.RunTask(1, "duplicate", noop, WithRetained(true)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConcurrentRunnerUniqueTask(b *testing.B) {
	runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Minute)
	noop := func(context.Context) {}
	if err := runner.RunTask(0, "unique", noop); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := runner.RunTask(uint64(i+1), "unique", noop); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConcurrentRunnerBurstRetention(b *testing.B) {
	const taskCount = 20000
	// Initialize the metric labels before measuring retained queue storage.
	_ = NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Minute)
	noop := func(context.Context) {}
	var totalRetainedBytes uint64
	for range b.N {
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		runner := NewConcurrentRunner("test", NewConcurrencyLimiter(1), time.Minute)
		if err := runner.RunTask(0, "burst", noop); err != nil {
			b.Fatal(err)
		}
		for i := 1; i <= taskCount; i++ {
			if err := runner.RunTask(uint64(i), "burst", noop); err != nil {
				b.Fatal(err)
			}
		}
		for len(runner.existTasks) > 0 {
			<-runner.taskChan
			runner.processPendingTasks()
		}
		<-runner.taskChan

		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		if after.HeapAlloc > before.HeapAlloc {
			totalRetainedBytes += after.HeapAlloc - before.HeapAlloc
		}
		runtime.KeepAlive(runner)
	}
	b.ReportMetric(float64(totalRetainedBytes)/float64(b.N), "retained-B/op")
}
