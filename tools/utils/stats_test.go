// Copyright 2025 TiKV Project Authors.
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

package utils

import (
	"context"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestDiffHistogramSnapshot(t *testing.T) {
	prev := histogramSnapshot{
		count: 10,
		sum:   100,
		buckets: map[float64]uint64{
			1:           4,
			2:           10,
			math.Inf(1): 10,
		},
	}
	cur := histogramSnapshot{
		count: 15,
		sum:   170,
		buckets: map[float64]uint64{
			1:           6,
			2:           13,
			math.Inf(1): 15,
		},
	}

	delta := diffHistogramSnapshot(cur, prev)
	if delta.count != 5 {
		t.Fatalf("unexpected delta count: got %d, want %d", delta.count, 5)
	}
	if math.Abs(delta.sum-70) > 1e-9 {
		t.Fatalf("unexpected delta sum: got %v, want %v", delta.sum, 70)
	}
	if got := delta.buckets[1]; got != 2 {
		t.Fatalf("unexpected delta bucket(1): got %d, want %d", got, 2)
	}
	if got := delta.buckets[2]; got != 3 {
		t.Fatalf("unexpected delta bucket(2): got %d, want %d", got, 3)
	}
	if got := delta.buckets[math.Inf(1)]; got != 5 {
		t.Fatalf("unexpected delta bucket(+Inf): got %d, want %d", got, 5)
	}
}

func TestDiffHistogramSnapshot_Reset(t *testing.T) {
	prev := histogramSnapshot{count: 10, sum: 100, buckets: map[float64]uint64{1: 10}}
	cur := histogramSnapshot{count: 3, sum: 9, buckets: map[float64]uint64{1: 3}}

	delta := diffHistogramSnapshot(cur, prev)
	if delta.count != cur.count || delta.sum != cur.sum || len(delta.buckets) != len(cur.buckets) {
		t.Fatalf("unexpected reset handling: got %+v, want %+v", delta, cur)
	}
}

func TestHistogramQuantile(t *testing.T) {
	buckets := map[float64]uint64{
		1:           50,
		2:           80,
		4:           100,
		math.Inf(1): 100,
	}

	if got := histogramQuantile(0.5, 100, buckets); math.Abs(got-1) > 1e-9 {
		t.Fatalf("unexpected p50: got %v, want %v", got, 1)
	}
	if got := histogramQuantile(0.99, 100, buckets); math.Abs(got-3.9) > 1e-9 {
		t.Fatalf("unexpected p99: got %v, want %v", got, 3.9)
	}
	if got := histogramQuantile(0.9, 30, map[float64]uint64{1: 10, 2: 20, math.Inf(1): 30}); got != 2 {
		t.Fatalf("unexpected quantile in +Inf bucket: got %v, want %v", got, 2)
	}

	if got := histogramQuantile(1.1, 100, buckets); !math.IsNaN(got) {
		t.Fatalf("expected NaN for invalid quantile, got %v", got)
	}
	if got := histogramQuantile(0.5, 0, buckets); !math.IsNaN(got) {
		t.Fatalf("expected NaN for empty histogram, got %v", got)
	}
}

func TestGatherHistogramSnapshot_Histogram(t *testing.T) {
	registry := prometheus.NewRegistry()
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    tsoBatchSizeMetricName,
		Buckets: []float64{1, 2, 4},
	})
	registry.MustRegister(h)

	withDefaultGatherer(t, registry)

	h.Observe(1)
	h.Observe(2)
	h.Observe(3)

	snap, ok, err := gatherHistogramSnapshot(tsoBatchSizeMetricName)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected metric to be found")
	}
	if snap.count != 3 {
		t.Fatalf("unexpected count: got %d, want %d", snap.count, 3)
	}
	if math.Abs(snap.sum-6) > 1e-9 {
		t.Fatalf("unexpected sum: got %v, want %v", snap.sum, 6)
	}
	if snap.buckets[1] != 1 || snap.buckets[2] != 2 || snap.buckets[4] != 3 {
		t.Fatalf("unexpected buckets: %+v", snap.buckets)
	}
	// Some gatherers may omit the +Inf bucket in the protobuf representation.
	if infCount, hasInf := snap.buckets[math.Inf(1)]; hasInf && infCount != 3 {
		t.Fatalf("unexpected +Inf bucket: got %d, want %d", infCount, 3)
	}
}

func TestGatherHistogramSnapshot_NotFound(t *testing.T) {
	withDefaultGatherer(t, prometheus.NewRegistry())

	_, ok, err := gatherHistogramSnapshot("non_existent_metric")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected metric to be missing")
	}
}

func TestGatherHistogramSnapshot_NotHistogram(t *testing.T) {
	registry := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "not_hist"})
	registry.MustRegister(g)

	withDefaultGatherer(t, registry)

	_, ok, err := gatherHistogramSnapshot("not_hist")
	if err == nil {
		t.Fatalf("expected error")
	}
	if ok {
		t.Fatalf("expected ok=false on error")
	}
}

func TestPrintBatchSizeStats(t *testing.T) {
	out := captureStdout(t, func() {
		printBatchSizeStats("empty", histogramSnapshot{count: 0})
		printBatchSizeStats("full", histogramSnapshot{
			count: 100,
			sum:   200,
			buckets: map[float64]uint64{
				1:           50,
				2:           80,
				4:           100,
				math.Inf(1): 100,
			},
		})
	})

	if !strings.Contains(out, "batch-size(empty): count=0") {
		t.Fatalf("unexpected output: %q", out)
	}
	if !strings.Contains(out, "batch-size(full): count=100 avg=2.000 p99=3.900") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestShowStats_VerboseBatchSize(t *testing.T) {
	registry := prometheus.NewRegistry()
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    tsoBatchSizeMetricName,
		Buckets: []float64{1, 2, 4},
	})
	registry.MustRegister(h)

	withDefaultGatherer(t, registry)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	durCh := make(chan time.Duration, 16)
	var wg sync.WaitGroup

	out := captureStdout(t, func() {
		wg.Add(1)
		go ShowStats(ctx, &wg, durCh, 10*time.Millisecond, true, nil)

		durCh <- time.Millisecond
		h.Observe(1)

		time.Sleep(25 * time.Millisecond)
		cancel()
		wg.Wait()
	})

	if !strings.Contains(out, "batch-size(interval)") {
		t.Fatalf("expected interval batch-size output, got %q", out)
	}
	if !strings.Contains(out, "batch-size(total)") {
		t.Fatalf("expected total batch-size output, got %q", out)
	}
}

func withDefaultGatherer(t *testing.T, g prometheus.Gatherer) {
	t.Helper()
	oldGatherer := prometheus.DefaultGatherer
	prometheus.DefaultGatherer = g
	t.Cleanup(func() {
		prometheus.DefaultGatherer = oldGatherer
	})
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	fn()

	_ = w.Close()
	os.Stdout = oldStdout

	data, readErr := io.ReadAll(r)
	_ = r.Close()
	if readErr != nil {
		t.Fatalf("read stdout: %v", readErr)
	}
	return string(data)
}
