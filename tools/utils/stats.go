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
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var latencyTDigest *tdigest.TDigest = tdigest.New()

const tsoBatchSizeMetricName = "pd_client_request_handle_tso_batch_size"

type histogramSnapshot struct {
	count   uint64
	sum     float64
	buckets map[float64]uint64 // upperBound => cumulativeCount
}

func gatherHistogramSnapshot(metricName string) (histogramSnapshot, bool, error) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return histogramSnapshot{}, false, err
	}

	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		if mf.GetType() != dto.MetricType_HISTOGRAM {
			return histogramSnapshot{}, false, fmt.Errorf("metric %q is not a histogram", metricName)
		}

		snap := histogramSnapshot{
			buckets: make(map[float64]uint64),
		}
		for _, m := range mf.GetMetric() {
			h := m.GetHistogram()
			if h == nil {
				continue
			}
			snap.count += h.GetSampleCount()
			snap.sum += h.GetSampleSum()
			for _, b := range h.GetBucket() {
				snap.buckets[b.GetUpperBound()] += b.GetCumulativeCount()
			}
		}
		return snap, true, nil
	}

	return histogramSnapshot{}, false, nil
}

func diffHistogramSnapshot(cur, prev histogramSnapshot) histogramSnapshot {
	if cur.count < prev.count {
		// Metrics got reset/re-registered, treat as starting from zero.
		return cur
	}

	delta := histogramSnapshot{
		count:   cur.count - prev.count,
		sum:     cur.sum - prev.sum,
		buckets: make(map[float64]uint64, len(cur.buckets)),
	}
	for upperBound, curCum := range cur.buckets {
		prevCum := prev.buckets[upperBound]
		if curCum < prevCum {
			// Unexpected, but keep it safe.
			delta.buckets[upperBound] = 0
			continue
		}
		delta.buckets[upperBound] = curCum - prevCum
	}
	return delta
}

// histogramQuantile approximates a quantile from a Prometheus histogram, using the same linear interpolation
// assumption as PromQL's histogram_quantile.
func histogramQuantile(q float64, count uint64, buckets map[float64]uint64) float64 {
	if math.IsNaN(q) || q < 0 || q > 1 {
		return math.NaN()
	}
	if count == 0 || len(buckets) == 0 {
		return math.NaN()
	}

	upperBounds := make([]float64, 0, len(buckets))
	for ub := range buckets {
		upperBounds = append(upperBounds, ub)
	}
	sort.Float64s(upperBounds)

	rank := q * float64(count)

	var (
		prevCum   uint64
		prevBound float64
	)
	for i, ub := range upperBounds {
		cum := buckets[ub]
		if float64(cum) < rank {
			prevCum = cum
			prevBound = ub
			continue
		}

		lowerBound := prevBound
		if i == 0 && ub > 0 {
			// For the first bucket, PromQL assumes a lower bound of 0.
			lowerBound = 0
			prevCum = 0
		}
		if math.IsInf(ub, 1) {
			return lowerBound
		}

		bucketCount := cum - prevCum
		if bucketCount == 0 {
			return lowerBound
		}
		pos := (rank - float64(prevCum)) / float64(bucketCount)
		return lowerBound + (ub-lowerBound)*pos
	}

	// Should be unreachable if +Inf bucket exists; return the largest finite upper bound as a fallback.
	return upperBounds[len(upperBounds)-1]
}

func printBatchSizeStats(prefix string, snap histogramSnapshot) {
	if snap.count == 0 {
		fmt.Printf("batch-size(%s): count=0\n", prefix)
		return
	}
	avg := snap.sum / float64(snap.count)
	p99 := histogramQuantile(0.99, snap.count, snap.buckets)
	fmt.Printf("batch-size(%s): count=%d avg=%.3f p99=%.3f\n", prefix, snap.count, avg, p99)
}

// ShowStats shows the current stats and updates them with the given duration.
func ShowStats(
	ctx context.Context,
	wg *sync.WaitGroup,
	durCh <-chan time.Duration,
	interval time.Duration,
	verbose bool,
	promServer *httptest.Server,
) {
	defer wg.Done()
	statCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s, total := NewStats(), NewStats()
	fmt.Println("start stats collecting")

	var (
		baseBatchSnap histogramSnapshot
		prevBatchSnap histogramSnapshot
		hasBatchSnap  bool
	)
	if verbose {
		if snap, ok, err := gatherHistogramSnapshot(tsoBatchSizeMetricName); err == nil && ok {
			baseBatchSnap, prevBatchSnap, hasBatchSnap = snap, snap, true
		}
	}
	for {
		select {
		case <-ticker.C:
			// runtime.GC()
			if verbose {
				fmt.Println(s.counter())
				// Print batch-size stats for the latest interval.
				if !hasBatchSnap {
					if snap, ok, err := gatherHistogramSnapshot(tsoBatchSizeMetricName); err == nil && ok {
						baseBatchSnap, prevBatchSnap, hasBatchSnap = snap, snap, true
					}
				} else if cur, ok, err := gatherHistogramSnapshot(tsoBatchSizeMetricName); err == nil && ok {
					printBatchSizeStats("interval", diffHistogramSnapshot(cur, prevBatchSnap))
					prevBatchSnap = cur
				}
			}
			total.merge(s)
			s = NewStats()
		case d := <-durCh:
			s.Update(d)
		case <-statCtx.Done():
			fmt.Println("\nTotal:")
			fmt.Println(total.counter())
			fmt.Println(total.percentage())
			// Calculate the percentiles by using the tDigest algorithm.
			fmt.Printf("P0.5: %.4fms, P0.8: %.4fms, P0.9: %.4fms, P0.99: %.4fms\n\n",
				latencyTDigest.Quantile(0.5), latencyTDigest.Quantile(0.8), latencyTDigest.Quantile(0.9), latencyTDigest.Quantile(0.99))
			if verbose {
				// Print batch-size stats for the whole run.
				if !hasBatchSnap {
					if snap, ok, err := gatherHistogramSnapshot(tsoBatchSizeMetricName); err == nil && ok {
						baseBatchSnap, hasBatchSnap = snap, true
					}
				}
				if hasBatchSnap {
					if cur, ok, err := gatherHistogramSnapshot(tsoBatchSizeMetricName); err == nil && ok {
						printBatchSizeStats("total", diffHistogramSnapshot(cur, baseBatchSnap))
					}
				}
				fmt.Println(collectMetrics(promServer))
			}
			return
		}
	}
}

func collectMetrics(server *httptest.Server) string {
	if server == nil {
		return ""
	}
	time.Sleep(1100 * time.Millisecond)
	res, _ := http.Get(server.URL)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	return string(body)
}

const (
	twoDur          = time.Millisecond * 2
	fiveDur         = time.Millisecond * 5
	tenDur          = time.Millisecond * 10
	thirtyDur       = time.Millisecond * 30
	fiftyDur        = time.Millisecond * 50
	oneHundredDur   = time.Millisecond * 100
	twoHundredDur   = time.Millisecond * 200
	fourHundredDur  = time.Millisecond * 400
	eightHundredDur = time.Millisecond * 800
	oneThousandDur  = time.Millisecond * 1000
)

// Stats is used to collect and calculate the latency performance.
type Stats struct {
	maxDur          time.Duration
	minDur          time.Duration
	totalDur        time.Duration
	count           int
	submilliCnt     int
	milliCnt        int
	twoMilliCnt     int
	fiveMilliCnt    int
	tenMSCnt        int
	thirtyCnt       int
	fiftyCnt        int
	oneHundredCnt   int
	twoHundredCnt   int
	fourHundredCnt  int
	eightHundredCnt int
	oneThousandCnt  int
}

// NewStats creates a new stats instance.
func NewStats() *Stats {
	return &Stats{
		minDur: time.Hour,
		maxDur: 0,
	}
}

// Update updates the stats with the given duration.
func (s *Stats) Update(dur time.Duration) {
	s.count++
	s.totalDur += dur
	latencyTDigest.Add(float64(dur.Nanoseconds())/1e6, 1)

	if dur > s.maxDur {
		s.maxDur = dur
	}
	if dur < s.minDur {
		s.minDur = dur
	}

	if dur > oneThousandDur {
		s.oneThousandCnt++
		return
	}

	if dur > eightHundredDur {
		s.eightHundredCnt++
		return
	}

	if dur > fourHundredDur {
		s.fourHundredCnt++
		return
	}

	if dur > twoHundredDur {
		s.twoHundredCnt++
		return
	}

	if dur > oneHundredDur {
		s.oneHundredCnt++
		return
	}

	if dur > fiftyDur {
		s.fiftyCnt++
		return
	}

	if dur > thirtyDur {
		s.thirtyCnt++
		return
	}

	if dur > tenDur {
		s.tenMSCnt++
		return
	}

	if dur > fiveDur {
		s.fiveMilliCnt++
		return
	}

	if dur > twoDur {
		s.twoMilliCnt++
		return
	}

	if dur > time.Millisecond {
		s.milliCnt++
		return
	}

	s.submilliCnt++
}

func (s *Stats) merge(other *Stats) {
	if s.maxDur < other.maxDur {
		s.maxDur = other.maxDur
	}
	if s.minDur > other.minDur {
		s.minDur = other.minDur
	}

	s.count += other.count
	s.totalDur += other.totalDur
	s.submilliCnt += other.submilliCnt
	s.milliCnt += other.milliCnt
	s.twoMilliCnt += other.twoMilliCnt
	s.fiveMilliCnt += other.fiveMilliCnt
	s.tenMSCnt += other.tenMSCnt
	s.thirtyCnt += other.thirtyCnt
	s.fiftyCnt += other.fiftyCnt
	s.oneHundredCnt += other.oneHundredCnt
	s.twoHundredCnt += other.twoHundredCnt
	s.fourHundredCnt += other.fourHundredCnt
	s.eightHundredCnt += other.eightHundredCnt
	s.oneThousandCnt += other.oneThousandCnt
}

func (s *Stats) counter() string {
	return fmt.Sprintf(
		"count: %d, max: %.4fms, min: %.4fms, avg: %.4fms\n<1ms: %d, >1ms: %d, >2ms: %d, >5ms: %d, >10ms: %d, >30ms: %d, >50ms: %d, >100ms: %d, >200ms: %d, >400ms: %d, >800ms: %d, >1s: %d",
		s.count, float64(s.maxDur.Nanoseconds())/float64(time.Millisecond), float64(s.minDur.Nanoseconds())/float64(time.Millisecond), float64(s.totalDur.Nanoseconds())/float64(s.count)/float64(time.Millisecond),
		s.submilliCnt, s.milliCnt, s.twoMilliCnt, s.fiveMilliCnt, s.tenMSCnt, s.thirtyCnt, s.fiftyCnt, s.oneHundredCnt, s.twoHundredCnt, s.fourHundredCnt,
		s.eightHundredCnt, s.oneThousandCnt)
}

func (s *Stats) percentage() string {
	return fmt.Sprintf(
		"count: %d, <1ms: %2.2f%%, >1ms: %2.2f%%, >2ms: %2.2f%%, >5ms: %2.2f%%, >10ms: %2.2f%%, >30ms: %2.2f%%, >50ms: %2.2f%%, >100ms: %2.2f%%, >200ms: %2.2f%%, >400ms: %2.2f%%, >800ms: %2.2f%%, >1s: %2.2f%%", s.count,
		s.calculate(s.submilliCnt), s.calculate(s.milliCnt), s.calculate(s.twoMilliCnt), s.calculate(s.fiveMilliCnt), s.calculate(s.tenMSCnt), s.calculate(s.thirtyCnt), s.calculate(s.fiftyCnt),
		s.calculate(s.oneHundredCnt), s.calculate(s.twoHundredCnt), s.calculate(s.fourHundredCnt), s.calculate(s.eightHundredCnt), s.calculate(s.oneThousandCnt))
}

func (s *Stats) calculate(count int) float64 {
	return float64(count) * 100 / float64(s.count)
}
