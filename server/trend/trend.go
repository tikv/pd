// Copyright 2024 TiKV Project Authors.
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

package trend

import (
	"container/list"
	"go.uber.org/zap"
	"math"
	"sync"
	"time"

	"github.com/pingcap/log"
)

// SampleValue is a sample value.
type SampleValue struct {
	value float64
	time  time.Time
}

// SampleWindow is a window of sample values.
type SampleWindow struct {
	endpoint string
	name     string
	sum      float64
	values   *list.List
	duration time.Duration
}

// NewSampleWindow creates a new instance of SampleWindow.
func NewSampleWindow(endpoint, name string, duration time.Duration) *SampleWindow {
	return &SampleWindow{
		endpoint: endpoint,
		name:     name,
		sum:      0,
		values:   list.New(),
		duration: duration,
	}
}

// Record adds a new sample value and maintains the window constraints.
func (sw *SampleWindow) Record(value float64, now time.Time) {
	sw.values.PushBack(SampleValue{value: value, time: now})
	sw.sum += value

	for sw.values.Len() > 0 {
		front := sw.values.Front().Value.(SampleValue)
		if now.Sub(front.time) >= sw.duration {
			sw.values.Remove(sw.values.Front())
			sw.sum -= front.value
		} else {
			break
		}
	}
}

// Avg calculates the average of the values in the window.
func (sw *SampleWindow) Avg() float64 {
	if sw.values.Len() == 0 {
		return 0.0
	}
	log.Info("sw.sum", zap.Float64("sw.sum", sw.sum), zap.Int("sw.values.Len()", sw.values.Len()))
	avg := sw.sum / float64(sw.values.Len())
	leaderTrendAvgRate.WithLabelValues(sw.endpoint, sw.name).Set(avg)
	return avg
}

// Trend is a trend of a value.
type Trend struct {
	sync.RWMutex

	endpoint string

	smallWindow *SampleWindow
	bigWindow   *SampleWindow

	upperSmallWindow *SampleWindow
	upperBigWindow   *SampleWindow

	base      float64
	upperBase float64
}

// NewTrend creates a new instance of Trend.
func NewTrend(endpoint string, dur time.Duration) *Trend {
	return &Trend{
		endpoint:         endpoint,
		smallWindow:      NewSampleWindow(endpoint, "small", dur),
		bigWindow:        NewSampleWindow(endpoint, "big", dur*3),
		base:             0.001,
		upperBase:        0.001,
		upperSmallWindow: NewSampleWindow(endpoint, "upperSmall", dur),
		upperBigWindow:   NewSampleWindow(endpoint, "upperBig", dur*3),
	}
}

// Record adds a new sample value and maintains the window constraints.
func (t *Trend) Record(base float64, value float64, now time.Time) {
	t.Lock()
	defer t.Unlock()

	t.base = base
	t.smallWindow.Record(value, now)
	t.bigWindow.Record(value, now)

	if value > t.upperBase {
		t.upperBase = value
	}
	absVal := math.Abs(value - t.upperBase)
	t.upperSmallWindow.Record(absVal, now)
	t.upperBigWindow.Record(absVal, now)
}

// AvgRate calculates the average rate of the values in the window.
func (t *Trend) AvgRate() (float64, float64) {
	t.RLock()
	defer t.RUnlock()

	smallAvg := t.smallWindow.Avg()
	bigAvg := t.bigWindow.Avg()
	// we use base val to make sure increasing must greater than base
	unSensitive := t.base * unSensitiveCause
	rate := laLbRateSimple(smallAvg, bigAvg, unSensitive)
	leaderTrendAvgRate.WithLabelValues(t.endpoint, "rate").Set(rate)

	upperRate := laLbRateSimple(t.upperSmallWindow.Avg(), t.upperBigWindow.Avg(), unSensitive)
	leaderTrendAvgRate.WithLabelValues(t.endpoint, "upperRate").Set(upperRate)

	log.Info("trend rate", zap.String("name", t.endpoint), zap.Float64("rate", rate), zap.Float64("upperBase", t.upperBase), zap.Float64("unSensitive", unSensitive),
		zap.Float64("base", t.base), zap.Float64("smallWindow.Avg()", smallAvg), zap.Float64("bigWindow.Avg()", bigAvg),
		zap.Float64("upperRate", upperRate), zap.Float64("upperSmallWindow.Avg()", t.upperSmallWindow.Avg()), zap.Float64("upperBigWindow.Avg()", t.upperBigWindow.Avg()))
	return rate, upperRate
}

// laLbRateSimple is a simple version of LaLbRate.
func laLbRateSimple(laAvg, lbAvg, marginError float64) float64 {
	if lbAvg < math.SmallestNonzeroFloat64 {
		return 0.0
	}
	increased := laAvg - lbAvg
	if math.Abs(increased) < math.SmallestNonzeroFloat64 {
		return 0.0
	}

	if increased > 0 && increased > marginError {
		increased -= marginError
	} else if increased < 0 && -increased > marginError {
		increased = increased + marginError
	} else {
		increased = 0.0
	}

	return increased / laAvg
}

// TODO: unSensitiveCause reduce influence of spike
const unSensitiveCause = 2.0

// A*(A-B)^2 / Sqrt(B) = x
func laLbRate(laAvg float64, lbAvg float64, marginError float64) float64 {
	if lbAvg < math.SmallestNonzeroFloat64 {
		return 0.0
	}
	increased := laAvg - lbAvg
	if math.Abs(increased) < math.SmallestNonzeroFloat64 {
		return 0.0
	}
	if laAvg < lbAvg {
		if -increased > marginError {
			increased = -increased - marginError
		} else {
			increased = 0.0
		}
	} else if increased > marginError {
		increased -= marginError
	} else {
		increased = 0.0
	}
	incSq := increased * increased
	if laAvg < lbAvg {
		incSq = -incSq
	}
	res := laAvg * incSq / math.Sqrt(lbAvg)
	if laAvg < lbAvg {
		return -math.Sqrt(-res)
	}
	return math.Sqrt(res)
}
