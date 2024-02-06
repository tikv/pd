// The MIT License (MIT)
// Copyright (c) 2022 go-kratos Project Authors.
//
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

package ratelimit

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/tikv/pd/pkg/window"
)

const (
	inf   = int64(^uint64(0) >> 1)
	infRT = int64(time.Hour)

	defaultWindowSize = time.Second * 10
	defaultBucketSize = 100
)

type cache struct {
	val  int64
	time time.Time
}

type bbrOption func(*bbrConfig)

type bbrConfig struct {
	// WindowSize defines time duration per window
	Window time.Duration
	// BucketNum defines bucket number for each window
	Bucket int
}

func newConfig(opts ...bbrOption) *bbrConfig {
	cfg := &bbrConfig{
		Window: defaultWindowSize,
		Bucket: defaultBucketSize,
	}
	for _, o := range opts {
		o(cfg)
	}
	return cfg
}

// WithWindow with window size.
func WithWindow(d time.Duration) bbrOption {
	return func(o *bbrConfig) {
		o.Window = d
	}
}

// WithBucket with bucket ize.
func WithBucket(b int) bbrOption {
	return func(o *bbrConfig) {
		o.Bucket = b
	}
}

type bbrStatus struct {
	maxInFlight atomic.Int64
	minRT       atomic.Int64
}

func (f *bbrStatus) storeMaxInFlight(m int64) {
	f.maxInFlight.Store(m)
}

func (f *bbrStatus) getMaxInFlight() int64 {
	return f.maxInFlight.Load()
}

func (f *bbrStatus) storeMinRT(m int64) {
	f.minRT.Store(m)
}

func (f *bbrStatus) getMinRT() int64 {
	return f.minRT.Load()
}

type feedbackOpt func(*bbrStatus)

type bbr struct {
	cfg             *bbrConfig
	bucketPerSecond int64
	bucketDuration  time.Duration

	// statistics
	passStat     window.RollingCounter
	rtStat       window.RollingCounter
	inFlightStat window.RollingCounter

	// full status
	lastUpdateTime atomic.Value
	inCheck        atomic.Int64
	bbrStatus      *bbrStatus

	feedbacks []feedbackOpt

	maxPASSCache atomic.Value
	minRtCache   atomic.Value
}

func newBBR(cfg *bbrConfig, feedbacks ...feedbackOpt) *bbr {
	bucketDuration := cfg.Window / time.Duration(cfg.Bucket)
	passStat := window.NewRollingCounter(window.RollingCounterOpts{Size: cfg.Bucket, BucketDuration: bucketDuration})
	rtStat := window.NewRollingCounter(window.RollingCounterOpts{Size: cfg.Bucket, BucketDuration: bucketDuration})
	inFlightStat := window.NewRollingCounter(window.RollingCounterOpts{Size: cfg.Bucket, BucketDuration: bucketDuration})

	limiter := &bbr{
		cfg:             cfg,
		feedbacks:       feedbacks,
		bucketDuration:  bucketDuration,
		bucketPerSecond: int64(time.Second / bucketDuration),
		passStat:        passStat,
		rtStat:          rtStat,
		inFlightStat:    inFlightStat,
		bbrStatus:       &bbrStatus{},
	}
	limiter.bbrStatus.storeMaxInFlight(inf)
	return limiter
}

// timespan returns the passed bucket count
// since lastTime, if it is one bucket duration earlier than
// the last recorded time, it will return the BucketNum.
func (l *bbr) timespan(lastTime time.Time) int {
	v := int(time.Since(lastTime) / l.bucketDuration)
	if v > -1 {
		return v
	}
	return l.cfg.Bucket
}

func (l *bbr) getBDP() float64 {
	return float64(l.getMaxPASS()*l.getMinRT()*l.bucketPerSecond) / 1e6
}

func (l *bbr) getMaxInFlight() int64 {
	return int64(math.Floor(l.getBDP()) + 0.5)
}

func (l *bbr) getMaxPASS() int64 {
	passCache := l.maxPASSCache.Load()
	if passCache != nil {
		ps := passCache.(*cache)
		if l.timespan(ps.time) < 1 {
			return ps.val
		}
	}
	rawMaxPass := int64(l.passStat.Reduce(func(iterator window.Iterator) float64 {
		var result = 0.0
		for i := 1; iterator.Next() && i < l.cfg.Bucket; i++ {
			bucket := iterator.Bucket()
			count := 0.0
			for _, p := range bucket.Points {
				count += p
			}
			result = math.Max(result, count)
		}
		return result
	}))
	// don't save cache when no update, and default is 1.
	if rawMaxPass < 1 {
		rawMaxPass = 1
	}
	if rawMaxPass == 1 {
		return rawMaxPass
	}
	l.maxPASSCache.Store(&cache{
		val:  rawMaxPass,
		time: time.Now(),
	})
	return rawMaxPass
}

func (l *bbr) getMinRT() int64 {
	rtCache := l.minRtCache.Load()
	if rtCache != nil {
		rc := rtCache.(*cache)
		if l.timespan(rc.time) < 1 {
			return rc.val
		}
	}
	rawMinRT := int64(math.Ceil(l.rtStat.Reduce(func(iterator window.Iterator) float64 {
		var result = float64(infRT)
		for i := 1; iterator.Next() && i < l.cfg.Bucket; i++ {
			bucket := iterator.Bucket()
			if len(bucket.Points) == 0 {
				continue
			}
			total := 0.0
			for _, p := range bucket.Points {
				total += p
			}
			avg := total / float64(bucket.Count)
			avg = math.Max(1., avg)
			result = math.Min(result, avg)
		}
		return result
	})))
	// if rtStat is empty, rawMinRT will be zero.
	if rawMinRT < 1 {
		rawMinRT = infRT
	}
	if rawMinRT == inf {
		return rawMinRT
	}
	l.minRtCache.Store(&cache{
		val:  rawMinRT,
		time: time.Now(),
	})
	return rawMinRT
}

func (l *bbr) checkFullStatus() {
	temp := l.lastUpdateTime.Load()
	if temp != nil {
		t := temp.(time.Time)
		if l.timespan(t) < 1 {
			return
		}
	}
	if !l.inCheck.CompareAndSwap(0, 1) {
		return
	}
	positive := 0
	negative := 0
	raises := math.Ceil(l.inFlightStat.Reduce(func(iterator window.Iterator) float64 {
		var result = 0.
		i := 1
		for ; iterator.Next() && i < l.cfg.Bucket/2; i++ {
			bucket := iterator.Bucket()
			total := 0.0
			for _, p := range bucket.Points {
				total += p
			}
			result += total
			if total > 1e-6 {
				positive++
			} else if total < -1e-6 {
				negative++
			}
			if positive < negative {
				break
			}
		}
		if positive <= 0 {
			return result
		}
		for ; iterator.Next() && i < l.cfg.Bucket; i++ {
			bucket := iterator.Bucket()
			total := 0.0
			for _, p := range bucket.Points {
				total += p
			}
			result += total
			if total > 1e-6 {
				positive++
			} else if total < -1e-6 {
				negative++
			}
			if positive < negative {
				break
			}
		}
		return result
	}))

	l.inCheck.Store(0)

	check1 := raises > 0 && positive > negative
	check2 := l.getBDP() > 1.0
	if check1 && check2 && l.bbrStatus.getMaxInFlight() == inf {
		maxInFlight := l.getMaxInFlight()
		l.bbrStatus.storeMaxInFlight(maxInFlight)
		l.bbrStatus.storeMinRT(l.getMinRT())
		for _, fd := range l.feedbacks {
			fd(l.bbrStatus)
		}
	}
	l.lastUpdateTime.Store(time.Now())
}

func (l *bbr) process() DoneFunc {
	l.inFlightStat.Add(1)
	start := time.Now().UnixMicro()
	l.checkFullStatus()
	return func() {
		if rt := time.Now().UnixMicro() - start; rt > 0 {
			l.rtStat.Add(rt)
		}
		l.inFlightStat.Add(-1)
		l.passStat.Add(1)
	}
}
