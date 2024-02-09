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

// cache stores some statistics information up to the most recent complete bucket cycle,
// such as the maximum processed request count and
// the shortest processing duration in a certain period of time.
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
	// RDP(Rate-Delay Product) refers to the product of pass request count and processing duration.
	// RDP is a performance metric used to measure the maximum capacity of request on process in the system.
	// It represents the maximum amount of unacknowledged request count that can exist in the system simultaneously.
	// This indicator is clearly closely related to goroutine numbers.
	// And for the BBR status, during normal operation, the RDP is not set and its value is "inf" (infinity).
	//  However, when the BBR's rate throttling conditions are triggered, the RDP will be set accordingly.
	RDP atomic.Int64
	// minimum duration is used to record the shortest processing duration.
	// Currently, this state value is only used in testing. In future implementations,
	// if it is observed that the minimum duration continues to increase
	// even after setting the RDP (Rate-Delay Product), it indicates that the system is
	// still in a deteriorating state, and the RDP needs to be lowered further.
	minDuration atomic.Int64
}

func (f *bbrStatus) storeRDP(m int64) {
	f.RDP.Store(m)
}

func (f *bbrStatus) getRDP() int64 {
	return f.RDP.Load()
}

func (f *bbrStatus) storeMinDuration(m int64) {
	f.minDuration.Store(m)
}

func (f *bbrStatus) getMinDuration() int64 {
	return f.minDuration.Load()
}

type feedbackOpt func(*bbrStatus)

// bbr is used to implment the maximum concurrency detection algorithm
// which borrows from TCP BBR algorithm for an API.
// If the amount of concurrency keeps increasing within a specific time window,
// it indicates that the bottleneck has been reached.
//  1. Divide the time window into multiple time buckets of equal duration.
//     Initialize a counter for each time bucket to keep track of the concurrency change.
//  2. Monitor the concurrency of requests. When request comes, increment the counter of arrival time bucket.
//     When processed request, decrease the counter of pass time bucket.
//  3. Monitor the amount of pass requests. When processed request, increment the counter.
//  4. Record the processing duration.
//  5. Periodically, check the counters in each time bucket to analyze the concurrency change.
//     If the value of any counter increases continuously over a certain threshold
//     for a consecutive number of time buckets, it indicates the API is reaching its maximum concurrency.
//  6. According the maxinum pass request and minimum duration, calculate RDP and use it as conccurency limit.
type bbr struct {
	cfg             *bbrConfig
	bucketPerSecond int64
	bucketDuration  time.Duration

	// statistics
	passStat         window.RollingCounter
	durationStat     window.RollingCounter
	onProcessingStat window.RollingCounter

	// full status
	lastUpdateTime atomic.Value
	inCheck        atomic.Int64
	bbrStatus      *bbrStatus

	feedbacks []feedbackOpt

	maxPassCache     atomic.Value
	minDurationCache atomic.Value
}

func newBBR(cfg *bbrConfig, feedbacks ...feedbackOpt) *bbr {
	bucketDuration := cfg.Window / time.Duration(cfg.Bucket)
	passStat := window.NewRollingCounter(window.RollingCounterOpts{Size: cfg.Bucket, BucketDuration: bucketDuration})
	durationStat := window.NewRollingCounter(window.RollingCounterOpts{Size: cfg.Bucket, BucketDuration: bucketDuration})
	onProcessingStat := window.NewRollingCounter(window.RollingCounterOpts{Size: cfg.Bucket, BucketDuration: bucketDuration})

	limiter := &bbr{
		cfg:              cfg,
		feedbacks:        feedbacks,
		bucketDuration:   bucketDuration,
		bucketPerSecond:  int64(time.Second / bucketDuration),
		passStat:         passStat,
		durationStat:     durationStat,
		onProcessingStat: onProcessingStat,
		bbrStatus:        &bbrStatus{},
	}
	limiter.bbrStatus.storeRDP(inf)
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

func (l *bbr) calcRDP() (rdp int64, dur int64) {
	dur = l.getMinDuration()
	return int64(math.Floor(float64(l.getMaxPass()*dur*l.bucketPerSecond)/1e6) + 0.5), dur
}

func (l *bbr) getMaxPass() int64 {
	passCache := l.maxPassCache.Load()
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
	l.maxPassCache.Store(&cache{
		val:  rawMaxPass,
		time: time.Now(),
	})
	return rawMaxPass
}

func (l *bbr) getMinDuration() int64 {
	rtCache := l.minDurationCache.Load()
	if rtCache != nil {
		rc := rtCache.(*cache)
		if l.timespan(rc.time) < 1 {
			return rc.val
		}
	}
	rawMinRT := int64(math.Ceil(l.durationStat.Reduce(func(iterator window.Iterator) float64 {
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
	l.minDurationCache.Store(&cache{
		val:  rawMinRT,
		time: time.Now(),
	})
	return rawMinRT
}

// The implementation of the detection algorithm is as follows:
//  1. All buckets are divided into two parts.
//  2. The sum of the concurrent change values in each part of the bucket
//     is required to be greater than 0.
//  3. The number of buckets greater than 0 is greater than that of buckets less than 0.
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
	raises := math.Ceil(l.onProcessingStat.Reduce(func(iterator window.Iterator) float64 {
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
	rdp, dur := l.calcRDP()
	check1 := raises > 0 && positive > negative
	check2 := rdp > 1
	if check1 && check2 && l.bbrStatus.getRDP() == inf {
		l.bbrStatus.storeRDP(rdp)
		l.bbrStatus.storeMinDuration(dur)
		for _, fd := range l.feedbacks {
			fd(l.bbrStatus)
		}
	}
	l.lastUpdateTime.Store(time.Now())
}

func (l *bbr) process() DoneFunc {
	l.onProcessingStat.Add(1)
	start := time.Now().UnixMicro()
	l.checkFullStatus()
	return func() {
		if rt := time.Now().UnixMicro() - start; rt > 0 {
			l.durationStat.Add(rt)
		}
		l.onProcessingStat.Add(-1)
		l.passStat.Add(1)
	}
}
