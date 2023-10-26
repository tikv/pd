// Copyright 2023 TiKV Project Authors.
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	windowSizeTest = time.Millisecond * 100
	bucketNumTest  = 10

	optsForTest = []bbrOption{
		WithWindow(windowSizeTest),
		WithBucket(bucketNumTest),
	}
	cfg = newConfig(optsForTest...)
)

func createConcurrencyFeedback() (*concurrencyLimiter, func(s *bbrStatus)) {
	cl := newConcurrencyLimiter(uint64(inf))
	return cl, func(s *bbrStatus) {
		cl.tryToSetLimit(uint64(s.getMaxInFlight()))
	}
}

func TestBBRMaxPass(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	bucketDuration := windowSizeTest / time.Duration(bucketNumTest)
	_, feedback := createConcurrencyFeedback()
	bbr := newBBR(cfg, feedback)
	// default max pass is equal to 1.
	re.Equal(int64(1), bbr.getMaxPASS())

	for i := 1; i <= 2; i++ {
		for j := 0; j < 5; j++ {
			bbr.process()()
		}
		time.Sleep(bucketDuration)
		re.Equal(int64(5), bbr.getMaxPASS())
	}

	for i := 1; i <= 20; i++ {
		for j := 0; j < 10; j++ {
			bbr.process()()
		}
		time.Sleep(bucketDuration)
		re.Equal(int64(10), bbr.getMaxPASS())
	}

	for i := 0; i < 10; i++ {
		for j := 0; j < 2; j++ {
			bbr.process()()
		}
		time.Sleep(bucketDuration)
	}
	re.Equal(int64(2), bbr.getMaxPASS())
}

func TestBBRMinRt(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	bucketDuration := windowSizeTest / time.Duration(bucketNumTest)
	_, feedback := createConcurrencyFeedback()
	bbr := newBBR(cfg, feedback)
	// default max min rt is equal to maxFloat64.
	re.Equal(int64(60000000000), bbr.getMinRT(true))

	for i := 0; i < 10; i++ {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2; j++ {
				done := bbr.process()
				time.Sleep(time.Millisecond)
				done()
			}
		}()
		time.Sleep(bucketDuration)
		wg.Wait()
		if i > 0 {
			// due to extra time cost in `Sleep`.
			re.Less(int64(1000), bbr.getMinRT(true))
			re.Greater(int64(1300), bbr.getMinRT(true))
		}
	}

	for i := 0; i < 10; i++ {
		for j := 0; j < 2; j++ {
			done := bbr.process()
			time.Sleep(time.Microsecond * 500)
			done()
		}
		time.Sleep(bucketDuration)
		if i > 0 {
			// due to extra time cost in `Sleep`.
			re.Less(int64(500), bbr.getMinRT(true))
			re.Greater(int64(700), bbr.getMinRT(true))
		}
	}

	for i := 0; i < 10; i++ {
		for j := 0; j < 2; j++ {
			done := bbr.process()
			time.Sleep(time.Millisecond * 2)
			done()
		}
		time.Sleep(bucketDuration)
	}
	// due to extra time cost in `Sleep`.
	re.Less(int64(2000), bbr.getMinRT(true))
	re.Greater(int64(2500), bbr.getMinRT(true))
}

func TestBDP(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	// to make test stabel, scale out bucket duration
	windowSizeTest := time.Second
	bucketNumTest := 10
	optsForTest := []bbrOption{
		WithWindow(windowSizeTest),
		WithBucket(bucketNumTest),
	}
	cfg := newConfig(optsForTest...)
	bucketDuration := windowSizeTest / time.Duration(bucketNumTest)
	_, feedback := createConcurrencyFeedback()
	bbr := newBBR(cfg, feedback)
	re.Equal(int64(600000), bbr.getMaxInFlight())

	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			go func() {
				done := bbr.process()
				time.Sleep(time.Millisecond * 10)
				done()
			}()
		}
		time.Sleep(bucketDuration)
		// due to extra time cost in `Sleep`.
		re.LessOrEqual(int64(10), bbr.getMaxInFlight())
		re.GreaterOrEqual(int64(12), bbr.getMaxInFlight())
	}

	for i := 0; i < 10; i++ {
		for j := 0; j < 300; j++ {
			go func() {
				done := bbr.process()
				time.Sleep(time.Millisecond * 5)
				done()
			}()
		}
		time.Sleep(bucketDuration)
		if i > 0 {
			// due to extra time cost in `Sleep`.
			re.LessOrEqual(int64(15), bbr.getMaxInFlight())
			re.GreaterOrEqual(int64(18), bbr.getMaxInFlight())
		}
	}
}

func TestFullStatus(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	bucketDuration := windowSizeTest / time.Duration(bucketNumTest)
	cl, feedback := createConcurrencyFeedback()
	bbr := newBBR(cfg, feedback)

	for i := 0; i < 14; i++ {
		for j := 0; j < i+2; j++ {
			go func() {
				done := bbr.process()
				time.Sleep(bucketDuration * 2)
				done()
			}()
		}
		time.Sleep(bucketDuration)
	}
	maxInFlight := bbr.bbrStatus.getMaxInFlight()
	re.LessOrEqual(int64(7), maxInFlight)
	re.GreaterOrEqual(int64(9), maxInFlight)
	re.Equal(cl.limit, uint64(maxInFlight))
	re.LessOrEqual(int64(20000), bbr.bbrStatus.getMinRT())
	re.GreaterOrEqual(int64(22000), bbr.bbrStatus.getMinRT())
}
