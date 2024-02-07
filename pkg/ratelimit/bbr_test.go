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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	windowSizeTest = time.Second
	bucketNumTest  = 10
	bucketDuration = windowSizeTest / time.Duration(bucketNumTest)

	optsForTest = []bbrOption{
		WithWindow(windowSizeTest),
		WithBucket(bucketNumTest),
	}
	cfg = newConfig(optsForTest...)
)

func createConcurrencyFeedback() (*concurrencyLimiter, func(s *bbrStatus)) {
	cl := newConcurrencyLimiter(uint64(inf))
	return cl, func(s *bbrStatus) {
		cl.tryToSetLimit(uint64(s.getRDP()))
	}
}

func TestBBRMaxPassStat(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	_, feedback := createConcurrencyFeedback()
	bbr := newBBR(cfg, feedback)
	// default max pass is equal to 1.
	re.Equal(int64(1), bbr.getMaxPass())

	for i := 1; i <= 2; i++ {
		for j := 0; j < 5; j++ {
			bbr.process()()
		}
		time.Sleep(bucketDuration)
		re.Equal(int64(5), bbr.getMaxPass())
	}

	for i := 1; i <= 20; i++ {
		for j := 0; j < 10; j++ {
			bbr.process()()
		}
		time.Sleep(bucketDuration)
		re.Equal(int64(10), bbr.getMaxPass())
	}

	for i := 0; i < 10; i++ {
		for j := 0; j < 2; j++ {
			bbr.process()()
		}
		time.Sleep(bucketDuration)
	}
	re.Equal(int64(2), bbr.getMaxPass())
}

func TestBBRMinDuration(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	_, feedback := createConcurrencyFeedback()
	bbr := newBBR(cfg, feedback)
	// default max min rt is equal to maxFloat64.
	re.Equal(int64(3600000000000), bbr.getMinDuration())

	for i := 0; i < 10; i++ {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2; j++ {
				done := bbr.process()
				time.Sleep(time.Millisecond * 10)
				done()
			}
		}()
		time.Sleep(bucketDuration)
		wg.Wait()
		if i > 0 {
			// due to extra time cost in `Sleep`.
			re.Less(int64(10000), bbr.getMinDuration())
			re.Greater(int64(12500), bbr.getMinDuration())
		}
	}

	for i := 0; i < 10; i++ {
		for j := 0; j < 2; j++ {
			done := bbr.process()
			time.Sleep(time.Millisecond * 5)
			done()
		}
		time.Sleep(bucketDuration)
		if i > 0 {
			// due to extra time cost in `Sleep`.
			re.Less(int64(5000), bbr.getMinDuration())
			re.Greater(int64(6500), bbr.getMinDuration())
		}
	}

	for i := 0; i < 10; i++ {
		for j := 0; j < 2; j++ {
			done := bbr.process()
			time.Sleep(time.Millisecond * 20)
			done()
		}
		time.Sleep(bucketDuration)
	}
	// due to extra time cost in `Sleep`.
	re.Less(int64(20000), bbr.getMinDuration())
	re.Greater(int64(24000), bbr.getMinDuration())
}

func TestRDP(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	_, feedback := createConcurrencyFeedback()
	bbr := newBBR(cfg, feedback)
	rdp, _ := bbr.caclcRDP()
	re.Equal(int64(36000000), rdp)

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
		rdp, _ := bbr.caclcRDP()
		re.LessOrEqual(int64(10), rdp)
		re.GreaterOrEqual(int64(14), rdp)
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
			rdp, _ := bbr.caclcRDP()
			re.LessOrEqual(int64(15), rdp)
			re.GreaterOrEqual(int64(22), rdp)
		}
	}
}

func TestFullStatus(t *testing.T) {
	t.Parallel()
	re := require.New(t)
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
	maxInFlight := bbr.bbrStatus.getRDP()
	re.LessOrEqual(int64(6), maxInFlight)
	re.GreaterOrEqual(int64(10), maxInFlight)
	re.Equal(cl.limit, uint64(maxInFlight))
	re.LessOrEqual(int64(200000), bbr.bbrStatus.getMinDuration())
	re.GreaterOrEqual(int64(220000), bbr.bbrStatus.getMinDuration())
}
