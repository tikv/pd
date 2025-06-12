// Copyright 2019 TiKV Project Authors.
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

package movingaverage

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPulse(t *testing.T) {
	re := require.New(t)
	aot := NewAvgOverTime(5 * time.Second)
	// warm up
	for range 5 {
		aot.Add(1000, time.Second)
		aot.Add(0, time.Second)
	}
	for i := range 100 {
		if i%2 == 0 {
			aot.Add(1000, time.Second)
		} else {
			aot.Add(0, time.Second)
		}
		re.LessOrEqual(aot.Get(), 600.)
		re.GreaterOrEqual(aot.Get(), 400.)
	}
}

func TestPulse2(t *testing.T) {
	re := require.New(t)
	dur := 5 * time.Second
	aot := NewAvgOverTime(dur)
	re.Equal(float64(0), aot.GetInstantaneous())
	aot.Add(1000, dur)
	re.Equal(float64(1000)/dur.Seconds(), aot.GetInstantaneous())
	re.True(aot.IsFull())
	aot.Clear()
	aot.Add(1000, dur)
	re.Equal(float64(1000)/dur.Seconds(), aot.GetInstantaneous())
}

func TestChange(t *testing.T) {
	re := require.New(t)
	aot := NewAvgOverTime(5 * time.Second)

	// phase 1: 1000
	for range 20 {
		aot.Add(1000, time.Second)
	}
	re.LessOrEqual(aot.Get(), 1010.)
	re.GreaterOrEqual(aot.Get(), 990.)

	// phase 2: 500
	for range 5 {
		aot.Add(500, time.Second)
	}
	re.LessOrEqual(aot.Get(), 900.)
	re.GreaterOrEqual(aot.Get(), 495.)
	for range 15 {
		aot.Add(500, time.Second)
	}

	// phase 3: 100
	for range 5 {
		aot.Add(100, time.Second)
	}
	re.LessOrEqual(aot.Get(), 678.)
	re.GreaterOrEqual(aot.Get(), 99.)

	// clear
	aot.Set(10)
	re.Equal(10., aot.Get())
}

func TestMinFilled(t *testing.T) {
	re := require.New(t)
	interval := 10 * time.Second
	rate := 1.0
	for aotSize := 2; aotSize < 10; aotSize++ {
		for mfSize := 2; mfSize < 10; mfSize++ {
			tm := NewTimeMedian(aotSize, mfSize, interval)
			for range aotSize {
				re.Equal(0.0, tm.Get())
				tm.Add(rate*interval.Seconds(), interval)
			}
			re.Equal(rate, tm.Get())
		}
	}
}

func TestUnstableInterval(t *testing.T) {
	re := require.New(t)
	aot := NewAvgOverTime(5 * time.Second)
	re.Equal(0., aot.Get())
	// warm up
	for range 5 {
		aot.Add(1000, time.Second)
	}
	// same rate, different interval
	for range 1000 {
		r := float64(rand.Intn(5))
		aot.Add(1000*r, time.Second*time.Duration(r))
		re.LessOrEqual(aot.Get(), 1010.)
		re.GreaterOrEqual(aot.Get(), 990.)
	}
	// warm up
	for range 5 {
		aot.Add(500, time.Second)
	}
	// different rate, same interval
	for i := range 1000 {
		rate := float64(i%5*100) + 500
		aot.Add(rate*3, time.Second*3)
		re.LessOrEqual(aot.Get(), 910.)
		re.GreaterOrEqual(aot.Get(), 490.)
	}
}
