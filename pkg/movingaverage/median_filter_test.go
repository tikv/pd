// Copyright 2022 TiKV Project Authors.
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
)

func BenchmarkMedianFilter(b *testing.B) {
	data := []float64{2, 1, 3, 4, 1, 1, 3, 3, 2, 0, 5}

	mf := NewMedianFilter(10)
	for _, n := range data {
		mf.Add(n)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mf.Get()
	}
}

type testcase struct {
	AddCount int
	GetCount int
}

var cases []testcase = []testcase{
	{
		AddCount: 1,
		GetCount: 1,
	},
	{
		AddCount: 1,
		GetCount: 3,
	},
	{
		AddCount: 1,
		GetCount: 10,
	},
	{
		AddCount: 3,
		GetCount: 1,
	},
}

func BenchmarkMedianFilterAddAndGet1(b *testing.B) {
	n := 5
	mf := NewMedianFilter(n)
	rand.Seed(time.Now().UnixNano())
	for j := 0; j < n; j++ {
		mf.Add(rand.Float64())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < cases[0].AddCount; j++ {
			mf.Add(rand.Float64())
		}
		for j := 0; j < cases[0].GetCount; j++ {
			mf.Get()
		}
	}
}

func BenchmarkMedianFilterAddAndGet2(b *testing.B) {
	n := 5
	mf := NewMedianFilter(n)
	rand.Seed(time.Now().UnixNano())
	for j := 0; j < n; j++ {
		mf.Add(rand.Float64())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < cases[1].AddCount; j++ {
			mf.Add(rand.Float64())
		}
		for j := 0; j < cases[1].GetCount; j++ {
			mf.Get()
		}
	}
}

func BenchmarkMedianFilterAddAndGet3(b *testing.B) {
	n := 5
	mf := NewMedianFilter(n)
	rand.Seed(time.Now().UnixNano())
	for j := 0; j < n; j++ {
		mf.Add(rand.Float64())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < cases[2].AddCount; j++ {
			mf.Add(rand.Float64())
		}
		for j := 0; j < cases[2].GetCount; j++ {
			mf.Get()
		}
	}
}

func BenchmarkMedianFilterAddAndGet4(b *testing.B) {
	n := 5
	mf := NewMedianFilter(n)
	rand.Seed(time.Now().UnixNano())
	for j := 0; j < n; j++ {
		mf.Add(rand.Float64())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < cases[3].AddCount; j++ {
			mf.Add(rand.Float64())
		}
		for j := 0; j < cases[3].GetCount; j++ {
			mf.Get()
		}
	}
}
