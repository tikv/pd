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

// The MIT License (MIT)
// Copyright (c) 2022 go-kratos Project Authors.

package window

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const defaultBucketDuration = 100 * time.Millisecond
const defaultSize = 3

func getRollingPolicy() *RollingPolicy {
	w := NewWindow(Options{Size: defaultSize})
	return NewRollingPolicy(w, RollingPolicyOpts{BucketDuration: defaultBucketDuration})
}

func TestRollingPolicy_Add(t *testing.T) {
	re := require.New(t)
	// test func timespan return real span
	tests := []struct {
		timeSleep []int
		offset    []int
		points    []float64
	}{
		{
			timeSleep: []int{150, 51},
			offset:    []int{1, 2},
			points:    []float64{1, 1},
		},
		{
			// In CI, the actual sleep time may be more than 100 (timeSleep = 94).
			timeSleep: []int{94, 250},
			offset:    []int{0, 0},
			points:    []float64{1, 1},
		},
		{
			timeSleep: []int{150, 300, 600},
			offset:    []int{1, 1, 1},
			points:    []float64{1, 1, 1},
		},
	}

	for _, test := range tests {
		t.Run("test policy add", func(_ *testing.T) {
			var totalTS, lastOffset int
			timeSleep := test.timeSleep
			beginTime := time.Now()
			policy := getRollingPolicy()
			points := make([]float64, defaultSize)
			asExpected := true
			for i, n := range timeSleep {
				totalTS += n
				time.Sleep(time.Duration(n) * time.Millisecond)
				point := test.points[i]
				offset := int(time.Since(beginTime)/defaultBucketDuration) % defaultSize
				points[i] += point
				policy.Add(point)
				if offset != test.offset[i] {
					asExpected = false
				}
				if asExpected {
					re.Lessf(math.Abs(point-policy.window.buckets[offset].Points[0]), 1e-6,
						"error, time since last append: %vms, last offset: %v", totalTS, lastOffset)
				}
				re.Lessf(math.Abs(points[i]-policy.window.buckets[offset].Points[0]), 1e-6,
					"error, time since last append: %vms, last offset: %v", totalTS, lastOffset)
				lastOffset = offset
			}
		})
	}
}

func TestRollingPolicy_AddWithTimespan(t *testing.T) {
	re := require.New(t)
	t.Run("timespan < bucket number", func(t *testing.T) {
		policy := getRollingPolicy()
		// bucket 0
		policy.Add(0)
		// bucket 1
		time.Sleep(101 * time.Millisecond)
		policy.Add(1)
		re.Equal(1, int(policy.window.buckets[1].Points[0]))
		// bucket 2
		time.Sleep(101 * time.Millisecond)
		policy.Add(2)
		// bucket 1
		time.Sleep(201 * time.Millisecond)
		policy.Add(4)

		for _, bkt := range policy.window.buckets {
			t.Logf("%+v", bkt)
		}

		re.Empty(policy.window.buckets[0].Points)
		re.Equal(4, int(policy.window.buckets[1].Points[0]))
		re.Equal(2, int(policy.window.buckets[2].Points[0]))
	})

	t.Run("timespan > bucket number", func(t *testing.T) {
		policy := getRollingPolicy()

		// bucket 0
		policy.Add(0)
		// bucket 1
		time.Sleep(101 * time.Millisecond)
		policy.Add(1)
		policy.Add(1)
		re.Equal(2, int(policy.window.buckets[1].Points[0]))
		// bucket 2
		time.Sleep(101 * time.Millisecond)
		policy.Add(2)
		// bucket 1
		time.Sleep(501 * time.Millisecond)
		policy.Add(4)

		for _, bkt := range policy.window.buckets {
			t.Logf("%+v", bkt)
		}

		re.Empty(policy.window.buckets[0].Points)
		re.Equal(4, int(policy.window.buckets[1].Points[0]))
		re.Empty(policy.window.buckets[2].Points)
	})
}
