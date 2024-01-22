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

package etcdutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test the endpoint picking and evicting logic.
func TestPickEps(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		healthProbes       []healthProbe
		expectedEvictedEps map[string]int
		expectedPickedEps  []string
	}{
		// {} -> {A, B}
		{
			[]healthProbe{
				{
					ep:      "A",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "B",
					healthy: true,
					took:    time.Millisecond,
				},
			},
			map[string]int{},
			[]string{"A", "B"},
		},
		// {A, B} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:      "A",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "B",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "C",
					healthy: true,
					took:    time.Millisecond,
				},
			},
			map[string]int{},
			[]string{"A", "B", "C"},
		},
		// {A, B, C} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:      "A",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "B",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "C",
					healthy: true,
					took:    time.Millisecond,
				},
			},
			map[string]int{},
			[]string{"A", "B", "C"},
		},
		// {A, B, C} -> {C}
		{
			[]healthProbe{
				{
					ep:      "C",
					healthy: true,
					took:    time.Millisecond,
				},
			},
			map[string]int{"A": 0, "B": 0},
			[]string{"C"},
		},
		// {C} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:      "A",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "B",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "C",
					healthy: true,
					took:    time.Millisecond,
				},
			},
			map[string]int{"A": 1, "B": 1},
			[]string{"C"},
		},
		// {C} -> {B, C}
		{
			[]healthProbe{
				{
					ep:      "B",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "C",
					healthy: true,
					took:    time.Millisecond,
				},
			},
			map[string]int{"A": 0, "B": 2},
			[]string{"C"},
		},
		// {C} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:      "A",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "B",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "C",
					healthy: true,
					took:    time.Millisecond,
				},
			},
			map[string]int{"A": 1},
			[]string{"B", "C"},
		},
		// {B, C} -> {D}
		{
			[]healthProbe{
				{
					ep:      "D",
					healthy: true,
					took:    time.Millisecond,
				},
			},
			map[string]int{"A": 0, "B": 0, "C": 0},
			[]string{"D"},
		},
		// {D} -> {B, C}
		{
			[]healthProbe{
				{
					ep:      "B",
					healthy: true,
					took:    time.Millisecond,
				},
				{
					ep:      "C",
					healthy: true,
					took:    time.Millisecond,
				},
			},
			map[string]int{"A": 0, "B": 1, "C": 1, "D": 0},
			[]string{},
		},
	}
	checker := &healthChecker{}
	lastEps := []string{}
	for idx, tc := range testCases {
		pickedEps := checker.pickEps(tc.healthProbes)
		checker.updateEvictedEps(lastEps, pickedEps)
		pickedEps = checker.filterEps(pickedEps)
		// Check the states after finishing picking.
		count := 0
		checker.evictedEps.Range(func(key, value interface{}) bool {
			count++
			ep := key.(string)
			times := value.(int)
			re.Equal(tc.expectedEvictedEps[ep], times, "case %d ep %s", idx, ep)
			return true
		})
		re.Len(tc.expectedEvictedEps, count, "case %d", idx)
		re.Equal(tc.expectedPickedEps, pickedEps, "case %d", idx)
		lastEps = pickedEps
	}
}
