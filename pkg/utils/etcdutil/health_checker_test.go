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

type testCase struct {
	healthProbes       []healthProbe
	expectedEvictedEps map[string]int
	expectedPickedEps  []string
}

func check(re *require.Assertions, testCases []*testCase) {
	checker := &healthChecker{}
	lastEps := []string{}
	for idx, tc := range testCases {
		// Send the health probes to the channel.
		probeCh := make(chan healthProbe, len(tc.healthProbes))
		for _, probe := range tc.healthProbes {
			probeCh <- probe
			// Mock that all the endpoints are healthy.
			checker.healthyClients.LoadOrStore(probe.ep, &healthyClient{})
		}
		close(probeCh)
		// Pick and filter the endpoints.
		pickedEps := checker.pickEps(probeCh)
		checker.updateEvictedEps(lastEps, pickedEps)
		pickedEps = checker.filterEps(pickedEps)
		// Check the evicted states after finishing picking.
		count := 0
		checker.evictedEps.Range(func(key, value any) bool {
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

// Test the endpoint picking and evicting logic.
func TestPickEps(t *testing.T) {
	re := require.New(t)
	testCases := []*testCase{
		// {} -> {A, B}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Millisecond,
				},
				{
					ep:   "B",
					took: time.Millisecond,
				},
			},
			map[string]int{},
			[]string{"A", "B"},
		},
		// {A, B} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Millisecond,
				},
				{
					ep:   "B",
					took: time.Millisecond,
				},
				{
					ep:   "C",
					took: time.Millisecond,
				},
			},
			map[string]int{},
			[]string{"A", "B", "C"},
		},
		// {A, B, C} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Millisecond,
				},
				{
					ep:   "B",
					took: time.Millisecond,
				},
				{
					ep:   "C",
					took: time.Millisecond,
				},
			},
			map[string]int{},
			[]string{"A", "B", "C"},
		},
		// {A, B, C} -> {C}
		{
			[]healthProbe{
				{
					ep:   "C",
					took: time.Millisecond,
				},
			},
			map[string]int{"A": 0, "B": 0},
			[]string{"C"},
		},
		// {C} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Millisecond,
				},
				{
					ep:   "B",
					took: time.Millisecond,
				},
				{
					ep:   "C",
					took: time.Millisecond,
				},
			},
			map[string]int{"A": 1, "B": 1},
			[]string{"C"},
		},
		// {C} -> {B, C}
		{
			[]healthProbe{
				{
					ep:   "B",
					took: time.Millisecond,
				},
				{
					ep:   "C",
					took: time.Millisecond,
				},
			},
			map[string]int{"A": 0, "B": 2},
			[]string{"C"},
		},
		// {C} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Millisecond,
				},
				{
					ep:   "B",
					took: time.Millisecond,
				},
				{
					ep:   "C",
					took: time.Millisecond,
				},
			},
			map[string]int{"A": 1},
			[]string{"B", "C"},
		},
		// {B, C} -> {D}
		{
			[]healthProbe{
				{
					ep:   "D",
					took: time.Millisecond,
				},
			},
			map[string]int{"A": 0, "B": 0, "C": 0},
			[]string{"D"},
		},
		// {D} -> {B, C}
		{
			[]healthProbe{
				{
					ep:   "B",
					took: time.Millisecond,
				},
				{
					ep:   "C",
					took: time.Millisecond,
				},
			},
			map[string]int{"A": 0, "B": 1, "C": 1, "D": 0},
			[]string{"B", "C"},
		},
		// {B, C} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Millisecond,
				},
				{
					ep:   "B",
					took: time.Millisecond,
				},
				{
					ep:   "C",
					took: time.Millisecond,
				},
			},
			map[string]int{"A": 1, "B": 2, "C": 2, "D": 0},
			[]string{"A", "B", "C"},
		},
		// {A, B, C} -> {A, C, E}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Millisecond,
				},
				{
					ep:   "C",
					took: time.Millisecond,
				},
				{
					ep:   "E",
					took: time.Millisecond,
				},
			},
			map[string]int{"A": 2, "B": 0, "D": 0},
			[]string{"C", "E"},
		},
	}
	check(re, testCases)
}

func TestLatencyPick(t *testing.T) {
	re := require.New(t)
	testCases := []*testCase{
		// {} -> {A, B}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Millisecond,
				},
				{
					ep:   "B",
					took: time.Millisecond,
				},
			},
			map[string]int{},
			[]string{"A", "B"},
		},
		// {A, B} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Millisecond,
				},
				{
					ep:   "B",
					took: time.Millisecond,
				},
				{
					ep:   "C",
					took: time.Second,
				},
			},
			map[string]int{},
			[]string{"A", "B"},
		},
		// {A, B} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Second,
				},
				{
					ep:   "B",
					took: time.Second,
				},
				{
					ep:   "C",
					took: 2 * time.Second,
				},
			},
			map[string]int{},
			[]string{"A", "B"},
		},
		// {A, B} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Second,
				},
				{
					ep:   "B",
					took: 2 * time.Second,
				},
				{
					ep:   "C",
					took: 3 * time.Second,
				},
			},
			map[string]int{"B": 0},
			[]string{"A"},
		},
		// {A} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Second,
				},
				{
					ep:   "B",
					took: time.Second,
				},
				{
					ep:   "C",
					took: time.Millisecond,
				},
			},
			map[string]int{"A": 0, "B": 0},
			[]string{"C"},
		},
		// {C} -> {A, B, C}
		{
			[]healthProbe{
				{
					ep:   "A",
					took: time.Millisecond,
				},
				{
					ep:   "B",
					took: time.Millisecond,
				},
				{
					ep:   "C",
					took: time.Second,
				},
			},
			map[string]int{"A": 1, "B": 1, "C": 0},
			[]string{"A", "B"},
		},
	}
	check(re, testCases)
}

func TestUpdateEvictedEpsAfterRemoval(t *testing.T) {
	re := require.New(t)
	var (
		checker   = &healthChecker{}
		lastEps   = []string{"A", "B", "C"}
		pickedEps = []string{"A", "C"}
	)
	// All endpoints are healthy.
	for _, ep := range lastEps {
		checker.healthyClients.Store(ep, &healthyClient{})
	}
	checker.updateEvictedEps(lastEps, pickedEps)
	// B should be evicted.
	_, ok := checker.evictedEps.Load("B")
	re.True(ok)
	// Remove the endpoint B to mock member removal.
	checker.healthyClients.Delete("B")
	checker.evictedEps.Delete("B")
	checker.updateEvictedEps(lastEps, pickedEps)
	// B should not be evicted since it has been removed.
	_, ok = checker.evictedEps.Load("B")
	re.False(ok)
}
