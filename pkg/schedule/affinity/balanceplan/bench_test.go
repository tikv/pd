// Copyright 2026 TiKV Project Authors.
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

package balanceplan

import "testing"

// BenchmarkPlan measures complete Plan calls up to the target scale of 1000
// groups and 300 stores, for every supported rule shape.
func BenchmarkPlan(b *testing.B) {
	for _, bc := range []struct {
		name string
		in   func() *Input
	}{
		{"one_per_zone/concentrated_1000x30", func() *Input { return broad(1000, 10, nil, true) }},
		{"one_per_zone/concentrated_1000x300", func() *Input { return broad(1000, 100, nil, true) }},
		{"one_per_zone/balanced_1000x300", func() *Input { return broad(1000, 100, nil, false) }},
		{"leader_on_3_stores/1000x12", func() *Input { return broad(1000, 4, []int{0, 4, 8}, true) }},
		{"restricted_90pct/1000x30", func() *Input { return bottleneck(1000, 10) }},
		{"restricted_90pct/1000x300", func() *Input { return bottleneck(1000, 100) }},
		{"3_replicas_2_zones/100x300", func() *Input { return general(100, 300, 3, 2) }},
		{"3_replicas_2_zones/1000x300", func() *Input { return general(1000, 300, 3, 2) }},
		{"5_replicas_3_zones/100x300", func() *Input { return general(100, 300, 5, 3) }},
		{"5_replicas_3_zones/1000x300", func() *Input { return general(1000, 300, 5, 3) }},
		{"zone_host/1000x300", func() *Input { return hosts(1000, 50) }},
		{"leader_rule_followers/1000x300", func() *Input { return primary(1000, 100, false) }},
		{"leader_rule_overlapping_voters/1000x300", func() *Input { return primary(1000, 100, true) }},
	} {
		b.Run(bc.name, func(b *testing.B) {
			in := bc.in()
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				Plan(in, Options{Seed: uint64(i)})
			}
		})
	}
}
