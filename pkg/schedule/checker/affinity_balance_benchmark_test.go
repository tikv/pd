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

package checker

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkAffinityBalance measures a worker step with 300 stores and 1000 groups.
func BenchmarkAffinityBalance(b *testing.B) {
	for _, name := range []string{"first", "replan"} {
		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			re := require.New(b)
			f := newAffinityBalanceTest(b, 300, "z0", "z1", "z2")
			ids := f.seed("100", 1, slices.Repeat([]uint64{1}, 1000)...)
			for rounds := 0; name == "replan" && f.step(); rounds++ {
				re.Less(rounds, 1000, "table did not converge")
				f.settle()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				if name == "first" {
					for _, id := range ids {
						_, err := f.affinityManager.UpdateAffinityGroupPeers(id, 1, []uint64{1, 2, 3})
						re.NoError(err)
					}
					f.settle()
				}
				clear(f.balancer.idle)
				b.StartTimer()
				re.Equal(name == "first", f.step())
			}
			b.StopTimer()
		})
	}
}
