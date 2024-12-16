// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package keypath

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRegionPath(t *testing.T) {
	re := require.New(t)
	f := func(id uint64) string {
		return fmt.Sprintf("/pd/0/raft/r/%020d", id)
	}
	rand.New(rand.NewSource(time.Now().Unix()))
	for range 1000 {
		id := rand.Uint64()
		re.Equal(f(id), RegionPath(id))
	}
}

// func regionPath(regionID uint64) string {
// 	var buf strings.Builder
// 	buf.Grow(len(regionPathPrefix) + 1 + keyLen) // Preallocate memory

// 	buf.WriteString(regionPathPrefix)
// 	buf.WriteString("/")
// 	s := strconv.FormatUint(regionID, 10)
// 	b := make([]byte, keyLen)
// 	copy(b, s)
// 	if len(s) < keyLen {
// 		diff := keyLen - len(s)
// 		copy(b[diff:], s)
// 		for i := range diff {
// 			b[i] = '0'
// 		}
// 	} else if len(s) > keyLen {
// 		copy(b, s[len(s)-keyLen:])
// 	}
// 	buf.Write(b)

// 	return buf.String()
// }
// func BenchmarkRegionPathOld(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		_ = path.Join("/pd/cluster_id", regionPath(uint64(i)))
// 	}
// }
// BenchmarkRegionPathOld-8   	  499732	      2281 ns/op	     143 B/op	       3 allocs/op
// BenchmarkRegionPath-8      	  507408	      2244 ns/op	     111 B/op	       2 allocs/op

func BenchmarkRegionPath(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = RegionPath(uint64(i))
	}
}
