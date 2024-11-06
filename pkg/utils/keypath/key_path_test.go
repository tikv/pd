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
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRegionPath(t *testing.T) {
	re := require.New(t)
	f := func(id uint64) string {
		return path.Join(regionPathPrefix, fmt.Sprintf("%020d", id))
	}
	rand.New(rand.NewSource(time.Now().Unix()))
	for range 1000 {
		id := rand.Uint64()
		re.Equal(f(id), RegionPath(id))
	}
}

func BenchmarkRegionPath(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = RegionPath(uint64(i))
	}
}
