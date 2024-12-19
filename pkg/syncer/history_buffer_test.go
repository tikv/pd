// Copyright 2018 TiKV Project Authors.
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

package syncer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage/kv"
)

func TestBufferSize(t *testing.T) {
	re := require.New(t)
	var regions []*core.RegionInfo
	for i := 0; i <= 100; i++ {
		regions = append(regions, core.NewRegionInfo(&metapb.Region{Id: uint64(i)}, nil))
	}

	// size equals 1
	h := newHistoryBuffer(1, kv.NewMemoryKV())
	re.Equal(0, h.len())
	for _, r := range regions {
		h.record(r)
	}
	re.Equal(1, h.len())
	re.Equal(regions[h.nextIndex()-1], h.get(100))
	re.Nil(h.get(99))

	// size equals 2
	h = newHistoryBuffer(2, kv.NewMemoryKV())
	for _, r := range regions {
		h.record(r)
	}
	re.Equal(2, h.len())
	re.Equal(regions[h.nextIndex()-1], h.get(100))
	re.Equal(regions[h.nextIndex()-2], h.get(99))
	re.Nil(h.get(98))

	// size equals 100
	kvMem := kv.NewMemoryKV()
	h1 := newHistoryBuffer(100, kvMem)
	for i := range 6 {
		h1.record(regions[i])
	}
	re.Equal(6, h1.len())
	re.Equal(uint64(6), h1.nextIndex())
	h1.persist()

	// restart the buffer
	h2 := newHistoryBuffer(100, kvMem)
	re.Equal(uint64(6), h2.nextIndex())
	re.Equal(uint64(6), h2.firstIndex())
	re.Nil(h2.get(h.nextIndex() - 1))
	re.Equal(0, h2.len())
	for _, r := range regions {
		index := h2.nextIndex()
		h2.record(r)
		re.Equal(r, h2.get(index))
	}

	re.Equal(uint64(107), h2.nextIndex())
	re.Nil(h2.get(h2.nextIndex()))
	s, err := h2.kv.Load(historyKey)
	re.NoError(err)
	// flush in index 106
	re.Equal("106", s)

	histories := h2.recordsFrom(uint64(1))
	re.Empty(histories)
	histories = h2.recordsFrom(h2.firstIndex())
	re.Len(histories, 100)
	re.Equal(uint64(7), h2.firstIndex())
	re.Equal(regions[1:], histories)
}
