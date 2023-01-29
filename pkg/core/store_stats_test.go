// Copyright 2020 TiKV Project Authors.
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

package core

import (
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
)

func TestStoreStats(t *testing.T) {
	re := require.New(t)
	meta := &metapb.Store{Id: 1, State: metapb.StoreState_Up}
	store := NewStoreInfo(meta, SetStoreStats(&pdpb.StoreStats{
		Capacity:  uint64(200 * units.GiB),
		UsedSize:  uint64(50 * units.GiB),
		Available: uint64(150 * units.GiB),
	}))

	re.Equal(uint64(200*units.GiB), store.GetCapacity())
	re.Equal(uint64(50*units.GiB), store.GetUsedSize())
	re.Equal(uint64(150*units.GiB), store.GetAvailable())
	re.Equal(uint64(150*units.GiB), store.GetAvgAvailable())

	store = store.Clone(SetStoreStats(&pdpb.StoreStats{
		Capacity:  uint64(200 * units.GiB),
		UsedSize:  uint64(50 * units.GiB),
		Available: uint64(160 * units.GiB),
	}))

	re.Equal(uint64(160*units.GiB), store.GetAvailable())
	re.Greater(store.GetAvgAvailable(), uint64(150*units.GiB))
	re.Less(store.GetAvgAvailable(), uint64(160*units.GiB))
}
