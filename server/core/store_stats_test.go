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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
)

func TestStoreStats(t *testing.T) {
	re := require.New(t)
	G := uint64(1024 * 1024 * 1024)
	meta := &metapb.Store{Id: 1, State: metapb.StoreState_Up}
	store := NewStoreInfo(meta, SetStoreStats(&pdpb.StoreStats{
		Capacity:  200 * G,
		UsedSize:  50 * G,
		Available: 150 * G,
	}))

	re.Equal(200*G, store.GetCapacity())
	re.Equal(50*G, store.GetUsedSize())
	re.Equal(150*G, store.GetAvailable())
	re.Equal(150*G, store.GetAvgAvailable())
	re.Equal(uint64(0), store.GetAvailableDeviation())

	store = store.Clone(SetStoreStats(&pdpb.StoreStats{
		Capacity:  200 * G,
		UsedSize:  50 * G,
		Available: 160 * G,
	}))

	re.Equal(160*G, store.GetAvailable())
	re.Greater(store.GetAvgAvailable(), 150*G)
	re.Less(store.GetAvgAvailable(), 160*G)
	re.Greater(store.GetAvailableDeviation(), uint64(0))
	re.Less(store.GetAvailableDeviation(), 10*G)
}
