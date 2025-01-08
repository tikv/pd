// Copyright 2025 TiKV Project Authors.
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

package filter

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
)

func TestRegionCompare(t *testing.T) {
	re := require.New(t)
	ids := []uint64{1, 2, 3, 4, 5}
	stores := make([]*core.StoreInfo, 0, len(ids))
	for _, id := range ids {
		stores = append(stores, core.NewStoreInfo(
			&metapb.Store{Id: id},
			core.SetRegionSize(int64(6-id)*1000),
		))
	}
	cs := NewCandidates(rand.New(rand.NewSource(time.Now().UnixNano())), stores)
	cfg := mockconfig.NewTestOptions()
	re.Equal(uint64(1), cs.PickFirst().GetID())
	cs.Sort(RegionScoreComparer(cfg))
	re.Equal(uint64(5), cs.PickFirst().GetID())
}

func TestLeaderCompare(t *testing.T) {
	re := require.New(t)
	ids := []uint64{1, 2, 3, 4, 5}
	stores := make([]*core.StoreInfo, 0, len(ids))
	for _, id := range ids {
		stores = append(stores, core.NewStoreInfo(
			&metapb.Store{Id: id},
			core.SetLeaderCount(int(6-id)*1000),
		))
	}
	cs := NewCandidates(rand.New(rand.NewSource(time.Now().UnixNano())), stores)
	cfg := mockconfig.NewTestOptions()
	re.Equal(uint64(1), cs.PickFirst().GetID())
	cs.Sort(LeaderScoreComparer(cfg))
	re.Equal(uint64(5), cs.PickFirst().GetID())
}
