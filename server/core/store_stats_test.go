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
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testStoreStatsSuite{})

type testStoreStatsSuite struct{}

func (s *testStoreStatsSuite) TestStoreStats(c *C) {
	G := uint64(1024 * 1024 * 1024)
	meta := &metapb.Store{Id: 1, State: metapb.StoreState_Up}
	store := NewStoreInfo(meta, SetStoreStats(&pdpb.StoreStats{
		Capacity:  200 * G,
		UsedSize:  50 * G,
		Available: 150 * G,
	}))

	c.Assert(store.GetCapacity(), Equals, 200*G)
	c.Assert(store.GetUsedSize(), Equals, 50*G)
	c.Assert(store.GetAvailable(), Equals, 150*G)
	c.Assert(store.GetAvgAvailable(), Equals, 150*G)
	c.Assert(store.GetAvailableDeviation(), Equals, uint64(0))

	store = store.Clone(SetStoreStats(&pdpb.StoreStats{
		Capacity:  200 * G,
		UsedSize:  50 * G,
		Available: 160 * G,
	}))

	c.Assert(store.GetAvailable(), Equals, 160*G)
	c.Assert(store.GetAvgAvailable(), Greater, 150*G)
	c.Assert(store.GetAvgAvailable(), Less, 160*G)
	c.Assert(store.GetAvailableDeviation(), Greater, uint64(0))
	c.Assert(store.GetAvailableDeviation(), Less, 10*G)
}
