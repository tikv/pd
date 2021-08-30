// Copyright 2021 TiKV Project Authors.
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

package schedulers

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
)

var _ = Suite(&testUtilsSuite{})

type testUtilsSuite struct{}

func (s *testUtilsSuite) TestRetryQuota(c *C) {
	q := newRetryQuota(10, 1, 2)
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1})
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2})
	keepStores := []*core.StoreInfo{store1}

	// test GetLimit
	c.Assert(q.GetLimit(store1), Equals, 10)

	// test Attenuate
	for _, expected := range []int{5, 2, 1, 1, 1} {
		q.Attenuate(store1)
		c.Assert(q.GetLimit(store1), Equals, expected)
	}

	// test GC
	c.Assert(q.GetLimit(store2), Equals, 10)
	q.Attenuate(store2)
	c.Assert(q.GetLimit(store2), Equals, 5)
	q.GC(keepStores)
	c.Assert(q.GetLimit(store1), Equals, 1)
	c.Assert(q.GetLimit(store2), Equals, 10)

	// test ResetLimit
	q.ResetLimit(store1)
	c.Assert(q.GetLimit(store1), Equals, 10)
}
