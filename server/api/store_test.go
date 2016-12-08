// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"net/url"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type testStoreSuite struct{}

var _ = Suite(&testStoreSuite{})

func (s *testStoreSuite) TestUrlStoreFilter(c *C) {
	stores := []*metapb.Store{
		&metapb.Store{
			// metapb.StoreState_Up == 0
			State: metapb.StoreState_Up,
		},
		&metapb.Store{
			State: metapb.StoreState_Up,
		},
		&metapb.Store{
			// metapb.StoreState_Tombstone == 2
			State: metapb.StoreState_Tombstone,
		},
	}

	var table = []struct {
		u    string
		want []*metapb.Store
	}{
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=0",
			want: stores[:2],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2",
			want: stores[2:],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=foo",
			want: stores,
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores",
			want: stores,
		},
	}

	for _, t := range table {
		uu, err := url.Parse(t.u)
		c.Assert(err, IsNil)
		f := newStoreStateFilter(uu)
		c.Assert(f.filter(stores), DeepEquals, t.want)
	}
}
