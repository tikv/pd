// Copyright 2017 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type testLabelsStoreSuite struct{}

var _ = Suite(&testLabelsStoreSuite{})

func (s *testLabelsStoreSuite) TestStroesLabelFilter(c *C) {
	stores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west-1",
				},
				{
					Key:   "disk",
					Value: "ssd",
				},
			},
		},
		{
			Id:    4,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west-2",
				},
				{
					Key:   "disk",
					Value: "hdd",
				},
			},
		},
		{
			Id:    6,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "beijing",
				},
				{
					Key:   "disk",
					Value: "ssd",
				},
			},
		},
		{
			Id:    7,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "hongkong",
				},
				{
					Key:   "disk",
					Value: "ssd",
				},
				{
					Key:   "other",
					Value: "test",
				},
			},
		},
	}

	var table = []struct {
		name, value string
		want        []*metapb.Store
	}{
		{
			name: "zone",
			want: stores[:],
		},
		{
			name: "other",
			want: stores[3:],
		},
		{
			name:  "zone",
			value: "us-west-1",
			want:  stores[:1],
		},
		{
			name:  "zone",
			value: "west",
			want:  stores[:2],
		},
		{
			name:  "zo",
			value: "beijing",
			want:  stores[2:3],
		},
		{
			name:  "zone",
			value: "ssd",
			want:  []*metapb.Store{},
		},
	}
	for _, t := range table {
		f, err := newStoresLabelFilter(t.name, t.value)
		c.Assert(err, IsNil)
		c.Assert(f.filter(stores), DeepEquals, t.want)
	}
	_, err := newStoresLabelFilter("test", ".[test")
	c.Assert(err, NotNil)
}
