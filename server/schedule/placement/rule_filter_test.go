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
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
)

func (s *testRuleSuite) TestRuleFilter(c *C) {
	testcases := []struct {
		name   string
		filter RuleFilter
		rule   *Rule
		pass   bool
	}{
		{
			name: "WithMatchStoreFilter, Pass",
			filter: func() RuleFilter {
				store1 := core.NewStoreInfo(&metapb.Store{
					Id: uint64(1),
					Labels: []*metapb.StoreLabel{
						{
							Key:   "dc",
							Value: "sh",
						},
						{
							Key:   "Host",
							Value: "host1",
						},
					},
				})
				stores := []*core.StoreInfo{
					store1,
				}
				return WithMatchStoreFilter(stores)
			}(),
			rule: &Rule{
				GroupID: "TiDB_DDL_51",
				ID:      "0",
				Role:    Follower,
				Count:   1,
				LabelConstraints: []LabelConstraint{
					{
						Key: "dc",
						Values: []string{
							"sh",
						},
						Op: In,
					},
				},
			},
			pass: true,
		},
		{
			name: "WithMatchStoreFilter, Abandon",
			filter: func() RuleFilter {
				store1 := core.NewStoreInfo(&metapb.Store{
					Id: uint64(1),
					Labels: []*metapb.StoreLabel{
						{
							Key:   "dc",
							Value: "sh",
						},
						{
							Key:   "Host",
							Value: "host1",
						},
					},
				})
				stores := []*core.StoreInfo{
					store1,
				}
				return WithMatchStoreFilter(stores)
			}(),
			rule: &Rule{
				GroupID: "TiDB_DDL_51",
				ID:      "0",
				Role:    Follower,
				Count:   1,
				LabelConstraints: []LabelConstraint{
					{
						Key: "dc",
						Values: []string{
							"bj",
						},
						Op: In,
					},
				},
			},
			pass: false,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		c.Assert(testcase.filter.Filter(testcase.rule), Equals, testcase.pass)
	}
}
