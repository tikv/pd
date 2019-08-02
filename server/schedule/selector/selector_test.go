// Copyright 2019 PingCAP, Inc.
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

package selector

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockoption"
	"github.com/pingcap/pd/server/core"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSelectorSuite{})

type testSelectorSuite struct {
	tc *mockcluster.Cluster
}

func (s *testSelectorSuite) SetUpSuite(c *C) {
	opt := mockoption.NewScheduleOptions()
	s.tc = mockcluster.NewCluster(opt)
}

func (s *testSelectorSuite) TestCompareStoreScore(c *C) {
	store1 := core.NewStoreInfoWithLabel(1, 1, nil)
	store2 := core.NewStoreInfoWithLabel(2, 1, nil)
	store3 := core.NewStoreInfoWithLabel(3, 3, nil)

	c.Assert(compareStoreScore(s.tc, store1, 2, store2, 1), Equals, 1)
	c.Assert(compareStoreScore(s.tc, store1, 1, store2, 1), Equals, 0)
	c.Assert(compareStoreScore(s.tc, store1, 1, store2, 2), Equals, -1)

	c.Assert(compareStoreScore(s.tc, store1, 2, store3, 1), Equals, 1)
	c.Assert(compareStoreScore(s.tc, store1, 1, store3, 1), Equals, 1)
	c.Assert(compareStoreScore(s.tc, store1, 1, store3, 2), Equals, -1)
}
