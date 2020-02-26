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
// limitations under the License

package cluster

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/v4/pkg/mock/mockcluster"
	"github.com/pingcap/pd/v4/pkg/mock/mockoption"
	"github.com/pingcap/pd/v4/server/schedule"
)

var _ = Suite(&testStoreLimiterSuite{})

type testStoreLimiterSuite struct {
	oc     *schedule.OperatorController
	cancel context.CancelFunc
}

func (s *testStoreLimiterSuite) SetUpSuite(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	// Create a server for testing
	opt := mockoption.NewScheduleOptions()
	cluster := mockcluster.NewCluster(opt)
	s.oc = schedule.NewOperatorController(ctx, cluster, nil)
}
func (s *testStoreLimiterSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testStoreLimiterSuite) TestCollect(c *C) {
	limiter := NewStoreLimiter(s.oc)

	limiter.Collect(&pdpb.StoreStats{})
	c.Assert(limiter.state.cst.total, Equals, int64(1))
}

func (s *testStoreLimiterSuite) TestStoreLimitScene(c *C) {
	limiter := NewStoreLimiter(s.oc)
	c.Assert(limiter.scene, DeepEquals, schedule.DefaultStoreLimitScene())
}

func (s *testStoreLimiterSuite) TestReplaceStoreLimitScene(c *C) {
	limiter := NewStoreLimiter(s.oc)

	scene := &schedule.StoreLimitScene{Idle: 4, Low: 3, Normal: 2, High: 1}
	limiter.ReplaceStoreLimitScene(scene)

	c.Assert(limiter.scene, DeepEquals, scene)
}
