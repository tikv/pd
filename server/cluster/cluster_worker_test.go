// Copyright 2016 TiKV Project Authors.
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

package cluster

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
	_ "github.com/tikv/pd/server/schedulers"
)

var _ = Suite(&testClusterWorkerSuite{})

type testClusterWorkerSuite struct{}

func (s *testClusterWorkerSuite) TestReportSplit(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	left := &metapb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	right := &metapb.Region{Id: 2, StartKey: []byte("b"), EndKey: []byte("c")}
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: left, Right: right})
	c.Assert(err, IsNil)
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: right, Right: left})
	c.Assert(err, NotNil)
}

func (s *testClusterWorkerSuite) TestReportBatchSplit(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestRaftCluster(mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	regions := []*metapb.Region{
		{Id: 1, StartKey: []byte(""), EndKey: []byte("a")},
		{Id: 2, StartKey: []byte("a"), EndKey: []byte("b")},
		{Id: 3, StartKey: []byte("b"), EndKey: []byte("c")},
		{Id: 3, StartKey: []byte("c"), EndKey: []byte("")},
	}
	_, err = cluster.HandleBatchReportSplit(&pdpb.ReportBatchSplitRequest{Regions: regions})
	c.Assert(err, IsNil)
}
