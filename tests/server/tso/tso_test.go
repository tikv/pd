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
// See the License for the specific language governing permissions and
// limitations under the License.

// +build tso_full_test tso_function_test

package tso_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

var _ = Suite(&testTSOSuite{})

type testTSOSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testTSOSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testTSOSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testTSOSuite) TestLoadTimestamp(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	lastTSMap := requestLocalTSOs(c, cluster, dcLocationConfig)

	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/systemTimeSlow", `return(true)`), IsNil)

	// Reboot the cluster.
	err = cluster.StopAll()
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	// Re-request the Local TSOs.
	newTSMap := requestLocalTSOs(c, cluster, dcLocationConfig)
	for dcLocation, newTS := range newTSMap {
		lastTS, ok := lastTSMap[dcLocation]
		c.Assert(ok, IsTrue)
		// The new physical time of TSO should be larger even if the system time is slow.
		c.Assert(newTS.GetPhysical()-lastTS.GetPhysical(), Greater, int64(0))
	}

	failpoint.Disable("github.com/tikv/pd/server/tso/systemTimeSlow")
}

func requestLocalTSOs(c *C, cluster *tests.TestCluster, dcLocationConfig map[string]string) map[string]*pdpb.Timestamp {
	dcClientMap := make(map[string]pdpb.PDClient)
	tsMap := make(map[string]*pdpb.Timestamp)
	leaderServer := cluster.GetServer(cluster.GetLeader())
	for _, dcLocation := range dcLocationConfig {
		pdName := leaderServer.GetAllocatorLeader(dcLocation).GetName()
		dcClientMap[dcLocation] = testutil.MustNewGrpcClient(c, cluster.GetServer(pdName).GetAddr())
	}
	for _, dcLocation := range dcLocationConfig {
		req := &pdpb.TsoRequest{
			Header:     testutil.NewRequestHeader(leaderServer.GetClusterID()),
			Count:      tsoCount,
			DcLocation: dcLocation,
		}
		ctx, cancel := context.WithCancel(context.Background())
		ctx = grpcutil.BuildForwardContext(ctx, cluster.GetServer(leaderServer.GetAllocatorLeader(dcLocation).GetName()).GetAddr())
		tsMap[dcLocation] = testGetTimestamp(c, ctx, dcClientMap[dcLocation], req)
		cancel()
	}
	return tsMap
}
