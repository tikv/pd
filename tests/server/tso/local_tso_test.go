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

package tso_test

import (
	"context"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

var _ = Suite(&testLocalTSOSuite{})

type testLocalTSOSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testLocalTSOSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testLocalTSOSuite) TearDownSuite(c *C) {
	s.cancel()
}

// TestNormalGlobalTSO is used to test the normal way of global TSO generation.
func (s *testLocalTSOSuite) TestLocalTSO(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.LocalTSO.EnableLocalTSO = true
		conf.LocalTSO.DCLocation = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	dcClientMap := make(map[string]pdpb.PDClient)
	for _, dcLocation := range dcLocationConfig {
		pdName := cluster.WaitAllocatorLeader(dcLocation)
		c.Assert(len(pdName), Greater, 0)
		dcClientMap[dcLocation] = testutil.MustNewGrpcClient(c, cluster.GetServer(pdName).GetAddr())
	}

	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lastList := make(map[string]*pdpb.Timestamp)
			for _, dcLocation := range dcLocationConfig {
				lastList[dcLocation] = &pdpb.Timestamp{
					Physical: 0,
					Logical:  0,
				}
			}
			for j := 0; j < 30; j++ {
				for _, dcLocation := range dcLocationConfig {
					req := &pdpb.TsoRequest{
						Header:     testutil.NewRequestHeader(leaderServer.GetClusterID()),
						Count:      tsoCount,
						DcLocation: dcLocation,
					}
					ts := s.testGetLocalTimestamp(c, dcClientMap[dcLocation], req)
					lastTS := lastList[dcLocation]
					c.Assert(ts.GetPhysical(), Not(Less), lastTS.GetPhysical())
					if ts.GetPhysical() == lastTS.GetPhysical() {
						c.Assert(ts.GetLogical(), Greater, lastTS.GetLogical())
					}
					lastList[dcLocation] = ts
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

func (s *testLocalTSOSuite) testGetLocalTimestamp(c *C, pdCli pdpb.PDClient, req *pdpb.TsoRequest) *pdpb.Timestamp {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tsoClient, err := pdCli.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, req.GetCount())
	res := resp.GetTimestamp()
	c.Assert(res.GetLogical(), Greater, int64(0))
	return res
}
