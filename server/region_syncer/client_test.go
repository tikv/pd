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

package syncer

import (
	"context"
	"os"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
)

var _ = Suite(&testClientSuite{})

type testClientSuite struct{}

// For issue https://github.com/tikv/pd/issues/3936
func (t *testClientSuite) TestLoadRegion(c *C) {
	tempDir, err := os.MkdirTemp(os.TempDir(), "region_syncer_load_region")
	c.Assert(err, IsNil)
	defer os.RemoveAll(tempDir)
	rs, err := core.NewRegionStorage(context.Background(), tempDir)
	c.Assert(err, IsNil)

	storage := core.NewStorage(kv.NewMemoryKV())
	storage.SetRegionStorage(rs)

	server := &mockServer{
		ctx:     context.Background(),
		storage: storage,
		bc:      core.NewBasicCluster(),
	}
	for i := 0; i < 30; i++ {
		rs.SaveRegion(&metapb.Region{Id: uint64(i) + 1})
	}
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/core/slowLoadRegion", "return(true)"), IsNil)
	defer func() { c.Assert(failpoint.Disable("github.com/tikv/pd/server/core/slowLoadRegion"), IsNil) }()

	rc := NewRegionSyncer(server)
	start := time.Now()
	rc.StartSyncWithLeader("")
	time.Sleep(time.Second)
	rc.StopSyncWithLeader()
	c.Assert(time.Since(start), Greater, time.Second) // make sure failpoint is injected
	c.Assert(time.Since(start), Less, time.Second*2)
}

type mockServer struct {
	ctx            context.Context
	member, leader *pdpb.Member
	storage        *core.Storage
	bc             *core.BasicCluster
}

func (s *mockServer) LoopContext() context.Context {
	return s.ctx
}

func (s *mockServer) ClusterID() uint64 {
	return 1
}

func (s *mockServer) GetMemberInfo() *pdpb.Member {
	return s.member
}

func (s *mockServer) GetLeader() *pdpb.Member {
	return s.leader
}

func (s *mockServer) GetStorage() *core.Storage {
	return s.storage
}

func (s *mockServer) Name() string {
	return "mock-server"
}

func (s *mockServer) GetRegions() []*core.RegionInfo {
	return s.bc.GetRegions()
}

func (s *mockServer) GetSecurityConfig() *grpcutil.SecurityConfig {
	return &grpcutil.SecurityConfig{}
}

func (s *mockServer) GetBasicCluster() *core.BasicCluster {
	return s.bc
}
