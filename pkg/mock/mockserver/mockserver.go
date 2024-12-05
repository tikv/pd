// Copyright 2024 TiKV Project Authors.
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

package mockserver

import (
	"context"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/grpcutil"
)

type MockServer struct {
	ctx            context.Context
	member, leader *pdpb.Member
	storage        storage.Storage
	bc             *core.BasicCluster
}

func NewMockServer(ctx context.Context, member, leader *pdpb.Member, storage storage.Storage, bc *core.BasicCluster) *MockServer {
	return &MockServer{
		ctx:     ctx,
		member:  member,
		leader:  leader,
		storage: storage,
		bc:      bc,
	}
}

func (s *MockServer) LoopContext() context.Context {
	return s.ctx
}

func (*MockServer) ClusterID() uint64 {
	return 1
}

func (s *MockServer) GetMemberInfo() *pdpb.Member {
	return s.member
}

func (s *MockServer) GetLeader() *pdpb.Member {
	return s.leader
}

func (s *MockServer) GetStorage() storage.Storage {
	return s.storage
}

func (*MockServer) Name() string {
	return "mock-server"
}

func (s *MockServer) GetRegions() []*core.RegionInfo {
	return s.bc.GetRegions()
}

func (*MockServer) GetTLSConfig() *grpcutil.TLSConfig {
	return &grpcutil.TLSConfig{}
}

func (s *MockServer) GetBasicCluster() *core.BasicCluster {
	return s.bc
}
