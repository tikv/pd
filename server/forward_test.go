// Copyright 2026 TiKV Project Authors.
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

package server

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
)

func TestForwardSchedulingHeartbeatChangeSplit(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		changeSplit *pdpb.ChangeSplit
	}{
		{name: "disable auto split", changeSplit: &pdpb.ChangeSplit{AutoSplitEnabled: false}},
		{name: "enable auto split", changeSplit: &pdpb.ChangeSplit{AutoSplitEnabled: true}},
		{name: "no change split"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			rc := cluster.NewRaftCluster(t.Context(), nil, core.NewBasicCluster(), storage.NewStorageWithMemoryBackend(), nil, nil, nil, nil)
			re.NoError(rc.InitCluster(nil, config.NewPersistOptions(config.NewConfig()), nil, nil))
			response := &schedulingpb.RegionHeartbeatResponse{
				Header:      &schedulingpb.ResponseHeader{ClusterId: 1},
				RegionId:    2,
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 3, Version: 4},
				TargetPeer:  &metapb.Peer{Id: 5, StoreId: 6},
				ChangeSplit: testCase.changeSplit,
			}
			source := &testSchedulingHeartbeatClient{response: response}
			destination := &testPDHeartbeatServer{}
			errCh := make(chan error, 1)
			forwardRegionHeartbeatToScheduling(rc, source, &heartbeatServer{stream: destination}, errCh)
			re.ErrorIs(<-errCh, io.EOF)
			re.Len(destination.responses, 1)
			forwarded := destination.responses[0]
			re.Equal(response.GetHeader().GetClusterId(), forwarded.GetHeader().GetClusterId())
			re.Equal(response.GetRegionId(), forwarded.GetRegionId())
			re.Equal(response.GetRegionEpoch(), forwarded.GetRegionEpoch())
			re.Equal(response.GetTargetPeer(), forwarded.GetTargetPeer())
			// In particular, an explicit false must not become an absent message.
			re.Equal(testCase.changeSplit, forwarded.GetChangeSplit())
		})
	}
}

type testSchedulingHeartbeatClient struct {
	schedulingpb.Scheduling_RegionHeartbeatClient
	response *schedulingpb.RegionHeartbeatResponse
}

func (s *testSchedulingHeartbeatClient) Recv() (*schedulingpb.RegionHeartbeatResponse, error) {
	if s.response == nil {
		return nil, io.EOF
	}
	response := s.response
	s.response = nil
	return response, nil
}

type testPDHeartbeatServer struct {
	pdpb.PD_RegionHeartbeatServer
	responses []*pdpb.RegionHeartbeatResponse
}

func (s *testPDHeartbeatServer) Send(response *pdpb.RegionHeartbeatResponse) error {
	s.responses = append(s.responses, response)
	return nil
}
