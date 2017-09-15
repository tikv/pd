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

package server

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testOperatorSuite{})

type testOperatorSuite struct{}

func doRegionHeartbeatResponse(region *core.RegionInfo, resp *pdpb.RegionHeartbeatResponse) {
	if resp == nil {
		return
	}

	if resp.GetTransferLeader() != nil {
		region.Leader = resp.GetTransferLeader().GetPeer()
		return
	}

	switch resp.GetChangePeer().GetChangeType() {
	case pdpb.ConfChangeType_AddNode:
		region.Peers = append(region.Peers, resp.GetChangePeer().GetPeer())
	case pdpb.ConfChangeType_RemoveNode:
		var index int
		for i, p := range region.GetPeers() {
			if p.GetId() == resp.GetChangePeer().GetPeer().GetId() {
				index = i
				break
			}
		}
		region.Peers = append(region.Peers[:index], region.Peers[index+1:]...)
	}
}
