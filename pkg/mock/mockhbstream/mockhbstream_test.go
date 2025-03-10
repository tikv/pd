// Copyright 2017 TiKV Project Authors.
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

package mockhbstream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestActivity(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	cluster.AddRegionStore(1, 1)
	cluster.AddRegionStore(2, 0)
	cluster.AddLeaderRegion(1, 1)
	region := cluster.GetRegion(1)
	hbs := hbstream.NewTestHeartbeatStreams(ctx, cluster, true)
	stream1, stream2 := NewHeartbeatStream(), NewHeartbeatStream()

	// Active stream is stream1.
	hbs.BindStream(1, stream1)
	testutil.Eventually(re, func() bool {
		msg := &hbstream.Operation{ChangePeer: &pdpb.ChangePeer{Peer: &metapb.Peer{Id: 2, StoreId: 2}, ChangeType: eraftpb.ConfChangeType_AddLearnerNode}}
		hbs.SendMsg(region, msg)
		return stream1.Recv() != nil && stream2.Recv() == nil
	})
	// Rebind to stream2.
	hbs.BindStream(1, stream2)
	testutil.Eventually(re, func() bool {
		msg := &hbstream.Operation{ChangePeer: &pdpb.ChangePeer{Peer: &metapb.Peer{Id: 2, StoreId: 2}, ChangeType: eraftpb.ConfChangeType_AddLearnerNode}}
		hbs.SendMsg(region, msg)
		return stream1.Recv() == nil && stream2.Recv() != nil
	})
	// SendErr to stream2.
	hbs.SendErr(pdpb.ErrorType_UNKNOWN, "test error", &metapb.Peer{Id: 1, StoreId: 1})
	res := stream2.Recv()
	re.NotNil(res)
	re.NotNil(res.(*pdpb.RegionHeartbeatResponse).GetHeader().GetError())
	// Switch back to 1 again.
	hbs.BindStream(1, stream1)
	testutil.Eventually(re, func() bool {
		msg := &hbstream.Operation{ChangePeer: &pdpb.ChangePeer{Peer: &metapb.Peer{Id: 2, StoreId: 2}, ChangeType: eraftpb.ConfChangeType_AddLearnerNode}}
		hbs.SendMsg(region, msg)
		return stream1.Recv() != nil && stream2.Recv() == nil
	})
}
