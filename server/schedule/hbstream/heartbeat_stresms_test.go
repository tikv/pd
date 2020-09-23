package hbstream

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockhbstream"
	"github.com/tikv/pd/server/config"
)

func TestHeaertbeatStreams(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testHeartbeatStreamSuite{})

type testHeartbeatStreamSuite struct {
}

func (s *testHeartbeatStreamSuite) TestActivity(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster := mockcluster.NewCluster(config.NewTestOptions())
	cluster.AddRegionStore(1, 1)
	cluster.AddRegionStore(2, 0)
	cluster.AddLeaderRegion(1, 1)
	region := cluster.GetRegion(1)
	msg := &pdpb.RegionHeartbeatResponse{
		ChangePeer: &pdpb.ChangePeer{Peer: &metapb.Peer{Id: 2, StoreId: 2}, ChangeType: eraftpb.ConfChangeType_AddLearnerNode},
	}

	hbs := NewTestHeartbeatStreams(ctx, cluster.ID, cluster, true)
	stream1, stream2 := mockhbstream.NewHeartbeatStream(), mockhbstream.NewHeartbeatStream()

	// Active stream is stream1.
	hbs.BindStream(1, stream1)
	time.Sleep(50 * time.Millisecond)
	hbs.SendMsg(region, msg)
	c.Assert(stream1.Recv(), NotNil)
	c.Assert(stream2.Recv(), IsNil)
	// Rebind to stream2.
	hbs.BindStream(1, stream2)
	time.Sleep(50 * time.Millisecond)
	hbs.SendMsg(region, msg)
	c.Assert(stream1.Recv(), IsNil)
	c.Assert(stream2.Recv(), NotNil)
	// SendErr to stream2.
	hbs.SendErr(pdpb.ErrorType_UNKNOWN, "test error", &metapb.Peer{Id: 1, StoreId: 1})
	time.Sleep(50 * time.Millisecond)
	res := stream2.Recv()
	c.Assert(res, NotNil)
	c.Assert(res.GetHeader().GetError(), NotNil)
	// Switch back to 1 again.
	hbs.BindStream(1, stream1)
	time.Sleep(50 * time.Millisecond)
	hbs.SendMsg(region, msg)
	c.Assert(stream1.Recv(), NotNil)
	c.Assert(stream2.Recv(), IsNil)
}
