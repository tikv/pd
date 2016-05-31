package server

import (
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raftpb"
	"github.com/pingcap/kvproto/pkg/util"
)

func (c *raftCluster) handleAddPeerReq(region *metapb.Region) (*metapb.Peer, error) {
	peerID, err := c.s.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	stores := c.cachedCluster.getStores()

	// Find a proper store which the region has not in.
	// Now we just choose the first store. Later we will do
	// a better choice.
	//
	// TODO:
	// 1, The stores may be in same machine, so we should choose the best
	// store which the region's current stores are not in. We can use IP to
	// check this.
	// 2, We can check the store statistics and find a low load store.
	// 3, more algorithms...
LOOP:
	for _, store := range stores {
		storeID := store.store.GetId()
		// we can't add peer in the same store.
		for _, peer := range region.Peers {
			if peer.GetStoreId() == storeID {
				continue LOOP
			}
		}
		return &metapb.Peer{
			Id:      proto.Uint64(peerID),
			StoreId: proto.Uint64(storeID),
		}, nil
	}
	log.Warnf("find no store to add peer for region %v", region)
	return nil, nil
}

// If leader is nil, we will return an error, or else we can remove none leader peer.
func (c *raftCluster) handleRemovePeerReq(region *metapb.Region, leaderID uint64) (*metapb.Peer, error) {
	if len(region.Peers) <= 1 {
		return nil, errors.Errorf("can not remove peer for region %v", region)
	}
	for _, peer := range region.Peers {
		if peer.GetId() != leaderID {
			return peer, nil
		}
	}
	// Maybe we can't enter here.
	return nil, errors.Errorf("find no proper peer to remove for region %v", region)
}

func (c *raftCluster) handleChangePeerReq(region *metapb.Region, leaderID uint64) (*pdpb.ChangePeer, error) {
	clusterMeta, err := c.GetConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var (
		maxPeerCount = int(clusterMeta.GetMaxPeerCount())
		regionID     = region.GetId()
		peerCount    = len(region.GetPeers())
		changeType   raftpb.ConfChangeType
		peer         *metapb.Peer
	)

	if peerCount == maxPeerCount {
		log.Infof("region %d peer count equals %d, no need to change peer", regionID, maxPeerCount)
		return nil, nil
	} else if peerCount < maxPeerCount {
		log.Infof("region %d peer count %d < %d, need to add peer", regionID, peerCount, maxPeerCount)
		changeType = raftpb.ConfChangeType_AddNode
		if peer, err = c.handleAddPeerReq(region); err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		log.Infof("region %d peer count %d > %d, need to remove peer", regionID, peerCount, maxPeerCount)
		changeType = raftpb.ConfChangeType_RemoveNode
		if peer, err = c.handleRemovePeerReq(region, leaderID); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if peer == nil {
		return nil, nil
	}

	changePeer := &pdpb.ChangePeer{
		ChangeType: changeType.Enum(),
		Peer:       peer,
	}

	return changePeer, nil
}

func (c *raftCluster) HandleAskSplit(request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	reqRegion := request.GetRegion()
	startKey := reqRegion.GetStartKey()
	region, err := c.GetRegion(startKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// If the request epoch is less than current region epoch, then returns an error.
	reqRegionEpoch := reqRegion.GetRegionEpoch()
	regionEpoch := region.GetRegionEpoch()
	if reqRegionEpoch.GetVersion() < regionEpoch.GetVersion() ||
		reqRegionEpoch.GetConfVer() < regionEpoch.GetConfVer() {
		return nil, errors.Errorf("invalid region epoch, request: %v, currenrt: %v", reqRegionEpoch, regionEpoch)
	}

	newRegionID, err := c.s.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	peerIDs := make([]uint64, len(request.Region.Peers))
	for i := 0; i < len(peerIDs); i++ {
		if peerIDs[i], err = c.s.idAlloc.Alloc(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	split := &pdpb.AskSplitResponse{
		NewRegionId: proto.Uint64(newRegionID),
		NewPeerIds:  peerIDs,
	}

	return split, nil
}

func (c *raftCluster) callCommand(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	storeID := request.Header.Peer.GetStoreId()
	store, err := c.GetStore(storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	nc, err := c.storeConns.GetConn(store.GetAddress())
	if err != nil {
		return nil, errors.Trace(err)
	}

	msg := &msgpb.Message{
		MsgType: msgpb.MessageType_Cmd.Enum(),
		CmdReq:  request,
	}

	msgID := atomic.AddUint64(&c.s.msgID, 1)
	if err = util.WriteMessage(nc.conn, msgID, msg); err != nil {
		c.storeConns.RemoveConn(store.GetAddress())
		return nil, errors.Trace(err)
	}

	msg.Reset()
	if _, err = util.ReadMessage(nc.conn, msg); err != nil {
		c.storeConns.RemoveConn(store.GetAddress())
		return nil, errors.Trace(err)
	}

	if msg.CmdResp == nil {
		// This is a very serious bug, should we panic here?
		return nil, errors.Errorf("invalid command response message but %v", msg)
	}

	return msg.CmdResp, nil
}
