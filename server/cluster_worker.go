package server

import (
	"sync/atomic"

	"github.com/coreos/etcd/clientv3"
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

func (c *raftCluster) maybeChangePeer(request *pdpb.RegionHeartbeatRequest, reqRegion *metapb.Region,
	region *metapb.Region) ([]clientv3.Op, *pdpb.ChangePeer, error) {
	leader := request.GetLeader()
	if leader == nil {
		return nil, nil, errors.Errorf("invalid request leader, %v", request)
	}
	if region == nil {
		return nil, nil, nil
	}

	regionEpoch := region.GetRegionEpoch()
	reqRegionEpoch := reqRegion.GetRegionEpoch()

	if reqRegionEpoch.GetConfVer() < regionEpoch.GetConfVer() {
		// If the request epoch configure version is less than the current one, return an error.
		return nil, nil, errors.Errorf("invalid region epoch, request: %v, currenrt: %v", reqRegionEpoch, regionEpoch)
	} else if reqRegionEpoch.GetConfVer() > regionEpoch.GetConfVer() {
		// If the request epoch configure version is greater than the current one, update region meta.
		regionSearchPath := makeRegionSearchKey(c.clusterRoot, reqRegion.GetEndKey())
		regionValue, err := proto.Marshal(reqRegion)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		var ops []clientv3.Op
		ops = append(ops, clientv3.OpPut(regionSearchPath, string(regionValue)))
		return ops, nil, nil
	}

	// If the request epoch configure version is equal to the current one, handle change peer request.
	changePeer, err := c.handleChangePeerReq(reqRegion, leader.GetId())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return nil, changePeer, nil
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

func (c *raftCluster) maybeSplit(request *pdpb.RegionHeartbeatRequest, reqRegion *metapb.Region,
	region *metapb.Region) ([]clientv3.Op, []byte, error) {
	// For split, we should handle heartbeat carefully.
	// E.g, for region 1 [a, c) -> 1 [a, b) + 2 [b, c).
	// after split, region 1 and 2 will do heartbeat independently.
	// We can know that now 1 can be found by region id but 2 is not.
	// So we must process the region range overlapped problem.

	var ops []clientv3.Op
	regionValue, err := proto.Marshal(reqRegion)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	reqRegionEncStartKey := encodeRegionStartKey(reqRegion.GetStartKey())
	reqRegionEncEndKey := encodeRegionSearchKey(reqRegion.GetEndKey())
	reqRegionPath := makeRegionKey(c.clusterRoot, reqRegion.GetId())
	reqSearchKey := makeRegionSearchKey(c.clusterRoot, reqRegion.GetEndKey())

	if region == nil {
		// Find no range after start key, insert directly.
		ops = append(ops, clientv3.OpPut(reqRegionPath, reqRegionEncEndKey))
		ops = append(ops, clientv3.OpPut(reqSearchKey, string(regionValue)))
		return ops, nil, nil
	}

	// If the request epoch is less than current region epoch, then returns an error.
	reqRegionEpoch := reqRegion.GetRegionEpoch()
	regionEpoch := region.GetRegionEpoch()
	if reqRegionEpoch.GetVersion() < regionEpoch.GetVersion() {
		// If the request epoch version is less than the current one, return an error.
		return nil, nil, errors.Errorf("invalid region epoch, request: %v, currenrt: %v", reqRegionEpoch, regionEpoch)
	} else if reqRegionEpoch.GetVersion() == regionEpoch.GetVersion() {
		// If the request epoch version is equal to the current one, do nothing.
		return nil, nil, nil
	}

	regionEncStartKey := encodeRegionStartKey(region.GetStartKey())
	regionEncEndKey := encodeRegionSearchKey(region.GetEndKey())

	delRegionKey := []byte{}
	if reqRegionEncStartKey == regionEncStartKey &&
		reqRegionEncEndKey == regionEncEndKey {
		// Seems there is something wrong? Do nothing.
	} else if regionEncStartKey >= reqRegionEncEndKey {
		// No range [start, end) in region now, insert directly.
		ops = append(ops, clientv3.OpPut(reqRegionPath, reqRegionEncEndKey))
		ops = append(ops, clientv3.OpPut(reqSearchKey, string(regionValue)))
	} else {
		// Region overlapped, remove old region, insert new one.
		// E.g, 1 [a, c) -> 1 [a, b) + 2 [b, c), either new 1 or 2 reports, the region
		// is overlapped with origin [a, c).
		regionPath := makeRegionKey(c.clusterRoot, region.GetId())
		regionSearchKey := makeRegionSearchKey(c.clusterRoot, region.GetEndKey())
		if regionPath != reqRegionPath {
			ops = append(ops, clientv3.OpDelete(regionPath))
		}
		if regionSearchKey != reqSearchKey {
			ops = append(ops, clientv3.OpDelete(regionSearchKey))
			delRegionKey = []byte(regionSearchKey)
		}
		ops = append(ops, clientv3.OpPut(reqRegionPath, reqRegionEncEndKey))
		ops = append(ops, clientv3.OpPut(reqSearchKey, string(regionValue)))
	}

	return ops, delRegionKey, nil
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
