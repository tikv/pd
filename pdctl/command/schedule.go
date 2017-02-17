package command

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/util"
	pd "github.com/pingcap/pd/pd-client"
	"github.com/pingcap/pd/pkg/rpcutil"
	"github.com/twinj/uuid"
)

type scheduler struct {
	addr      string
	client    pd.Client
	clusterID uint64
}

func newScheduler(addr string) *scheduler {
	client, err := pd.NewClient([]string{addr})
	if err != nil {
		log.Fatal(err)
	}
	return &scheduler{
		addr:      addr,
		client:    client,
		clusterID: client.GetClusterID(),
	}
}

func (s *scheduler) allocID() uint64 {
	req := s.newRequest(pdpb.CommandType_AllocId)
	req.AllocId = &pdpb.AllocIdRequest{}
	return s.sendRequest(req).AllocId.GetId()
}

func (s *scheduler) allocPeer(storeID uint64) *metapb.Peer {
	return &metapb.Peer{
		Id:      s.allocID(),
		StoreId: storeID,
	}
}

func (s *scheduler) getRegion(regionID uint64) (*metapb.Region, *metapb.Peer) {
	req := s.newRequest(pdpb.CommandType_GetRegionByID)
	req.GetRegionById = &pdpb.GetRegionByIDRequest{RegionId: regionID}
	resp := s.sendRequest(req).GetRegionById
	return resp.Region, resp.Leader
}

func (s *scheduler) transferLeader(regionID uint64, storeID uint64) {
	region, leader := s.getRegion(regionID)
	for _, peer := range region.GetPeers() {
		if peer.GetStoreId() == storeID {
			_, err := s.transferLeaderRequest(region, leader, peer)
			if err != nil {
				fmt.Println("Error: ", err)
			}
			return
		}
	}
}

func (s *scheduler) transferRegion(regionID uint64, storeIDs []uint64) {
	for i := 0; i < 60; i++ {
		finished, err := s.changeOnePeer(regionID, storeIDs)
		if finished {
			return
		}
		if err != nil {
			fmt.Println("Error: ", err)
		}
		time.Sleep(10 * time.Second)
	}
}

func (s *scheduler) changeOnePeer(regionID uint64, storeIDs []uint64) (bool, error) {
	region, leader := s.getRegion(regionID)
	fmt.Println("region peers", region.GetPeers())

	stores := make(map[uint64]struct{})
	for _, id := range storeIDs {
		stores[id] = struct{}{}
	}

	peers := make(map[uint64]*metapb.Peer)
	for _, peer := range region.GetPeers() {
		peers[peer.GetStoreId()] = peer
	}

	if len(peers) > len(stores) {
		for id, peer := range peers {
			if _, ok := stores[id]; !ok {
				fmt.Println("Remove peer", peer)
				_, err := s.removePeerRequest(region, leader, peer)
				return false, errors.Trace(err)
			}
		}
	} else {
		for id := range stores {
			if _, ok := peers[id]; !ok {
				newPeer := s.allocPeer(id)
				fmt.Println("Add peer", newPeer)
				_, err := s.addPeerRequest(region, leader, newPeer)
				return false, errors.Trace(err)
			}
		}
	}

	return true, nil
}

func (s *scheduler) addPeerRequest(
	region *metapb.Region,
	leader *metapb.Peer,
	peer *metapb.Peer,
) (*raft_cmdpb.RaftCmdResponse, error) {
	return s.changePeerRequest(region, leader, peer, raftpb.ConfChangeType_AddNode)
}

func (s *scheduler) removePeerRequest(
	region *metapb.Region,
	leader *metapb.Peer,
	peer *metapb.Peer,
) (*raft_cmdpb.RaftCmdResponse, error) {
	return s.changePeerRequest(region, leader, peer, raftpb.ConfChangeType_RemoveNode)
}

func (s *scheduler) changePeerRequest(
	region *metapb.Region,
	leader *metapb.Peer,
	peer *metapb.Peer,
	kind raftpb.ConfChangeType,
) (*raft_cmdpb.RaftCmdResponse, error) {
	request := s.newRaftRequest(region, leader)
	request.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_ChangePeer.Enum(),
		ChangePeer: &raft_cmdpb.ChangePeerRequest{
			ChangeType: kind.Enum(),
			Peer:       peer,
		},
	}
	return s.sendRaftRequest(leader, request)
}

func (s *scheduler) transferLeaderRequest(
	region *metapb.Region,
	leader *metapb.Peer,
	newLeader *metapb.Peer,
) (*raft_cmdpb.RaftCmdResponse, error) {
	request := s.newRaftRequest(region, leader)
	request.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_TransferLeader.Enum(),
		TransferLeader: &raft_cmdpb.TransferLeaderRequest{
			Peer: newLeader,
		},
	}
	return s.sendRaftRequest(leader, request)
}

func (s *scheduler) newRequest(cmd pdpb.CommandType) *pdpb.Request {
	return &pdpb.Request{
		Header: &pdpb.RequestHeader{
			Uuid:      uuid.NewV4().Bytes(),
			ClusterId: s.clusterID,
		},
		CmdType: cmd,
	}
}

func (s *scheduler) sendRequest(req *pdpb.Request) *pdpb.Response {
	resp, err := rpcutil.Request(s.addr, 0, req)
	if err != nil {
		log.Fatal(err)
	}
	return resp
}

func (s *scheduler) newRaftRequest(region *metapb.Region, leader *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			Uuid:        uuid.NewV4().Bytes(),
			Peer:        leader,
			RegionId:    proto.Uint64(region.GetId()),
			RegionEpoch: region.GetRegionEpoch(),
		},
	}
}

func (s *scheduler) sendRaftRequest(leader *metapb.Peer, request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	store, err := s.client.GetStore(leader.GetStoreId())
	if err != nil {
		return nil, errors.Trace(err)
	}
	conn, err := net.Dial("tcp", store.GetAddress())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer conn.Close()

	req := &msgpb.Message{
		MsgType: msgpb.MessageType_Cmd,
		CmdReq:  request,
	}
	if err := util.WriteMessage(conn, 0, req); err != nil {
		return nil, errors.Trace(err)
	}

	resp := &msgpb.Message{}
	if _, err := util.ReadMessage(conn, resp); err != nil {
		return nil, errors.Trace(err)
	}
	return resp.GetCmdResp(), nil
}
