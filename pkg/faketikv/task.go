package faketikv

import (
	"github.com/pingcap/kvproto/pkg/metapb"
)

type Task interface {
	Step(cluster *ClusterInfo)
	IsFinished() bool
}

type addPeer struct {
	regionID uint64
	peerID   uint64
	storeID  uint64
	name     string
	size     uint64
	speed    uint64
}

func (a *addPeer) Step(cluster *ClusterInfo) {
	a.size -= a.speed
	if a.size < 0 {
		region := cluster.GetRegion(a.regionID)
		if region.GetPeer(a.peerID) == nil {
			peer := &metapb.Peer{
				Id:      a.peerID,
				StoreId: a.storeID,
			}
			region.Peers = append(region.Peers, peer)
			cluster.SetRegion(region)
		}
	}
}

func (a *addPeer) IsFinished() {
	return a.size < 0
}

type deletePeer struct {
	regionID uint64
	peerID   uint64
	name     string
	size     uint64
	speed    uint64
}

type transferLeader struct{}
