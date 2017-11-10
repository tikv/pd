package faketikv

import (
	"context"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type Node struct {
	*metapb.Store
	stats      *pdpb.StoreStats
	tick       uint64
	storeTick  int
	regionTick int
	tasks      []Task
	client     Client
	// share cluster info
	clusterInfo *ClusterInfo
}

func NewNode(id uint64, addr string, client Client) (*Node, error) {
	store := &metapb.Store{
		Id:      id,
		Address: addr,
	}
	ctx, cancel := context.WithTimeout(context.Background(), updateLeaderTimeout)
	err := client.PutStore(ctx, store)
	cancel()
	if err != nil {
		return nil, err
	}
	return &Node{Store: store, stats: &pdpb.StoreStats{}, client: client}, nil
}

func (n *Node) Tick() {
	if n.storeTick == 0 {
		n.storeHeartBeat()
	}
	if n.regionTick == 0 {
		n.regionHeartBeat()
	}

	n.storeTick = (n.storeTick + 1) % 10
	n.regionTick = (n.regionTick + 1) % 60
	n.processTask()
	n.tick++
}

func (n *Node) storeHeartBeat() {
	n.client.StoreHeartbeat(context.Background(), n.stats)
}

func (n *Node) regionHeartBeat() {
	regions := n.clusterInfo.GetStoreLeaderRegions(n.GetId())
	for _, region := range regions {
		n.client.RegionHeartbeat(context.Background(), region)
	}
}

func (n *Node) AddTask(task Task) {}
func (n *Node) processTask() {
	for _, task := range n.tasks {
		task.Step()
	}
}
