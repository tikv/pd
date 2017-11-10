package faketikv

import (
	"context"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type Driver struct {
	clusterInfo             *ClusterInfo
	client                  Client
	reciveRegionHeartbeatCh chan *pdpb.RegionHeartbeatResponse
}

func NewDriver(addrs []string) *Driver {
	client, reciveRegionHeartbeatCh, err := NewClient(addrs)
	if err != nil {
		log.Fatal(err)
	}
	return &Driver{client: client}
}

func (c *Driver) Start() {
	initCase := NewTiltCase()
	clusterInfo := initCase.Init(c.client, "./case1.toml")
	c.clusterInfo = clusterInfo
	store, region := clusterInfo.GetBootstrapInfo()

	ctx, cancel := context.WithTimeout(context.Background(), updateLeaderTimeout)
	err := c.client.Bootstrap(ctx, store, region)
	cancel()
	if err != nil {
		panic("bootstrapped error")
	}
}

func (c *Driver) reciveRegionHeartbeat(ctx context.Context) {
	for {
		select {
		case resp := c.reciveRegionHeartbeat():
			c.dispatch(resp)
		case <-ctx.Done():
		}
	}
}

// dispatch add the task in specify node
func (c *Driver) dispatch(resp *pdpb.RegionHeartbeatResponse) {}

func (c *Driver) Tick() {
	for _, n := range c.clusterInfo.Nodes {
		n.Tick()
	}
}

func (c *Driver) AddNode() {
	id, err := c.client.AllocID(context.Background())
	n, err := NewNode(id, fmt.Sprintf("mock://tikv-%d", id), c.client)
	if err != nil {
		log.Info("Add Node Failed")
	}
	c.clusterInfo.Nodes[n.Id] = n
}

func (c *Driver) DeleteNode() {

}
