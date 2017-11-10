package faketikv

import (
	"context"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

type Alloc interface {
	AllocID() (uint64, error)
}

type localAlloc struct {
	id uint64
}

func (l *localAlloc) AllocID() (uint64, error) {
	l.id++
	return l.id, nil
}

type ClusterInfo struct {
	*core.RegionsInfo
	Nodes       map[uint64]*Node
	firstRegion *core.RegionInfo
}

func (c *ClusterInfo) GetBootstrapInfo() (*metapb.Store, *metapb.Region) {
	storeID := c.firstRegion.Leader.GetStoreId()
	store := c.Nodes[storeID]
	return store.Store, c.firstRegion.Region
}

type Initializer interface {
	Init(args ...string) *ClusterInfo
}

type TiltCase struct {
	NodeNumber   int `toml:"node-number" json:"node-number"`
	RegionNumber int `toml:"region-number" json:"region-number"`
	alloc        *localAlloc
}

func NewTiltCase() *TiltCase {
	return &TiltCase{alloc: &localAlloc{}}
}

func (c *TiltCase) Init(client Client, args ...string) *ClusterInfo {
	path := args[0]
	err := c.Parser(path)
	if err != nil {
		panic("initalize failed: parser config")
	}
	nodes := make(map[uint64]*Node)
	regions := core.NewRegionsInfo()
	var ids []uint64
	for i := 0; i < c.NodeNumber; i++ {
		id, err1 := c.alloc.AllocID()
		if err1 != nil {
			panic("alloc failed")
		}
		nodes[id], err1 = NewNode(id, fmt.Sprintf("mock://tikv-%d", id), client)
		if err1 != nil {
			panic("new node failed")
		}
		if len(ids) < 3 {
			ids = append(ids, id)
		}
	}
	var firstRegion *core.RegionInfo
	for i := 0; i < c.RegionNumber; i++ {
		start := i * 1000
		region := c.genRegion(ids, start)
		regions.SetRegion(region)
		if i == 0 {
			firstRegion = region
		}
	}
	// TODO: remove this
	for i := 0; i < c.NodeNumber+c.RegionNumber+10; i++ {
		_, err = client.AllocID(context.Background())
		if err != nil {
			panic("initalize failed: alloc ID")
		}
	}

	cluster := &ClusterInfo{
		regions,
		nodes,
		firstRegion,
	}
	for _, n := range nodes {
		n.clusterInfo = cluster
	}
	return cluster
}

func (c *TiltCase) Parser(path string) error {
	_, err := toml.DecodeFile(path, c)
	return err
}

func (c *TiltCase) genRegion(ids []uint64, start int) *core.RegionInfo {
	if len(ids) == 0 {
		return nil
	}
	regionID, _ := c.alloc.AllocID()
	peers := make([]*metapb.Peer, 0, len(ids))
	for _, storeID := range ids {
		id, err := c.alloc.AllocID()
		if err != nil {
			panic("alloc failed")
		}
		peer := &metapb.Peer{
			Id:      id,
			StoreId: storeID,
		}
		peers = append(peers, peer)
	}
	region := &metapb.Region{
		Id:       regionID,
		StartKey: []byte(fmt.Sprintf("zt_%d", start)),
		EndKey:   []byte(fmt.Sprintf("zt_%d", start+1000)),
		Peers:    peers,
	}
	return core.NewRegionInfo(region, peers[0])
}
