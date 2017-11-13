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

package faketikv

import (
	"context"
	"time"

	log "github.com/Sirupsen/logrus"
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
	// share cluster information
	clusterInfo *ClusterInfo
}

func NewNode(id uint64, addr string, client Client) *Node {
	store := &metapb.Store{
		Id:      id,
		Address: addr,
	}
	stats := &pdpb.StoreStats{
		StoreId:   id,
		Capacity:  1000000000000,
		Available: 1000000000000,
		StartTime: uint32(time.Now().Second()),
	}
	return &Node{Store: store, stats: stats, client: client}
}

func (n *Node) Prepare() error {
	ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
	err := n.client.PutStore(ctx, n.Store)
	cancel()
	return err
}

func (n *Node) Tick() {
	n.processHeartBeat()
	n.processTask()
	n.clusterInfo.Step()
	n.tick++
}

func (n *Node) processTask() {
	for _, task := range n.tasks {
		task.Step(n.clusterInfo)
	}
}

func (n *Node) processHeartBeat() {
	if n.storeTick == 0 {
		n.storeHeartBeat()
	}
	if n.regionTick == 0 {
		n.regionHeartBeat()
	}
	n.storeTick = (n.storeTick + 1) % 10
	n.regionTick = (n.regionTick + 1) % 60
}

func (n *Node) storeHeartBeat() {
	ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
	err := n.client.StoreHeartbeat(ctx, n.stats)
	if err != nil {
		log.Infof("[store %d] report heartbeat error: %s", n.GetId(), err)
	}
	cancel()
}

func (n *Node) regionHeartBeat() {
	regions := n.clusterInfo.GetRegions()
	for _, region := range regions {
		if region.Leader.GetStoreId() == n.Id {
			ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
			err := n.client.RegionHeartbeat(ctx, region)
			if err != nil {
				log.Infof("[region %d] report heartbeat error: %s", region.GetId(), err)
			}
			cancel()
		}
	}
	log.Infoln()
	log.Infoln()
}

func (n *Node) AddTask(task Task) {}
