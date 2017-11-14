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
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type Node struct {
	*metapb.Store
	stats                   *pdpb.StoreStats
	tick                    uint64
	storeTick               int
	regionTick              int
	wg                      sync.WaitGroup
	tasks                   []Task
	client                  Client
	reciveRegionHeartbeatCh <-chan *pdpb.RegionHeartbeatResponse
	ctx                     context.Context
	cancel                  context.CancelFunc
	isBlock                 bool
	// share cluster information
	clusterInfo *ClusterInfo
}

func NewNode(id uint64, addr string, pdAddr string) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())
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
	client, reciveRegionHeartbeatCh, err := NewClient(pdAddr)
	if err != nil {
		return nil, err
	}
	return &Node{Store: store, stats: stats, client: client, reciveRegionHeartbeatCh: reciveRegionHeartbeatCh, ctx: ctx, cancel: cancel, isBlock: true}, nil
}

func (n *Node) Start() error {
	ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
	err := n.client.PutStore(ctx, n.Store)
	cancel()
	if err != nil {
		return err
	}
	n.wg.Add(1)
	go n.reciveRegionHeartbeat()
	n.isBlock = false
	return nil
}

func (n *Node) reciveRegionHeartbeat() {
	defer n.wg.Done()
	for {
		select {
		case resp := <-n.reciveRegionHeartbeatCh:
			log.Infof("[node %d]Debug: recive %+v", n.Id, resp)
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) Tick() {
	if n.isBlock {
		return
	}
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
	n.storeTick = (n.storeTick + 1) % 10
	n.regionTick = (n.regionTick + 1) % 60
	if n.storeTick == 0 {
		n.storeHeartBeat()
	}
	if n.regionTick == 0 {
		n.regionHeartBeat()
	}
}

func (n *Node) storeHeartBeat() {
	ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
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
			ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
			err := n.client.RegionHeartbeat(ctx, region)
			if err != nil {
				log.Infof("[region %d] report heartbeat error: %s", region.GetId(), err)
			}
			cancel()
		}
	}
}

func (n *Node) AddTask(task Task) {}

func (n *Node) Stop() {
	n.cancel()
	n.client.Close()
	n.wg.Wait()
	log.Info("node %d stoped", n.Id)
}
