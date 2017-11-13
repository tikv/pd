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
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type Driver struct {
	clusterInfo             *ClusterInfo
	client                  Client
	reciveRegionHeartbeatCh <-chan *pdpb.RegionHeartbeatResponse
}

func NewDriver(addrs []string) *Driver {
	client, reciveRegionHeartbeatCh, err := NewClient(addrs)
	if err != nil {
		log.Fatal(err)
	}
	return &Driver{client: client, reciveRegionHeartbeatCh: reciveRegionHeartbeatCh}
}

func (c *Driver) Prepare() error {
	initCase := NewTiltCase()
	clusterInfo := initCase.Init(c.client, "./case1.toml")
	c.clusterInfo = clusterInfo
	store, region := clusterInfo.GetBootstrapInfo()

	ctx, cancel := context.WithTimeout(context.Background(), updateLeaderTimeout)
	err := c.client.Bootstrap(ctx, store, region)
	cancel()
	if err != nil {
		return err
	}
	for _, n := range clusterInfo.Nodes {
		err = n.Prepare()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Driver) reciveRegionHeartbeat(ctx context.Context) {
	for {
		select {
		case resp := <-c.reciveRegionHeartbeatCh:
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
	if err != nil {
		log.Info("Add node error:", err)
	}
	n := NewNode(id, fmt.Sprintf("mock://tikv-%d", id), c.client)
	c.clusterInfo.Nodes[n.Id] = n
}

func (c *Driver) DeleteNode() {

}
