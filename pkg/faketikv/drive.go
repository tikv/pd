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

	"github.com/juju/errors"
	"github.com/pingcap/pd/pkg/faketikv/cases"
	log "github.com/sirupsen/logrus"
)

// Driver promotes the cluster status change.
type Driver struct {
	addr        string
	confName    string
	conf        *cases.Conf
	clusterInfo *ClusterInfo
	client      Client
	tickCount   int64
}

// NewDriver returns a driver.
func NewDriver(addr string, confName string) *Driver {
	return &Driver{
		addr:     addr,
		confName: confName,
	}
}

// Prepare initializes cluster information, bootstraps cluster and starts nodes.
func (d *Driver) Prepare() error {
	d.conf = cases.NewConf(d.confName)
	if d.conf == nil {
		return errors.Errorf("failed to create conf %s", d.confName)
	}

	clusterInfo, err := NewClusterInfo(d.addr, d.conf)
	if err != nil {
		return errors.Trace(err)
	}
	d.clusterInfo = clusterInfo

	// Bootstrap.
	store, region, err := clusterInfo.GetBootstrapInfo()
	if err != nil {
		return errors.Trace(err)
	}
	d.client = clusterInfo.Nodes[store.GetId()].client

	ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
	err = d.client.Bootstrap(ctx, store, region)
	cancel()
	if err != nil {
		log.Fatal("bootstrapped error: ", err)
	} else {
		log.Info("Bootstrap sucess")
	}

	// Setup alloc id.
	for {
		id, err := d.client.AllocID(context.Background())
		if err != nil {
			return errors.Trace(err)
		}
		if id > d.conf.MaxID {
			break
		}
	}

	for _, n := range d.clusterInfo.Nodes {
		err := n.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

// Tick invokes nodes' Tick.
func (d *Driver) Tick() {
	d.tickCount++
	for _, n := range d.clusterInfo.Nodes {
		n.Tick()
	}
}

// Check checks if the simulation is completed.
func (d *Driver) Check() bool {
	return d.conf.Checker(d.clusterInfo.RegionsInfo)
}

// Stop stops all nodes.
func (d *Driver) Stop() {
	for _, n := range d.clusterInfo.Nodes {
		n.Stop()
	}
}

// TickCount returns the simulation's tick count.
func (d *Driver) TickCount() int64 {
	return d.tickCount
}

// AddNode adds new node.
func (d *Driver) AddNode() {
	id, err := d.client.AllocID(context.Background())
	n, err := NewNode(id, fmt.Sprintf("mock://tikv-%d", id), d.addr)
	if err != nil {
		log.Info("Add node failed:", err)
		return
	}
	err = n.Start()
	if err != nil {
		log.Info("Start node failed:", err)
		return
	}
	n.clusterInfo = d.clusterInfo
	d.clusterInfo.Nodes[n.Id] = n
}

// DeleteNode deletes a node.
func (d *Driver) DeleteNode() {}
