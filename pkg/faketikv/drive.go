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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/faketikv/cases"
	"github.com/pingcap/pd/pkg/faketikv/simutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pkg/errors"
)

// Driver promotes the cluster status change.
type Driver struct {
	pdAddr      string
	conf        *cases.Conf
	client      Client
	tickCount   int64
	eventRunner *EventRunner
	raftEngine  *RaftEngine
	conn        *Connection
}

// NewDriver returns a driver.
func NewDriver(pdAddr string, confName string) (*Driver, error) {
	conf := cases.NewConf(confName)
	if conf == nil {
		return nil, errors.Errorf("failed to create conf %s", confName)
	}
	return &Driver{
		pdAddr: pdAddr,
		conf:   conf,
	}, nil
}

// Prepare initializes cluster information, bootstraps cluster and starts nodes.
func (d *Driver) Prepare() error {
	conn, err := NewConnection(d.conf, d.pdAddr)
	if err != nil {
		return err
	}
	d.conn = conn

	d.raftEngine = NewRaftEngine(d.conf, d.conn)
	d.eventRunner = NewEventRunner(d.conf.Events, d.raftEngine)

	// Bootstrap.
	store, region, err := d.GetBootstrapInfo(d.raftEngine)
	if err != nil {
		return err
	}
	d.client = d.conn.Nodes[store.GetId()].client

	ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
	err = d.client.Bootstrap(ctx, store, region)
	cancel()
	if err != nil {
		simutil.Logger.Fatal("bootstrapped error: ", err)
	} else {
		simutil.Logger.Debug("Bootstrap success")
	}

	// Setup alloc id.
	for {
		var id uint64
		id, err = d.client.AllocID(context.Background())
		if err != nil {
			return errors.AddStack(err)
		}
		if id > d.conf.MaxID {
			break
		}
	}

	err = d.Start()
	if err != nil {
		return err
	}

	return nil
}

// Tick invokes nodes' Tick.
func (d *Driver) Tick() {
	d.tickCount++
	d.raftEngine.stepRegions()
	d.eventRunner.Tick(d.tickCount)
	for _, n := range d.conn.Nodes {
		n.reportRegionChange()
		n.Tick()
	}
}

// Check checks if the simulation is completed.
func (d *Driver) Check() bool {
	return d.conf.Checker(d.raftEngine.regionsInfo)
}

// PrintStatistics prints the statistics of the scheduler.
func (d *Driver) PrintStatistics() {
	d.raftEngine.schedulerStats.PrintStatistics()
}

// Start starts all nodes.
func (d *Driver) Start() error {
	for _, n := range d.conn.Nodes {
		err := n.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

// Stop stops all nodes.
func (d *Driver) Stop() {
	for _, n := range d.conn.Nodes {
		n.Stop()
	}
}

// TickCount returns the simulation's tick count.
func (d *Driver) TickCount() int64 {
	return d.tickCount
}

// GetBootstrapInfo returns a valid bootstrap store and region.
func (d *Driver) GetBootstrapInfo(r *RaftEngine) (*metapb.Store, *metapb.Region, error) {
	origin := r.RandRegion()
	if origin == nil {
		return nil, nil, errors.New("no region found for bootstrap")
	}
	region := origin.Clone(
		core.WithStartKey([]byte("")),
		core.WithEndKey([]byte("")),
		core.SetRegionConfVer(1),
		core.SetRegionVersion(1),
		core.SetPeers([]*metapb.Peer{origin.GetLeader()}),
	)
	if region.GetLeader() == nil {
		return nil, nil, errors.New("bootstrap region has no leader")
	}
	store := d.conn.Nodes[region.GetLeader().GetStoreId()]
	if store == nil {
		return nil, nil, errors.Errorf("bootstrap store %v not found", region.GetLeader().GetStoreId())
	}
	return store.Store, region.GetMeta(), nil
}
