// Copyright 2018 PingCAP, Inc.
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

package simulator

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/cases"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

// Event affects the status of the cluster.
type Event interface {
	Run(raft *RaftEngine, tickCount int64) bool
}

// EventRunner includes all events.
type EventRunner struct {
	events     []Event
	raftEngine *RaftEngine
}

// NewEventRunner creates an event runner.
func NewEventRunner(events []cases.EventDescriptor, raftEngine *RaftEngine) *EventRunner {
	er := &EventRunner{events: make([]Event, 0, len(events)), raftEngine: raftEngine}
	for _, e := range events {
		event := parserEvent(e)
		if event != nil {
			er.events = append(er.events, event)
		}
	}
	return er
}

func parserEvent(e cases.EventDescriptor) Event {
	switch t := e.(type) {
	case *cases.WriteFlowOnSpotDescriptor:
		return &WriteFlowOnSpot{descriptor: t}
	case *cases.WriteFlowOnRegionDescriptor:
		return &WriteFlowOnRegion{descriptor: t}
	case *cases.ReadFlowOnRegionDescriptor:
		return &ReadFlowOnRegion{descriptor: t}
	case *cases.AddNodesDescriptor:
		return &AddNodes{descriptor: t}
	case *cases.DeleteNodesDescriptor:
		return &DeleteNodes{descriptor: t}
	}
	return nil
}

// Tick ticks the event run
func (er *EventRunner) Tick(tickCount int64) {
	var finishedIndex int
	for i, e := range er.events {
		isFinished := e.Run(er.raftEngine, tickCount)
		if isFinished {
			er.events[i], er.events[finishedIndex] = er.events[finishedIndex], er.events[i]
			finishedIndex++
		}
	}
	er.events = er.events[finishedIndex:]
}

// WriteFlowOnSpot writes bytes in some range.
type WriteFlowOnSpot struct {
	descriptor *cases.WriteFlowOnSpotDescriptor
}

// Run implements the event interface.
func (e *WriteFlowOnSpot) Run(raft *RaftEngine, tickCount int64) bool {
	res := e.descriptor.Step(tickCount)
	for key, size := range res {
		region := raft.SearchRegion([]byte(key))
		simutil.Logger.Debug("search the region", zap.Reflect("region", region.GetMeta()))
		if region == nil {
			simutil.Logger.Error("region not found for key", zap.String("key", key))
			continue
		}
		raft.updateRegionStore(region, size)
	}
	return false
}

// WriteFlowOnRegion writes bytes in some region.
type WriteFlowOnRegion struct {
	descriptor *cases.WriteFlowOnRegionDescriptor
}

// Run implements the event interface.
func (e *WriteFlowOnRegion) Run(raft *RaftEngine, tickCount int64) bool {
	res := e.descriptor.Step(tickCount)
	for id, bytes := range res {
		region := raft.GetRegion(id)
		if region == nil {
			simutil.Logger.Error("region is not found", zap.Uint64("region-id", id))
			continue
		}
		raft.updateRegionStore(region, bytes)
	}
	return false
}

// ReadFlowOnRegion reads bytes in some region
type ReadFlowOnRegion struct {
	descriptor *cases.ReadFlowOnRegionDescriptor
}

// Run implements the event interface.
func (e *ReadFlowOnRegion) Run(raft *RaftEngine, tickCount int64) bool {
	res := e.descriptor.Step(tickCount)
	raft.updateRegionReadBytes(res)
	return false
}

// AddNodes adds nodes.
type AddNodes struct {
	descriptor *cases.AddNodesDescriptor
}

// Run implements the event interface.
func (e *AddNodes) Run(raft *RaftEngine, tickCount int64) bool {
	id := e.descriptor.Step(tickCount)
	if id == 0 {
		return false
	}

	if _, ok := raft.conn.Nodes[id]; ok {
		simutil.Logger.Info("node has already existed", zap.Uint64("node-id", id))
		return false
	}

	config := raft.storeConfig
	s := &cases.Store{
		ID:        id,
		Status:    metapb.StoreState_Up,
		Capacity:  config.StoreCapacityGB * cases.GB,
		Available: config.StoreAvailableGB * cases.GB,
		Version:   config.StoreVersion,
	}
	n, err := NewNode(s, raft.conn.pdAddr, config.StoreIOMBPerSecond)
	if err != nil {
		simutil.Logger.Error("add node failed", zap.Uint64("node-id", id), zap.Error(err))
		return false
	}
	raft.conn.Nodes[id] = n
	n.raftEngine = raft
	err = n.Start()
	if err != nil {
		simutil.Logger.Error("start node failed", zap.Uint64("node-id", id), zap.Error(err))
	}
	return false
}

// DeleteNodes deletes nodes.
type DeleteNodes struct {
	descriptor *cases.DeleteNodesDescriptor
}

// Run implements the event interface.
func (e *DeleteNodes) Run(raft *RaftEngine, tickCount int64) bool {
	id := e.descriptor.Step(tickCount)
	if id == 0 {
		return false
	}

	node := raft.conn.Nodes[id]
	if node == nil {
		simutil.Logger.Error("node is not existed", zap.Uint64("node-id", id))
		return false
	}
	delete(raft.conn.Nodes, id)
	node.Stop()

	regions := raft.GetRegions()
	for _, region := range regions {
		storeIDs := region.GetStoreIds()
		if _, ok := storeIDs[id]; ok {
			downPeer := &pdpb.PeerStats{
				Peer:        region.GetStorePeer(id),
				DownSeconds: 24 * 60 * 60,
			}
			region = region.Clone(core.WithDownPeers(append(region.GetDownPeers(), downPeer)))
			raft.SetRegion(region)
		}
	}
	return false
}
