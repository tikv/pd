// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package simulator

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/cases"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

// Event affects the status of the cluster.
type Event interface {
	Run(raft *RaftEngine, tickCount int64) bool
}

// EventRunner includes all events.
type EventRunner struct {
	sync.RWMutex
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

type eventHandler struct {
	er *EventRunner
}

func newEventHandler(er *EventRunner) *eventHandler {
	return &eventHandler{
		er: er,
	}
}

func (e *eventHandler) createEvent(w http.ResponseWriter, r *http.Request) {
	event := r.URL.Query().Get("event")
	if len(event) < 1 {
		fmt.Fprintf(w, "no given event")
		return
	}
	switch event {
	case "add-node":
		e.er.addEvent(&AddNode{})
		return
	case "down-node":
		id := r.URL.Query().Get("node-id")
		var ID int
		if len(id) != 0 {
			ID, _ = strconv.Atoi(id)
		}
		e.er.addEvent(&DownNode{ID: ID})
		return
	default:
	}
}

func parserEvent(e cases.EventDescriptor) Event {
	switch t := e.(type) {
	case *cases.WriteFlowOnSpotDescriptor:
		return &WriteFlowOnSpot{descriptor: t}
	case *cases.WriteFlowOnRegionDescriptor:
		return &WriteFlowOnRegion{descriptor: t}
	case *cases.ReadFlowOnRegionDescriptor:
		return &ReadFlowOnRegion{descriptor: t}
	}
	return nil
}

func (er *EventRunner) addEvent(e Event) {
	er.Lock()
	defer er.Unlock()
	er.events = append(er.events, e)
}

// Tick ticks the event run
func (er *EventRunner) Tick(tickCount int64) {
	er.Lock()
	defer er.Unlock()
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
		region := raft.GetRegionByKey([]byte(key))
		simutil.Logger.Debug("search the region", zap.Reflect("region", region.GetMeta()))
		if region == nil {
			simutil.Logger.Error("region not found for key", zap.String("key", key), zap.Any("byte(key)", []byte(key)))
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

// AddNode adds nodes.
type AddNode struct{}

// Run implements the event interface.
func (*AddNode) Run(raft *RaftEngine, _ int64) bool {
	config := raft.storeConfig
	nodes := raft.conn.getNodes()
	id, err := nodes[0].client.allocID(context.TODO())
	if err != nil {
		simutil.Logger.Error("alloc node id failed", zap.Error(err))
		return false
	}
	s := &cases.Store{
		ID:       id,
		Status:   metapb.StoreState_Up,
		Capacity: uint64(config.RaftStore.Capacity),
		Version:  config.StoreVersion,
	}
	n, err := NewNode(s, config)
	if err != nil {
		simutil.Logger.Error("create node failed", zap.Error(err))
		return false
	}

	raft.conn.Nodes[s.ID] = n
	n.raftEngine = raft
	n.client = newRetryClient(n)

	err = n.Start()
	if err != nil {
		delete(raft.conn.Nodes, s.ID)
		simutil.Logger.Error("start node failed", zap.Uint64("node-id", s.ID), zap.Error(err))
		return false
	}
	return true
}

// DownNode deletes nodes.
type DownNode struct {
	ID int
}

// Run implements the event interface.
func (e *DownNode) Run(raft *RaftEngine, _ int64) bool {
	nodes := raft.conn.Nodes
	if len(nodes) == 0 {
		simutil.Logger.Error("can not find any node")
		return false
	}
	var node *Node
	if e.ID == 0 {
		arrNodes := raft.conn.getNodes()
		i := rand.Intn(len(arrNodes))
		node = nodes[arrNodes[i].Store.GetId()]
	} else {
		node = nodes[uint64(e.ID)]
	}
	if node == nil {
		simutil.Logger.Error("node is not existed")
		return true
	}
	delete(raft.conn.Nodes, node.Id)
	// delete store
	err := PDHTTPClient.DeleteStore(context.Background(), node.Id)
	if err != nil {
		simutil.Logger.Error("put store failed", zap.Uint64("node-id", node.Id), zap.Error(err))
		return false
	}
	node.Stop()

	regions := raft.GetRegions()
	for _, region := range regions {
		storeIDs := region.GetStoreIDs()
		if _, ok := storeIDs[node.Id]; ok {
			downPeer := &pdpb.PeerStats{
				Peer:        region.GetStorePeer(node.Id),
				DownSeconds: 24 * 60 * 60,
			}
			region = region.Clone(core.WithDownPeers(append(region.GetDownPeers(), downPeer)))
			raft.SetRegion(region)
		}
	}
	return true
}
