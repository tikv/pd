// Copyright 2017 TiKV Project Authors.
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
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/tools/pd-simulator/simulator/cases"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

const (
	storeHeartBeatPeriod  = 10
	compactionDelayPeriod = 600
)

// Node simulates a TiKV.
type Node struct {
	*metapb.Store
	stats struct {
		sync.RWMutex
		*info.StoreStats
	}
	tick uint64
	wg   sync.WaitGroup

	tasks struct {
		sync.RWMutex
		tasks map[uint64]*Task
	}
	client                   Client
	receiveRegionHeartbeatCh <-chan *pdpb.RegionHeartbeatResponse
	ctx                      context.Context
	cancel                   context.CancelFunc
	raftEngine               *RaftEngine
	limiter                  *ratelimit.RateLimiter
	hasExtraUsedSpace        bool
}

// NewNode returns a Node.
func NewNode(s *cases.Store, pdAddr string, config *SimConfig) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &metapb.Store{
		Id:      s.ID,
		Address: fmt.Sprintf("mock:://tikv-%d", s.ID),
		Version: config.StoreVersion,
		Labels:  s.Labels,
		State:   s.Status,
	}
	stats := &info.StoreStats{
		StoreStats: pdpb.StoreStats{
			StoreId:   s.ID,
			Capacity:  uint64(config.RaftStore.Capacity),
			StartTime: uint32(time.Now().Unix()),
		},
	}
	tag := fmt.Sprintf("store %d", s.ID)
	var (
		client                   Client
		receiveRegionHeartbeatCh <-chan *pdpb.RegionHeartbeatResponse
		err                      error
	)

	// Client should wait if PD server is not ready.
	for i := 0; i < maxInitClusterRetries; i++ {
		client, receiveRegionHeartbeatCh, err = NewClient(pdAddr, tag)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	if err != nil {
		cancel()
		return nil, err
	}
	ratio := int64(time.Second) / config.SimTickInterval.Milliseconds()
	speed := config.StoreIOMBPerSecond * units.MiB * ratio
	return &Node{
		Store: store,
		stats: struct {
			sync.RWMutex
			*info.StoreStats
		}{StoreStats: stats},
		client: client,
		ctx:    ctx,
		cancel: cancel,
		tasks: struct {
			sync.RWMutex
			tasks map[uint64]*Task
		}{tasks: make(map[uint64]*Task)},
		receiveRegionHeartbeatCh: receiveRegionHeartbeatCh,
		limiter:                  ratelimit.NewRateLimiter(float64(speed), int(speed)),
		tick:                     uint64(rand.Intn(storeHeartBeatPeriod)),
		hasExtraUsedSpace:        s.HasExtraUsedSpace,
	}, nil
}

// Start starts the node.
func (n *Node) Start() error {
	ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
	err := n.client.PutStore(ctx, n.Store)
	cancel()
	if err != nil {
		return err
	}
	n.wg.Add(1)
	go n.receiveRegionHeartbeat()
	n.Store.State = metapb.StoreState_Up
	return nil
}

func (n *Node) receiveRegionHeartbeat() {
	defer n.wg.Done()
	for {
		select {
		case resp := <-n.receiveRegionHeartbeatCh:
			task := responseToTask(n.raftEngine, resp)
			if task != nil {
				n.AddTask(task)
			}
		case <-n.ctx.Done():
			return
		}
	}
}

// Tick steps node status change.
func (n *Node) Tick(wg *sync.WaitGroup) {
	defer wg.Done()
	if n.GetNodeState() != metapb.NodeState_Preparing && n.GetNodeState() != metapb.NodeState_Serving {
		return
	}
	n.tick++
	n.reportRegionChange()
	n.stepStoreHeartbeat()
	n.stepRegionHeartbeat()
	n.stepCompaction()
	n.stepTask()
}

// GetState returns current node state.
func (n *Node) GetState() metapb.StoreState {
	return n.Store.State
}

func (n *Node) stepTask() {
	n.tasks.Lock()
	defer n.tasks.Unlock()
	for _, task := range n.tasks.tasks {
		if isFinished := task.Step(n.raftEngine); isFinished {
			simutil.Logger.Debug("task status",
				zap.Uint64("node-id", n.Id),
				zap.Uint64("region-id", task.RegionID()),
				zap.String("task", task.Desc()))
			delete(n.tasks.tasks, task.RegionID())
		}
	}
}

func (n *Node) stepRegionHeartbeat() {
	config := n.raftEngine.storeConfig

	period := uint64(config.RaftStore.RegionHeartBeatInterval.Duration / config.SimTickInterval.Duration)
	if n.tick%period == 0 {
		n.regionHeartBeat()
	}
}

func (n *Node) stepStoreHeartbeat() {
	config := n.raftEngine.storeConfig

	period := uint64(config.RaftStore.StoreHeartBeatInterval.Duration / config.SimTickInterval.Duration)
	if n.tick%period == 0 {
		n.storeHeartBeat()
	}
}

func (n *Node) stepCompaction() {
	if n.tick%compactionDelayPeriod == 0 {
		n.compaction()
	}
}

func (n *Node) storeHeartBeat() {
	if n.GetNodeState() != metapb.NodeState_Preparing && n.GetNodeState() != metapb.NodeState_Serving {
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, pdTimeout)
	err := n.client.StoreHeartbeat(ctx, &n.stats.StoreStats.StoreStats)
	if err != nil {
		simutil.Logger.Info("report heartbeat error",
			zap.Uint64("node-id", n.GetId()),
			zap.Error(err))
	}
	cancel()
}

func (n *Node) compaction() {
	n.stats.Lock()
	defer n.stats.Unlock()
	n.stats.Available += n.stats.ToCompactionSize
	n.stats.UsedSize -= n.stats.ToCompactionSize
	n.stats.ToCompactionSize = 0
}

func (n *Node) regionHeartBeat() {
	if n.GetNodeState() != metapb.NodeState_Preparing && n.GetNodeState() != metapb.NodeState_Serving {
		return
	}
	regions := n.raftEngine.cachedRegions
	for _, region := range regions {
		if region.GetLeader() != nil && region.GetLeader().GetStoreId() == n.Id {
			err := n.client.RegionHeartbeat(region)
			if err != nil {
				simutil.Logger.Info("report heartbeat error",
					zap.Uint64("node-id", n.Id),
					zap.Uint64("region-id", region.GetID()),
					zap.Error(err))
			}
		}
	}
}

func (n *Node) reportRegionChange() {
	regions := n.raftEngine.GetRegionChange(n.Id)
	for _, region := range regions {
		err := n.client.RegionHeartbeat(region)
		if err != nil {
			simutil.Logger.Info("report heartbeat error",
				zap.Uint64("node-id", n.Id),
				zap.Uint64("region-id", region.GetID()),
				zap.Error(err))
		}
		n.raftEngine.ResetRegionChange(n.Id, region.GetID())
	}
}

// AddTask adds task in this node.
func (n *Node) AddTask(task *Task) {
	n.tasks.Lock()
	defer n.tasks.Unlock()
	if t, ok := n.tasks.tasks[task.RegionID()]; ok {
		simutil.Logger.Debug("task has already existed",
			zap.Uint64("node-id", n.Id),
			zap.Uint64("region-id", task.RegionID()),
			zap.String("task", t.Desc()))
		return
	}
	n.tasks.tasks[task.RegionID()] = task
}

// Stop stops this node.
func (n *Node) Stop() {
	n.cancel()
	n.client.Close()
	n.wg.Wait()
	simutil.Logger.Info("node stopped", zap.Uint64("node-id", n.Id))
}

func (n *Node) incUsedSize(size uint64) {
	n.stats.Lock()
	defer n.stats.Unlock()
	n.stats.Available -= size
	n.stats.UsedSize += size
}

func (n *Node) decUsedSize(size uint64) {
	n.stats.Lock()
	defer n.stats.Unlock()
	n.stats.ToCompactionSize += size
}
