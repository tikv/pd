// Copyright 2022 TiKV Project Authors.
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

package statistics

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/core"
)

// PeerScoreStats contains the score info of regions
type PeerScoreStats struct {
	ctx          context.Context
	mu struct {
		sync.Mutex
		// store_id --> CPU percentage
		storeCPUs   map[uint64]float64
	}
	regionScores map[uint64]*RegionScore
	taskQueue    chan *RegionScoreTask
}

// NewPeerScoreStats build an empty PeerScoreStats
func NewPeerScoreStats(ctx context.Context) *PeerScoreStats {
	s := &PeerScoreStats{
		ctx:          ctx,
		mu: struct {
			sync.Mutex
			storeCPUs map[uint64]float64
		}{storeCPUs: make(map[uint64]float64)},
		regionScores: make(map[uint64]*RegionScore),
		taskQueue:    make(chan *RegionScoreTask, 1024),
	}
	go s.handleTasks(s.taskQueue)
	return s
}

// UpdateRegionScoreAsync add an update region score task to task chan.
func (w *PeerScoreStats) UpdateRegionScoreAsync(task *RegionScoreTask) {
	select {
	case w.taskQueue <- task:
	case <-w.ctx.Done():
	}
}

// GetRegionScore return the region score of target regions
func (w *PeerScoreStats) GetRegionScore(regionIds []uint64) []*RegionScore {
	respChan := make(chan GetRegionScoreResp, 1)
	task := NewGetRegionScoreTask(regionIds, respChan)
	select {
	case w.taskQueue <- task:
		res := <-respChan
		return res.regions
	default:
		// return empty result when it's too busy.
		return nil
	}
}

// ShouldUpdateForStore check if region scores adjust can be skipped. This can avoid too much traverse
// of the region tree in the common case when the storage CPU usage is not high.
func (w *PeerScoreStats) ShouldUpdateForStore(storeId uint64, cpuPercent float64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	lastCpu := w.mu.storeCPUs[storeId]
	return lastCpu <= storeCPUPercentWaterMarkLow && cpuPercent <= storeCPUPercentWaterMarkLow
}

const (
	// the low watermark of store CPU usage, should reset region score under this.
	storeCPUPercentWaterMarkLow float64 = 0.5
	// the high watermark of store CPU usage, should increase replica score
	// if cpu percent exceed this threshold.
	storeCPUPercentWaterMarkHigh float64 = 0.7
	// the period of time that replica score is valid after updated.
	replicaScoreExpireDuration time.Duration = 30 * time.Second
)


func (w *PeerScoreStats) handleTasks(queue <-chan *RegionScoreTask) {
	for {
		select {
		case <-w.ctx.Done():
		consumeLoop:
			for {
				select {
				case task := <-queue:
					if task.taskType == getTaskType {
						task.data.(getRegionScoreTask).respChan <- GetRegionScoreResp{}
					}
				default:
					break consumeLoop
				}
			}
		case task := <-queue:
			switch task.taskType {
			case updateTaskType:
				w.updateRegionScore(task.data.(updateRegionScoreTask))
			case getTaskType:
				w.batchGetRegionScores(task.data.(getRegionScoreTask))
			}
		}
	}
}

func (w *PeerScoreStats) updateRegionScore(task updateRegionScoreTask) {
	storeCpus := make(map[uint64]float64)
	w.mu.Lock()
	w.mu.storeCPUs[task.storeID] = task.storeCpuPercent
	for id, cpu := range w.mu.storeCPUs {
		storeCpus[id] = cpu
	}
	w.mu.Unlock()
	now := time.Now()
	for _, r := range task.regions {
		var peerID uint64
		meta := r.GetMeta()
		for _, p := range meta.Peers {
			if p.StoreId == task.storeID {
				peerID = p.Id
				break
			}
		}
		if peerID == 0 {
			continue
		}
		if r.GetLeader() == nil {
			continue
		}
		regionScore := w.regionScores[r.GetID()]
		if regionScore == nil {
			regionScore = &RegionScore{
				Peers:    make(map[uint64]*PeerScore),
				RegionId: r.GetID(),
			}
		}
		regionScore.Epoch = r.GetMeta().RegionEpoch

		leaderStoreCPU, ok := storeCpus[r.GetLeader().StoreId]
		if !ok {
			continue
		}
		peerScore := regionScore.Peers[task.storeID]
		if peerScore == nil {
			peerScore = &PeerScore{
				PeerID:  peerID,
				StoreID: task.storeID,
			}
		}

		newScore := peerScore.Score
		if task.storeCpuPercent <= storeCPUPercentWaterMarkLow {
			newScore = 0
		} else if task.storeCpuPercent < storeCPUPercentWaterMarkHigh {
			// try to decrease the peer scores a bit in favor of closest read
			delta := int((leaderStoreCPU - task.storeCpuPercent) * 5)
			if delta < 1 {
				delta = 1
			}
			newScore -= delta
		} else if task.storeCpuPercent <= leaderStoreCPU*0.9 {
			delta := int((leaderStoreCPU - task.storeCpuPercent) * 10)
			newScore -= delta
		} else if task.storeCpuPercent >= leaderStoreCPU*1.1 {
			delta := int((task.storeCpuPercent - leaderStoreCPU) * 25)
			newScore += delta
		}
		if newScore > 100 {
			newScore = 100
		} else if newScore < 0 {
			newScore = 0
		}

		peerScore.Score = newScore
		peerScore.UpdatedAt = now
		regionScore.Peers[task.storeID] = peerScore
		w.regionScores[r.GetID()] = regionScore
	}
}

func (w *PeerScoreStats) batchGetRegionScores(task getRegionScoreTask) {
	now := time.Now()
	respRegions := make([]*RegionScore, 0, len(task.regionIDs))
	for _, id := range task.regionIDs {
		if regionScore, ok := w.regionScores[id]; ok {
			respRegions = append(respRegions, regionScore.filter(now))
		}
	}
	task.respChan <- GetRegionScoreResp{regions: respRegions}
}

// RegionScore contains the score for all peers of this region.
type RegionScore struct {
	// last updated region epoch
	Epoch    *metapb.RegionEpoch
	// store_id --> peer score
	// NOTE: it may not contains all peers, missing peer mean the score is 0.
	Peers    map[uint64]*PeerScore
	RegionId uint64
}

// filter outdated replica scores.
func (r *RegionScore) filter(ts time.Time) *RegionScore {
	peers := make(map[uint64]*PeerScore, len(r.Peers))
	for storeID, p := range r.Peers {
		if ts.Sub(p.UpdatedAt) > replicaScoreExpireDuration {
			continue
		}
		cloned := *p
		peers[storeID] = &cloned
	}
	return &RegionScore{
		Peers:    peers,
		Epoch:    r.Epoch,
		RegionId: r.RegionId,
	}
}

// PeerScore represent the score of a single region replica.
type PeerScore struct {
	PeerID    uint64
	StoreID   uint64
	// Score is a num between [0, 100], represent the percentage that a read request
	// should be dispatch to region lead instead of the closest peer.
	Score     int
	UpdatedAt time.Time
}

type regionScoreTaskKind uint32

const (
	updateTaskType regionScoreTaskKind = iota
	getTaskType
)

type RegionScoreTask struct {
	taskType regionScoreTaskKind
	data     interface{}
}

func NewUpdateRegionScoreTask(storeID uint64, storeLoad float64, regions []*core.RegionInfo) *RegionScoreTask {
	return &RegionScoreTask{
		taskType: updateTaskType,
		data: updateRegionScoreTask{
			storeID:         storeID,
			storeCpuPercent: storeLoad,
			regions:         regions,
		},
	}
}

func NewGetRegionScoreTask(regionIDs []uint64, respCh chan GetRegionScoreResp) *RegionScoreTask {
	return &RegionScoreTask{
		taskType: getTaskType,
		data: getRegionScoreTask{
			regionIDs: regionIDs,
			respChan:  respCh,
		},
	}
}

type updateRegionScoreTask struct {
	storeID         uint64
	storeCpuPercent float64
	regions         []*core.RegionInfo
}

type getRegionScoreTask struct {
	regionIDs []uint64
	respChan  chan GetRegionScoreResp
}

type GetRegionScoreResp struct {
	regions []*RegionScore
}
