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
	"github.com/tikv/pd/server/core"
	"time"
)

type PeerScoreStats struct {
	ctx context.Context
	storeLoads map[uint64]float64
	regionScores map[uint64]*RegionScore
	taskQueue chan *RegionScoreTask
}

func NewPeerScoreStats(ctx context.Context) *PeerScoreStats {
	s := &PeerScoreStats{
		ctx: ctx,
		storeLoads: make(map[uint64]float64),
		regionScores: make(map[uint64]*RegionScore),
		taskQueue: make(chan *RegionScoreTask, 1024),
	}
	go s.HandleTasks(s.taskQueue)
	return s
}

func (w *PeerScoreStats) UpdateRegionScoreAsync(task *RegionScoreTask) {
	select {
	case w.taskQueue <- task:
	case <-w.ctx.Done():
	}
}

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

func (w *PeerScoreStats) HandleTasks(queue <-chan *RegionScoreTask) {
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
	w.storeLoads[task.storeID] = task.storeLoad

	now := time.Now()
	for regionID, r := range task.regions {
		var peerID uint64
		meta := r.GetMeta()
		for _, p := range meta.Peers {
			if p.StoreId == task.storeID {
				peerID = p.StoreId
				break
			}
		}
		if peerID == 0 {
			continue
		}
		var leaderID uint64
		leader := r.GetLeader()
		if leader == nil {
			continue
		}
		regionScore := w.regionScores[regionID]
		if regionScore == nil {
			regionScore = &RegionScore{
				Peers: make(map[uint64]*PeerScore),
				leaderID: leaderID,
			}
		}

		leaderStoreLoad, ok := w.storeLoads[r.GetLeader().StoreId]
		if !ok {
			continue
		}
		peerScore := regionScore.Peers[task.storeID]
		if peerScore == nil {
			peerScore = &PeerScore{
				PeerID: peerID,
				StoreID: task.storeID,
				Score: 100,
			}
		}

		newScore := peerScore.Score
		if task.storeLoad <= 0.5 {
			newScore = 100
		} else if task.storeLoad < 0.7 {
			delta := int((leaderStoreLoad - task.storeLoad) * 5.0)
			if delta > 10 {
				delta = 10
			}
			if delta < 1 {
				delta = 1
			}
			newScore += delta
		} else if task.storeLoad <= leaderStoreLoad * 0.8 {
			delta := int((leaderStoreLoad - task.storeLoad) * 5)
			if delta > 10 {
				delta = 10
			}
			newScore -= delta
		} else if task.storeLoad >= leaderStoreLoad * 1.2 {
			delta := int((task.storeLoad - leaderStoreLoad) * 5)
			if delta > 10 {
				delta = 10
			}
			newScore -= delta
			//newScore -= math.Min(int((task.storeLoad - leaderStoreLoad) * 5), 10)
		}
		if newScore > 100 {
			newScore = 100
		} else if newScore < 0 {
			newScore = 0
		}
		peerScore.Score = newScore
		peerScore.UpdatedAt = now
		regionScore.Peers[task.storeID] = peerScore
		w.regionScores[regionID] = regionScore
	}
}

func (w *PeerScoreStats) batchGetRegionScores(task getRegionScoreTask) {
	now := time.Now()
	respRegions := make([]*RegionScore, 0, len(task.regionIDs))
	for _, id := range task.regionIDs {
		if regionScore, ok := w.regionScores[id]; ok {
			respRegions = append(respRegions, regionScore.valid(now))
		}
	}
	task.respChan <- GetRegionScoreResp{regions: respRegions}
}

type RegionScore struct {
	// store_id --> peer score
	Peers map[uint64]*PeerScore
	leaderID uint64
}

func (r *RegionScore) valid(ts time.Time) *RegionScore {
	peers := make(map[uint64]*PeerScore, len(r.Peers))
	for storeID, p := range r.Peers {
		if ts.Sub(p.UpdatedAt) > time.Second * 10 {
			continue
		}
		peers[storeID] = &*p
	}
	return &RegionScore{
		Peers: peers,
		leaderID: r.leaderID,
	}
}

type PeerScore struct {
	PeerID   uint64
	StoreID  uint64
	Score    int
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

func NewUpdateRegionScoreTask(storeID uint64, storeLoad float64, regions map[uint64]*core.RegionInfo) *RegionScoreTask {
	return &RegionScoreTask {
		taskType: updateTaskType,
		data: updateRegionScoreTask{
			storeID: storeID,
			storeLoad: storeLoad,
			regions: regions,
		},
	}
}

func NewGetRegionScoreTask(regionIDs []uint64, respCh chan GetRegionScoreResp) *RegionScoreTask {
	return &RegionScoreTask {
		taskType: getTaskType,
		data: getRegionScoreTask{
			regionIDs: regionIDs,
			respChan: respCh,
		},
	}
}

type updateRegionScoreTask struct {
	storeID uint64
	storeLoad float64
	regions map[uint64]*core.RegionInfo
}

type getRegionScoreTask struct {
	regionIDs []uint64
	respChan chan GetRegionScoreResp
}

type GetRegionScoreResp struct {
	regions []*RegionScore
}

