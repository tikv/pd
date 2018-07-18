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
	"fmt"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// Task running in node.
type Task interface {
	Desc() string
	RegionID() uint64
	Step(r *RaftEngine)
	IsFinished() bool
}

func responseToTask(resp *pdpb.RegionHeartbeatResponse, r *RaftEngine) Task {
	regionID := resp.GetRegionId()
	region := r.GetRegion(regionID)
	epoch := resp.GetRegionEpoch()

	//  change peer
	if resp.GetChangePeer() != nil {
		changePeer := resp.GetChangePeer()
		switch changePeer.GetChangeType() {
		case eraftpb.ConfChangeType_AddNode:
			return &addPeer{
				regionID: regionID,
				size:     region.ApproximateSize,
				keys:     region.ApproximateKeys,
				speed:    100 * 1000 * 1000,
				epoch:    epoch,
				peer:     changePeer.GetPeer(),
			}
		case eraftpb.ConfChangeType_RemoveNode:
			return &removePeer{
				regionID: regionID,
				size:     region.ApproximateSize,
				keys:     region.ApproximateKeys,
				speed:    100 * 1000 * 1000,
				epoch:    epoch,
				peer:     changePeer.GetPeer(),
			}
		case eraftpb.ConfChangeType_AddLearnerNode:
			return &addLearner{
				regionID: regionID,
				size:     region.ApproximateSize,
				keys:     region.ApproximateKeys,
				speed:    100 * 1000 * 1000,
				epoch:    epoch,
				peer:     changePeer.GetPeer(),
			}
		}
	} else if resp.GetTransferLeader() != nil {
		changePeer := resp.GetTransferLeader().GetPeer()
		fromPeer := region.Leader
		return &transferLeader{
			regionID: regionID,
			epoch:    epoch,
			fromPeer: fromPeer,
			peer:     changePeer,
		}
	}
	return nil
}

type transferLeader struct {
	regionID uint64
	epoch    *metapb.RegionEpoch
	fromPeer *metapb.Peer
	peer     *metapb.Peer
	finished bool
}

func (t *transferLeader) Desc() string {
	return fmt.Sprintf("transfer leader from store %d to store %d", t.fromPeer.GetStoreId(), t.peer.GetStoreId())
}

func (t *transferLeader) Step(r *RaftEngine) {
	if t.finished {
		return
	}
	region := r.GetRegion(t.regionID)
	if region.RegionEpoch.Version > t.epoch.Version || region.RegionEpoch.ConfVer > t.epoch.ConfVer {
		t.finished = true
		return
	}
	if region.GetPeer(t.peer.GetId()) != nil {
		region.Leader = t.peer
	}
	t.finished = true
	r.SetRegion(region)
	r.recordRegionChange(region)
}

func (t *transferLeader) RegionID() uint64 {
	return t.regionID
}

func (t *transferLeader) IsFinished() bool {
	return t.finished
}

type addPeer struct {
	regionID uint64
	size     int64
	keys     int64
	speed    int64
	epoch    *metapb.RegionEpoch
	peer     *metapb.Peer
	finished bool
}

func (a *addPeer) Desc() string {
	return fmt.Sprintf("add peer %+v for region %d", a.peer, a.regionID)
}

func (a *addPeer) Step(r *RaftEngine) {
	if a.finished {
		return
	}
	region := r.GetRegion(a.regionID)
	if region.RegionEpoch.Version > a.epoch.Version || region.RegionEpoch.ConfVer > a.epoch.ConfVer {
		a.finished = true
		return
	}

	a.size -= a.speed
	if a.size < 0 {
		if region.GetPeer(a.peer.GetId()) == nil {
			region.AddPeer(a.peer)
		} else {
			region.GetPeer(a.peer.GetId()).IsLearner = false
		}
		region.RegionEpoch.ConfVer++
		r.SetRegion(region)
		r.recordRegionChange(region)
		a.finished = true
	}
}

func (a *addPeer) RegionID() uint64 {
	return a.regionID
}

func (a *addPeer) IsFinished() bool {
	return a.finished
}

type removePeer struct {
	regionID uint64
	size     int64
	keys     int64
	speed    int64
	epoch    *metapb.RegionEpoch
	peer     *metapb.Peer
	finished bool
}

func (a *removePeer) Desc() string {
	return fmt.Sprintf("remove peer %+v for region %d", a.peer, a.regionID)
}

func (a *removePeer) Step(r *RaftEngine) {
	if a.finished {
		return
	}
	region := r.GetRegion(a.regionID)
	if region.RegionEpoch.Version > a.epoch.Version || region.RegionEpoch.ConfVer > a.epoch.ConfVer {
		a.finished = true
		return
	}

	a.size -= a.speed
	if a.size < 0 {
		for _, peer := range region.GetPeers() {
			if peer.GetId() == a.peer.GetId() {
				region.RemoveStorePeer(peer.GetStoreId())
				region.RegionEpoch.ConfVer++
				r.SetRegion(region)
				r.recordRegionChange(region)
				break
			}
		}
		a.finished = true
	}
}

func (a *removePeer) RegionID() uint64 {
	return a.regionID
}

func (a *removePeer) IsFinished() bool {
	return a.finished
}

type addLearner struct {
	regionID uint64
	size     int64
	keys     int64
	speed    int64
	epoch    *metapb.RegionEpoch
	peer     *metapb.Peer
	finished bool
}

func (a *addLearner) Desc() string {
	return fmt.Sprintf("add learner %+v for region %d", a.peer, a.regionID)
}

func (a *addLearner) Step(r *RaftEngine) {
	if a.finished {
		return
	}
	region := r.GetRegion(a.regionID)
	if region.RegionEpoch.Version > a.epoch.Version || region.RegionEpoch.ConfVer > a.epoch.ConfVer {
		a.finished = true
		return
	}

	a.size -= a.speed
	if a.size < 0 {
		if region.GetPeer(a.peer.GetId()) == nil {
			region.AddPeer(a.peer)
			region.RegionEpoch.ConfVer++
			r.SetRegion(region)
			r.recordRegionChange(region)
		}
		a.finished = true
	}
}

func (a *addLearner) RegionID() uint64 {
	return a.regionID
}

func (a *addLearner) IsFinished() bool {
	return a.finished
}
