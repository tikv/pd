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
	"bytes"
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
				// This two variables are used to simulate sending and receiving snapshot processes.
				sendingStat:   &snapshotStat{"sending", region.ApproximateSize, false},
				receivingStat: &snapshotStat{"receiving", region.ApproximateSize, false},
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
	} else if resp.GetMerge() != nil {
		targetRegion := resp.GetMerge().GetTarget()
		return &mergeRegion{
			regionID:     regionID,
			epoch:        epoch,
			targetRegion: targetRegion,
		}
	}
	return nil
}

type snapshotStat struct {
	kind       string
	remainSize int64
	finished   bool
}

type mergeRegion struct {
	regionID     uint64
	epoch        *metapb.RegionEpoch
	targetRegion *metapb.Region
	finished     bool
}

func (m *mergeRegion) Desc() string {
	return fmt.Sprintf("merge region %d into %d", m.regionID, m.targetRegion.GetId())
}

func (m *mergeRegion) Step(r *RaftEngine) {
	if m.finished {
		return
	}

	region := r.GetRegion(m.regionID)
	// If region equals to nil, it means that the region has already been merged.
	if region == nil || region.RegionEpoch.ConfVer > m.epoch.ConfVer || region.RegionEpoch.Version > m.epoch.Version {
		m.finished = true
		return
	}

	targetRegion := r.GetRegion(m.targetRegion.Id)
	if bytes.Equal(m.targetRegion.EndKey, region.StartKey) {
		targetRegion.EndKey = region.EndKey
	} else {
		targetRegion.StartKey = region.StartKey
	}

	targetRegion.ApproximateSize += region.ApproximateSize
	targetRegion.ApproximateKeys += region.ApproximateKeys

	if m.epoch.ConfVer > m.targetRegion.RegionEpoch.ConfVer {
		targetRegion.RegionEpoch.ConfVer = m.epoch.ConfVer
	}

	if m.epoch.Version > m.targetRegion.RegionEpoch.Version {
		targetRegion.RegionEpoch.Version = m.epoch.Version
	}
	targetRegion.RegionEpoch.Version++

	r.SetRegion(targetRegion)
	r.recordRegionChange(targetRegion)
	m.finished = true
}

func (m *mergeRegion) RegionID() uint64 {
	return m.regionID
}

func (m *mergeRegion) IsFinished() bool {
	return m.finished
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
	regionID      uint64
	size          int64
	keys          int64
	speed         int64
	epoch         *metapb.RegionEpoch
	peer          *metapb.Peer
	finished      bool
	sendingStat   *snapshotStat
	receivingStat *snapshotStat
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

	snapshotSize := region.ApproximateSize
	sendNode := r.conn.Nodes[region.Leader.GetStoreId()]
	if !processSnapshot(sendNode, a.sendingStat, snapshotSize) {
		return
	}

	recvNode := r.conn.Nodes[a.peer.GetStoreId()]
	if !processSnapshot(recvNode, a.receivingStat, snapshotSize) {
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
		recvNode.incUsedSize(uint64(snapshotSize))
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

	regionSize := uint64(region.ApproximateSize)
	a.size -= a.speed
	if a.size < 0 {
		for _, peer := range region.GetPeers() {
			if peer.GetId() == a.peer.GetId() {
				storeID := peer.GetStoreId()
				region.RemoveStorePeer(storeID)
				region.RegionEpoch.ConfVer++
				r.SetRegion(region)
				r.recordRegionChange(region)
				r.conn.Nodes[storeID].decUsedSize(regionSize)
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

func processSnapshot(n *Node, stat *snapshotStat, snapshotSize int64) bool {
	// If the statement is true, it will start to send or receive the snapshot.
	if stat.remainSize == snapshotSize {
		if stat.kind == "sending" {
			n.stats.SendingSnapCount++
		} else {
			n.stats.ReceivingSnapCount++
		}
	}
	stat.remainSize -= n.ioRate
	// The sending or receiving process has not finished yet.
	if stat.remainSize > 0 {
		return false
	}
	if !stat.finished {
		stat.finished = true
		if stat.kind == "sending" {
			n.stats.SendingSnapCount--
		} else {
			n.stats.ReceivingSnapCount--
		}
	}
	return true
}
