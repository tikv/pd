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
// See the License for the specific language governing permissions and
// limitations under the License.

package simulator

import (
	"bytes"
	"fmt"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tools/pd-analysis/analysis"
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
				size:     region.GetApproximateSize(),
				keys:     region.GetApproximateKeys(),
				speed:    100 * 1000 * 1000,
				epoch:    epoch,
				peer:     changePeer.GetPeer(),
				// This two variables are used to simulate sending and receiving snapshot processes.
				sendingStat:   &snapshotStat{"sending", region.GetApproximateSize(), false},
				receivingStat: &snapshotStat{"receiving", region.GetApproximateSize(), false},
			}
		case eraftpb.ConfChangeType_RemoveNode:
			return &removePeer{
				regionID: regionID,
				size:     region.GetApproximateSize(),
				keys:     region.GetApproximateKeys(),
				speed:    100 * 1000 * 1000,
				epoch:    epoch,
				peer:     changePeer.GetPeer(),
			}
		case eraftpb.ConfChangeType_AddLearnerNode:
			return &addLearner{
				regionID: regionID,
				size:     region.GetApproximateSize(),
				keys:     region.GetApproximateKeys(),
				speed:    100 * 1000 * 1000,
				epoch:    epoch,
				peer:     changePeer.GetPeer(),
			}
		}
	} else if resp.GetTransferLeader() != nil {
		changePeer := resp.GetTransferLeader().GetPeer()
		fromPeer := region.GetLeader()
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
	if region == nil || region.GetRegionEpoch().GetConfVer() > m.epoch.ConfVer || region.GetRegionEpoch().GetVersion() > m.epoch.Version {
		m.finished = true
		return
	}

	targetRegion := r.GetRegion(m.targetRegion.Id)
	var startKey, endKey []byte
	if bytes.Equal(m.targetRegion.EndKey, region.GetStartKey()) {
		startKey = targetRegion.GetStartKey()
		endKey = region.GetEndKey()
	} else {
		startKey = region.GetStartKey()
		endKey = targetRegion.GetEndKey()
	}

	epoch := targetRegion.GetRegionEpoch()
	if m.epoch.ConfVer > m.targetRegion.RegionEpoch.ConfVer {
		epoch.ConfVer = m.epoch.ConfVer
	}

	if m.epoch.Version > m.targetRegion.RegionEpoch.Version {
		epoch.Version = m.epoch.Version
	}
	epoch.Version++
	mergeRegion := targetRegion.Clone(
		core.WithStartKey(startKey),
		core.WithEndKey(endKey),
		core.SetRegionConfVer(epoch.ConfVer),
		core.SetRegionVersion(epoch.Version),
		core.SetApproximateSize(targetRegion.GetApproximateSize()+region.GetApproximateSize()),
		core.SetApproximateKeys(targetRegion.GetApproximateKeys()+region.GetApproximateKeys()),
	)
	r.SetRegion(mergeRegion)
	r.recordRegionChange(mergeRegion)
	r.schedulerStats.taskStats.incMergeRegion()
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
	if region.GetRegionEpoch().GetVersion() > t.epoch.Version || region.GetRegionEpoch().GetConfVer() > t.epoch.ConfVer {
		t.finished = true
		return
	}
	var newRegion *core.RegionInfo
	if region.GetPeer(t.peer.GetId()) != nil {
		newRegion = region.Clone(core.WithLeader(t.peer))
	} else {
		// This branch will be executed
		t.finished = true
		return
	}
	t.finished = true
	r.SetRegion(newRegion)
	r.recordRegionChange(newRegion)
	fromPeerID := t.fromPeer.GetId()
	toPeerID := t.peer.GetId()
	r.schedulerStats.taskStats.incTransferLeader(fromPeerID, toPeerID)
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
	if region.GetRegionEpoch().GetVersion() > a.epoch.Version || region.GetRegionEpoch().GetConfVer() > a.epoch.ConfVer {
		a.finished = true
		return
	}

	snapshotSize := region.GetApproximateSize()
	sendNode := r.conn.Nodes[region.GetLeader().GetStoreId()]
	if sendNode == nil {
		a.finished = true
		return
	}
	if !processSnapshot(sendNode, a.sendingStat, snapshotSize) {
		return
	}
	r.schedulerStats.snapshotStats.incSendSnapshot(sendNode.Id)

	recvNode := r.conn.Nodes[a.peer.GetStoreId()]
	if recvNode == nil {
		a.finished = true
		return
	}
	if !processSnapshot(recvNode, a.receivingStat, snapshotSize) {
		return
	}
	r.schedulerStats.snapshotStats.incReceiveSnapshot(recvNode.Id)

	a.size -= a.speed
	if a.size < 0 {
		var opts []core.RegionCreateOption
		if region.GetPeer(a.peer.GetId()) == nil {
			opts = append(opts, core.WithAddPeer(a.peer))
			r.schedulerStats.taskStats.incAddPeer(region.GetID())
		} else {
			opts = append(opts, core.WithPromoteLearner(a.peer.GetId()))
			r.schedulerStats.taskStats.incPromoteLeaner(region.GetID())
		}
		opts = append(opts, core.WithIncConfVer())
		newRegion := region.Clone(opts...)
		r.SetRegion(newRegion)
		r.recordRegionChange(newRegion)
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
	if region.GetRegionEpoch().GetVersion() > a.epoch.Version || region.GetRegionEpoch().GetConfVer() > a.epoch.ConfVer {
		a.finished = true
		return
	}

	regionSize := uint64(region.GetApproximateSize())
	a.size -= a.speed
	if a.size < 0 {
		for _, peer := range region.GetPeers() {
			if peer.GetId() == a.peer.GetId() {
				storeID := peer.GetStoreId()
				var downPeers []*pdpb.PeerStats
				if r.conn.Nodes[storeID] == nil {
					for _, downPeer := range region.GetDownPeers() {
						if downPeer.Peer.StoreId != storeID {
							downPeers = append(downPeers, downPeer)
						}
					}
				}
				newRegion := region.Clone(
					core.WithRemoveStorePeer(storeID),
					core.WithIncConfVer(),
					core.WithDownPeers(downPeers),
				)
				r.SetRegion(newRegion)
				r.recordRegionChange(newRegion)
				r.schedulerStats.taskStats.incRemovePeer(region.GetID())
				if r.conn.Nodes[storeID] == nil {
					a.finished = true
					return
				}
				r.conn.Nodes[storeID].decUsedSize(regionSize)
				break
			}
		}
		a.finished = true
		if analysis.GetTransferCounter().IsValid {
			analysis.GetTransferCounter().AddSource(a.regionID, a.peer.StoreId)
		}
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
	if region.GetRegionEpoch().GetVersion() > a.epoch.Version || region.GetRegionEpoch().GetConfVer() > a.epoch.ConfVer {
		a.finished = true
		return
	}

	a.size -= a.speed
	if a.size < 0 {
		if region.GetPeer(a.peer.GetId()) == nil {
			newRegion := region.Clone(
				core.WithAddPeer(a.peer),
				core.WithIncConfVer(),
			)
			r.SetRegion(newRegion)
			r.recordRegionChange(newRegion)
			r.schedulerStats.taskStats.incAddLeaner(region.GetID())
		}
		a.finished = true
		if analysis.GetTransferCounter().IsValid {
			analysis.GetTransferCounter().AddTarget(a.regionID, a.peer.StoreId)
		}
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
