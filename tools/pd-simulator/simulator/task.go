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
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/docker/go-units"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/tools/pd-analysis/analysis"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
)

const (
	removeSpeed              = 100 * units.MiB
	chunkSize                = 4 * units.KiB
	maxSnapGeneratorPoolSize = 2
	maxSnapReceivePoolSize   = 4
	compressionRatio         = 2
)

type snapAction int

const (
	generate snapAction = iota
	receive
)

type snapStatus int

const (
	pending snapStatus = iota
	running
	finished
)

func responseToTask(engine *RaftEngine, resp *pdpb.RegionHeartbeatResponse) *Task {
	var (
		regionID = resp.GetRegionId()
		region   = engine.GetRegion(regionID)
		op       operator
		desc     string
	)

	switch {
	case resp.GetChangePeer() != nil:
		op, desc = changePeerToOperator(region, resp.GetChangePeer())
	case resp.GetChangePeerV2() != nil:
		cps := resp.GetChangePeerV2().GetChanges()
		if len(cps) == 0 {
			// leave joint state
			desc = fmt.Sprintf("leave joint state for region %d", regionID)
			op = &changePeerV2Leave{}
		} else if len(cps) == 1 {
			// original ChangePeer
			op, desc = changePeerToOperator(region, cps[0])
		} else {
			// enter joint state, it can only contain PromoteLearner and DemoteVoter.
			subDesc := make([]string, 0, len(cps))
			cp2 := &changePeerV2Enter{}
			for _, cp := range cps {
				peer := cp.GetPeer()
				subOp, _ := changePeerToOperator(region, cp)
				switch subOp.(type) {
				case *promoteLearner:
					subDesc = append(subDesc, fmt.Sprintf("promote peer %+v", peer))
					cp2.promoteLearners = append(cp2.promoteLearners, peer)
				case *demoteVoter:
					subDesc = append(subDesc, fmt.Sprintf("demote peer %+v", peer))
					cp2.demoteVoters = append(cp2.demoteVoters, peer)
				default:
					simutil.Logger.Error("cannot exec AddPeer or RemovePeer when using joint state")
					return nil
				}
			}
			desc = fmt.Sprintf("%s for region %d", strings.Join(subDesc, ", "), regionID)
			op = cp2
		}
	case resp.GetTransferLeader() != nil:
		fromPeerStoreID := region.GetLeader().GetStoreId()
		// When this field is included, it means that TiKV needs to decide the optimal Leader by itself.
		toPeers := resp.GetTransferLeader().GetPeers()
		// When no Peers are included, use Peer to build Peers of length 1.
		if len(toPeers) == 0 {
			toPeers = []*metapb.Peer{resp.GetTransferLeader().GetPeer()}
		}
		desc = fmt.Sprintf("transfer leader from store %d to store %d", fromPeerStoreID, toPeers[0].GetStoreId())
		op = &transferLeader{
			fromPeerStoreID: fromPeerStoreID,
			toPeers:         toPeers,
		}
	case resp.GetMerge() != nil:
		targetRegion := resp.GetMerge().GetTarget()
		desc = fmt.Sprintf("merge region %d into %d", regionID, targetRegion.GetId())
		op = &mergeRegion{targetRegion: targetRegion}
	case resp.GetSplitRegion() != nil:
		// TODO: support split region
		simutil.Logger.Error("split region scheduling is currently not supported")
		return nil
	default:
		return nil
	}

	if op == nil {
		return nil
	}

	return &Task{
		operator:   op,
		desc:       desc,
		regionID:   regionID,
		epoch:      resp.GetRegionEpoch(),
		isFinished: false,
	}
}

func changePeerToOperator(region *core.RegionInfo, cp *pdpb.ChangePeer) (operator, string) {
	regionID := region.GetID()
	peer := cp.GetPeer()
	switch cp.GetChangeType() {
	case eraftpb.ConfChangeType_AddNode:
		if region.GetStoreLearner(peer.GetStoreId()) != nil {
			return &promoteLearner{peer: peer}, fmt.Sprintf("promote learner %+v for region %d", peer, regionID)
		}
		return &addPeer{
			peer:          peer,
			size:          region.GetApproximateSize(),
			keys:          region.GetApproximateKeys(),
			sendingStat:   newSnapshotState(region.GetApproximateSize(), generate),
			receivingStat: newSnapshotState(region.GetApproximateSize(), receive),
		}, fmt.Sprintf("add voter %+v for region %d", peer, regionID)
	case eraftpb.ConfChangeType_AddLearnerNode:
		if region.GetStoreVoter(peer.GetStoreId()) != nil {
			return &demoteVoter{peer: peer}, fmt.Sprintf("demote voter %+v for region %d", peer, regionID)
		}
		return &addPeer{
			peer:          peer,
			size:          region.GetApproximateSize(),
			keys:          region.GetApproximateKeys(),
			sendingStat:   newSnapshotState(region.GetApproximateSize(), generate),
			receivingStat: newSnapshotState(region.GetApproximateSize(), receive),
		}, fmt.Sprintf("add learner %+v for region %d", peer, regionID)
	case eraftpb.ConfChangeType_RemoveNode:
		return &removePeer{
			peer:  peer,
			size:  region.GetApproximateSize(),
			speed: removeSpeed,
		}, fmt.Sprintf("remove peer %+v for region %d", peer, regionID)
	default:
		return nil, ""
	}
}

// Simulate the execution of the Operator.
type operator interface {
	// Returns new region if the execution is finished, otherwise returns nil.
	tick(engine *RaftEngine, region *core.RegionInfo) (newRegion *core.RegionInfo, isFinished bool)
}

// Task running in node.
type Task struct {
	operator
	desc       string
	regionID   uint64
	epoch      *metapb.RegionEpoch
	isFinished bool
}

// Desc returns the description of the Task.
func (t *Task) Desc() string {
	return t.desc
}

// RegionID returns the region-id of the Task.
func (t *Task) RegionID() uint64 {
	return t.regionID
}

// Step execute once on the Task.
func (t *Task) Step(engine *RaftEngine) (isFinished bool) {
	if t.isFinished {
		return true
	}

	region := engine.GetRegion(t.regionID)
	if region == nil || region.GetRegionEpoch().GetConfVer() > t.epoch.ConfVer || region.GetRegionEpoch().GetVersion() > t.epoch.Version {
		t.isFinished = true
		return
	}

	var newRegion *core.RegionInfo
	newRegion, t.isFinished = t.tick(engine, region)

	if newRegion != nil {
		t.epoch = newRegion.GetRegionEpoch()
		engine.SetRegion(newRegion)
		engine.recordRegionChange(newRegion)
	}

	return t.isFinished
}

type mergeRegion struct {
	targetRegion *metapb.Region
}

func (m *mergeRegion) tick(engine *RaftEngine, region *core.RegionInfo) (newRegion *core.RegionInfo, isFinished bool) {
	targetRegion := engine.GetRegion(m.targetRegion.Id)

	var startKey, endKey []byte
	if bytes.Equal(m.targetRegion.GetEndKey(), region.GetStartKey()) {
		startKey = targetRegion.GetStartKey()
		endKey = region.GetEndKey()
	} else {
		startKey = region.GetStartKey()
		endKey = targetRegion.GetEndKey()
	}

	epoch := targetRegion.Clone().GetRegionEpoch()
	if region.GetRegionEpoch().GetConfVer() > epoch.GetConfVer() {
		epoch.ConfVer = region.GetRegionEpoch().GetConfVer()
	}
	if region.GetRegionEpoch().GetVersion() > epoch.GetVersion() {
		epoch.Version = region.GetRegionEpoch().GetVersion()
	}
	epoch.Version++

	newRegion = targetRegion.Clone(
		core.WithStartKey(startKey),
		core.WithEndKey(endKey),
		core.SetRegionConfVer(epoch.ConfVer),
		core.SetRegionVersion(epoch.Version),
		core.SetApproximateSize(targetRegion.GetApproximateSize()+region.GetApproximateSize()),
		core.SetApproximateKeys(targetRegion.GetApproximateKeys()+region.GetApproximateKeys()),
	)
	schedulingCounter.WithLabelValues("merge").Inc()
	return newRegion, true
}

type transferLeader struct {
	fromPeerStoreID uint64
	toPeers         []*metapb.Peer
}

func (t *transferLeader) tick(_ *RaftEngine, region *core.RegionInfo) (newRegion *core.RegionInfo, isFinished bool) {
	isFinished = true
	toPeer := t.toPeers[0] // TODO: Support selection logic
	if peer := region.GetPeer(toPeer.GetId()); peer == nil || peer.GetRole() != toPeer.GetRole() || core.IsLearner(peer) {
		return
	}
	if toPeer.GetRole() == metapb.PeerRole_DemotingVoter {
		simutil.Logger.Error("set demoting-voter as leader",
			zap.Uint64("region-id", region.GetID()),
			zap.String("peer", toPeer.String()))
	}

	newRegion = region.Clone(core.WithLeader(toPeer))
	schedulingCounter.WithLabelValues("transfer-leader").Inc()
	return
}

func checkAndCreateChangePeerOption(region *core.RegionInfo,
	peer *metapb.Peer, from, to metapb.PeerRole) []core.RegionCreateOption {
	// `from` and `to` need to satisfy the combination in switch.

	// check `from` Role
	if peer.GetRole() != from {
		simutil.Logger.Error(
			"unexpected role",
			zap.String("role", peer.GetRole().String()),
			zap.String("expected", from.String()))
		return nil
	}
	// Leader cannot be demoted
	if (to == metapb.PeerRole_DemotingVoter || to == metapb.PeerRole_Learner) && region.GetLeader().GetId() == peer.GetId() {
		simutil.Logger.Error("demote leader", zap.String("region", region.GetMeta().String()))
		return nil
	}
	// create option
	switch to {
	case metapb.PeerRole_Voter: // Learner/IncomingVoter -> Voter
		schedulingCounter.WithLabelValues("promote-learner").Inc()
	case metapb.PeerRole_Learner: // Voter/DemotingVoter -> Learner
		schedulingCounter.WithLabelValues("demote-voter").Inc()
	case metapb.PeerRole_IncomingVoter: // Learner -> IncomingVoter, only in joint state
	case metapb.PeerRole_DemotingVoter: // Voter -> DemotingVoter, only in joint state
	default:
		return nil
	}
	return []core.RegionCreateOption{core.WithRole(peer.GetId(), to), core.WithIncConfVer()}
}

type promoteLearner struct {
	peer *metapb.Peer
}

func (pl *promoteLearner) tick(_ *RaftEngine, region *core.RegionInfo) (newRegion *core.RegionInfo, isFinished bool) {
	isFinished = true
	peer := region.GetPeer(pl.peer.GetId())
	opts := checkAndCreateChangePeerOption(region, peer, metapb.PeerRole_Learner, metapb.PeerRole_Voter)
	if len(opts) > 0 {
		newRegion = region.Clone(opts...)
	}
	return
}

type demoteVoter struct {
	peer *metapb.Peer
}

func (dv *demoteVoter) tick(_ *RaftEngine, region *core.RegionInfo) (newRegion *core.RegionInfo, isFinished bool) {
	isFinished = true
	peer := region.GetPeer(dv.peer.GetId())
	opts := checkAndCreateChangePeerOption(region, peer, metapb.PeerRole_Voter, metapb.PeerRole_Learner)
	if len(opts) > 0 {
		newRegion = region.Clone(opts...)
	}
	return
}

type changePeerV2Enter struct {
	promoteLearners []*metapb.Peer
	demoteVoters    []*metapb.Peer
}

func (ce *changePeerV2Enter) tick(_ *RaftEngine, region *core.RegionInfo) (newRegion *core.RegionInfo, isFinished bool) {
	isFinished = true
	var opts []core.RegionCreateOption
	for _, pl := range ce.promoteLearners {
		peer := region.GetPeer(pl.GetId())
		subOpts := checkAndCreateChangePeerOption(region, peer, metapb.PeerRole_Learner, metapb.PeerRole_IncomingVoter)
		if len(subOpts) == 0 {
			return
		}
		opts = append(opts, subOpts...)
	}
	for _, dv := range ce.demoteVoters {
		peer := region.GetPeer(dv.GetId())
		subOpts := checkAndCreateChangePeerOption(region, peer, metapb.PeerRole_Voter, metapb.PeerRole_DemotingVoter)
		if len(subOpts) == 0 {
			return
		}
		opts = append(opts, subOpts...)
	}
	newRegion = region.Clone(opts...)
	return
}

type changePeerV2Leave struct{}

func (*changePeerV2Leave) tick(_ *RaftEngine, region *core.RegionInfo) (newRegion *core.RegionInfo, isFinished bool) {
	isFinished = true
	var opts []core.RegionCreateOption
	for _, peer := range region.GetPeers() {
		switch peer.GetRole() {
		case metapb.PeerRole_IncomingVoter:
			opts = append(opts, checkAndCreateChangePeerOption(region, peer, metapb.PeerRole_IncomingVoter, metapb.PeerRole_Voter)...)
		case metapb.PeerRole_DemotingVoter:
			opts = append(opts, checkAndCreateChangePeerOption(region, peer, metapb.PeerRole_DemotingVoter, metapb.PeerRole_Learner)...)
		}
	}
	if len(opts) < 4 {
		simutil.Logger.Error("fewer than two peers should not need to leave the joint state")
		return
	}
	newRegion = region.Clone(opts...)
	return
}

type addPeer struct {
	peer          *metapb.Peer
	size          int64
	keys          int64
	sendingStat   *snapshotStat
	receivingStat *snapshotStat
}

func (a *addPeer) tick(engine *RaftEngine, region *core.RegionInfo) (newRegion *core.RegionInfo, isFinished bool) {
	// Check
	sendNode := engine.conn.Nodes[region.GetLeader().GetStoreId()]
	if sendNode == nil {
		return nil, true
	}
	recvNode := engine.conn.Nodes[a.peer.GetStoreId()]
	if recvNode == nil {
		return nil, true
	}
	// Step 1: Generate Pending Peers
	if region.GetPeer(a.peer.GetId()) == nil {
		switch a.peer.GetRole() {
		case metapb.PeerRole_Voter:
			schedulingCounter.WithLabelValues("add-voter").Inc()
		case metapb.PeerRole_Learner:
			schedulingCounter.WithLabelValues("add-learner").Inc()
		}
		pendingPeers := append(region.GetPendingPeers(), a.peer)
		return region.Clone(core.WithAddPeer(a.peer), core.WithIncConfVer(), core.WithPendingPeers(pendingPeers)), false
	}
	speed := engine.storeConfig.Speed()
	// Step 2: Process Snapshot
	if !processSnapshot(sendNode, a.sendingStat, speed) {
		return nil, false
	}
	sendStoreID := fmt.Sprintf("store-%d", sendNode.Id)
	snapshotCounter.WithLabelValues(sendStoreID, "send").Inc()
	if !processSnapshot(recvNode, a.receivingStat, speed) {
		return nil, false
	}
	recvStoreID := fmt.Sprintf("store-%d", recvNode.Id)
	snapshotCounter.WithLabelValues(recvStoreID, "recv").Inc()
	recvNode.incUsedSize(uint64(region.GetApproximateSize()))
	// Step 3: Remove the Pending state
	newRegion = region.Clone(removePendingPeer(region, a.peer))
	isFinished = true

	// analysis
	if analysis.GetTransferCounter().IsValid {
		analysis.GetTransferCounter().AddTarget(region.GetID(), a.peer.GetStoreId())
	}

	return
}

type removePeer struct {
	peer  *metapb.Peer
	size  int64
	speed int64
}

func (r *removePeer) tick(engine *RaftEngine, region *core.RegionInfo) (newRegion *core.RegionInfo, isFinished bool) {
	// Step 1: Delete data
	r.size -= r.speed
	if r.size > 0 {
		return nil, false
	}
	// Step 2: Remove Peer
	schedulingCounter.WithLabelValues("remove-peer").Inc()
	newRegion = region.Clone(
		core.WithIncConfVer(),
		core.WithRemoveStorePeer(r.peer.GetStoreId()),
		removePendingPeer(region, r.peer),
		removeDownPeers(region, r.peer))
	isFinished = true

	if store := engine.conn.Nodes[r.peer.GetStoreId()]; store != nil {
		store.decUsedSize(uint64(region.GetApproximateSize()))
		// analysis
		if analysis.GetTransferCounter().IsValid {
			analysis.GetTransferCounter().AddSource(region.GetID(), r.peer.GetStoreId())
		}
	}

	return
}

func removePendingPeer(region *core.RegionInfo, removePeer *metapb.Peer) core.RegionCreateOption {
	pendingPeers := make([]*metapb.Peer, 0, len(region.GetPendingPeers()))
	for _, peer := range region.GetPendingPeers() {
		if peer.GetId() != removePeer.GetId() {
			pendingPeers = append(pendingPeers, peer)
		}
	}
	return core.WithPendingPeers(pendingPeers)
}

func removeDownPeers(region *core.RegionInfo, removePeer *metapb.Peer) core.RegionCreateOption {
	downPeers := make([]*pdpb.PeerStats, 0, len(region.GetDownPeers()))
	for _, peer := range region.GetDownPeers() {
		if peer.GetPeer().GetId() != removePeer.GetId() {
			downPeers = append(downPeers, peer)
		}
	}
	return core.WithDownPeers(downPeers)
}

type snapshotStat struct {
	action        snapAction
	remainSize    int64
	status        snapStatus
	start         time.Time
	generateStart time.Time
}

func newSnapshotState(size int64, action snapAction) *snapshotStat {
	if action == receive {
		size /= compressionRatio
	}
	return &snapshotStat{
		remainSize: size,
		action:     action,
		status:     pending,
		start:      time.Now(),
	}
}

func processSnapshot(n *Node, stat *snapshotStat, speed uint64) bool {
	if stat.status == finished {
		return true
	}
	if stat.status == pending {
		n.statsMutex.RLock()
		sendSnapshot, receiveSnapshot := n.stats.SendingSnapCount, n.stats.ReceivingSnapCount
		n.statsMutex.RUnlock()
		if stat.action == generate && sendSnapshot > maxSnapGeneratorPoolSize {
			return false
		}
		if stat.action == receive && receiveSnapshot > maxSnapReceivePoolSize {
			return false
		}
		stat.status = running
		stat.generateStart = time.Now()
		n.statsMutex.Lock()
		// If the statement is true, it will start to send or Receive the snapshot.
		if stat.action == generate {
			n.stats.SendingSnapCount++
		} else {
			n.stats.ReceivingSnapCount++
		}
		n.statsMutex.Unlock()
	}

	// store should Generate/Receive snapshot by chunk size.
	// TODO: the process of snapshot is single thread, the later snapshot task must wait the first one.
	for stat.remainSize > 0 && n.limiter.AllowN(chunkSize) {
		stat.remainSize -= chunkSize
	}

	// The sending or receiving process has not status yet.
	if stat.remainSize > 0 {
		return false
	}
	if stat.status == running {
		stat.status = finished
		totalSec := uint64(time.Since(stat.start).Seconds()) * speed
		generateSec := uint64(time.Since(stat.generateStart).Seconds()) * speed
		n.registerSnapStats(generateSec, 0, totalSec)
		n.statsMutex.Lock()
		if stat.action == generate {
			n.stats.SendingSnapCount--
		} else {
			n.stats.ReceivingSnapCount--
		}
		n.statsMutex.Unlock()
	}
	return true
}
