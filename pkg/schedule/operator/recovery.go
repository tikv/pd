// Copyright 2026 TiKV Project Authors.
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

package operator

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/hbstream"
)

// ValidateSchedulingCommand validates capability and recovery safety at the
// actual dispatch boundary, including commands returned by an independent
// Scheduling service.
func ValidateSchedulingCommand(ci *core.BasicCluster, region *core.RegionInfo, cmd *hbstream.Operation) error {
	checkChange := func(change *pdpb.ChangePeer) error {
		if change == nil || change.GetChangeType() == eraftpb.ConfChangeType_RemoveNode {
			return nil // Removing an incompatible peer must remain possible.
		}
		peer := change.GetPeer()
		return validatePeerCapability(ci, peer.GetStoreId(), peer.GetIsWitness())
	}
	if err := checkChange(cmd.ChangePeer); err != nil {
		return err
	}
	for _, change := range cmd.ChangePeerV2.GetChanges() {
		if err := checkChange(change); err != nil {
			return err
		}
	}
	for _, change := range cmd.SwitchWitnesses.GetSwitchWitnesses() {
		peer := region.GetPeer(change.GetPeerId())
		if peer == nil {
			return fmt.Errorf("peer %d does not exist", change.GetPeerId())
		}
		if err := validatePeerCapability(ci, peer.GetStoreId(), change.GetIsWitness()); err != nil {
			return err
		}
	}
	if transfer := cmd.TransferLeader; transfer != nil {
		for _, peer := range append([]*metapb.Peer{transfer.GetPeer()}, transfer.GetPeers()...) {
			if peer == nil {
				continue
			}
			if core.IsWitness(peer) {
				return fmt.Errorf("normal leader transfer cannot target witness %d", peer.GetId())
			}
			if err := validatePeerCapability(ci, peer.GetStoreId(), false); err != nil {
				return err
			}
			store := ci.GetStore(peer.GetStoreId())
			if store == nil || store.GetMeta().GetLogOnlyProtocolVersion() < core.RequiredPeerProtocolVersion(ci, region) {
				return fmt.Errorf("leader target Store %d does not support required reader protocol", peer.GetStoreId())
			}
		}
	}
	if err := validateCommandPeerProtocols(ci, region, cmd); err != nil {
		return err
	}
	return validateRecoveryCommand(ci, region, cmd)
}

// Failed peers can be retired only after normal Raft membership safety checks.
// This exception permits replica repair; it never evicts the recovery leader.
func isFailedRecoveryPeer(ci *core.BasicCluster, region *core.RegionInfo, storeID uint64) bool {
	peer := region.GetStorePeer(storeID)
	if peer == nil {
		return true
	}
	if peer.GetId() == region.GetLeader().GetId() {
		return false
	}
	if region.GetDownPeer(peer.GetId()) != nil {
		return true
	}
	store := ci.GetStore(storeID)
	return store != nil && (store.IsRemoving() || store.IsRemoved() || store.IsDisconnected())
}

// Data readiness governs membership repair separately from size provenance:
// an empty or historical-sized NORMAL Region can still need its first witness.
func isDataServiceRecovering(region *core.RegionInfo) bool {
	status := region.GetRecoveryStatus()
	return status != nil && (status.GetState() != pdpb.RegionRecoveryState_NORMAL || !status.GetDataServiceReady())
}

func validateRecoveryStep(ci *core.BasicCluster, region *core.RegionInfo, step OpStep) error {
	allowed := false
	status := region.GetRecoveryStatus()
	repairOnly := isDataServiceRecovering(region)
	unknownState := status != nil && status.GetState() != pdpb.RegionRecoveryState_NORMAL && status.GetState() != pdpb.RegionRecoveryState_RECOVERING_LEADER
	if unknownState {
		switch s := step.(type) {
		case ChangePeerV2Leave:
			return nil
		case RemovePeer:
			if isFailedRecoveryPeer(ci, region, s.FromStore) {
				return nil
			}
		}
		return errors.New("unknown recovery state blocks membership activation")
	}
	switch s := step.(type) {
	case AddLearner:
		allowed = !repairOnly || !s.IsWitness
	case AddPeer:
		allowed = !repairOnly || !s.IsWitness
	case PromoteLearner:
		allowed = !repairOnly || !s.IsWitness
	case RemovePeer:
		allowed = !repairOnly || isFailedRecoveryPeer(ci, region, s.FromStore)
	case BecomeWitness, BecomeNonWitness, BatchSwitchWitness:
		allowed = !repairOnly
	case ChangePeerV2Enter:
		allowed = true
		if repairOnly {
			for _, p := range s.PromoteLearners {
				allowed = allowed && !p.IsWitness
			}
			for _, p := range s.DemoteVoters {
				allowed = allowed && !p.IsWitness && isFailedRecoveryPeer(ci, region, p.ToStore)
			}
		}
	case ChangePeerV2Leave:
		allowed = true // Completing an existing joint transition must not deadlock.
	}
	if !allowed {
		return fmt.Errorf("operation is paused during region recovery: %s", step)
	}
	return nil
}

func validateRecoveryCommand(ci *core.BasicCluster, region *core.RegionInfo, cmd *hbstream.Operation) error {
	if !region.IsRecoverySchedulingBlocked() {
		return nil
	}
	repairOnly := isDataServiceRecovering(region)
	if (cmd.ChangeSplit != nil && cmd.ChangeSplit.GetAutoSplitEnabled()) || cmd.TransferLeader != nil || cmd.Merge != nil || cmd.SplitRegion != nil || (repairOnly && cmd.SwitchWitnesses != nil) {
		return errors.New("conflicting scheduling command is paused during region recovery")
	}
	if !repairOnly {
		return nil // Missing current size must not prevent placement or replica repair.
	}
	checkChange := func(change *pdpb.ChangePeer) error {
		if change == nil {
			return nil
		}
		peer := change.GetPeer()
		if change.GetChangeType() == eraftpb.ConfChangeType_RemoveNode {
			if isFailedRecoveryPeer(ci, region, peer.GetStoreId()) {
				return nil
			}
			return errors.New("cannot remove a healthy peer during region recovery")
		}
		status := region.GetRecoveryStatus()
		if status != nil && status.GetState() != pdpb.RegionRecoveryState_NORMAL && status.GetState() != pdpb.RegionRecoveryState_RECOVERING_LEADER {
			return errors.New("unknown recovery state blocks membership activation")
		}
		if peer.GetIsWitness() {
			return errors.New("only Full replica repair is allowed during region recovery")
		}
		existing := region.GetStorePeer(peer.GetStoreId())
		if existing != nil && existing.GetIsWitness() {
			return errors.New("cannot convert witness during region recovery")
		}
		if change.GetChangeType() == eraftpb.ConfChangeType_AddNode && existing == nil {
			return errors.New("full repair must initialize a learner before promotion")
		}
		if change.GetChangeType() == eraftpb.ConfChangeType_AddLearnerNode && existing != nil && !core.IsLearner(existing) && !isFailedRecoveryPeer(ci, region, peer.GetStoreId()) {
			return errors.New("cannot demote a healthy voter during region recovery")
		}
		return nil
	}
	if err := checkChange(cmd.ChangePeer); err != nil {
		return err
	}
	for _, change := range cmd.ChangePeerV2.GetChanges() {
		if err := checkChange(change); err != nil {
			return err
		}
	}
	return nil
}

func validateCommandPeerProtocols(ci *core.BasicCluster, region *core.RegionInfo, cmd *hbstream.Operation) error {
	peers := make(map[uint64]*metapb.Peer, len(region.GetPeers())+1)
	for _, peer := range region.GetPeers() {
		peers[peer.GetStoreId()] = peer
	}
	activatesMembership := false
	applyChange := func(change *pdpb.ChangePeer) {
		if change == nil {
			return
		}
		peer := change.GetPeer()
		if change.GetChangeType() == eraftpb.ConfChangeType_RemoveNode {
			delete(peers, peer.GetStoreId())
			return
		}
		previous := peers[peer.GetStoreId()]
		if previous == nil || (!previous.GetIsWitness() && peer.GetIsWitness()) || (core.IsLearner(previous) && change.GetChangeType() == eraftpb.ConfChangeType_AddNode) {
			activatesMembership = true
		}
		peers[peer.GetStoreId()] = peer
	}
	applyChange(cmd.ChangePeer)
	for _, change := range cmd.ChangePeerV2.GetChanges() {
		applyChange(change)
	}
	for _, change := range cmd.SwitchWitnesses.GetSwitchWitnesses() {
		previous := region.GetPeer(change.GetPeerId())
		if previous == nil {
			continue
		}
		if !previous.GetIsWitness() && change.GetIsWitness() {
			activatesMembership = true
		}
		peers[previous.GetStoreId()] = &metapb.Peer{Id: previous.GetId(), StoreId: previous.GetStoreId(), Role: previous.GetRole(), IsWitness: change.GetIsWitness()}
	}
	if !activatesMembership {
		return nil
	}
	targets := make([]*metapb.Peer, 0, len(peers))
	for _, peer := range peers {
		targets = append(targets, peer)
	}
	return core.CheckLogOnlyPeerProtocols(ci, targets, region.GetRequiredReaderProtocolVersion())
}
