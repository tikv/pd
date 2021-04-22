// Copyright 2020 TiKV Project Authors.
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

package core

import (
	"github.com/pingcap/kvproto/pkg/metapb"
)

// IsLearner judges whether the Peer's Role is Learner.
func IsLearner(peer *metapb.Peer) bool {
	return peer.GetRole() == metapb.PeerRole_Learner
}

// IsVoterOrIncomingVoter judges whether peer role will become Voter.
// The peer is not nil and the role is equal to IncomingVoter or Voter.
func IsVoterOrIncomingVoter(peer *metapb.Peer) bool {
	if peer == nil {
		return false
	}
	switch peer.GetRole() {
	case metapb.PeerRole_IncomingVoter, metapb.PeerRole_Voter:
		return true
	}
	return false
}

// IsLearnerOrDemotingVoter judges whether peer role will become Learner.
// The peer is not nil and the role is equal to DemotingVoter or Learner.
func IsLearnerOrDemotingVoter(peer *metapb.Peer) bool {
	if peer == nil {
		return false
	}
	switch peer.GetRole() {
	case metapb.PeerRole_DemotingVoter, metapb.PeerRole_Learner:
		return true
	}
	return false
}

// IsInJointState judges whether the Peer is in joint state.
func IsInJointState(peers ...*metapb.Peer) bool {
	for _, peer := range peers {
		switch peer.GetRole() {
		case metapb.PeerRole_IncomingVoter, metapb.PeerRole_DemotingVoter:
			return true
		default:
		}
	}
	return false
}

// CountInJointState count the peers are in joint state.
func CountInJointState(peers ...*metapb.Peer) int {
	count := 0
	for _, peer := range peers {
		switch peer.GetRole() {
		case metapb.PeerRole_IncomingVoter, metapb.PeerRole_DemotingVoter:
			count++
		default:
		}
	}
	return count
}

// PeerInfo provides peer information
type PeerInfo struct {
	peerID       uint64
	StoreID      uint64
	writtenBytes uint64
	writtenKeys  uint64
	readBytes    uint64
	readKeys     uint64
}

// GetWrittenKeys provides peer written keys
func (p *PeerInfo) GetWrittenKeys() uint64 {
	return p.writtenKeys
}

// GetWrittenBytes provides peer written bytes
func (p *PeerInfo) GetWrittenBytes() uint64 {
	return p.writtenBytes
}

// GetReadBytes provides peer read bytes
func (p *PeerInfo) GetReadBytes() uint64 {
	return p.readBytes
}

// GetReadKeys provides read keys
func (p *PeerInfo) GetReadKeys() uint64 {
	return p.readKeys
}

// GetStoreID provides located storeID
func (p *PeerInfo) GetStoreID() uint64 {
	return p.StoreID
}

// GetPeerID provides peer id
func (p *PeerInfo) GetPeerID() uint64 {
	return p.peerID
}

// FromMetaPeer provides PeerInfo from metapb.Peer
func FromMetaPeer(peer *metapb.Peer) *PeerInfo {
	return &PeerInfo{
		peerID:  peer.GetId(),
		StoreID: peer.GetStoreId(),
	}
}

// SetReadBytes sets read bytes
func (p *PeerInfo) SetReadBytes(b uint64) *PeerInfo {
	p.readBytes = b
	return p
}

// SetReadKeys sets read keys
func (p *PeerInfo) SetReadKeys(k uint64) *PeerInfo {
	p.readKeys = k
	return p
}

// SetWriteBytes sets write bytes
func (p *PeerInfo) SetWriteBytes(b uint64) *PeerInfo {
	p.writtenBytes = b
	return p
}

// SetWriteKeys sets write keys
func (p *PeerInfo) SetWriteKeys(k uint64) *PeerInfo {
	p.writtenKeys = k
	return p
}
