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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

// IsVoter judges whether the Peer's Role is Voter
func IsVoter(peer *metapb.Peer) bool {
	return peer.GetRole() == metapb.PeerRole_Voter
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
	*metapb.Peer
	loads    []float64
	interval uint64
}

// NewPeerInfo creates PeerInfo
func NewPeerInfo(meta *metapb.Peer, loads []float64, interval uint64) *PeerInfo {
	return &PeerInfo{
		Peer:     meta,
		loads:    loads,
		interval: interval,
	}
}

// GetStoreID provides located storeID
func (p *PeerInfo) GetStoreID() uint64 {
	return p.GetStoreId()
}

// GetLoads provides loads
func (p *PeerInfo) GetLoads() []float64 {
	return p.loads
}

// GetPeerID provides peer id
func (p *PeerInfo) GetPeerID() uint64 {
	return p.GetId()
}

// GetInterval returns reporting interval
func (p *PeerInfo) GetInterval() uint64 {
	return p.interval
}
