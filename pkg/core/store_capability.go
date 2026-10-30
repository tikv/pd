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

package core

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
)

const (
	// StoreCapabilityLabel transports TiKV's startup-fixed Store capability.
	// Ordinary placement labels do not authorize changing this declaration.
	StoreCapabilityLabel = "cse-store-capability"
	// FullStoreCapability admits only peers that maintain user data.
	FullStoreCapability = "full-v1"
	// LogOnlyStoreCapability admits only LogOnly peers, including learners.
	LogOnlyStoreCapability = "log-only-v1"
)

// ParseStoreCapability validates the versioned declaration. An absent label
// retains legacy Store semantics; unknown versions and duplicates fail closed.
func ParseStoreCapability(store *metapb.Store) (string, error) {
	capability, found := "", false
	for _, label := range store.GetLabels() {
		if !strings.EqualFold(label.GetKey(), StoreCapabilityLabel) {
			continue
		}
		if found {
			return "", errors.New("duplicate Store capability declaration")
		}
		found = true
		capability = label.GetValue()
		if capability != FullStoreCapability && capability != LogOnlyStoreCapability {
			return "", errors.Errorf("unsupported Store capability %q", capability)
		}
	}
	return capability, nil
}

// SupportsPeerCapability checks data capability independently of voter/learner
// membership. Unmarked Stores preserve the existing witness behavior.
func (s *StoreInfo) SupportsPeerCapability(isWitness bool) bool {
	if s == nil {
		return false
	}
	capability, err := ParseStoreCapability(s.GetMeta())
	if err != nil {
		return false
	}
	switch capability {
	case "":
		return true
	case FullStoreCapability:
		return !isWitness
	case LogOnlyStoreCapability:
		return isWitness
	default:
		return false
	}
}

// HasFixedPeerCapability reports whether in-place data capability changes are
// forbidden on this Store. Invalid declarations are also non-convertible.
func (s *StoreInfo) HasFixedPeerCapability() bool {
	if s == nil {
		return false
	}
	capability, err := ParseStoreCapability(s.GetMeta())
	return err != nil || capability != ""
}

// HasDedicatedLogOnlyPeer reports whether a peer group uses fixed-role LogOnly
// hardware, independently of legacy witness groups and their protocol version.
func HasDedicatedLogOnlyPeer(stores StoreSetInformer, peers []*metapb.Peer) bool {
	for _, peer := range peers {
		store := stores.GetStore(peer.GetStoreId())
		if peer.GetIsWitness() && store != nil && store.GetLabelValue(StoreCapabilityLabel) == LogOnlyStoreCapability {
			return true
		}
	}
	return false
}

// RequiredPeerProtocolVersion combines the Region's durable reader requirement
// with the protocol required by its current dedicated LogOnly membership.
func RequiredPeerProtocolVersion(stores StoreSetInformer, region *RegionInfo) uint32 {
	minimum := region.GetRequiredReaderProtocolVersion()
	if HasDedicatedLogOnlyPeer(stores, region.GetPeers()) {
		minimum = max(minimum, LogOnlyProtocolVersion)
	}
	return minimum
}

// CheckLogOnlyPeerProtocols validates the complete target group on membership
// activation. Learners are readers too. A durable reader requirement outlives
// dedicated LogOnly membership and cannot be inferred from Store capability.
func CheckLogOnlyPeerProtocols(stores StoreSetInformer, peers []*metapb.Peer, minimum uint32) error {
	for _, peer := range peers {
		if peer.GetIsWitness() && stores.GetStore(peer.GetStoreId()) == nil {
			return errors.Errorf("cannot verify protocol for unknown witness Store %d", peer.GetStoreId())
		}
	}
	if HasDedicatedLogOnlyPeer(stores, peers) {
		minimum = max(minimum, LogOnlyProtocolVersion)
	}
	if minimum == 0 {
		return nil
	}
	for _, peer := range peers {
		store := stores.GetStore(peer.GetStoreId())
		if store == nil || store.GetMeta().GetLogOnlyProtocolVersion() < minimum {
			return errors.Errorf("store %d does not support LogOnly protocol %d", peer.GetStoreId(), minimum)
		}
	}
	return nil
}
