// Copyright 2025 TiKV Project Authors.
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

package checker

import (
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
)

const (
	// nolint:unused
	affinityLabel = "affinity"
)

// AffinityChecker groups regions with affinity labels together by affinity group.
// It ensures regions adhere to affinity group constraints by creating operators.
type AffinityChecker struct {
	PauseController
	cluster         sche.CheckerCluster
	affinityManager *affinity.Manager
	conf            config.CheckerConfigProvider
}

// NewAffinityChecker create an affinity checker.
func NewAffinityChecker(cluster sche.CheckerCluster, affinityManager *affinity.Manager, conf config.CheckerConfigProvider) *AffinityChecker {
	return &AffinityChecker{
		cluster:         cluster,
		affinityManager: affinityManager,
		conf:            conf,
	}
}

// GetType return AffinityChecker's type.
// nolint:unused
func (*AffinityChecker) GetType() types.CheckerSchedulerType {
	return types.AffinityChecker
}

// Name returns AffinityChecker's name.
func (*AffinityChecker) Name() string {
	return types.AffinityChecker.String()
}

// Check verifies a region's replicas according to affinity group constraints, creating an Operator if needed.
func (c *AffinityChecker) Check(region *core.RegionInfo) []*operator.Operator {
	affinityCheckerCounter.Inc()

	if c.IsPaused() {
		affinityCheckerPausedCounter.Inc()
		return nil
	}

	// Check if region has a leader
	if region.GetLeader() == nil {
		affinityCheckerRegionNoLeaderCounter.Inc()
		return nil
	}

	// Get the affinity group for this region
	groupInfo := c.affinityManager.GetRegionAffinityGroup(region.GetID())
	if groupInfo == nil {
		// Region doesn't belong to any affinity group
		return nil
	}

	// Check if the group is in effect
	if !groupInfo.Effect {
		affinityCheckerGroupNotInEffectCounter.Inc()
		return nil
	}

	// Create operator to adjust region according to affinity group
	op := c.createAffinityOperator(region, groupInfo)
	if op != nil {
		affinityCheckerNewOpCounter.Inc()
		return []*operator.Operator{op}
	}

	return nil
}

// createAffinityOperator creates an operator to adjust region replicas according to affinity group constraints.
// Parameters:
//   - region: The region to adjust
//   - groupState: The affinity group info that defines the desired peer distribution
//
// Returns:
//   - *operator.Operator: The operator to adjust the region, or nil if no adjustment is needed
func (c *AffinityChecker) createAffinityOperator(region *core.RegionInfo, groupInfo *affinity.GroupState) *operator.Operator {
	currentLeaderStoreID := region.GetLeader().GetStoreId()
	expectedLeaderStoreID := groupInfo.LeaderStoreID

	// Check if leader needs transfer
	if currentLeaderStoreID != expectedLeaderStoreID {
		// Check if target leader store has a peer
		hasPeer := false
		for _, peer := range region.GetPeers() {
			if peer.GetStoreId() == expectedLeaderStoreID {
				hasPeer = true
				break
			}
		}

		if hasPeer {
			// Simple leader transfer
			op, err := operator.CreateTransferLeaderOperator(
				"affinity-transfer-leader",
				c.cluster,
				region,
				expectedLeaderStoreID,
				[]uint64{},
				operator.OpAffinity,
			)
			if err != nil {
				affinityCheckerCreateOpFailedCounter.Inc()
				return nil
			}
			return op
		}
	}

	// Check voters distribution
	currentVoterStores := make(map[uint64]bool)
	for _, peer := range region.GetVoters() {
		currentVoterStores[peer.GetStoreId()] = true
	}

	expectedVoterStores := make(map[uint64]bool)
	for _, storeID := range groupInfo.VoterStoreIDs {
		expectedVoterStores[storeID] = true
	}

	// Find a peer to remove (not in expected stores)
	var removeStoreID uint64
	for _, peer := range region.GetVoters() {
		storeID := peer.GetStoreId()
		if !expectedVoterStores[storeID] {
			removeStoreID = storeID
			break
		}
	}

	// Find a store to add (in expected but not in current)
	var addStoreID uint64
	for _, storeID := range groupInfo.VoterStoreIDs {
		if !currentVoterStores[storeID] {
			addStoreID = storeID
			break
		}
	}

	// Create appropriate operator based on what needs to be adjusted
	if removeStoreID != 0 && addStoreID != 0 {
		// Move peer from removeStore to addStore
		newPeer := &metapb.Peer{
			StoreId: addStoreID,
			Role:    metapb.PeerRole_Voter,
		}

		op, err := operator.CreateMovePeerOperator(
			"affinity-move-peer",
			c.cluster,
			region,
			operator.OpAffinity,
			removeStoreID,
			newPeer,
		)
		if err != nil {
			affinityCheckerCreateOpFailedCounter.Inc()
			return nil
		}
		return op
	}

	if addStoreID != 0 {
		// Add a peer
		newPeer := &metapb.Peer{
			StoreId: addStoreID,
			Role:    metapb.PeerRole_Voter,
		}

		op, err := operator.CreateAddPeerOperator(
			"affinity-add-peer",
			c.cluster,
			region,
			newPeer,
			operator.OpAffinity,
		)
		if err != nil {
			affinityCheckerCreateOpFailedCounter.Inc()
			return nil
		}
		return op
	}

	if removeStoreID != 0 {
		// Remove a peer
		op, err := operator.CreateRemovePeerOperator(
			"affinity-remove-peer",
			c.cluster,
			operator.OpAffinity,
			region,
			removeStoreID,
		)
		if err != nil {
			affinityCheckerCreateOpFailedCounter.Inc()
			return nil
		}
		return op
	}

	// No adjustment needed
	return nil
}
