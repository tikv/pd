// Copyright 2026 TiKV Project Authors.
// Licensed under the Apache License, Version 2.0.

package affinity

import (
	"fmt"
	"slices"
	"strings"

	"github.com/tikv/pd/pkg/core"
)

// AutoBalanceGroup performs at most one automatic target update for the table
// containing groupID. The caller should invoke it again after the target has
// converged; this keeps each scheduling round limited to one group change.
func (m *Manager) AutoBalanceGroup(groupID string) (*BalancePlan, error) {
	m.RLock()
	group, ok := m.groups[groupID]
	if !ok {
		m.RUnlock()
		return nil, fmt.Errorf("affinity group %q does not exist", groupID)
	}
	if group.BalancePolicy == BalancePolicyFixed || group.GetAvailability() != groupAvailable {
		m.RUnlock()
		return nil, nil
	}
	groups := groupsForTableLocked(m.groups, groupID)
	available := availableStoreIDs(m.storeSetInformer, m.unavailableStores)
	replicaCount := len(group.VoterStoreIDs)
	affinityVer := group.AffinityVer
	m.RUnlock()

	if replicaCount == 0 || len(available) < replicaCount {
		return nil, nil
	}
	plans, err := PlanBalance(groups, BalanceOptions{
		Stores:       available,
		ReplicaCount: replicaCount,
		ValidTarget:  m.validBalanceTarget,
	})
	if err != nil {
		return nil, err
	}
	for i := range plans {
		if !plans[i].Changed {
			continue
		}
		if plans[i].GroupID != groupID {
			continue
		}
		// Carry the version read with the snapshot into the write path. A
		// concurrent admin update changes the version and causes this plan to
		// be discarded instead of overwriting the newer target.
		if _, err := m.updateAffinityGroupPeersWithBalanceVer(plans[i].GroupID, affinityVer, plans[i].LeaderStoreID, plans[i].VoterStoreIDs); err != nil {
			return nil, err
		}
		return &plans[i], nil
	}
	return nil, nil
}

func groupsForTableLocked(groups map[string]*runtimeGroupInfo, groupID string) []*Group {
	prefix := tableGroupPrefix(groupID)
	result := make([]*Group, 0, len(groups))
	for id, group := range groups {
		if tableGroupPrefix(id) != prefix {
			continue
		}
		copy := group.Group
		copy.VoterStoreIDs = slices.Clone(group.VoterStoreIDs)
		result = append(result, &copy)
	}
	return result
}

func tableGroupPrefix(groupID string) string {
	if strings.HasPrefix(groupID, "_tidb_pt_") {
		if index := strings.LastIndex(groupID, "_p"); index > 0 {
			return groupID[:index]
		}
	}
	return groupID
}

func availableStoreIDs(informer core.StoreSetInformer, unavailable map[uint64]storeCondition) []uint64 {
	if informer == nil {
		return nil
	}
	ids := make([]uint64, 0)
	for _, store := range informer.GetStores() {
		if store == nil || !store.IsUp() || !store.IsTiKV() {
			continue
		}
		if _, unavailable := unavailable[store.GetID()]; unavailable {
			continue
		}
		ids = append(ids, store.GetID())
	}
	return ids
}

func (m *Manager) validBalanceTarget(_ *Group, leader uint64, voters []uint64) bool {
	if leader == 0 || !slices.Contains(voters, leader) {
		return false
	}
	for _, storeID := range voters {
		if store := m.storeSetInformer.GetStore(storeID); store == nil || !store.IsUp() || !store.IsTiKV() {
			return false
		}
	}
	return true
}
