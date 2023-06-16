// Copyright 2023 TiKV Project Authors.
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

package keyspace

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/balancer"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

const (
	defaultBalancerPolicy              = balancer.PolicyRoundRobin
	allocNodesToKeyspaceGroupsInterval = 1 * time.Second
	allocNodesTimeout                  = 1 * time.Second
	allocNodesInterval                 = 10 * time.Millisecond
)

const (
	opAdd int = iota
	opDelete
)

// GroupManager is the manager of keyspace group related data.
type GroupManager struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	client    *clientv3.Client
	clusterID uint64

	sync.RWMutex
	// groups is the cache of keyspace group related information.
	// user kind -> keyspace group
	groups map[endpoint.UserKind]*indexedHeap

	// store is the storage for keyspace group related information.
	store endpoint.KeyspaceGroupStorage

	// nodeBalancer is the balancer for tso nodes.
	// TODO: add user kind with different balancer when we ensure where the correspondence between tso node and user kind will be found
	nodesBalancer balancer.Balancer[string]
	// serviceRegistryMap stores the mapping from the service registry key to the service address.
	// Note: it is only used in tsoNodesWatcher.
	serviceRegistryMap map[string]string
	// tsoNodesWatcher is the watcher for the registered tso servers.
	tsoNodesWatcher *etcdutil.LoopWatcher
}

// NewKeyspaceGroupManager creates a Manager of keyspace group related data.
func NewKeyspaceGroupManager(
	ctx context.Context,
	store endpoint.KeyspaceGroupStorage,
	client *clientv3.Client,
	clusterID uint64,
) *GroupManager {
	ctx, cancel := context.WithCancel(ctx)
	groups := make(map[endpoint.UserKind]*indexedHeap)
	for i := 0; i < int(endpoint.UserKindCount); i++ {
		groups[endpoint.UserKind(i)] = newIndexedHeap(int(utils.MaxKeyspaceGroupCountInUse))
	}
	m := &GroupManager{
		ctx:                ctx,
		cancel:             cancel,
		store:              store,
		groups:             groups,
		client:             client,
		clusterID:          clusterID,
		nodesBalancer:      balancer.GenByPolicy[string](defaultBalancerPolicy),
		serviceRegistryMap: make(map[string]string),
	}

	// If the etcd client is not nil, start the watch loop for the registered tso servers.
	// The PD(TSO) Client relies on this info to discover tso servers.
	if m.client != nil {
		m.initTSONodesWatcher(m.client, m.clusterID)
		m.wg.Add(1)
		go m.tsoNodesWatcher.StartWatchLoop()
	}
	return m
}

// Bootstrap saves default keyspace group info and init group mapping in the memory.
func (m *GroupManager) Bootstrap(ctx context.Context) error {
	// Force the membership restriction that the default keyspace must belong to default keyspace group.
	// Have no information to specify the distribution of the default keyspace group replicas, so just
	// leave the replica/member list empty. The TSO service will assign the default keyspace group replica
	// to every tso node/pod by default.
	defaultKeyspaceGroup := &endpoint.KeyspaceGroup{
		ID:        utils.DefaultKeyspaceGroupID,
		UserKind:  endpoint.Basic.String(),
		Keyspaces: []uint32{utils.DefaultKeyspaceID},
	}

	m.Lock()
	defer m.Unlock()

	// Ignore the error if default keyspace group already exists in the storage (e.g. PD restart/recover).
	err := m.saveKeyspaceGroups([]*endpoint.KeyspaceGroup{defaultKeyspaceGroup}, false)
	if err != nil && err != ErrKeyspaceGroupExists {
		return err
	}

	// Load all the keyspace groups from the storage and add to the respective userKind groups.
	groups, err := m.store.LoadKeyspaceGroups(utils.DefaultKeyspaceGroupID, 0)
	if err != nil {
		return err
	}
	for _, group := range groups {
		userKind := endpoint.StringUserKind(group.UserKind)
		m.groups[userKind].Put(group)
	}

	// It will only alloc node when the group manager is on API leader.
	if m.client != nil {
		m.wg.Add(1)
		go m.allocNodesToAllKeyspaceGroups(ctx)
	}
	return nil
}

// Close closes the manager.
func (m *GroupManager) Close() {
	m.cancel()
	m.wg.Wait()
}

func (m *GroupManager) allocNodesToAllKeyspaceGroups(ctx context.Context) {
	defer logutil.LogPanic()
	defer m.wg.Done()
	ticker := time.NewTicker(allocNodesToKeyspaceGroupsInterval)
	failpoint.Inject("acceleratedAllocNodes", func() {
		ticker.Stop()
		ticker = time.NewTicker(time.Millisecond * 100)
	})
	defer ticker.Stop()
	log.Info("start to alloc nodes to all keyspace groups")
	for {
		select {
		case <-ctx.Done():
			log.Info("stop to alloc nodes to all keyspace groups")
			return
		case <-ticker.C:
		}
		countOfNodes := m.GetNodesCount()
		if countOfNodes < utils.DefaultKeyspaceGroupReplicaCount {
			continue
		}
		groups, err := m.store.LoadKeyspaceGroups(utils.DefaultKeyspaceGroupID, 0)
		if err != nil {
			log.Error("failed to load all keyspace groups", zap.Error(err))
			continue
		}
		// if the default keyspace is not initialized, we should wait for the default keyspace to be initialized.
		if len(groups) == 0 {
			continue
		}
		withError := false
		for _, group := range groups {
			if len(group.Members) < utils.DefaultKeyspaceGroupReplicaCount {
				nodes, err := m.AllocNodesForKeyspaceGroup(group.ID, utils.DefaultKeyspaceGroupReplicaCount)
				if err != nil {
					withError = true
					log.Error("failed to alloc nodes for keyspace group", zap.Uint32("keyspace-group-id", group.ID), zap.Error(err))
					continue
				}
				group.Members = nodes
			}
		}
		if !withError {
			// all keyspace groups have equal or more than default replica count
			log.Info("all keyspace groups have equal or more than default replica count, stop to alloc node")
			return
		}
	}
}

func (m *GroupManager) initTSONodesWatcher(client *clientv3.Client, clusterID uint64) {
	tsoServiceKey := discovery.TSOPath(clusterID)
	tsoServiceEndKey := clientv3.GetPrefixRangeEnd(tsoServiceKey)

	putFn := func(kv *mvccpb.KeyValue) error {
		s := &discovery.ServiceRegistryEntry{}
		if err := json.Unmarshal(kv.Value, s); err != nil {
			log.Warn("failed to unmarshal service registry entry",
				zap.String("event-kv-key", string(kv.Key)), zap.Error(err))
			return err
		}
		m.nodesBalancer.Put(s.ServiceAddr)
		m.serviceRegistryMap[string(kv.Key)] = s.ServiceAddr
		return nil
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		if serviceAddr, ok := m.serviceRegistryMap[key]; ok {
			delete(m.serviceRegistryMap, key)
			m.nodesBalancer.Delete(serviceAddr)
			return nil
		}
		return errors.Errorf("failed to find the service address for key %s", key)
	}

	m.tsoNodesWatcher = etcdutil.NewLoopWatcher(
		m.ctx,
		&m.wg,
		client,
		"tso-nodes-watcher",
		tsoServiceKey,
		putFn,
		deleteFn,
		func() error { return nil },
		clientv3.WithRange(tsoServiceEndKey),
	)
}

// CreateKeyspaceGroups creates keyspace groups.
func (m *GroupManager) CreateKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup) error {
	m.Lock()
	defer m.Unlock()
	if err := m.saveKeyspaceGroups(keyspaceGroups, false); err != nil {
		return err
	}

	for _, keyspaceGroup := range keyspaceGroups {
		userKind := endpoint.StringUserKind(keyspaceGroup.UserKind)
		m.groups[userKind].Put(keyspaceGroup)
	}

	return nil
}

// GetTSOServiceAddrs gets all TSO service addresses.
func (m *GroupManager) GetTSOServiceAddrs() []string {
	if m == nil || m.nodesBalancer == nil {
		return nil
	}
	return m.nodesBalancer.GetAll()
}

// GetKeyspaceGroups gets keyspace groups from the start ID with limit.
// If limit is 0, it will load all keyspace groups from the start ID.
func (m *GroupManager) GetKeyspaceGroups(startID uint32, limit int) ([]*endpoint.KeyspaceGroup, error) {
	return m.store.LoadKeyspaceGroups(startID, limit)
}

// GetKeyspaceGroupByID returns the keyspace group by ID.
func (m *GroupManager) GetKeyspaceGroupByID(id uint32) (*endpoint.KeyspaceGroup, error) {
	var (
		kg  *endpoint.KeyspaceGroup
		err error
	)

	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return kg, nil
}

// DeleteKeyspaceGroupByID deletes the keyspace group by ID.
func (m *GroupManager) DeleteKeyspaceGroupByID(id uint32) (*endpoint.KeyspaceGroup, error) {
	var (
		kg  *endpoint.KeyspaceGroup
		err error
	)

	m.Lock()
	defer m.Unlock()
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if kg == nil {
			return nil
		}
		if kg.IsSplitting() {
			return ErrKeyspaceGroupInSplit
		}
		return m.store.DeleteKeyspaceGroup(txn, id)
	}); err != nil {
		return nil, err
	}

	userKind := endpoint.StringUserKind(kg.UserKind)
	// TODO: move out the keyspace to another group
	// we don't need the keyspace group as the return value
	m.groups[userKind].Remove(id)

	return kg, nil
}

// saveKeyspaceGroups will try to save the given keyspace groups into the storage.
// If any keyspace group already exists and `overwrite` is false, it will return ErrKeyspaceGroupExists.
func (m *GroupManager) saveKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup, overwrite bool) error {
	return m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		for _, keyspaceGroup := range keyspaceGroups {
			// Check if keyspace group has already existed.
			oldKG, err := m.store.LoadKeyspaceGroup(txn, keyspaceGroup.ID)
			if err != nil {
				return err
			}
			if oldKG != nil && !overwrite {
				return ErrKeyspaceGroupExists
			}
			if oldKG.IsSplitting() && overwrite {
				return ErrKeyspaceGroupInSplit
			}
			if oldKG.IsMerging() && overwrite {
				return ErrKeyspaceGroupInMerging
			}
			newKG := &endpoint.KeyspaceGroup{
				ID:        keyspaceGroup.ID,
				UserKind:  keyspaceGroup.UserKind,
				Members:   keyspaceGroup.Members,
				Keyspaces: keyspaceGroup.Keyspaces,
			}
			err = m.store.SaveKeyspaceGroup(txn, newKG)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// GetKeyspaceConfigByKind returns the keyspace config for the given user kind.
func (m *GroupManager) GetKeyspaceConfigByKind(userKind endpoint.UserKind) (map[string]string, error) {
	// when server is not in API mode, we don't need to return the keyspace config
	if m == nil {
		return map[string]string{}, nil
	}
	m.RLock()
	defer m.RUnlock()
	return m.getKeyspaceConfigByKindLocked(userKind)
}

func (m *GroupManager) getKeyspaceConfigByKindLocked(userKind endpoint.UserKind) (map[string]string, error) {
	groups, ok := m.groups[userKind]
	if !ok {
		return map[string]string{}, errors.Errorf("user kind %s not found", userKind)
	}
	kg := groups.Top()
	if kg == nil {
		return map[string]string{}, errors.Errorf("no keyspace group for user kind %s", userKind)
	}
	id := strconv.FormatUint(uint64(kg.ID), 10)
	config := map[string]string{
		UserKindKey:           userKind.String(),
		TSOKeyspaceGroupIDKey: id,
	}
	return config, nil
}

var failpointOnce sync.Once

// UpdateKeyspaceForGroup updates the keyspace field for the keyspace group.
func (m *GroupManager) UpdateKeyspaceForGroup(userKind endpoint.UserKind, groupID string, keyspaceID uint32, mutation int) error {
	// when server is not in API mode, we don't need to update the keyspace for keyspace group
	if m == nil {
		return nil
	}
	id, err := strconv.ParseUint(groupID, 10, 64)
	if err != nil {
		return err
	}

	failpoint.Inject("externalAllocNode", func(val failpoint.Value) {
		failpointOnce.Do(func() {
			addrs := val.(string)
			m.SetNodesForKeyspaceGroup(utils.DefaultKeyspaceGroupID, strings.Split(addrs, ","))
		})
	})
	m.Lock()
	defer m.Unlock()
	return m.updateKeyspaceForGroupLocked(userKind, id, keyspaceID, mutation)
}

func (m *GroupManager) updateKeyspaceForGroupLocked(userKind endpoint.UserKind, groupID uint64, keyspaceID uint32, mutation int) error {
	kg := m.groups[userKind].Get(uint32(groupID))
	if kg == nil {
		return errors.Errorf("keyspace group %d not found", groupID)
	}
	if kg.IsSplitting() {
		return ErrKeyspaceGroupInSplit
	}
	if kg.IsMerging() {
		return ErrKeyspaceGroupInMerging
	}

	changed := false

	switch mutation {
	case opAdd:
		if !slice.Contains(kg.Keyspaces, keyspaceID) {
			kg.Keyspaces = append(kg.Keyspaces, keyspaceID)
			changed = true
		}
	case opDelete:
		lenOfKeyspaces := len(kg.Keyspaces)
		kg.Keyspaces = slice.Remove(kg.Keyspaces, keyspaceID)
		if lenOfKeyspaces != len(kg.Keyspaces) {
			changed = true
		}
	}

	if changed {
		if err := m.saveKeyspaceGroups([]*endpoint.KeyspaceGroup{kg}, true); err != nil {
			return err
		}
		m.groups[userKind].Put(kg)
	}
	return nil
}

// UpdateKeyspaceGroup updates the keyspace group.
func (m *GroupManager) UpdateKeyspaceGroup(oldGroupID, newGroupID string, oldUserKind, newUserKind endpoint.UserKind, keyspaceID uint32) error {
	// when server is not in API mode, we don't need to update the keyspace group
	if m == nil {
		return nil
	}
	oldID, err := strconv.ParseUint(oldGroupID, 10, 64)
	if err != nil {
		return err
	}
	newID, err := strconv.ParseUint(newGroupID, 10, 64)
	if err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()
	oldKG := m.groups[oldUserKind].Get(uint32(oldID))
	if oldKG == nil {
		return errors.Errorf("keyspace group %s not found in %s group", oldGroupID, oldUserKind)
	}
	newKG := m.groups[newUserKind].Get(uint32(newID))
	if newKG == nil {
		return errors.Errorf("keyspace group %s not found in %s group", newGroupID, newUserKind)
	}
	if oldKG.IsSplitting() || newKG.IsSplitting() {
		return ErrKeyspaceGroupInSplit
	}
	if oldKG.IsMerging() || newKG.IsMerging() {
		return ErrKeyspaceGroupInMerging
	}

	var updateOld, updateNew bool
	if !slice.Contains(newKG.Keyspaces, keyspaceID) {
		newKG.Keyspaces = append(newKG.Keyspaces, keyspaceID)
		updateNew = true
	}

	lenOfOldKeyspaces := len(oldKG.Keyspaces)
	oldKG.Keyspaces = slice.Remove(oldKG.Keyspaces, keyspaceID)
	if lenOfOldKeyspaces != len(oldKG.Keyspaces) {
		updateOld = true
	}

	if err := m.saveKeyspaceGroups([]*endpoint.KeyspaceGroup{oldKG, newKG}, true); err != nil {
		return err
	}

	if updateOld {
		m.groups[oldUserKind].Put(oldKG)
	}

	if updateNew {
		m.groups[newUserKind].Put(newKG)
	}

	return nil
}

// SplitKeyspaceGroupByID splits the keyspace group by ID into a new keyspace group with the given new ID.
// And the keyspaces in the old keyspace group will be moved to the new keyspace group.
func (m *GroupManager) SplitKeyspaceGroupByID(splitSourceID, splitTargetID uint32, keyspaces []uint32) error {
	var splitSourceKg, splitTargetKg *endpoint.KeyspaceGroup
	m.Lock()
	defer m.Unlock()
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
		// Load the old keyspace group first.
		splitSourceKg, err = m.store.LoadKeyspaceGroup(txn, splitSourceID)
		if err != nil {
			return err
		}
		if splitSourceKg == nil {
			return ErrKeyspaceGroupNotExists
		}
		// A keyspace group can not take part in multiple split processes.
		if splitSourceKg.IsSplitting() {
			return ErrKeyspaceGroupInSplit
		}
		// A keyspace group can not be split when it is in merging.
		if splitSourceKg.IsMerging() {
			return ErrKeyspaceGroupInMerging
		}
		// Check if the source keyspace group has enough replicas.
		if len(splitSourceKg.Members) < utils.DefaultKeyspaceGroupReplicaCount {
			return ErrKeyspaceGroupNotEnoughReplicas
		}
		// Check if the new keyspace group already exists.
		splitTargetKg, err = m.store.LoadKeyspaceGroup(txn, splitTargetID)
		if err != nil {
			return err
		}
		if splitTargetKg != nil {
			return ErrKeyspaceGroupExists
		}
		keyspaceNum := len(keyspaces)
		sourceKeyspaceNum := len(splitSourceKg.Keyspaces)
		// Check if the keyspaces are all in the old keyspace group.
		if keyspaceNum == 0 || keyspaceNum > sourceKeyspaceNum {
			return ErrKeyspaceNotInKeyspaceGroup
		}
		var (
			oldKeyspaceMap = make(map[uint32]struct{}, sourceKeyspaceNum)
			newKeyspaceMap = make(map[uint32]struct{}, keyspaceNum)
		)
		for _, keyspace := range splitSourceKg.Keyspaces {
			oldKeyspaceMap[keyspace] = struct{}{}
		}
		for _, keyspace := range keyspaces {
			if _, ok := oldKeyspaceMap[keyspace]; !ok {
				return ErrKeyspaceNotInKeyspaceGroup
			}
			newKeyspaceMap[keyspace] = struct{}{}
		}
		// Get the split keyspace group for the old keyspace group.
		splitKeyspaces := make([]uint32, 0, sourceKeyspaceNum-keyspaceNum)
		for _, keyspace := range splitSourceKg.Keyspaces {
			if _, ok := newKeyspaceMap[keyspace]; !ok {
				splitKeyspaces = append(splitKeyspaces, keyspace)
			}
		}
		// Update the old keyspace group.
		splitSourceKg.Keyspaces = splitKeyspaces
		splitSourceKg.SplitState = &endpoint.SplitState{
			SplitSource: splitSourceKg.ID,
		}
		if err = m.store.SaveKeyspaceGroup(txn, splitSourceKg); err != nil {
			return err
		}
		splitTargetKg = &endpoint.KeyspaceGroup{
			ID: splitTargetID,
			// Keep the same user kind and members as the old keyspace group.
			UserKind:  splitSourceKg.UserKind,
			Members:   splitSourceKg.Members,
			Keyspaces: keyspaces,
			SplitState: &endpoint.SplitState{
				SplitSource: splitSourceKg.ID,
			},
		}
		// Create the new split keyspace group.
		return m.store.SaveKeyspaceGroup(txn, splitTargetKg)
	}); err != nil {
		return err
	}
	// Update the keyspace group cache.
	m.groups[endpoint.StringUserKind(splitSourceKg.UserKind)].Put(splitSourceKg)
	m.groups[endpoint.StringUserKind(splitTargetKg.UserKind)].Put(splitTargetKg)
	return nil
}

// FinishSplitKeyspaceByID finishes the split keyspace group by the split target ID.
func (m *GroupManager) FinishSplitKeyspaceByID(splitTargetID uint32) error {
	var splitTargetKg, splitSourceKg *endpoint.KeyspaceGroup
	m.Lock()
	defer m.Unlock()
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
		// Load the split target keyspace group first.
		splitTargetKg, err = m.store.LoadKeyspaceGroup(txn, splitTargetID)
		if err != nil {
			return err
		}
		if splitTargetKg == nil {
			return ErrKeyspaceGroupNotExists
		}
		// Check if it's in the split state.
		if !splitTargetKg.IsSplitTarget() {
			return ErrKeyspaceGroupNotInSplit
		}
		// Load the split source keyspace group then.
		splitSourceKg, err = m.store.LoadKeyspaceGroup(txn, splitTargetKg.SplitSource())
		if err != nil {
			return err
		}
		if splitSourceKg == nil {
			return ErrKeyspaceGroupNotExists
		}
		if !splitSourceKg.IsSplitSource() {
			return ErrKeyspaceGroupNotInSplit
		}
		splitTargetKg.SplitState = nil
		splitSourceKg.SplitState = nil
		err = m.store.SaveKeyspaceGroup(txn, splitTargetKg)
		if err != nil {
			return err
		}
		return m.store.SaveKeyspaceGroup(txn, splitSourceKg)
	}); err != nil {
		return err
	}
	// Update the keyspace group cache.
	m.groups[endpoint.StringUserKind(splitTargetKg.UserKind)].Put(splitTargetKg)
	m.groups[endpoint.StringUserKind(splitSourceKg.UserKind)].Put(splitSourceKg)
	return nil
}

// GetNodesCount returns the count of nodes.
func (m *GroupManager) GetNodesCount() int {
	if m.nodesBalancer == nil {
		return 0
	}
	return m.nodesBalancer.Len()
}

// AllocNodesForKeyspaceGroup allocates nodes for the keyspace group.
func (m *GroupManager) AllocNodesForKeyspaceGroup(id uint32, desiredReplicaCount int) ([]endpoint.KeyspaceGroupMember, error) {
	m.Lock()
	defer m.Unlock()
	ctx, cancel := context.WithTimeout(m.ctx, allocNodesTimeout)
	defer cancel()
	ticker := time.NewTicker(allocNodesInterval)
	defer ticker.Stop()

	var kg *endpoint.KeyspaceGroup
	nodes := make([]endpoint.KeyspaceGroupMember, 0, desiredReplicaCount)
	err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		var err error
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if kg == nil {
			return ErrKeyspaceGroupNotExists
		}
		if kg.IsSplitting() {
			return ErrKeyspaceGroupInSplit
		}
		if kg.IsMerging() {
			return ErrKeyspaceGroupInMerging
		}
		exists := make(map[string]struct{})
		for _, member := range kg.Members {
			exists[member.Address] = struct{}{}
			nodes = append(nodes, member)
		}
		if len(exists) >= desiredReplicaCount {
			return nil
		}
		for len(exists) < desiredReplicaCount {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
			countOfNodes := m.GetNodesCount()
			if countOfNodes < desiredReplicaCount || countOfNodes == 0 { // double check
				return ErrNoAvailableNode
			}
			addr := m.nodesBalancer.Next()
			if addr == "" {
				return ErrNoAvailableNode
			}
			if _, ok := exists[addr]; ok {
				continue
			}
			exists[addr] = struct{}{}
			nodes = append(nodes, endpoint.KeyspaceGroupMember{
				Address:  addr,
				Priority: utils.DefaultKeyspaceGroupReplicaPriority,
			})
		}
		kg.Members = nodes
		return m.store.SaveKeyspaceGroup(txn, kg)
	})
	if err != nil {
		return nil, err
	}
	m.groups[endpoint.StringUserKind(kg.UserKind)].Put(kg)
	log.Info("alloc nodes for keyspace group", zap.Uint32("keyspace-group-id", id), zap.Reflect("nodes", nodes))
	return nodes, nil
}

// SetNodesForKeyspaceGroup sets the nodes for the keyspace group.
func (m *GroupManager) SetNodesForKeyspaceGroup(id uint32, nodes []string) error {
	m.Lock()
	defer m.Unlock()
	var kg *endpoint.KeyspaceGroup
	err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		var err error
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if kg == nil {
			return ErrKeyspaceGroupNotExists
		}
		if kg.IsSplitting() {
			return ErrKeyspaceGroupInSplit
		}
		if kg.IsMerging() {
			return ErrKeyspaceGroupInMerging
		}
		members := make([]endpoint.KeyspaceGroupMember, 0, len(nodes))
		for _, node := range nodes {
			members = append(members, endpoint.KeyspaceGroupMember{
				Address:  node,
				Priority: utils.DefaultKeyspaceGroupReplicaPriority,
			})
		}
		kg.Members = members
		return m.store.SaveKeyspaceGroup(txn, kg)
	})
	if err != nil {
		return err
	}
	m.groups[endpoint.StringUserKind(kg.UserKind)].Put(kg)
	return nil
}

// SetPriorityForKeyspaceGroup sets the priority of node for the keyspace group.
func (m *GroupManager) SetPriorityForKeyspaceGroup(id uint32, node string, priority int) error {
	m.Lock()
	defer m.Unlock()
	var kg *endpoint.KeyspaceGroup
	err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		var err error
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if kg == nil {
			return ErrKeyspaceGroupNotExists
		}
		if kg.IsSplitting() {
			return ErrKeyspaceGroupInSplit
		}
		if kg.IsMerging() {
			return ErrKeyspaceGroupInMerging
		}
		inKeyspaceGroup := false
		members := make([]endpoint.KeyspaceGroupMember, 0, len(kg.Members))
		for _, member := range kg.Members {
			if member.Address == node {
				inKeyspaceGroup = true
				member.Priority = priority
			}
			members = append(members, member)
		}
		if !inKeyspaceGroup {
			return ErrNodeNotInKeyspaceGroup
		}
		kg.Members = members
		return m.store.SaveKeyspaceGroup(txn, kg)
	})
	if err != nil {
		return err
	}
	m.groups[endpoint.StringUserKind(kg.UserKind)].Put(kg)
	return nil
}

// IsExistNode checks if the node exists.
func (m *GroupManager) IsExistNode(addr string) bool {
	nodes := m.nodesBalancer.GetAll()
	for _, node := range nodes {
		if node == addr {
			return true
		}
	}
	return false
}

// MergeKeyspaceGroups merges the keyspace group in the list into the target keyspace group.
func (m *GroupManager) MergeKeyspaceGroups(mergeTargetID uint32, mergeList []uint32) error {
	mergeListNum := len(mergeList)
	if mergeListNum == 0 {
		return nil
	}
	// The transaction below will:
	//   - Load and delete the keyspace groups in the merge list.
	//   - Load and update the target keyspace group.
	// So we pre-check the number of operations to avoid exceeding the maximum number of etcd transaction.
	if (mergeListNum+1)*2 > maxEtcdTxnOps {
		return ErrExceedMaxEtcdTxnOps
	}
	var (
		groups        = make(map[uint32]*endpoint.KeyspaceGroup, mergeListNum+1)
		mergeTargetKg *endpoint.KeyspaceGroup
	)
	m.Lock()
	defer m.Unlock()
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
		// Load and check all keyspace groups first.
		for _, kgID := range append(mergeList, mergeTargetID) {
			kg, err := m.store.LoadKeyspaceGroup(txn, kgID)
			if err != nil {
				return err
			}
			if kg == nil {
				return ErrKeyspaceGroupNotExists
			}
			// A keyspace group can not be merged if it's in splitting.
			if kg.IsSplitting() {
				return ErrKeyspaceGroupInSplit
			}
			// A keyspace group can not be split when it is in merging.
			if kg.IsMerging() {
				return ErrKeyspaceGroupInMerging
			}
			groups[kgID] = kg
		}
		mergeTargetKg = groups[mergeTargetID]
		keyspaces := make(map[uint32]struct{})
		for _, keyspace := range mergeTargetKg.Keyspaces {
			keyspaces[keyspace] = struct{}{}
		}
		// Delete the keyspace groups in merge list and move the keyspaces in it to the target keyspace group.
		for _, kgID := range mergeList {
			kg := groups[kgID]
			for _, keyspace := range kg.Keyspaces {
				keyspaces[keyspace] = struct{}{}
			}
			if err := m.store.DeleteKeyspaceGroup(txn, kg.ID); err != nil {
				return err
			}
		}
		mergedKeyspaces := make([]uint32, 0, len(keyspaces))
		for keyspace := range keyspaces {
			mergedKeyspaces = append(mergedKeyspaces, keyspace)
		}
		sort.Slice(mergedKeyspaces, func(i, j int) bool {
			return mergedKeyspaces[i] < mergedKeyspaces[j]
		})
		mergeTargetKg.Keyspaces = mergedKeyspaces
		// Update the merge state of the target keyspace group.
		mergeTargetKg.MergeState = &endpoint.MergeState{
			MergeList: mergeList,
		}
		return m.store.SaveKeyspaceGroup(txn, mergeTargetKg)
	}); err != nil {
		return err
	}
	// Update the keyspace group cache.
	m.groups[endpoint.StringUserKind(mergeTargetKg.UserKind)].Put(mergeTargetKg)
	for _, kgID := range mergeList {
		kg := groups[kgID]
		m.groups[endpoint.StringUserKind(kg.UserKind)].Remove(kgID)
	}
	return nil
}

// FinishMergeKeyspaceByID finishes the merging keyspace group by the merge target ID.
func (m *GroupManager) FinishMergeKeyspaceByID(mergeTargetID uint32) error {
	var mergeTargetKg *endpoint.KeyspaceGroup
	m.Lock()
	defer m.Unlock()
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
		// Load the merge target keyspace group first.
		mergeTargetKg, err = m.store.LoadKeyspaceGroup(txn, mergeTargetID)
		if err != nil {
			return err
		}
		if mergeTargetKg == nil {
			return ErrKeyspaceGroupNotExists
		}
		// Check if it's in the merging state.
		if !mergeTargetKg.IsMergeTarget() {
			return ErrKeyspaceGroupNotInMerging
		}
		// Make sure all merging keyspace groups are deleted.
		for _, kgID := range mergeTargetKg.MergeState.MergeList {
			kg, err := m.store.LoadKeyspaceGroup(txn, kgID)
			if err != nil {
				return err
			}
			if kg != nil {
				return ErrKeyspaceGroupNotInMerging
			}
		}
		mergeTargetKg.MergeState = nil
		return m.store.SaveKeyspaceGroup(txn, mergeTargetKg)
	}); err != nil {
		return err
	}
	// Update the keyspace group cache.
	m.groups[endpoint.StringUserKind(mergeTargetKg.UserKind)].Put(mergeTargetKg)
	return nil
}
