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
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/balancer"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	defaultBalancerPolicy = balancer.PolicyRoundRobin
	allocNodeTimeout      = 1 * time.Second
	allocNodeInterval     = 10 * time.Millisecond
	// TODO: move it to etcdutil
	watchEtcdChangeRetryInterval = 1 * time.Second
	maxRetryTimes                = 25
	retryInterval                = 100 * time.Millisecond
)

const (
	opAdd int = iota
	opDelete
)

// GroupManager is the manager of keyspace group related data.
type GroupManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// the lock for the groups
	sync.RWMutex
	// groups is the cache of keyspace group related information.
	// user kind -> keyspace group
	groups map[endpoint.UserKind]*indexedHeap

	// store is the storage for keyspace group related information.
	store endpoint.KeyspaceGroupStorage

	client *clientv3.Client

	// tsoServiceKey is the path of TSO service in etcd.
	tsoServiceKey string
	// tsoServiceEndKey is the end key of TSO service in etcd.
	tsoServiceEndKey string

	policy balancer.Policy

	// TODO: add user kind with different balancer
	// when we ensure where the correspondence between tso node and user kind will be found
	nodesBalancer balancer.Balancer[string]
}

// NewKeyspaceGroupManager creates a Manager of keyspace group related data.
func NewKeyspaceGroupManager(ctx context.Context, store endpoint.KeyspaceGroupStorage, client *clientv3.Client, clusterID uint64) *GroupManager {
	ctx, cancel := context.WithCancel(ctx)
	key := discovery.TSOPath(clusterID)
	groups := make(map[endpoint.UserKind]*indexedHeap)
	for i := 0; i < int(endpoint.UserKindCount); i++ {
		groups[endpoint.UserKind(i)] = newIndexedHeap(int(utils.MaxKeyspaceGroupCountInUse))
	}
	return &GroupManager{
		ctx:              ctx,
		cancel:           cancel,
		store:            store,
		client:           client,
		tsoServiceKey:    key,
		tsoServiceEndKey: clientv3.GetPrefixRangeEnd(key) + "/",
		policy:           defaultBalancerPolicy,
		groups:           groups,
	}
}

// Bootstrap saves default keyspace group info and init group mapping in the memory.
func (m *GroupManager) Bootstrap() error {
	defaultKeyspaceGroup := &endpoint.KeyspaceGroup{
		ID:       utils.DefaultKeySpaceGroupID,
		UserKind: endpoint.Basic.String(),
	}

	m.Lock()
	defer m.Unlock()
	// Ignore the error if default keyspace group already exists in the storage (e.g. PD restart/recover).
	err := m.saveKeyspaceGroups([]*endpoint.KeyspaceGroup{defaultKeyspaceGroup}, false)
	if err != nil && err != ErrKeyspaceGroupExists {
		return err
	}

	userKind := endpoint.StringUserKind(defaultKeyspaceGroup.UserKind)
	m.groups[userKind].Put(defaultKeyspaceGroup)

	// Load all the keyspace groups from the storage and add to the respective userKind groups.
	groups, err := m.store.LoadKeyspaceGroups(utils.DefaultKeySpaceGroupID, 0)
	if err != nil {
		return err
	}
	for _, group := range groups {
		userKind := endpoint.StringUserKind(group.UserKind)
		m.groups[userKind].Put(group)
	}

	// If the etcd client is not nil, start the watch loop.
	if m.client != nil {
		m.nodesBalancer = balancer.GenByPolicy[string](m.policy)
		m.wg.Add(1)
		go m.startWatchLoop()
	}
	return nil
}

// Close closes the manager.
func (m *GroupManager) Close() {
	m.cancel()
	m.wg.Wait()
}

func (m *GroupManager) startWatchLoop() {
	defer m.wg.Done()
	ctx, cancel := context.WithCancel(m.ctx)
	defer cancel()
	var (
		revision int64
		err      error
	)
	for i := 0; i < maxRetryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryInterval):
		}
		resp, err := etcdutil.EtcdKVGet(m.client, m.tsoServiceKey, clientv3.WithRange(m.tsoServiceEndKey))
		if err == nil {
			revision = resp.Header.Revision
			for _, item := range resp.Kvs {
				s := &discovery.ServiceRegistryEntry{}
				if err := json.Unmarshal(item.Value, s); err != nil {
					log.Warn("failed to unmarshal service registry entry", zap.Error(err))
					continue
				}
				m.nodesBalancer.Put(s.ServiceAddr)
			}
			break
		}
	}
	if err != nil {
		log.Warn("failed to get tso service addrs from etcd", zap.Error(err))
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		nextRevision, err := m.watchServiceAddrs(ctx, revision)
		if err != nil {
			log.Error("watcher canceled unexpectedly and a new watcher will start after a while",
				zap.Int64("next-revision", nextRevision),
				zap.Time("retry-at", time.Now().Add(watchEtcdChangeRetryInterval)),
				zap.Error(err))
			revision = nextRevision
			time.Sleep(watchEtcdChangeRetryInterval)
		}
	}
}

func (m *GroupManager) watchServiceAddrs(ctx context.Context, revision int64) (int64, error) {
	watcher := clientv3.NewWatcher(m.client)
	defer watcher.Close()
	for {
	WatchChan:
		watchChan := watcher.Watch(ctx, m.tsoServiceKey, clientv3.WithRange(m.tsoServiceEndKey), clientv3.WithRev(revision))
		select {
		case <-ctx.Done():
			return revision, nil
		case wresp := <-watchChan:
			if wresp.CompactRevision != 0 {
				log.Warn("required revision has been compacted, the watcher will watch again with the compact revision",
					zap.Int64("required-revision", revision),
					zap.Int64("compact-revision", wresp.CompactRevision))
				revision = wresp.CompactRevision
				goto WatchChan
			}
			if wresp.Err() != nil {
				log.Error("watch is canceled or closed",
					zap.Int64("required-revision", revision),
					zap.Error(wresp.Err()))
				return revision, wresp.Err()
			}
			for _, event := range wresp.Events {
				s := &discovery.ServiceRegistryEntry{}
				if err := json.Unmarshal(event.Kv.Value, s); err != nil {
					log.Warn("failed to unmarshal service registry entry", zap.Error(err))
				}
				switch event.Type {
				case clientv3.EventTypePut:
					m.nodesBalancer.Put(s.ServiceAddr)
				case clientv3.EventTypeDelete:
					m.nodesBalancer.Delete(s.ServiceAddr)
				}
			}
		}
	}
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
		if kg.InSplit {
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
			if oldKG != nil && oldKG.InSplit && overwrite {
				return ErrKeyspaceGroupInSplit
			}
			m.store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
				ID:        keyspaceGroup.ID,
				UserKind:  keyspaceGroup.UserKind,
				Members:   keyspaceGroup.Members,
				Keyspaces: keyspaceGroup.Keyspaces,
				InSplit:   keyspaceGroup.InSplit,
				SplitFrom: keyspaceGroup.SplitFrom,
			})
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
	groups, ok := m.groups[userKind]
	if !ok {
		return map[string]string{}, errors.Errorf("user kind %s not found", userKind)
	}
	kg := groups.Top()
	id := strconv.FormatUint(uint64(kg.ID), 10)
	config := map[string]string{
		UserKindKey:           userKind.String(),
		TSOKeyspaceGroupIDKey: id,
	}
	return config, nil
}

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

	m.Lock()
	defer m.Unlock()
	kg := m.groups[userKind].Get(uint32(id))
	if kg == nil {
		return errors.Errorf("keyspace group %d not found", id)
	}
	if kg.InSplit {
		return ErrKeyspaceGroupInSplit
	}
	switch mutation {
	case opAdd:
		if !slice.Contains(kg.Keyspaces, keyspaceID) {
			kg.Keyspaces = append(kg.Keyspaces, keyspaceID)
		}
	case opDelete:
		if slice.Contains(kg.Keyspaces, keyspaceID) {
			kg.Keyspaces = slice.Remove(kg.Keyspaces, keyspaceID)
		}
	}
	if err := m.saveKeyspaceGroups([]*endpoint.KeyspaceGroup{kg}, true); err != nil {
		return err
	}

	m.groups[userKind].Put(kg)
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
	if oldKG.InSplit || newKG.InSplit {
		return ErrKeyspaceGroupInSplit
	}

	var updateOld, updateNew bool
	if !slice.Contains(newKG.Keyspaces, keyspaceID) {
		newKG.Keyspaces = append(newKG.Keyspaces, keyspaceID)
		updateNew = true
	}

	if slice.Contains(oldKG.Keyspaces, keyspaceID) {
		oldKG.Keyspaces = slice.Remove(oldKG.Keyspaces, keyspaceID)
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
func (m *GroupManager) SplitKeyspaceGroupByID(splitFromID, splitToID uint32, keyspaces []uint32) error {
	var splitFromKg, splitToKg *endpoint.KeyspaceGroup
	m.Lock()
	defer m.Unlock()
	// TODO: avoid to split when the keyspaces is empty.
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
		// Load the old keyspace group first.
		splitFromKg, err = m.store.LoadKeyspaceGroup(txn, splitFromID)
		if err != nil {
			return err
		}
		if splitFromKg == nil {
			return ErrKeyspaceGroupNotExists
		}
		if splitFromKg.InSplit {
			return ErrKeyspaceGroupInSplit
		}
		// Check if the new keyspace group already exists.
		splitToKg, err = m.store.LoadKeyspaceGroup(txn, splitToID)
		if err != nil {
			return err
		}
		if splitToKg != nil {
			return ErrKeyspaceGroupExists
		}
		// Check if the keyspaces are all in the old keyspace group.
		if len(keyspaces) > len(splitFromKg.Keyspaces) {
			return ErrKeyspaceNotInKeyspaceGroup
		}
		var (
			oldKeyspaceMap = make(map[uint32]struct{}, len(splitFromKg.Keyspaces))
			newKeyspaceMap = make(map[uint32]struct{}, len(keyspaces))
		)
		for _, keyspace := range splitFromKg.Keyspaces {
			oldKeyspaceMap[keyspace] = struct{}{}
		}
		for _, keyspace := range keyspaces {
			if _, ok := oldKeyspaceMap[keyspace]; !ok {
				return ErrKeyspaceNotInKeyspaceGroup
			}
			newKeyspaceMap[keyspace] = struct{}{}
		}
		// Get the split keyspace group for the old keyspace group.
		splitKeyspaces := make([]uint32, 0, len(splitFromKg.Keyspaces)-len(keyspaces))
		for _, keyspace := range splitFromKg.Keyspaces {
			if _, ok := newKeyspaceMap[keyspace]; !ok {
				splitKeyspaces = append(splitKeyspaces, keyspace)
			}
		}
		// Update the old keyspace group.
		splitFromKg.Keyspaces = splitKeyspaces
		splitFromKg.InSplit = true
		if err = m.store.SaveKeyspaceGroup(txn, splitFromKg); err != nil {
			return err
		}
		splitToKg = &endpoint.KeyspaceGroup{
			ID: splitToID,
			// Keep the same user kind and members as the old keyspace group.
			UserKind:  splitFromKg.UserKind,
			Members:   splitFromKg.Members,
			Keyspaces: keyspaces,
			// Only set the new keyspace group in split state.
			InSplit:   true,
			SplitFrom: splitFromKg.ID,
		}
		// Create the new split keyspace group.
		return m.store.SaveKeyspaceGroup(txn, splitToKg)
	}); err != nil {
		return err
	}
	// Update the keyspace group cache.
	m.groups[endpoint.StringUserKind(splitFromKg.UserKind)].Put(splitFromKg)
	m.groups[endpoint.StringUserKind(splitToKg.UserKind)].Put(splitToKg)
	return nil
}

// FinishSplitKeyspaceByID finishes the split keyspace group by the split-to ID.
func (m *GroupManager) FinishSplitKeyspaceByID(splitToID uint32) error {
	var splitToKg, splitFromKg *endpoint.KeyspaceGroup
	m.Lock()
	defer m.Unlock()
	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
		// Load the split-to keyspace group first.
		splitToKg, err = m.store.LoadKeyspaceGroup(txn, splitToID)
		if err != nil {
			return err
		}
		if splitToKg == nil {
			return ErrKeyspaceGroupNotExists
		}
		// Check if it's in the split state.
		if !splitToKg.InSplit {
			return ErrKeyspaceGroupNotInSplit
		}
		// Load the split-from keyspace group then.
		splitFromKg, err = m.store.LoadKeyspaceGroup(txn, splitToKg.SplitFrom)
		if err != nil {
			return err
		}
		if splitFromKg == nil {
			return ErrKeyspaceGroupNotExists
		}
		if !splitFromKg.InSplit {
			return ErrKeyspaceGroupNotInSplit
		}
		splitToKg.InSplit = false
		splitFromKg.InSplit = false
		err = m.store.SaveKeyspaceGroup(txn, splitToKg)
		if err != nil {
			return err
		}
		err = m.store.SaveKeyspaceGroup(txn, splitFromKg)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	// Update the keyspace group cache.
	m.groups[endpoint.StringUserKind(splitToKg.UserKind)].Put(splitToKg)
	m.groups[endpoint.StringUserKind(splitFromKg.UserKind)].Put(splitFromKg)
	return nil
}

// GetNodesNum returns the number of nodes.
func (m *GroupManager) GetNodesNum() int {
	return len(m.nodesBalancer.GetAll())
}

// AllocNodesForKeyspaceGroup allocates nodes for the keyspace group.
func (m *GroupManager) AllocNodesForKeyspaceGroup(id uint32, replica int) ([]endpoint.KeyspaceGroupMember, error) {
	ctx, cancel := context.WithTimeout(m.ctx, allocNodeTimeout)
	defer cancel()
	ticker := time.NewTicker(allocNodeInterval)
	defer ticker.Stop()
	nodes := make([]endpoint.KeyspaceGroupMember, 0, replica)
	err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		kg, err := m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if kg == nil {
			return ErrKeyspaceGroupNotExists
		}
		exists := make(map[string]struct{})
		for _, member := range kg.Members {
			exists[member.Address] = struct{}{}
			nodes = append(nodes, member)
		}
		for len(exists) < replica {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
			num := m.GetNodesNum()
			if num < replica || num == 0 { // double check
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
			nodes = append(nodes, endpoint.KeyspaceGroupMember{Address: addr})
		}
		kg.Members = nodes
		m.store.SaveKeyspaceGroup(txn, kg)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}
