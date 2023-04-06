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
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/balancer"
	"github.com/pingcap/errors"
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

	nodesBalancer balancer.Balancer[string]
}

// NewKeyspaceGroupManager creates a Manager of keyspace group related data.
func NewKeyspaceGroupManager(ctx context.Context, store endpoint.KeyspaceGroupStorage, client *clientv3.Client, clusterID uint64) *GroupManager {
	ctx, cancel := context.WithCancel(ctx)
	key := discovery.TSOAddrsPath(clusterID)
	groups := make(map[endpoint.UserKind]*indexedHeap)
	for i := 0; i < int(endpoint.UserKindCount); i++ {
		groups[endpoint.UserKind(i)] = newIndexedHeap(int(utils.MaxKeyspaceGroupCountInUse))
	}
	return &GroupManager{
		ctx:              ctx,
		cancel:           cancel,
		groups: groups,
		store:            store,
		client:           client,
		tsoServiceKey:    key,
		tsoServiceEndKey: clientv3.GetPrefixRangeEnd(key) + "/",
		policy:           defaultBalancerPolicy,
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
	// If the group for the userKind does not exist, create a new one.
	if _, ok := m.groups[userKind]; !ok {
		m.groups[userKind] = newIndexedHeap(int(utils.MaxKeyspaceGroupCountInUse))
	}
	m.groups[userKind].Put(defaultKeyspaceGroup)

	// Load all the keyspace groups from the storage and add to the respective userKind groups.
	groups, err := m.store.LoadKeyspaceGroups(utils.DefaultKeySpaceGroupID, 0)
	if err != nil {
		return err
	}
	for _, group := range groups {
		userKind := endpoint.StringUserKind(group.UserKind)
		if _, ok := m.groups[userKind]; !ok {
			m.groups[userKind] = newIndexedHeap(int(utils.MaxKeyspaceGroupCountInUse))
		}
		m.groups[userKind].Put(group)
	}

	if m.client != nil {
		m.nodesBalancer = balancer.GenByPolicy[string](m.policy)
		resp, err := etcdutil.EtcdKVGet(m.client, m.tsoServiceKey, clientv3.WithRange(m.tsoServiceEndKey))
		if err != nil {
			return err
		}
		for _, item := range resp.Kvs {
			m.nodesBalancer.Put(string(item.Value))
		}
		m.wg.Add(1)
		go m.startWatchLoop(resp.Header.GetRevision())
	}
	return nil
}

// Close closes the manager.
func (m *GroupManager) Close() {
	m.cancel()
	m.wg.Wait()
}

func (m *GroupManager) startWatchLoop(revision int64) {
	defer m.wg.Done()
	if err := m.watchServiceAddrs(revision); err != nil {
		log.Error("watch service addresses failed", zap.Error(err))
	}
}

func (m *GroupManager) watchServiceAddrs(revision int64) error {
	ctx, cancel := context.WithCancel(m.ctx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		watchChan := m.client.Watch(ctx, m.tsoServiceKey, clientv3.WithRange(m.tsoServiceEndKey), clientv3.WithRev(revision))
		for {
			select {
			case <-ctx.Done():
				return nil
			case wresp := <-watchChan:
				if wresp.CompactRevision != 0 {
					log.Warn("required revision has been compacted, the watcher will watch again with the compact revision",
						zap.Int64("required-revision", revision),
						zap.Int64("compact-revision", wresp.CompactRevision))
					revision = wresp.CompactRevision
					break
				}
				if wresp.Err() != nil {
					log.Error("watch is canceled or closed",
						zap.Int64("required-revision", revision),
						zap.Error(wresp.Err()))
					return wresp.Err()
				}
				for _, event := range wresp.Events {
					addr := string(event.Kv.Value)
					switch event.Type {
					case clientv3.EventTypePut:
						m.nodesBalancer.Put(addr)
					case clientv3.EventTypeDelete:
						m.nodesBalancer.Delete(addr)
					}
				}
			}
		}
	}
}

// CreateKeyspaceGroups creates keyspace groups.
func (m *GroupManager) CreateKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup) error {
	m.Lock()
	defer m.Unlock()
	enableAllocate := true
	failpoint.Inject("disableAllocate", func() {
		enableAllocate = false
	})
	if enableAllocate {
		for _, keyspaceGroup := range keyspaceGroups {
			// TODO: consider the case that the node offline
			members := m.AllocNodesForGroup(keyspaceGroup.Replica)
			if len(members) == 0 {
				// directly return error if no available node.
				// It means that the number of nodes is reducing between the check of controller and the execution of this function.
				return errNoAvailableNode
			}
			keyspaceGroup.Members = members
		}
	}
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

// GetKeyspaceGroupByID returns the keyspace group by id.
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

// DeleteKeyspaceGroupByID deletes the keyspace group by id.
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
			newKG := &endpoint.KeyspaceGroup{
				ID:       keyspaceGroup.ID,
				UserKind: keyspaceGroup.UserKind,
				Keyspaces: keyspaceGroup.Keyspaces,
				Replica:  keyspaceGroup.Replica,
			}
			m.store.SaveKeyspaceGroup(txn, newKG)
		}
		return nil
	})
}

// GetAvailableKeyspaceGroupIDByKind returns the available keyspace group id by user kind.
func (m *GroupManager) GetAvailableKeyspaceGroupIDByKind(userKind endpoint.UserKind) (string, error) {
	m.RLock()
	defer m.RUnlock()
	groups, ok := m.groups[userKind]
	if !ok {
		return "", errors.Errorf("user kind %s not found", userKind)
	}
	kg := groups.Top()
	return strconv.FormatUint(uint64(kg.ID), 10), nil
}

// UpdateKeyspaceForGroup updates the keyspace field for the keyspace group.
func (m *GroupManager) UpdateKeyspaceForGroup(userKind endpoint.UserKind, groupID string, keyspaceID uint32, mutation int) error {
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

// GetNodesNum returns the number of nodes.
func (m *GroupManager) GetNodesNum() int {
	return len(m.nodesBalancer.GetAll())
}

// AllocNodesForGroup allocates nodes for the keyspace group.
// Note: the replica should be less than the number of nodes.
func (m *GroupManager) AllocNodesForGroup(replica int) []endpoint.KeyspaceGroupMember {
	ctx, cancel := context.WithTimeout(m.ctx, allocNodeTimeout)
	defer cancel()
	ticker := time.NewTicker(allocNodeInterval)
	defer ticker.Stop()
	exists := make(map[string]struct{})
	nodes := make([]endpoint.KeyspaceGroupMember, 0, replica)
	for len(exists) < replica {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		addr := m.nodesBalancer.Next()
		if addr == "" { // no node
			return nil
		}
		if _, ok := exists[addr]; ok {
			continue
		}
		exists[addr] = struct{}{}
		nodes = append(nodes, endpoint.KeyspaceGroupMember{Address: addr})
	}
	return nodes
}
