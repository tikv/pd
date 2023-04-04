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

	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

const (
	opAdd    = "add"
	opDelete = "delete"
)

// GroupManager is the manager of keyspace group related data.
type GroupManager struct {
	ctx context.Context
	sync.RWMutex
	// groups is the cache of keyspace group related information.
	// user kind -> keyspace group
	groups map[endpoint.UserKind]*indexedHeap
	// store is the storage for keyspace group related information.
	store endpoint.KeyspaceGroupStorage
}

// NewKeyspaceGroupManager creates a Manager of keyspace group related data.
func NewKeyspaceGroupManager(ctx context.Context, store endpoint.KeyspaceGroupStorage) *GroupManager {
	return &GroupManager{
		ctx:    ctx,
		store:  store,
		groups: make(map[endpoint.UserKind]*indexedHeap),
	}
}

// Bootstrap saves default keyspace group info.
func (m *GroupManager) Bootstrap() error {
	defaultKeyspaceGroup := &endpoint.KeyspaceGroup{
		ID:       utils.DefaultKeySpaceGroupID,
		UserKind: endpoint.Basic.String(),
	}
	err := m.saveKeyspaceGroups([]*endpoint.KeyspaceGroup{defaultKeyspaceGroup}, false)
	// It's possible that default keyspace group already exists in the storage (e.g. PD restart/recover),
	// so we ignore the ErrKeyspaceGroupExists.
	if err != nil && err != ErrKeyspaceGroupExists {
		return err
	}

	m.Lock()
	defer m.Unlock()
	userKind := endpoint.StringUserKind(defaultKeyspaceGroup.UserKind)
	if _, ok := m.groups[userKind]; !ok {
		m.groups[userKind] = newIndexedHeap(int(utils.MaxKeyspaceGroupCountInUse))
	}
	m.groups[userKind].Put(defaultKeyspaceGroup)

	return nil
}

// CreateKeyspaceGroups creates keyspace groups.
func (m *GroupManager) CreateKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup) error {
	if err := m.saveKeyspaceGroups(keyspaceGroups, false); err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()
	for _, keyspaceGroup := range keyspaceGroups {
		userKind := endpoint.StringUserKind(keyspaceGroup.UserKind)
		if _, ok := m.groups[userKind]; !ok {
			m.groups[userKind] = newIndexedHeap(int(utils.MaxKeyspaceGroupCountInUse))
		}
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
func (m *GroupManager) DeleteKeyspaceGroupByID(id uint32) error {
	var (
		kg  *endpoint.KeyspaceGroup
		err error
	)

	if err := m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		return m.store.DeleteKeyspaceGroup(txn, id)
	}); err != nil {
		return err
	}

	userKind := endpoint.StringUserKind(kg.UserKind)
	m.Lock()
	defer m.Unlock()
	// we don't need the keyspace group as the return value
	m.groups[userKind].Remove(id)

	return nil
}

func (m *GroupManager) saveKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup, overwrite bool) error {
	return m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		for _, keyspaceGroup := range keyspaceGroups {
			// TODO: add replica count
			newKG := &endpoint.KeyspaceGroup{
				ID:        keyspaceGroup.ID,
				UserKind:  keyspaceGroup.UserKind,
				Keyspaces: keyspaceGroup.Keyspaces,
			}
			// Check if keyspace group has already existed.
			oldKG, err := m.store.LoadKeyspaceGroup(txn, keyspaceGroup.ID)
			if err != nil {
				return err
			}
			if oldKG != nil && !overwrite {
				return ErrKeyspaceGroupExists
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
func (m *GroupManager) UpdateKeyspaceForGroup(userKind endpoint.UserKind, groupID string, keyspaceID uint32, mutation string) error {
	id, err := strconv.ParseUint(groupID, 10, 64)
	if err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()
	kg := m.groups[userKind].Get(uint32(id))
	switch mutation {
	case opAdd:
		if !slice.Contains(kg.Keyspaces, keyspaceID) {
			kg.Keyspaces = append(kg.Keyspaces, keyspaceID)
			m.groups[userKind].Put(kg)
		}
	case opDelete:
		if slice.Contains(kg.Keyspaces, keyspaceID) {
			kg.Keyspaces = slice.Remove(kg.Keyspaces, keyspaceID)
			m.groups[userKind].Put(kg)
		}
	}

	return m.saveKeyspaceGroups([]*endpoint.KeyspaceGroup{kg}, true)
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
	if !slice.Contains(newKG.Keyspaces, keyspaceID) {
		newKG.Keyspaces = append(newKG.Keyspaces, keyspaceID)
		m.groups[newUserKind].Put(newKG)
	}

	if slice.Contains(oldKG.Keyspaces, keyspaceID) {
		oldKG.Keyspaces = slice.Remove(oldKG.Keyspaces, keyspaceID)
		m.groups[oldUserKind].Put(oldKG)
	}

	return m.saveKeyspaceGroups([]*endpoint.KeyspaceGroup{oldKG, newKG}, true)
}
