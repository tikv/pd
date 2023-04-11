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
	opAdd int = iota
	opDelete
)

// GroupManager is the manager of keyspace group related data.
type GroupManager struct {
	ctx context.Context
	// the lock for the groups
	sync.RWMutex
	// groups is the cache of keyspace group related information.
	// user kind -> keyspace group
	groups map[endpoint.UserKind]*indexedHeap
	// store is the storage for keyspace group related information.
	store endpoint.KeyspaceGroupStorage
}

// NewKeyspaceGroupManager creates a Manager of keyspace group related data.
func NewKeyspaceGroupManager(ctx context.Context, store endpoint.KeyspaceGroupStorage) *GroupManager {
	groups := make(map[endpoint.UserKind]*indexedHeap)
	for i := 0; i < int(endpoint.UserKindCount); i++ {
		groups[endpoint.UserKind(i)] = newIndexedHeap(int(utils.MaxKeyspaceGroupCountInUse))
	}
	return &GroupManager{
		ctx:    ctx,
		store:  store,
		groups: groups,
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

	return nil
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
// If any keyspace group already exists and the overwrite is false, it will return ErrKeyspaceGroupExists.
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
			m.store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
				ID:        keyspaceGroup.ID,
				UserKind:  keyspaceGroup.UserKind,
				Members:   keyspaceGroup.Members,
				Keyspaces: keyspaceGroup.Keyspaces,
				InSplit:   keyspaceGroup.InSplit,
			})
		}
		return nil
	})
}

// updateKeyspaceGroups will try to update the given keyspace group in the storage.
// If any keyspace group does not exist, it returns ErrKeyspaceGroupNotFound.
func (m *GroupManager) updateKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup) error {
	return m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		for _, keyspaceGroup := range keyspaceGroups {
			// Check if the keyspace group exists.
			kg, err := m.store.LoadKeyspaceGroup(txn, keyspaceGroup.ID)
			if err != nil {
				return err
			}
			if kg == nil {
				return ErrKeyspaceGroupNotFound
			}
			m.store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
				ID:        keyspaceGroup.ID,
				UserKind:  keyspaceGroup.UserKind,
				Members:   keyspaceGroup.Members,
				Keyspaces: keyspaceGroup.Keyspaces,
				InSplit:   keyspaceGroup.InSplit,
			})
		}
		return nil
	})
}

// GetAvailableKeyspaceGroupIDByKind returns the available keyspace group ID by user kind.
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

// SplitKeyspaceGroupByID splits the keyspace group by ID into a new keyspace group with the given new ID.
// And the keyspaces in the old keyspace group will be moved to the new keyspace group.
func (m *GroupManager) SplitKeyspaceGroupByID(id, newID uint32, keyspaces []uint32) error {
	return m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		// Load the old keyspace group first.
		oldKg, err := m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if oldKg == nil {
			return ErrKeyspaceGroupNotFound
		}
		// Check if the new keyspace group already exists.
		newKg, err := m.store.LoadKeyspaceGroup(txn, newID)
		if err != nil {
			return err
		}
		if newKg != nil {
			return ErrKeyspaceGroupExists
		}
		// Check if the keyspaces are all in the old keyspace group.
		if len(keyspaces) > len(oldKg.Keyspaces) {
			return ErrKeyspaceNotInKeyspaceGroup
		}
		var (
			oldKeyspaceMap = make(map[uint32]struct{}, len(oldKg.Keyspaces))
			newKeyspaceMap = make(map[uint32]struct{}, len(keyspaces))
		)
		for _, keyspace := range oldKg.Keyspaces {
			oldKeyspaceMap[keyspace] = struct{}{}
		}
		for _, keyspace := range keyspaces {
			if _, ok := oldKeyspaceMap[keyspace]; !ok {
				return ErrKeyspaceNotInKeyspaceGroup
			}
			newKeyspaceMap[keyspace] = struct{}{}
		}
		// Get the split keyspace group for the old keyspace group.
		splitKeyspaces := make([]uint32, 0, len(oldKg.Keyspaces)-len(keyspaces))
		for _, keyspace := range oldKg.Keyspaces {
			if _, ok := newKeyspaceMap[keyspace]; !ok {
				splitKeyspaces = append(splitKeyspaces, keyspace)
			}
		}
		// Update the old keyspace group.
		oldKg.Keyspaces = splitKeyspaces
		if err = m.store.SaveKeyspaceGroup(txn, oldKg); err != nil {
			return err
		}
		// Create the new split keyspace group.
		return m.store.SaveKeyspaceGroup(txn, &endpoint.KeyspaceGroup{
			ID: newID,
			// Keep the same user kind and members as the old keyspace group.
			UserKind:  oldKg.UserKind,
			Members:   oldKg.Members,
			Keyspaces: keyspaces,
			// Only set the new keyspace group in split state.
			InSplit: true,
		})
	})
}

// FinishSplitKeyspaceByID finishes the split keyspace group by ID.
func (m *GroupManager) FinishSplitKeyspaceByID(id uint32) error {
	// Load the keyspace group first.
	kg, err := m.GetKeyspaceGroupByID(id)
	if err != nil {
		return err
	}
	if kg == nil {
		return ErrKeyspaceGroupNotFound
	}
	// Check if it's in the split state.
	if !kg.InSplit {
		return ErrKeyspaceGroupNotInSplit
	}
	kg.InSplit = false
	return m.updateKeyspaceGroups([]*endpoint.KeyspaceGroup{kg})
}
