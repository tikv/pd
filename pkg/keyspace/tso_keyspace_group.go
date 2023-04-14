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
			return ErrKeyspaceGroupNotFound
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
			return ErrKeyspaceGroupNotFound
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
			return ErrKeyspaceGroupNotFound
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
