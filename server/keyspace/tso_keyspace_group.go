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

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"go.uber.org/zap"
)

// GroupManager is the manager of keyspace group related data.
type GroupManager struct {
	ctx context.Context
	// store is the storage for keyspace group related information.
	store endpoint.KeyspaceGroupStorage
}

// NewKeyspaceGroupManager creates a Manager of keyspace group related data.
func NewKeyspaceGroupManager(ctx context.Context, store endpoint.KeyspaceGroupStorage) *GroupManager {
	return &GroupManager{
		ctx:   ctx,
		store: store,
	}
}

// Bootstrap saves default keyspace group info.
func (m *GroupManager) Bootstrap() error {
	defaultKeyspaceGroup := &endpoint.KeyspaceGroup{
		ID: utils.DefaultKeySpaceGroupID,
		// TODO: define a user kind type
		UserKind: "default",
	}
	err := m.saveKeyspaceGroup(defaultKeyspaceGroup)
	// It's possible that default keyspace group already exists in the storage (e.g. PD restart/recover),
	// so we ignore the ErrKeyspaceGroupExists.
	if err != nil && err != ErrKeyspaceGroupExists {
		return err
	}

	return nil
}

// CreateKeyspaceGroups creates keyspace groups.
func (m *GroupManager) CreateKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup) error {
	for _, keyspaceGroup := range keyspaceGroups {
		// TODO: add replica count
		kg := &endpoint.KeyspaceGroup{
			ID:       keyspaceGroup.ID,
			UserKind: keyspaceGroup.UserKind,
		}
		err := m.saveKeyspaceGroup(kg)
		if err != nil {
			log.Warn("failed to create keyspace group",
				zap.Uint32("id", kg.ID),
				zap.String("use-kind", kg.UserKind),
				zap.Error(err),
			)
		}
	}
	return nil
}

// GetKeyspaceGroups returns all keyspace groups.
func (m *GroupManager) GetKeyspaceGroups(startID uint32, limit int) ([]*endpoint.KeyspaceGroup, error) {
	return m.store.LoadKeyspaceGroups(startID, limit)
}

// GetKeyspaceGroupByID returns the keyspace group by id.
func (m *GroupManager) GetKeyspaceGroupByID(id uint32) (*endpoint.KeyspaceGroup, error) {
	var (
		kg  *endpoint.KeyspaceGroup
		err error
	)

	m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		return nil
	})
	return kg, nil
}

// DeleteKeyspaceGroupByID deletes the keyspace group by id.
func (m *GroupManager) DeleteKeyspaceGroupByID(id uint32) error {
	m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.store.DeleteKeyspaceGroup(txn, id)
	})
	return nil
}

func (m *GroupManager) saveKeyspaceGroup(keyspaceGroup *endpoint.KeyspaceGroup) error {
	return m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		// Save keyspace ID.
		// Check if keyspace with that name already exists.
		kg, err := m.store.LoadKeyspaceGroup(txn, keyspaceGroup.ID)
		if err != nil {
			return err
		}
		if kg != nil {
			return ErrKeyspaceGroupExists
		}
		return m.store.SaveKeyspaceGroup(txn, keyspaceGroup)
	})
}
