// Copyright 2022 TiKV Project Authors.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/id"
	"github.com/tikv/pd/server/storage/endpoint"
)

const (
	spaceIDMin = uint32(1)       // 1 is the minimum value of spaceID, 0 is reserved.
	spaceIDMax = ^uint32(0) >> 8 // 16777215 (Uint24Max) is the maximum value of spaceID.
)

var (
	errKeyspaceArchived   = errors.New("Keyspace already archived")
	ErrKeyspaceNotFound   = errors.New("Keyspace does not exist")
	ErrKeyspaceNameExists = errors.New("Keyspace name already in use")
	errIllegalID          = errors.New("Cannot create keyspace with that ID")
	errIllegalName        = errors.New("Cannot create keyspace with that name")
)

// Manager manages keyspace related data.
// It validates requests and provides concurrency control.
type Manager struct {
	// idLock guards keyspace name to id lookup entries.
	idLock syncutil.Mutex
	// metaLock guards keyspace meta.
	metaLock syncutil.Mutex
	// idAllocator allocates keyspace id.
	idAllocator id.Allocator
	// store is the storage for keyspace related information.
	store endpoint.KeyspaceStorage
}

// CreateKeyspaceRequest represents necessary arguments to create a keyspace.
type CreateKeyspaceRequest struct {
	Name          string
	InitialConfig map[string]string
	Now           time.Time
}

// UpdateKeyspaceRequest represents necessary arguments to update keyspace.
type UpdateKeyspaceRequest struct {
	Name string
	// Whether to update the state of keyspace,
	// if set to false, NewState and Now will be ignored.
	UpdateState bool
	NewState    keyspacepb.KeyspaceState
	Now         time.Time
	// New KVs to put in keyspace config.
	ToPut map[string]string
	// KVs to remove from keyspace config.
	// Removal will be performed after put.
	ToDelete []string
}

// NewKeyspaceManager creates a Manager of keyspace related data.
func NewKeyspaceManager(store endpoint.KeyspaceStorage, idAllocator id.Allocator) *Manager {
	return &Manager{
		store:       store,
		idAllocator: idAllocator,
	}
}

// allocID allocate a new keyspace id.
func (manager *Manager) allocID() (uint32, error) {
	id64, err := manager.idAllocator.Alloc()
	if err != nil {
		return 0, err
	}
	id32 := uint32(id64)
	// skip id 0
	if id32 < spaceIDMin {
		return manager.allocID()
	}
	// if allocated id too big, return error
	if id32 > spaceIDMax {
		return 0, errIllegalID
	}
	return id32, nil
}

// createNameToID create a keyspace name to ID lookup entry.
// It returns error if saving keyspace name meet error or if name has already been taken.
func (manager *Manager) createNameToID(spaceID uint32, name string) error {
	manager.idLock.Lock()
	defer manager.idLock.Unlock()

	nameExists, _, err := manager.store.LoadKeyspaceID(name)
	if err != nil {
		return err
	}
	if nameExists {
		return ErrKeyspaceNameExists
	}
	return manager.store.SaveKeyspaceID(spaceID, name)
}

// CreateKeyspace create a keyspace meta with initial config and save it to storage.
func (manager *Manager) CreateKeyspace(request *CreateKeyspaceRequest) (*keyspacepb.KeyspaceMeta, error) {
	// allocate new id
	newID, err := manager.allocID()
	if err != nil {
		return nil, err
	}
	// bind name to that id
	if err = manager.createNameToID(newID, request.Name); err != nil {
		return nil, err
	}

	if request.Name == "" {
		return nil, errIllegalName
	}

	manager.metaLock.Lock()
	defer manager.metaLock.Unlock()

	keyspace := &keyspacepb.KeyspaceMeta{
		Id:             newID,
		Name:           request.Name,
		State:          keyspacepb.KeyspaceState_ENABLED,
		CreatedAt:      request.Now.Unix(),
		StateChangedAt: request.Now.Unix(),
		Config:         request.InitialConfig,
	}
	if err := manager.store.SaveKeyspace(keyspace); err != nil {
		return nil, err
	}
	return keyspace, nil
}

// loadKeyspaceID returns the id of keyspace specified by name.
// It returns error if loading met error, or if keyspace not exists.
func (manager *Manager) loadKeyspaceID(name string) (uint32, error) {
	loaded, spaceID, err := manager.store.LoadKeyspaceID(name)
	if err != nil {
		return 0, err
	}
	if !loaded {
		return 0, ErrKeyspaceNotFound
	}
	return spaceID, nil
}

// LoadKeyspace returns the keyspace specified by name.
// It returns error if loading or unmarshalling met error or if keyspace does not exist.
func (manager *Manager) LoadKeyspace(name string) (*keyspacepb.KeyspaceMeta, error) {
	spaceID, err := manager.loadKeyspaceID(name)
	if err != nil {
		return nil, err
	}
	keyspace := &keyspacepb.KeyspaceMeta{}
	loaded, err := manager.store.LoadKeyspace(spaceID, keyspace)
	if err != nil {
		return nil, err
	}
	if !loaded {
		return nil, ErrKeyspaceNotFound
	}
	return keyspace, nil
}

// UpdateKeyspace update keyspace's state and config according to request.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspace(request *UpdateKeyspaceRequest) (*keyspacepb.KeyspaceMeta, error) {
	manager.metaLock.Lock()
	defer manager.metaLock.Unlock()
	// load keyspace by id
	keyspace, err := manager.LoadKeyspace(request.Name)
	if err != nil {
		return nil, err
	}

	// disallow modifying archived keyspace
	if keyspace.State == keyspacepb.KeyspaceState_ARCHIVED {
		return nil, errKeyspaceArchived
	}

	// update keyspace state and config
	if request.UpdateState {
		if err = changeKeyspaceState(keyspace, request.NewState, request.Now); err != nil {
			return nil, err
		}
	}
	changeKeyspaceConfig(keyspace, request.ToPut, request.ToDelete)
	// save keyspace
	if err = manager.store.SaveKeyspace(keyspace); err != nil {
		return nil, err
	}
	return keyspace, nil
}

// LoadRangeKeyspace load up to limit keyspaces starting from keyspace with startID.
func (manager *Manager) LoadRangeKeyspace(startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error) {
	return manager.store.LoadRangeKeyspace(startID, limit)
}

// changeKeyspaceState changes the state of given keyspace meta.
// It also records the time state change happened.
func changeKeyspaceState(keyspace *keyspacepb.KeyspaceMeta, newState keyspacepb.KeyspaceState, now time.Time) error {
	if keyspace.State == newState {
		return nil
	}
	// ARCHIVED is the terminal state that cannot be changed from.
	if keyspace.State == keyspacepb.KeyspaceState_ARCHIVED {
		return errKeyspaceArchived
	}

	keyspace.StateChangedAt = now.Unix()
	keyspace.State = newState
	return nil
}

// changeKeyspaceConfig update a keyspace's config according to toPut and toDelete.
func changeKeyspaceConfig(keyspace *keyspacepb.KeyspaceMeta, toPut map[string]string, toDelete []string) {
	for k, v := range toPut {
		keyspace.Config[k] = v
	}
	for _, key := range toDelete {
		delete(keyspace.Config, key)
	}
}
