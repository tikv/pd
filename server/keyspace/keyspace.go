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
	// AllocStep set idAllocator's step when write persistent window boundary.
	// Use a lower value for denser idAllocation in the event of frequent pd leader change.
	AllocStep = uint64(100)
	// AllocLabel is used to label keyspace idAllocator's metrics.
	AllocLabel = "keyspace-idAlloc"
	// DefaultKeyspaceName is the name reserved for default keyspace.
	DefaultKeyspaceName = "DEFAULT"
	// DefaultKeyspaceID is the id of default keyspace.
	DefaultKeyspaceID = uint32(0)
)

// Manager manages keyspace related data.
// It validates requests and provides concurrency control.
type Manager struct {
	// idLock guards keyspace name to id lookup entries.
	idLock syncutil.Mutex
	// metaLock guards keyspace meta.
	metaLock *syncutil.LockGroup
	// idAllocator allocates keyspace id.
	idAllocator id.Allocator
	// store is the storage for keyspace related information.
	store endpoint.KeyspaceStorage
}

// CreateKeyspaceRequest represents necessary arguments to create a keyspace.
type CreateKeyspaceRequest struct {
	// Name of the keyspace to be created.
	// Using an existing name will result in error.
	Name   string
	Config map[string]string
	Now    time.Time
}

// NewKeyspaceManager creates a Manager of keyspace related data.
func NewKeyspaceManager(store endpoint.KeyspaceStorage, idAllocator id.Allocator) (*Manager, error) {
	manager := &Manager{
		store:       store,
		idAllocator: idAllocator,
		metaLock:    syncutil.NewLockGroup(syncutil.WithHash(SpaceIDHash)),
	}
	now := time.Now()
	// Initialize default keyspace.
	defaultKeyspace := &keyspacepb.KeyspaceMeta{
		Id:             DefaultKeyspaceID,
		Name:           DefaultKeyspaceName,
		State:          keyspacepb.KeyspaceState_ENABLED,
		CreatedAt:      now.Unix(),
		StateChangedAt: now.Unix(),
	}
	_, err := manager.saveNewKeyspace(defaultKeyspace)
	if err != nil {
		return nil, err
	}
	return manager, nil
}

// CreateKeyspace create a keyspace meta with given config and save it to storage.
func (manager *Manager) CreateKeyspace(request *CreateKeyspaceRequest) (*keyspacepb.KeyspaceMeta, error) {
	// Validate purposed name's legality.
	if err := validateName(request.Name); err != nil {
		return nil, err
	}
	// Allocate new keyspaceID.
	newID, err := manager.allocID()
	if err != nil {
		return nil, err
	}
	// Create and save keyspace metadata.
	keyspace := &keyspacepb.KeyspaceMeta{
		Id:             newID,
		Name:           request.Name,
		State:          keyspacepb.KeyspaceState_ENABLED,
		CreatedAt:      request.Now.Unix(),
		StateChangedAt: request.Now.Unix(),
		Config:         request.Config,
	}
	return manager.saveNewKeyspace(keyspace)
}

func (manager *Manager) saveNewKeyspace(keyspace *keyspacepb.KeyspaceMeta) (*keyspacepb.KeyspaceMeta, error) {
	manager.idLock.Lock()
	defer manager.idLock.Unlock()
	// Check if keyspace id with that name already exists.
	nameExists, _, err := manager.store.LoadKeyspaceIDByName(keyspace.Name)
	if err != nil {
		return nil, err
	}
	if nameExists {
		return nil, ErrKeyspaceExists
	}
	manager.metaLock.Lock(keyspace.Id)
	defer manager.metaLock.Unlock(keyspace.Id)
	// Check if keyspace meta with that id already exists.
	keyspaceExists, err := manager.store.LoadKeyspace(keyspace.Id, &keyspacepb.KeyspaceMeta{})
	if err != nil {
		return nil, err
	}
	if keyspaceExists {
		return nil, ErrKeyspaceExists
	}
	// TODO: Enable Transaction at storage layer to save MetaData and NameToID in a single transaction.
	// Save keyspace keyspace before saving id.
	if err = manager.store.SaveKeyspace(keyspace); err != nil {
		return nil, err
	}
	// Create name to ID entry,
	// if this failed, previously stored keyspace meta should be removed.
	if err = manager.createNameToID(keyspace.Id, keyspace.Name); err != nil {
		if removeErr := manager.store.RemoveKeyspace(keyspace.Id); removeErr != nil {
			return nil, errors.Wrap(removeErr, "failed to remove keyspace keyspace after save spaceID failure")
		}
		return nil, err
	}

	return keyspace, nil
}

// LoadKeyspace returns the keyspace specified by name.
// It returns error if loading or unmarshalling met error or if keyspace does not exist.
func (manager *Manager) LoadKeyspace(name string) (*keyspacepb.KeyspaceMeta, error) {
	// First get keyspace ID from the name given.
	loaded, spaceID, err := manager.store.LoadKeyspaceIDByName(name)
	if err != nil {
		return nil, err
	}
	if !loaded {
		return nil, ErrKeyspaceNotFound
	}
	return manager.loadKeyspaceByID(spaceID)
}

// LoadState returns the state of the keyspace specified by ID.
func (manager *Manager) LoadState(spaceID uint32) (keyspacepb.KeyspaceState, error) {
	meta, err := manager.loadKeyspaceByID(spaceID)
	if err != nil {
		return 0, err
	}
	return meta.State, nil
}

func (manager *Manager) loadKeyspaceByID(spaceID uint32) (*keyspacepb.KeyspaceMeta, error) {
	// Load the keyspace with target ID.
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

// Mutation represents a single operation to be applied on keyspace config.
type Mutation struct {
	Op    OpType
	Key   string
	Value string
}

// OpType defines the type of keyspace config operation.
type OpType int

const (
	// OpPut denotes a put operation onto the given config.
	// If target key exists, it will put a new value,
	// otherwise, it creates a new config entry.
	OpPut OpType = iota + 1 // Operation type starts at 1.
	// OpDel denotes a deletion operation onto the given config.
	// Note: OpDel is idempotent, deleting a non-existing key
	// will not result in error.
	OpDel
)

// UpdateKeyspaceConfig changes target keyspace's config in the order specified in mutations.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceConfig(name string, mutations []*Mutation) (*keyspacepb.KeyspaceMeta, error) {
	// First get KeyspaceID from Name.
	loaded, spaceID, err := manager.store.LoadKeyspaceIDByName(name)
	if err != nil {
		return nil, err
	}
	if !loaded {
		return nil, ErrKeyspaceNotFound
	}
	manager.metaLock.Lock(spaceID)
	defer manager.metaLock.Unlock(spaceID)
	// Load keyspace by id.
	keyspace, err := manager.loadKeyspaceByID(spaceID)
	if err != nil {
		return nil, err
	}
	// Changing ARCHIVED keyspace's config is not allowed.
	if keyspace.State == keyspacepb.KeyspaceState_ARCHIVED {
		return nil, errKeyspaceArchived
	}
	if keyspace.Config == nil {
		keyspace.Config = map[string]string{}
	}
	// Update keyspace config according to mutations.
	for _, mutation := range mutations {
		switch mutation.Op {
		case OpPut:
			keyspace.Config[mutation.Key] = mutation.Value
		case OpDel:
			delete(keyspace.Config, mutation.Key)
		default:
			return nil, errIllegalOperation
		}
	}
	// Save the updated keyspace.
	if err = manager.store.SaveKeyspace(keyspace); err != nil {
		return nil, err
	}
	return keyspace, nil
}

// UpdateKeyspaceState updates target keyspace to the given state if it's not already in that state.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceState(name string, newState keyspacepb.KeyspaceState, now time.Time) (*keyspacepb.KeyspaceMeta, error) {
	// Changing the state of default keyspace is not allowed.
	if name == DefaultKeyspaceName {
		return nil, errModifyDefault
	}
	// First get KeyspaceID from Name.
	loaded, spaceID, err := manager.store.LoadKeyspaceIDByName(name)
	if err != nil {
		return nil, err
	}
	if !loaded {
		return nil, ErrKeyspaceNotFound
	}
	manager.metaLock.Lock(spaceID)
	defer manager.metaLock.Unlock(spaceID)
	// Load keyspace by id.
	keyspace, err := manager.loadKeyspaceByID(spaceID)
	if err != nil {
		return nil, err
	}
	// If keyspace is already in target state, then nothing needs to be change.
	if keyspace.State == newState {
		return keyspace, nil
	}
	// ARCHIVED is the terminal state that cannot be changed from.
	if keyspace.State == keyspacepb.KeyspaceState_ARCHIVED {
		return nil, errKeyspaceArchived
	}
	// Archiving an enabled keyspace directly is not allowed.
	if keyspace.State == keyspacepb.KeyspaceState_ENABLED && newState == keyspacepb.KeyspaceState_ARCHIVED {
		return nil, errArchiveEnabled
	}
	// Change keyspace state and record change time.
	keyspace.StateChangedAt = now.Unix()
	keyspace.State = newState
	// Save the updated keyspace.
	if err = manager.store.SaveKeyspace(keyspace); err != nil {
		return nil, err
	}
	return keyspace, nil
}

// LoadRangeKeyspace load up to limit keyspaces starting from keyspace with startID.
func (manager *Manager) LoadRangeKeyspace(startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error) {
	// Load Start should fall within acceptable ID range.
	if startID > spaceIDMax {
		return nil, errors.Errorf("startID of the scan %d exceeds spaceID Max %d", startID, spaceIDMax)
	}
	return manager.store.LoadRangeKeyspace(startID, limit)
}

// allocID allocate a new keyspace id.
func (manager *Manager) allocID() (uint32, error) {
	id64, err := manager.idAllocator.Alloc()
	if err != nil {
		return 0, err
	}
	id32 := uint32(id64)
	if err = validateID(id32); err != nil {
		return 0, err
	}
	return id32, nil
}

// createNameToID create a keyspace name to ID lookup entry.
// It returns error if saving keyspace name meet error.
func (manager *Manager) createNameToID(spaceID uint32, name string) error {
	return manager.store.SaveKeyspaceIDByName(spaceID, name)
}
