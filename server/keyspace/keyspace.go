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
	"strings"
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
	// AllocStep set idAllocator's step when write persistent window boundary.
	// Use a lower value for denser idAllocation in the event of frequent pd leader change.
	AllocStep = uint64(100)
	// AllocLabel is used to label keyspace idAllocator's metrics.
	AllocLabel = "keyspace-idAlloc"
	// illegalChars contains forbidden characters for keyspace name.
	illegalChars = "/"
)

var (
	// ErrKeyspaceNotFound is used to indicate target keyspace does not exist.
	ErrKeyspaceNotFound = errors.New("Keyspace does not exist")
	// ErrKeyspaceExists indicates target keyspace already exists.
	// Used when creating a new keyspace.
	ErrKeyspaceExists   = errors.New("Keyspace already exists")
	errKeyspaceArchived = errors.New("Keyspace already archived")
	errArchiveEnabled   = errors.New("Cannot archive ENABLED keyspace")
	errIllegalID        = errors.New("Cannot create keyspace with that ID")
	errIllegalName      = errors.New("Cannot create keyspace with that name")
	errIllegalOperation = errors.New("Illegal operation")
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
	// Name of the keyspace to be created.
	// Using an existing name will result in error.
	Name          string
	InitialConfig map[string]string
	Now           time.Time
}

// NewKeyspaceManager creates a Manager of keyspace related data.
func NewKeyspaceManager(store endpoint.KeyspaceStorage, idAllocator id.Allocator) *Manager {
	return &Manager{
		store:       store,
		idAllocator: idAllocator,
	}
}

// CreateKeyspace create a keyspace meta with initial config and save it to storage.
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
	// TODO: Enable Transaction at storage layer to save MetaData and NameToID in a single transaction.
	// Create and save keyspace metadata.
	keyspace := &keyspacepb.KeyspaceMeta{
		Id:             newID,
		Name:           request.Name,
		State:          keyspacepb.KeyspaceState_ENABLED,
		CreatedAt:      request.Now.Unix(),
		StateChangedAt: request.Now.Unix(),
		Config:         request.InitialConfig,
	}
	manager.metaLock.Lock()
	defer manager.metaLock.Unlock()
	// Check if keyspace with that id already exists.
	idExists, err := manager.store.LoadKeyspace(newID, &keyspacepb.KeyspaceMeta{})
	if err != nil {
		return nil, err
	}
	if idExists {
		return nil, ErrKeyspaceExists
	}
	// Save keyspace meta before saving id.
	if err := manager.store.SaveKeyspace(keyspace); err != nil {
		return nil, err
	}
	// Create name to ID entry,
	// if this failed, previously stored keyspace meta should be removed.
	if err = manager.createNameToID(newID, request.Name); err != nil {
		if removeErr := manager.store.RemoveKeyspace(newID); removeErr != nil {
			return nil, errors.Wrap(removeErr, "failed to remove keyspace meta after save spaceID failure")
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
	// Load the keyspace with target ID.
	keyspace := &keyspacepb.KeyspaceMeta{}
	loaded, err = manager.store.LoadKeyspace(spaceID, keyspace)
	if err != nil {
		return nil, err
	}
	if !loaded {
		return nil, ErrKeyspaceNotFound
	}
	return keyspace, nil
}

// UpdateKeyspaceConfig apply mutations to target keyspace in order.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceConfig(name string, mutations []*keyspacepb.Mutation) (*keyspacepb.KeyspaceMeta, error) {
	manager.metaLock.Lock()
	defer manager.metaLock.Unlock()
	// Load keyspace by name.
	keyspace, err := manager.LoadKeyspace(name)
	if err != nil {
		return nil, err
	}
	// Changing ARCHIVED keyspace's config is not allowed.
	if keyspace.State == keyspacepb.KeyspaceState_ARCHIVED {
		return nil, errKeyspaceArchived
	}
	// Update keyspace config according to mutations.
	for _, mutation := range mutations {
		switch mutation.Op {
		case keyspacepb.Op_PUT:
			keyspace.Config[string(mutation.Key)] = string(mutation.Value)
		case keyspacepb.Op_DEL:
			delete(keyspace.Config, string(mutation.Key))
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
	manager.metaLock.Lock()
	defer manager.metaLock.Unlock()
	// Load keyspace by name.
	keyspace, err := manager.LoadKeyspace(name)
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
	return manager.store.LoadRangeKeyspace(startID, limit)
}

// allocID allocate a new keyspace id.
func (manager *Manager) allocID() (uint32, error) {
	id64, err := manager.idAllocator.Alloc()
	if err != nil {
		return 0, err
	}
	id32 := uint32(id64)
	// If obtained id is too small, re-allocate a higher ID.
	if id32 < spaceIDMin {
		return manager.allocID()
	}
	if err = validateID(id32); err != nil {
		return 0, err
	}
	return id32, nil
}

// validateID check if keyspace falls within the acceptable range.
// It throws errIllegalID when input id is our of range.
func validateID(spaceID uint32) error {
	if spaceID < spaceIDMin || spaceID > spaceIDMax {
		return errIllegalID
	}
	return nil
}

// validateName check if name contains illegal character.
// It throws errIllegalName when name contains illegal character.
func validateName(name string) error {
	// Name should not be empty.
	if name == "" {
		return errIllegalName
	}
	// Name should not contain any illegal character.
	if strings.ContainsAny(name, illegalChars) {
		return errIllegalName
	}
	return nil
}

// createNameToID create a keyspace name to ID lookup entry.
// It returns error if saving keyspace name meet error or if name has already been taken.
func (manager *Manager) createNameToID(spaceID uint32, name string) error {
	manager.idLock.Lock()
	defer manager.idLock.Unlock()

	nameExists, _, err := manager.store.LoadKeyspaceIDByName(name)
	if err != nil {
		return err
	}
	if nameExists {
		return ErrKeyspaceExists
	}
	return manager.store.SaveKeyspaceIDByName(spaceID, name)
}
