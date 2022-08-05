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
	"fmt"
	"regexp"

	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/syncutil"
)

const (
	spaceIDMax = ^uint32(0) >> 8 // 16777215 (Uint24Max) is the maximum value of spaceID.
	// namePattern is a regex that specifies acceptable characters of the keyspace name.
	// Name must be non-empty and contains only alphanumerical, `_` and `-`.
	namePattern = "^[-A-Za-z0-9_]+$"
)

var (
	// ErrKeyspaceNotFound is used to indicate target keyspace does not exist.
	ErrKeyspaceNotFound = errors.New("keyspace does not exist")
	// ErrKeyspaceExists indicates target keyspace already exists.
	// Used when creating a new keyspace.
	ErrKeyspaceExists   = errors.New("keyspace already exists")
	errKeyspaceArchived = errors.New("keyspace already archived")
	errArchiveEnabled   = errors.New("cannot archive ENABLED keyspace")
	errModifyDefault    = errors.New("cannot modify default keyspace's state")
	errIllegalID        = errors.New("illegal keyspace ID")
	errIllegalName      = errors.New("illegal keyspace name")
	errIllegalOperation = errors.New("unknown operation")
)

// validateID check if keyspace falls within the acceptable range.
// It throws errIllegalID when input id is our of range,
// or if it collides with reserved id.
func validateID(spaceID uint32) error {
	if spaceID > spaceIDMax {
		return errIllegalID
	}
	if spaceID == DefaultKeyspaceID {
		return errIllegalID
	}
	return nil
}

// validateName check if user provided name is legal.
// It throws errIllegalName when name contains illegal character,
// or if it collides with reserved name.
func validateName(name string) error {
	isValid, err := regexp.MatchString(namePattern, name)
	if err != nil {
		return err
	}
	if !isValid {
		return errIllegalName
	}
	if name == DefaultKeyspaceName {
		return errIllegalName
	}
	return nil
}

// lockGroup is a map of mutex that locks each keyspace separately.
// It's used to guarantee that operations on different keyspace won't block each other.
type lockGroup struct {
	groupLock syncutil.Mutex             // protects group.
	entries   map[uint32]*syncutil.Mutex // map of locks with keyspaceID as key.
	// hashFn hashes spaceID to map key, it's main purpose is to limit the total
	// number of mutexes in the group, as using a mutex for every keyspace is too memory heavy.
	hashFn func(spaceID uint32) uint32
}

// newLockGroup create and return a empty lockGroup.
func newLockGroup() *lockGroup {
	return &lockGroup{
		entries: make(map[uint32]*syncutil.Mutex),
		// A simple mask is applied to spaceID to use its last byte as map key,
		// limiting the maximum map length to 256.
		// Since keyspaceID is sequentially allocated, this can also reduce the chance
		// of collision when comparing with random hashes.
		hashFn: func(spaceID uint32) uint32 {
			return spaceID & 0xFF
		},
	}
}

func (g *lockGroup) lock(spaceID uint32) {
	g.groupLock.Lock()
	hashedID := spaceID & 0xFF
	e, ok := g.entries[hashedID]
	// If target keyspace's lock has not been initialized.
	if !ok {
		e = &syncutil.Mutex{}
		g.entries[hashedID] = e
	}
	g.groupLock.Unlock()
	e.Lock()
}

func (g *lockGroup) unlock(spaceID uint32) {
	g.groupLock.Lock()
	hashedID := spaceID & 0xFF
	e, ok := g.entries[hashedID]
	if !ok {
		// Entry must exist, otherwise there should be a run-time error and panic.
		g.groupLock.Unlock()
		panic(fmt.Errorf("unlock requested for key %v, but no entry found", spaceID))
	}
	g.groupLock.Unlock()
	e.Unlock()
}
