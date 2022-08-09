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

package syncutil

import "fmt"

// LockGroup is a map of mutex that locks each keyspace separately.
// It's used to guarantee that operations on different keyspace won't block each other.
type LockGroup struct {
	groupLock Mutex             // protects group.
	entries   map[uint32]*Mutex // map of locks with keyspaceID as key.
	// hashFn hashes spaceID to map key, it's main purpose is to limit the total
	// number of mutexes in the group, as using a mutex for every keyspace is too memory heavy.
	hashFn func(spaceID uint32) uint32
}

// NewLockGroup create and return an empty lockGroup.
func NewLockGroup() *LockGroup {
	return &LockGroup{
		entries: make(map[uint32]*Mutex),
		// A simple mask is applied to spaceID to use its last byte as map key,
		// limiting the maximum map length to 256.
		// Since keyspaceID is sequentially allocated, this can also reduce the chance
		// of collision when comparing with random hashes.
		hashFn: func(spaceID uint32) uint32 {
			return spaceID & 0xFF
		},
	}
}

// Lock locks the given keyspace based on the hash of the spaceID.
func (g *LockGroup) Lock(spaceID uint32) {
	g.groupLock.Lock()
	hashedID := spaceID & 0xFF
	e, ok := g.entries[hashedID]
	// If target keyspace's lock has not been initialized, create a new lock.
	if !ok {
		e = &Mutex{}
		g.entries[hashedID] = e
	}
	g.groupLock.Unlock()
	e.Lock()
}

// Unlock unlocks the keyspace based on the hash of the spaceID.
func (g *LockGroup) Unlock(spaceID uint32) {
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
