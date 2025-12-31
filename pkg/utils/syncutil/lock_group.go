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

type lockEntry struct {
	mu       *Mutex
	refCount int
}

// LockGroup is a map of mutex that locks entries with different id separately.
// It's used levitate lock contentions of using a global lock.
type LockGroup struct {
	groupLock           Mutex                 // protects group.
	removeEntryOnUnlock bool                  // if remove entry from entries on Unlock().
	entries             map[uint32]*lockEntry // map of locks with id as key.
	// hashFn hashes id to map key, it's main purpose is to limit the total
	// number of mutexes in the group, as using a mutex for every id is too memory heavy.
	hashFn func(id uint32) uint32
}

// LockGroupOption configures the lock group.
type LockGroupOption func(lg *LockGroup)

// WithHash sets the lockGroup's hash function to provided hashFn.
func WithHash(hashFn func(id uint32) uint32) LockGroupOption {
	return func(lg *LockGroup) {
		lg.hashFn = hashFn
	}
}

// WithRemoveEntryOnUnlock sets the lockGroup's removeEntryOnUnlock to provided value.
func WithRemoveEntryOnUnlock(removeEntryOnUnlock bool) LockGroupOption {
	return func(lg *LockGroup) {
		lg.removeEntryOnUnlock = removeEntryOnUnlock
	}
}

// NewLockGroup create and return an empty lockGroup.
func NewLockGroup(options ...LockGroupOption) *LockGroup {
	lockGroup := &LockGroup{
		entries: make(map[uint32]*lockEntry),
		// If no custom hash function provided, use identity hash.
		hashFn: func(id uint32) uint32 { return id },
	}
	for _, op := range options {
		op(lockGroup)
	}
	return lockGroup
}

// Lock locks the target mutex base on the hash of id.
func (g *LockGroup) Lock(id uint32) {
	g.groupLock.Lock()
	hashedID := g.hashFn(id)
	e, ok := g.entries[hashedID]
	// If target id's lock has not been initialized, create a new lock.
	if !ok {
		e = &lockEntry{
			mu:       &Mutex{},
			refCount: 0,
		}
		g.entries[hashedID] = e
	}
	e.refCount++
	g.groupLock.Unlock()
	e.mu.Lock()
}

// Unlock unlocks the target mutex based on the hash of the id.
func (g *LockGroup) Unlock(id uint32) {
	g.groupLock.Lock()
	hashedID := g.hashFn(id)
	e, ok := g.entries[hashedID]
	if !ok {
		// Entry must exist, otherwise there should be a run-time error and panic.
		g.groupLock.Unlock()
		panic(fmt.Errorf("unlock requested for key %v, but no entry found", id))
	}
	e.refCount--
	if e.refCount == -1 {
		// Ref count should never be negative, otherwise there should be a run-time error and panic.
		g.groupLock.Unlock()
		panic(fmt.Errorf("unlock requested for key %v, but ref count is negative", id))
	}
	if g.removeEntryOnUnlock && e.refCount == 0 {
		delete(g.entries, hashedID)
	}
	g.groupLock.Unlock()
	e.mu.Unlock()
}
