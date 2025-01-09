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

type mutexPointer[M any] interface {
	Lock()
	Unlock()
	*M
}

type lockEntry[M any, PM mutexPointer[M]] struct {
	mu       PM
	refCount int
}

// lockGroup is a map of mutex that locks entries with different id separately.
// It's used levitate lock contentions of using a global lock.
type lockGroup[M any, PM mutexPointer[M]] struct {
	groupLock           Mutex                        // protects group.
	removeEntryOnUnlock bool                         // if remove entry from entries on Unlock().
	entries             map[uint32]*lockEntry[M, PM] // map of locks with id as key.
	// hashFn hashes id to map key, it's main purpose is to limit the total
	// number of mutexes in the group, as using a mutex for every id is too memory heavy.
	hashFn func(id uint32) uint32
}

type LockGroup = lockGroup[Mutex, *Mutex]

type lockGroupOptions struct {
	hashFn              func(id uint32) uint32
	removeEntryOnUnlock bool
}

// LockGroupOption configures the lock group.
type LockGroupOption func(lg *lockGroupOptions)

// WithHash sets the lockGroup's hash function to provided hashFn.
func WithHash(hashFn func(id uint32) uint32) LockGroupOption {
	return func(lg *lockGroupOptions) {
		lg.hashFn = hashFn
	}
}

// WithRemoveEntryOnUnlock sets the lockGroup's removeEntryOnUnlock to provided value.
func WithRemoveEntryOnUnlock(removeEntryOnUnlock bool) LockGroupOption {
	return func(lg *lockGroupOptions) {
		lg.removeEntryOnUnlock = removeEntryOnUnlock
	}
}

func newLockGroupImpl[M any, PM mutexPointer[M]](options ...LockGroupOption) *lockGroup[M, PM] {
	mergedOptions := lockGroupOptions{
		// If no custom hash function provided, use identity hash.
		hashFn: func(id uint32) uint32 { return id },
	}
	for _, op := range options {
		op(&mergedOptions)
	}
	lg := &lockGroup[M, PM]{
		entries:             make(map[uint32]*lockEntry[M, PM]),
		hashFn:              mergedOptions.hashFn,
		removeEntryOnUnlock: mergedOptions.removeEntryOnUnlock,
	}
	return lg
}

// NewLockGroup create and return an empty lockGroup.
func NewLockGroup(options ...LockGroupOption) *LockGroup {
	return newLockGroupImpl[Mutex, *Mutex](options...)
}

// Lock locks the target mutex base on the hash of id.
func (g *lockGroup[M, PM]) lockImpl(id uint32, lockFn func(PM)) {
	g.groupLock.Lock()
	hashedID := g.hashFn(id)
	e, ok := g.entries[hashedID]
	// If target id's lock has not been initialized, create a new lock.
	if !ok {
		e = &lockEntry[M, PM]{
			mu:       new(M),
			refCount: 0,
		}
		g.entries[hashedID] = e
	}
	e.refCount++
	g.groupLock.Unlock()
	lockFn(e.mu)
}

// Lock locks the target mutex base on the hash of id.
func (g *lockGroup[M, PM]) Lock(id uint32) {
	g.lockImpl(id, func(mu PM) {
		mu.Lock()
	})
}

func (g *lockGroup[M, PM]) unlockImpl(id uint32, unlockFn func(PM)) {
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
	unlockFn(e.mu)
}

// Unlock unlocks the target mutex based on the hash of the id.
func (g *lockGroup[M, PM]) Unlock(id uint32) {
	g.unlockImpl(id, func(mu PM) {
		mu.Unlock()
	})
}

type RWLockGroup lockGroup[RWMutex, *RWMutex]

func NewRWLockGroup(options ...LockGroupOption) *RWLockGroup {
	return (*RWLockGroup)(newLockGroupImpl[RWMutex, *RWMutex](options...))
}

func (g *RWLockGroup) Lock(id uint32) {
	(*lockGroup[RWMutex, *RWMutex])(g).Lock(id)
}

func (g *RWLockGroup) Unlock(id uint32) {
	(*lockGroup[RWMutex, *RWMutex])(g).Unlock(id)
}

func (g *RWLockGroup) RLock(id uint32) {
	(*lockGroup[RWMutex, *RWMutex])(g).lockImpl(id, func(mu *RWMutex) {
		mu.RLock()
	})
}

func (g *RWLockGroup) RUnlock(id uint32) {
	(*lockGroup[RWMutex, *RWMutex])(g).unlockImpl(id, func(mu *RWMutex) {
		mu.RUnlock()
	})
}
