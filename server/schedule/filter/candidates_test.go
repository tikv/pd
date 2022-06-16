// Copyright 2020 TiKV Project Authors.
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

package filter

import (
	"container/heap"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

// A dummy comparer for testing.
func idComparer(a, b *core.StoreInfo) int {
	if a.GetID() > b.GetID() {
		return 1
	}
	if a.GetID() < b.GetID() {
		return -1
	}
	return 0
}

// Another dummy comparer for testing.
func idComparer2(a, b *core.StoreInfo) int {
	if a.GetID()/10 > b.GetID()/10 {
		return 1
	}
	if a.GetID()/10 < b.GetID()/10 {
		return -1
	}
	return 0
}

type idFilter func(uint64) bool

func (f idFilter) Scope() string { return "idFilter" }
func (f idFilter) Type() string  { return "idFilter" }
func (f idFilter) Source(opt *config.PersistOptions, store *core.StoreInfo) bool {
	return f(store.GetID())
}
func (f idFilter) Target(opt *config.PersistOptions, store *core.StoreInfo) bool {
	return f(store.GetID())
}

func TestCandidates(t *testing.T) {
	re := require.New(t)
	cs := newTestCandidates(1, 2, 3, 4, 5)
	cs.FilterSource(nil, idFilter(func(id uint64) bool { return id > 2 }))
	check(re, cs, 3, 4, 5)
	cs.FilterTarget(nil, idFilter(func(id uint64) bool { return id%2 == 1 }))
	check(re, cs, 3, 5)
	cs.FilterTarget(nil, idFilter(func(id uint64) bool { return id > 100 }))
	check(re, cs)
	store := cs.PickFirst()
	re.Nil(store)
	store = cs.RandomPick()
	re.Nil(store)

	cs = newTestCandidates(1, 3, 5, 7, 6, 2, 4)
	minStore := cs.PickTheTopStore(idComparer, true)
	re.Equal(uint64(1), minStore.GetID())
	maxStore := cs.PickTheTopStore(idComparer, false)
	re.Equal(uint64(7), maxStore.GetID())

	cs.Sort(idComparer)
	check(re, cs, 1, 2, 3, 4, 5, 6, 7)
	store = cs.PickFirst()
	re.Equal(uint64(1), store.GetID())
	store = cs.PickTheTopStore(idComparer, false)
	re.Equal(uint64(7), store.GetID())
	cs.Shuffle()
	cs.Sort(idComparer)
	check(re, cs, 1, 2, 3, 4, 5, 6, 7)
	store = cs.RandomPick()
	re.Greater(store.GetID(), uint64(0))
	re.Less(store.GetID(), uint64(8))

	cs = newTestCandidates(10, 15, 23, 20, 33, 32, 31)
	cs.KeepTheTopStores(idComparer2, false)
	check(re, cs, 33, 32, 31)

	cs = newTestCandidates(10, 15, 23, 20, 33, 32, 31)
	cs.KeepTheTopStores(idComparer2, true)
	check(re, cs, 10, 15)
}

func TestCandidatesHeap(t *testing.T) {
	re := require.New(t)
	csh := newTestCandidatesHeap(5, 3, 4, 2, 1)
	heap.Init(csh)
	check(re, csh.StoreCandidates, 1, 2, 4, 5, 3)

	h := heap.Pop(csh)
	re.Equal(h.(*core.StoreInfo).GetID(), uint64(1))
	check(re, csh.StoreCandidates, 2, 3, 4, 5)

	id1 := core.NewStoreInfo(&metapb.Store{Id: 1})
	heap.Push(csh, id1)
	check(re, csh.StoreCandidates, 1, 2, 4, 5, 3)

	id7 := core.NewStoreInfo(&metapb.Store{Id: 7})
	heap.Push(csh, id7)
	check(re, csh.StoreCandidates, 1, 2, 4, 5, 3, 7)

	id0 := core.NewStoreInfo(&metapb.Store{Id: 0})
	heap.Push(csh, id0)
	check(re, csh.StoreCandidates, 0, 2, 1, 5, 3, 7, 4)

	// Remove element at index 1, which should be of value 2
	heap.Remove(csh, 1)
	check(re, csh.StoreCandidates, 0, 3, 1, 5, 4, 7)

	csh.StoreCandidates.Stores[2].GetMeta().Id = uint64(9)
	check(re, csh.StoreCandidates, 0, 3, 9, 5, 4, 7)

	heap.Fix(csh, 2)
	check(re, csh.StoreCandidates, 0, 3, 7, 5, 4, 9)
}

func newTestCandidates(ids ...uint64) *StoreCandidates {
	stores := make([]*core.StoreInfo, 0, len(ids))
	for _, id := range ids {
		stores = append(stores, core.NewStoreInfo(&metapb.Store{Id: id}))
	}
	return NewCandidates(stores)
}

func check(re *require.Assertions, candidates *StoreCandidates, ids ...uint64) {
	re.Len(candidates.Stores, len(ids))
	for i, s := range candidates.Stores {
		re.Equal(ids[i], s.GetID())
	}
}

func newTestCandidatesHeap(ids ...uint64) *StoreCandidatesHeap {
	candidates := newTestCandidates(ids...)
	// NewCandidatesHeap creates StoreCandidatesHeap with storeCandidates and comparer
	candidatesHeap := NewStoreCandidatesHeap(candidates, idComparer)
	return candidatesHeap
}
