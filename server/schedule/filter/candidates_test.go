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
// See the License for the specific language governing permissions and
// limitations under the License.

package filter

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"

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

type testCandidatesSuite struct{}

var _ = Suite(&testCandidatesSuite{})

func (s *testCandidatesSuite) TestCandidates(c *C) {
	cs := s.newCandidates(1, 2, 3, 4, 5)
	cs.FilterSource(nil, idFilter(func(id uint64) bool { return id > 2 }))
	s.check(c, cs, 3, 4, 5)
	cs.FilterTarget(nil, idFilter(func(id uint64) bool { return id%2 == 1 }))
	s.check(c, cs, 3, 5)
	cs.FilterTarget(nil, idFilter(func(id uint64) bool { return id > 100 }))
	s.check(c, cs)
	store := cs.PickFirst()
	c.Assert(store, IsNil)
	store = cs.RandomPick()
	c.Assert(store, IsNil)

	cs = s.newCandidates(1, 3, 5, 7, 6, 2, 4)
	cs.Sort(idComparer)
	s.check(c, cs, 1, 2, 3, 4, 5, 6, 7)
	store = cs.PickFirst()
	c.Assert(store.GetID(), Equals, uint64(1))
	cs.Reverse()
	s.check(c, cs, 7, 6, 5, 4, 3, 2, 1)
	store = cs.PickFirst()
	c.Assert(store.GetID(), Equals, uint64(7))
	cs.Shuffle()
	cs.Sort(idComparer)
	s.check(c, cs, 1, 2, 3, 4, 5, 6, 7)
	store = cs.RandomPick()
	c.Assert(store.GetID(), Greater, uint64(0))
	c.Assert(store.GetID(), Less, uint64(8))

	cs = s.newCandidates(10, 15, 23, 20, 33, 32, 31)
	cs.Sort(idComparer).Reverse().Top(idComparer2)
	s.check(c, cs, 33, 32, 31)
}

func (s *testCandidatesSuite) newCandidates(ids ...uint64) *StoreCandidates {
	var stores []*core.StoreInfo
	for _, id := range ids {
		stores = append(stores, core.NewStoreInfo(&metapb.Store{Id: id}))
	}
	return NewCandidates(stores)
}

func (s *testCandidatesSuite) check(c *C, candidates *StoreCandidates, ids ...uint64) {
	c.Assert(candidates.Stores, HasLen, len(ids))
	for i, s := range candidates.Stores {
		c.Assert(s.GetID(), Equals, ids[i])
	}
}
