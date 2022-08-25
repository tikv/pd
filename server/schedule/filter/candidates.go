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
	"math/rand"
	"sort"

	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/plan"
)

// StoreCandidates wraps store list and provide utilities to select source or
// target store to schedule.
type StoreCandidates struct {
	Stores []*core.StoreInfo
}

// NewCandidates creates StoreCandidates with store list.
func NewCandidates(stores []*core.StoreInfo) *StoreCandidates {
	return &StoreCandidates{Stores: stores}
}

// FilterSource keeps stores that can pass all source filters.
func (c *StoreCandidates) FilterSource(opt *config.PersistOptions, collector *plan.Collector, filters ...Filter) *StoreCandidates {
	c.Stores = SelectSourceStores(c.Stores, filters, opt, collector)
	return c
}

// FilterTarget keeps stores that can pass all target filters.
func (c *StoreCandidates) FilterTarget(opt *config.PersistOptions, collector *plan.Collector, filters ...Filter) *StoreCandidates {
	c.Stores = SelectTargetStores(c.Stores, filters, opt, collector)
	return c
}

// Sort sorts store list by given comparer in ascending order.
func (c *StoreCandidates) Sort(less StoreComparer) *StoreCandidates {
	sort.Slice(c.Stores, func(i, j int) bool { return less(c.Stores[i], c.Stores[j]) < 0 })
	return c
}

// Shuffle reorders all candidates randomly.
func (c *StoreCandidates) Shuffle() *StoreCandidates {
	rand.Shuffle(len(c.Stores), func(i, j int) { c.Stores[i], c.Stores[j] = c.Stores[j], c.Stores[i] })
	return c
}

// KeepTheTopStores keeps the slice of the stores in the front order by asc.
func (c *StoreCandidates) KeepTheTopStores(cmp StoreComparer, asc bool) *StoreCandidates {
	if len(c.Stores) <= 1 {
		return c
	}
	topIdx := 0
	for idx := 1; idx < c.Len(); idx++ {
		compare := cmp(c.Stores[topIdx], c.Stores[idx])
		if compare == 0 {
			topIdx++
		} else if (compare > 0 && asc) || (!asc && compare < 0) {
			topIdx = 0
		} else {
			continue
		}
		c.Stores[idx], c.Stores[topIdx] = c.Stores[topIdx], c.Stores[idx]
	}
	c.Stores = c.Stores[:topIdx+1]
	return c
}

// PickTheTopStore returns the first store order by asc.
// It returns the min item when asc is true, returns the max item when asc is false.
func (c *StoreCandidates) PickTheTopStore(cmp StoreComparer, asc bool) *core.StoreInfo {
	if len(c.Stores) == 0 {
		return nil
	}
	topIdx := 0
	for idx := 1; idx < len(c.Stores); idx++ {
		compare := cmp(c.Stores[topIdx], c.Stores[idx])
		if (compare > 0 && asc) || (!asc && compare < 0) {
			topIdx = idx
		}
	}
	return c.Stores[topIdx]
}

// PickFirst returns the first store in candidate list.
func (c *StoreCandidates) PickFirst() *core.StoreInfo {
	if len(c.Stores) == 0 {
		return nil
	}
	return c.Stores[0]
}

// RandomPick returns a random store from the list.
func (c *StoreCandidates) RandomPick() *core.StoreInfo {
	if len(c.Stores) == 0 {
		return nil
	}
	return c.Stores[rand.Intn(len(c.Stores))]
}

// PickAll return all stores in candidate list.
func (c *StoreCandidates) PickAll() []*core.StoreInfo {
	return c.Stores
}

// Len returns a length of candidate list.
func (c *StoreCandidates) Len() int {
	return len(c.Stores)
}
