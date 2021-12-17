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
func (c *StoreCandidates) FilterSource(opt *config.PersistOptions, filters ...Filter) *StoreCandidates {
	c.Stores = SelectSourceStores(c.Stores, filters, opt)
	return c
}

// FilterTarget keeps stores that can pass all target filters.
func (c *StoreCandidates) FilterTarget(opt *config.PersistOptions, filters ...Filter) *StoreCandidates {
	c.Stores = SelectTargetStores(c.Stores, filters, opt)
	return c
}

// Sort sorts store list by given comparer in ascending order.
func (c *StoreCandidates) Sort(less StoreComparer) *StoreCandidates {
	sort.Slice(c.Stores, func(i, j int) bool { return less(c.Stores[i], c.Stores[j]) < 0 })
	return c
}

// Reverse reverses the candidate store list.
func (c *StoreCandidates) Reverse() *StoreCandidates {
	for i := len(c.Stores)/2 - 1; i >= 0; i-- {
		opp := len(c.Stores) - 1 - i
		c.Stores[i], c.Stores[opp] = c.Stores[opp], c.Stores[i]
	}
	return c
}

// Shuffle reorders all candidates randomly.
func (c *StoreCandidates) Shuffle() *StoreCandidates {
	rand.Shuffle(len(c.Stores), func(i, j int) { c.Stores[i], c.Stores[j] = c.Stores[j], c.Stores[i] })
	return c
}

// Top keeps all stores that have the same priority with the first store.
// The store list should be sorted before calling Top.
func (c *StoreCandidates) Top(less StoreComparer) *StoreCandidates {
	var i int
	for i < len(c.Stores) && less(c.Stores[0], c.Stores[i]) == 0 {
		i++
	}
	c.Stores = c.Stores[:i]
	return c
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
