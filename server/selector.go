// Copyright 2016 PingCAP, Inc.
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

package server

import "math/rand"

// Selector is an interface to select source and target store to schedule.
type Selector interface {
	SelectSource(stores []*StoreInfo, filters ...Filter) *StoreInfo
	SelectTarget(stores []*StoreInfo, filters ...Filter) *StoreInfo
}

type balanceSelector struct {
	kind    ResourceKind
	filters []Filter
}

func newBalanceSelector(kind ResourceKind, filters []Filter) *balanceSelector {
	return &balanceSelector{
		kind:    kind,
		filters: filters,
	}
}

func (s *balanceSelector) SelectSource(stores []*StoreInfo, filters ...Filter) *StoreInfo {
	filters = append(filters, s.filters...)

	var result *StoreInfo
	for _, store := range stores {
		if filterSource(store, filters) {
			continue
		}
		if result == nil || result.resourceScore(s.kind) < store.resourceScore(s.kind) {
			result = store
		}
	}
	return result
}

func (s *balanceSelector) SelectTarget(stores []*StoreInfo, filters ...Filter) *StoreInfo {
	filters = append(filters, s.filters...)

	var result *StoreInfo
	for _, store := range stores {
		if filterTarget(store, filters) {
			continue
		}
		if result == nil || result.resourceScore(s.kind) > store.resourceScore(s.kind) {
			result = store
		}
	}
	return result
}

type replicaSelector struct {
	regionStores []*StoreInfo
	rep          *Replication
	filters      []Filter
}

func newReplicaSelector(regionStores []*StoreInfo, rep *Replication, filters ...Filter) Selector {
	return &replicaSelector{
		regionStores: regionStores,
		rep:          rep,
		filters:      filters,
	}
}

func (s *replicaSelector) SelectSource(stores []*StoreInfo, filters ...Filter) *StoreInfo {
	var (
		best      *StoreInfo
		bestScore float64
	)
	for _, store := range stores {
		if filterSource(store, filters) {
			continue
		}
		score := s.rep.GetDistinctScore(s.regionStores, store)
		if best == nil || compareStoreScore(store, score, best, bestScore) < 0 {
			best, bestScore = store, score
		}
	}
	if best == nil || filterSource(best, s.filters) {
		return nil
	}
	return best
}

func (s *replicaSelector) SelectTarget(stores []*StoreInfo, filters ...Filter) *StoreInfo {
	var (
		best      *StoreInfo
		bestScore float64
	)
	for _, store := range stores {
		if filterTarget(store, filters) {
			continue
		}
		score := s.rep.GetDistinctScore(s.regionStores, store)
		if best == nil || compareStoreScore(store, score, best, bestScore) > 0 {
			best, bestScore = store, score
		}
	}
	if best == nil || filterTarget(best, s.filters) {
		return nil
	}
	return best
}

type randomSelector struct {
	filters []Filter
}

func newRandomSelector(filters []Filter) *randomSelector {
	return &randomSelector{filters: filters}
}

func (s *randomSelector) Select(stores []*StoreInfo) *StoreInfo {
	if len(stores) == 0 {
		return nil
	}
	return stores[rand.Int()%len(stores)]
}

func (s *randomSelector) SelectSource(stores []*StoreInfo, filters ...Filter) *StoreInfo {
	filters = append(filters, s.filters...)

	var candidates []*StoreInfo
	for _, store := range stores {
		if filterSource(store, filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.Select(candidates)
}

func (s *randomSelector) SelectTarget(stores []*StoreInfo, filters ...Filter) *StoreInfo {
	filters = append(filters, s.filters...)

	var candidates []*StoreInfo
	for _, store := range stores {
		if filterTarget(store, filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.Select(candidates)
}
