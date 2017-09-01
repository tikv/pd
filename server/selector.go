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

import (
	"math/rand"

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

// Selector is an interface to select source and target store to schedule.
type Selector interface {
	SelectSource(stores []*core.StoreInfo, filters ...schedule.Filter) *core.StoreInfo
	SelectTarget(stores []*core.StoreInfo, filters ...schedule.Filter) *core.StoreInfo
}

type balanceSelector struct {
	kind    core.ResourceKind
	filters []schedule.Filter
}

func newBalanceSelector(kind core.ResourceKind, filters []schedule.Filter) *balanceSelector {
	return &balanceSelector{
		kind:    kind,
		filters: filters,
	}
}

func (s *balanceSelector) SelectSource(stores []*core.StoreInfo, filters ...schedule.Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)

	var result *core.StoreInfo
	for _, store := range stores {
		if schedule.FilterSource(store, filters) {
			continue
		}
		if result == nil || result.ResourceScore(s.kind) < store.ResourceScore(s.kind) {
			result = store
		}
	}
	return result
}

func (s *balanceSelector) SelectTarget(stores []*core.StoreInfo, filters ...schedule.Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)

	var result *core.StoreInfo
	for _, store := range stores {
		if schedule.FilterTarget(store, filters) {
			continue
		}
		if result == nil || result.ResourceScore(s.kind) > store.ResourceScore(s.kind) {
			result = store
		}
	}
	return result
}

type replicaSelector struct {
	regionStores []*core.StoreInfo
	rep          *Replication
	filters      []schedule.Filter
}

func newReplicaSelector(regionStores []*core.StoreInfo, rep *Replication, filters ...schedule.Filter) Selector {
	return &replicaSelector{
		regionStores: regionStores,
		rep:          rep,
		filters:      filters,
	}
}

func (s *replicaSelector) SelectSource(stores []*core.StoreInfo, filters ...schedule.Filter) *core.StoreInfo {
	var (
		best      *core.StoreInfo
		bestScore float64
	)
	for _, store := range stores {
		if schedule.FilterSource(store, filters) {
			continue
		}
		score := schedule.DistinctScore(s.rep.GetLocationLabels(), s.regionStores, store)
		if best == nil || compareStoreScore(store, score, best, bestScore) < 0 {
			best, bestScore = store, score
		}
	}
	if best == nil || schedule.FilterSource(best, s.filters) {
		return nil
	}
	return best
}

func (s *replicaSelector) SelectTarget(stores []*core.StoreInfo, filters ...schedule.Filter) *core.StoreInfo {
	var (
		best      *core.StoreInfo
		bestScore float64
	)
	for _, store := range stores {
		if schedule.FilterTarget(store, filters) {
			continue
		}
		score := schedule.DistinctScore(s.rep.GetLocationLabels(), s.regionStores, store)
		if best == nil || compareStoreScore(store, score, best, bestScore) > 0 {
			best, bestScore = store, score
		}
	}
	if best == nil || schedule.FilterTarget(best, s.filters) {
		return nil
	}
	return best
}

type randomSelector struct {
	filters []schedule.Filter
}

func newRandomSelector(filters []schedule.Filter) *randomSelector {
	return &randomSelector{filters: filters}
}

func (s *randomSelector) Select(stores []*core.StoreInfo) *core.StoreInfo {
	if len(stores) == 0 {
		return nil
	}
	return stores[rand.Int()%len(stores)]
}

func (s *randomSelector) SelectSource(stores []*core.StoreInfo, filters ...schedule.Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)

	var candidates []*core.StoreInfo
	for _, store := range stores {
		if schedule.FilterSource(store, filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.Select(candidates)
}

func (s *randomSelector) SelectTarget(stores []*core.StoreInfo, filters ...schedule.Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)

	var candidates []*core.StoreInfo
	for _, store := range stores {
		if schedule.FilterTarget(store, filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.Select(candidates)
}
