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
	SelectSource(stores []*storeInfo, filters ...Filter) *storeInfo
	SelectTarget(stores []*storeInfo, filters ...Filter) *storeInfo
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

func (s *balanceSelector) SelectSource(stores []*storeInfo, filters ...Filter) *storeInfo {
	filters = append(s.filters, filters...)

	var result *storeInfo
	for _, store := range stores {
		if filterSource(store, filters) {
			continue
		}
		if result == nil || result.resourceRatio(s.kind) < store.resourceRatio(s.kind) {
			result = store
		}
	}
	return result
}

func (s *balanceSelector) SelectTarget(stores []*storeInfo, filters ...Filter) *storeInfo {
	filters = append(s.filters, filters...)

	var result *storeInfo
	for _, store := range stores {
		if filterTarget(store, filters) {
			continue
		}
		if result == nil || result.resourceRatio(s.kind) > store.resourceRatio(s.kind) {
			result = store
		}
	}
	return result
}

type filterFunc func(*storeInfo) bool

type randomSelector struct {
	kind ResourceKind
}

func newRandomSelector(kind ResourceKind) *randomSelector {
	return &randomSelector{kind: kind}
}

func (s *randomSelector) Select(stores []*storeInfo) *storeInfo {
	if len(stores) == 0 {
		return nil
	}
	return stores[rand.Int()%len(stores)]
}

func (s *randomSelector) SelectSource(stores []*storeInfo, filters ...Filter) *storeInfo {
	var candidates []*storeInfo
	for _, store := range stores {
		if filterSource(store, filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.Select(candidates)
}

func (s *randomSelector) SelectTarget(stores []*storeInfo, filters ...Filter) *storeInfo {
	var candidates []*storeInfo
	for _, store := range stores {
		if filterTarget(store, filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.Select(candidates)
}
