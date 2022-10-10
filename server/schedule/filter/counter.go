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

package filter

import (
	"fmt"
	"strconv"
)

type action int

const (
	sourceFilter action = iota
	targetFilter

	ActionLen
)

var actions = [ActionLen]string{
	"filter-source",
	"filter-target",
}

// String implements fmt.Stringer interface.
func (a action) String() string {
	if a < ActionLen {
		return actions[a]
	}
	return "unknown"
}

type scope int

const (
	BalanceLeader scope = iota
	BalanceRegion
	BalanceHotRegion
	Label

	EvictLeader
	RegionScatter
	ReplicaChecker
	RuleChecker

	GrantHotLeader
	ShuffleHotRegion
	ShuffleRegion
	RandomMerge
	ScopeLen
)

var scopes = [ScopeLen]string{
	"balance-leader-scheduler",
	"balance-region-scheduler",
	"balance-hot-region-scheduler",
	"label-scheduler",

	"evict-leader-scheduler",
	"region-scatter",
	"replica-checker",
	"rule-checker",

	"grant-hot-leader-scheduler",
	"shuffle-region-scheduler",
	"shuffle-region-scheduler",
	"random-merge-scheduler",
}

// String implements fmt.Stringer interface.
func (s scope) String() string {
	if s >= ScopeLen {
		return "unknown"
	}
	return scopes[s]
}

type filterType int

const (
	excludedFilterType filterType = iota
	storageThresholdFilterType
	distinctScoreFilterType
	labelConstraintFilterType
	ruleFitFilterType
	ruleLeaderFilterType
	engineFilterType
	specialUseFilterType
	isolationFilterType
	regionScoreFilterType
	idFilterType
	// the following filters are used for store state, the other filter should be placed above it.
	storeStateFilterType

	filtersLen = iota + ReasonLen - 1
)

var filters = [filtersLen]string{
	"exclude-filter",
	"storage-threshold-filter",
	"distinct-filter",
	"label-constraint-filter",
	"rule-fit-filter",
	"rule-fit-leader-filter",
	"engine-filter",
	"special-use-filter",
	"isolation-filter",
	"region-score-filter",
	"idFilter",
	"store-state",
}

type storeStateReason int

const (
	ok storeStateReason = iota
	tombstone
	down
	offline
	pauseLeader
	slowStore
	disconnected
	busy
	exceedRemoveLimit
	exceedAddLimit
	tooManySnapshot
	tooManyPendingPeer
	rejectLeader
	ReasonLen
)

var storeStateReasons = [ReasonLen]string{
	"",
	"tombstone",
	"down",
	"offline",
	"pause-leader",
	"slow-leader",
	"disconnected",
	"busy",
	"exceed-remove-limit",
	"exceed-add-limit",
	"too-many-snapshot",
	"too-many-pending-peer",
	"reject-leader",
}

// String implements fmt.Stringer interface.
func (r storeStateReason) String() string {
	if r < ReasonLen {
		return storeStateReasons[r]
	}
	return "unknown"
}

// String implements fmt.Stringer interface.
func (f filterType) String() string {
	if f < storeStateFilterType {
		return filters[f]
	}
	if int(f) < int(filtersLen) {
		return fmt.Sprintf("%s-%s-filter", filters[storeStateFilterType], storeStateReasons[f-storeStateFilterType])
	}
	return "unknown"
}

// FilterCounter records the filter counter.
type FilterCounter struct {
	scope string
	// record filter counter for each store.
	// [action][type][sourceID][targetID]count
	// [source-filter][rule-fit-filter]<1->2><10>
	counter [][]map[uint64]map[uint64]int
}

// NewFilterCounter creates a FilterCounter.
func NewFilterCounter(scope string) *FilterCounter {
	counter := make([][]map[uint64]map[uint64]int, ActionLen)
	for i := range counter {
		counter[i] = make([]map[uint64]map[uint64]int, filtersLen)
		for k := range counter[i] {
			counter[i][k] = make(map[uint64]map[uint64]int)
		}
	}
	return &FilterCounter{counter: counter, scope: scope}
}

// Add adds the filter counter.
func (c *FilterCounter) inc(action action, filterType filterType, sourceID uint64, targetID uint64) {
	if _, ok := c.counter[action][filterType][sourceID]; !ok {
		c.counter[action][filterType][sourceID] = make(map[uint64]int)
	}
	c.counter[action][filterType][sourceID][targetID]++
}

// Flush flushes the counter to the metrics.
func (c *FilterCounter) Flush() {
	for i, actions := range c.counter {
		actionName := action(i).String()
		for j, counters := range actions {
			filterName := filterType(j).String()
			for sourceID, count := range counters {
				sourceIDStr := strconv.FormatUint(sourceID, 10)
				for targetID, value := range count {
					targetIDStr := strconv.FormatUint(sourceID, 10)
					if value > 0 {
						filterCounter.WithLabelValues(actionName, c.scope, filterName, sourceIDStr, targetIDStr).
							Add(float64(value))
						counters[sourceID][targetID] = 0
					}
				}
			}
		}
	}
}
