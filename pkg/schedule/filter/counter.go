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
	"strconv"
)

type action int

const (
	source action = iota
	target

	actionLen
)

var actions = [actionLen]string{
	"filter-source",
	"filter-target",
}

// String implements fmt.Stringer interface.
func (a action) String() string {
	if a < actionLen {
		return actions[a]
	}
	return "unknown"
}

type filterType int

const (
	excluded filterType = iota
	storageThreshold
	distinctScore
	labelConstraint
	ruleFit
	ruleLeader
	engine
	specialUse
	isolation

	storeStateOK
	storeStateTombstone
	storeStateDown
	storeStateOffline
	storeStatePauseLeader
	storeStateSlow
	storeStateDisconnected
	storeStateBusy
	storeStateExceedRemoveLimit
	storeStateExceedAddLimit
	storeStateTooManySnapshot
	storeStateTooManyPendingPeer
	storeStateRejectLeader
	storeStateSlowTrend

	filtersLen
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

	"store-state-ok-filter",
	"store-state-tombstone-filter",
	"store-state-down-filter",
	"store-state-offline-filter",
	"store-state-pause-leader-filter",
	"store-state-slow-filter",
	"store-state-disconnect-filter",
	"store-state-busy-filter",
	"store-state-exceed-remove-limit-filter",
	"store-state-exceed-add-limit-filter",
	"store-state-too-many-snapshots-filter",
	"store-state-too-many-pending-peers-filter",
	"store-state-reject-leader-filter",
	"store-state-slow-trend-filter",
}

// String implements fmt.Stringer interface.
func (f filterType) String() string {
	if f < filtersLen {
		return filters[f]
	}

	return "unknown"
}

// Counter records the filter counter.
type Counter struct {
	scope string
	// record filter counter for each store.
	// [action][type][sourceID][targetID]count
	// [source-filter][rule-fit-filter]<1->2><10>
	counter [][]map[uint64]map[uint64]int
}

// NewCounter creates a Counter.
func NewCounter(scope string) *Counter {
	counter := make([][]map[uint64]map[uint64]int, actionLen)
	for i := range counter {
		counter[i] = make([]map[uint64]map[uint64]int, filtersLen)
		for k := range counter[i] {
			counter[i][k] = make(map[uint64]map[uint64]int)
		}
	}
	return &Counter{counter: counter, scope: scope}
}

// SetScope sets the scope for the counter.
func (c *Counter) SetScope(scope string) {
	c.scope = scope
}

// Add adds the filter counter.
func (c *Counter) inc(action action, filterType filterType, sourceID uint64, targetID uint64) {
	if _, ok := c.counter[action][filterType][sourceID]; !ok {
		c.counter[action][filterType][sourceID] = make(map[uint64]int)
	}
	c.counter[action][filterType][sourceID][targetID]++
}

// Flush flushes the counter to the metrics.
func (c *Counter) Flush() {
	for i, actions := range c.counter {
		actionName := action(i).String()
		for j, counters := range actions {
			filterName := filterType(j).String()
			for sourceID, count := range counters {
				sourceIDStr := strconv.FormatUint(sourceID, 10)
				for targetID, value := range count {
					targetIDStr := strconv.FormatUint(targetID, 10)
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
