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
	storeStateStopping
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
	"store-state-stopping-filter",
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
	// [action][type][storeID]count
	// source: [source][rule-fit-filter][sourceID]count
	// target: [target][rule-fit-filter][targetID]count
	counter [][]map[uint64]int
}

// NewCounter creates a Counter.
func NewCounter(scope string) *Counter {
	counter := make([][]map[uint64]int, actionLen)
	for i := range counter {
		counter[i] = make([]map[uint64]int, filtersLen)
		for k := range counter[i] {
			counter[i][k] = make(map[uint64]int)
		}
	}
	return &Counter{counter: counter, scope: scope}
}

// SetScope sets the scope for the counter.
func (c *Counter) SetScope(scope string) {
	c.scope = scope
}

// Add adds the filter counter.
func (c *Counter) inc(action action, filterType filterType, storeID uint64) {
	c.counter[action][filterType][storeID]++
}

// Flush flushes the counter to the metrics.
func (c *Counter) Flush() {
	for i, actions := range c.counter {
		actionType := action(i)
		for j, counters := range actions {
			filterName := filterType(j).String()
			for storeID, value := range counters {
				if value > 0 {
					storeIDStr := strconv.FormatUint(storeID, 10)
					switch actionType {
					case source:
						filterSourceCounter.WithLabelValues(c.scope, filterName, storeIDStr).Add(float64(value))
					case target:
						filterTargetCounter.WithLabelValues(c.scope, filterName, storeIDStr).Add(float64(value))
					}
					counters[storeID] = 0
				}
			}
		}
	}
}
