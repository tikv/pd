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
	"fmt"
	"sort"
)

// Constraint is a replica constraint to place region peers.
type Constraint struct {
	Labels   map[string]string
	Replicas int
}

func (c *Constraint) String() string {
	return fmt.Sprintf("Constraint(%+v)", *c)
}

// Match returns true if store matches the constraint.
func (c *Constraint) Match(store *storeInfo) bool {
	labels := make(map[string]string)
	for _, label := range store.GetLabels() {
		labels[label.GetKey()] = label.GetValue()
	}
	for k, v := range c.Labels {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// Constraints contains all replica constraints.
type Constraints struct {
	MaxReplicas int
	Constraints []*Constraint
}

func newConstraints(maxReplicas int, constraints []*Constraint) *Constraints {
	sumReplicas := 0
	for _, constraint := range constraints {
		sumReplicas += constraint.Replicas
	}
	if maxReplicas <= sumReplicas {
		// Max replicas should not be smaller than the sum replicas.
		maxReplicas = sumReplicas
	} else {
		// Add a default constraint to cover other replicas.
		defaultConstraint := &Constraint{Replicas: maxReplicas - sumReplicas}
		constraints = append(constraints, defaultConstraint)
	}
	return &Constraints{
		MaxReplicas: maxReplicas,
		Constraints: constraints,
	}
}

func (c *Constraints) String() string {
	return fmt.Sprintf("Constraints(%+v)", *c)
}

// MatchResult is the result of matched stores and constraints.
type MatchResult struct {
	stores      map[uint64]*Constraint
	constraints []*MatchConstraint
}

// MatchConstraint is the result of matched store of a constraint.
type MatchConstraint struct {
	constraint *Constraint
	stores     []*storeInfo
}

// storesByID implements the Sort interface to sort stores by store ID.
type storesByID []*storeInfo

func (stores storesByID) Len() int           { return len(stores) }
func (stores storesByID) Swap(i, j int)      { stores[i], stores[j] = stores[j], stores[i] }
func (stores storesByID) Less(i, j int) bool { return stores[i].GetId() < stores[j].GetId() }

// Match matches stores and constraints.
// Although we can use a more optimize algorithm, but it may be hard
// to understand by most users.
// So we decide to use a straight forward algorithm to match stores
// and constraints one by one in order, and it works find in most
// cases. However, if we add more complicated constraints in the
// future, we may need to reconsider the algorithm.
func (c *Constraints) Match(stores []*storeInfo) *MatchResult {
	sort.Sort(storesByID(stores))

	constraints := make([]*MatchConstraint, 0, len(c.Constraints))
	for _, constraint := range c.Constraints {
		constraints = append(constraints, &MatchConstraint{constraint: constraint})
	}
	result := &MatchResult{
		stores:      make(map[uint64]*Constraint),
		constraints: constraints,
	}

	for _, store := range stores {
		for _, matched := range result.constraints {
			constraint := matched.constraint
			if len(matched.stores) >= constraint.Replicas {
				// The constraint has been satisfied.
				continue
			}
			if constraint.Match(store) {
				result.stores[store.GetId()] = constraint
				matched.stores = append(matched.stores, store)
				break
			}
		}
	}

	return result
}
