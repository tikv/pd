// Copyright 2019 TiKV Project Authors.
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

package placement

import (
	"strings"

	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
)

// LabelConstraintOp defines how a LabelConstraint matches a store. It can be one of
// 'in', 'notIn', 'exists', or 'notExists'.
type LabelConstraintOp string

const (
	// In restricts the store label value should in the value list.
	// If label does not exist, `in` is always false.
	In LabelConstraintOp = "in"
	// NotIn restricts the store label value should not in the value list.
	// If label does not exist, `notIn` is always true.
	NotIn LabelConstraintOp = "notIn"
	// Exists restricts the store should have the label.
	Exists LabelConstraintOp = "exists"
	// NotExists restricts the store should not have the label.
	NotExists LabelConstraintOp = "notExists"
)

func validateOp(op LabelConstraintOp) bool {
	return op == In || op == NotIn || op == Exists || op == NotExists
}

// LabelConstraint is used to filter store when trying to place peer of a region.
type LabelConstraint struct {
	Key    string            `json:"key,omitempty"`
	Op     LabelConstraintOp `json:"op,omitempty"`
	Values []string          `json:"values,omitempty"`
}

// MatchStore checks if a store matches the constraint.
func (c *LabelConstraint) MatchStore(store *core.StoreInfo) bool {
	switch c.Op {
	case In:
		label := store.GetLabelValue(c.Key)
		return label != "" && slice.AnyOf(c.Values, func(i int) bool { return c.Values[i] == label })
	case NotIn:
		label := store.GetLabelValue(c.Key)
		return label == "" || slice.NoneOf(c.Values, func(i int) bool { return c.Values[i] == label })
	case Exists:
		return store.GetLabelValue(c.Key) != ""
	case NotExists:
		return store.GetLabelValue(c.Key) == ""
	}
	return false
}

// For backward compatibility. Need to remove later.
var legacyExclusiveLabels = []string{core.EngineKey, "exclusive"}

// If a store has exclusiveLabels, it can only be selected when the label is
// explicitly specified in constraints.
func isExclusiveLabel(key string) bool {
	return strings.HasPrefix(key, "$") || slice.AnyOf(legacyExclusiveLabels, func(i int) bool {
		return key == legacyExclusiveLabels[i]
	})
}

// MatchLabelConstraints checks if a store matches label constraints list.
func MatchLabelConstraints(store *core.StoreInfo, constraints []LabelConstraint) bool {
	if store == nil {
		return false
	}

	for _, l := range store.GetLabels() {
		if isExclusiveLabel(l.GetKey()) &&
			slice.NoneOf(constraints, func(i int) bool { return constraints[i].Key == l.GetKey() }) {
			return false
		}
	}

	return slice.AllOf(constraints, func(i int) bool { return constraints[i].MatchStore(store) })
}
