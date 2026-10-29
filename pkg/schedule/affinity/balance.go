// Copyright 2026 TiKV Project Authors.
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

package affinity

// BalancePolicy controls whether an affinity group may be balanced.
type BalancePolicy string

const (
	// BalancePolicyAuto allows the group's placement to be improved.
	BalancePolicyAuto BalancePolicy = "auto"
	// BalancePolicyFixed keeps the group's placement unchanged.
	BalancePolicyFixed BalancePolicy = "fixed"
)

// IsFixed reports whether a group with policy p keeps its target. An empty
// policy means auto. Any other value, such as one that a newer version writes,
// keeps the target like fixed.
func (p BalancePolicy) IsFixed() bool {
	return p != "" && p != BalancePolicyAuto
}
