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

package filter

import (
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/plan"
)

type storeCapabilityFilter struct {
	scope     string
	isWitness bool
}

// NewStoreCapabilityFilter prevents placing Full peers on LogOnly Stores and
// witnesses on explicit Full Stores, regardless of their placement labels.
func NewStoreCapabilityFilter(scope string, isWitness bool) Filter {
	return storeCapabilityFilter{scope: scope, isWitness: isWitness}
}

// Scope returns the scheduling component applying the filter.
func (f storeCapabilityFilter) Scope() string { return f.scope }

// Type returns the capability rejection metric category.
func (storeCapabilityFilter) Type() filterType { return storeCapability }

// Source permits removing a peer from any Store.
func (storeCapabilityFilter) Source(config.SharedConfigProvider, *core.StoreInfo) *plan.Status {
	return statusOK
}

// Target checks the destination Store independently of placement labels.
func (f storeCapabilityFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	if store.SupportsPeerCapability(f.isWitness) {
		return statusOK
	}
	return statusStoreNotMatchRule
}

type logOnlyProtocolFilter struct {
	scope     string
	required  uint32
	isWitness bool
}

// NewLogOnlyProtocolFilter preserves legacy groups while requiring advertised
// protocol compatibility for candidates entering a dedicated LogOnly group.
func NewLogOnlyProtocolFilter(scope string, required uint32, isWitness bool) Filter {
	return logOnlyProtocolFilter{scope: scope, required: required, isWitness: isWitness}
}

// Scope returns the scheduling component applying the protocol check.
func (f logOnlyProtocolFilter) Scope() string { return f.scope }

// Type returns the protocol rejection metric category.
func (logOnlyProtocolFilter) Type() filterType { return storeProtocol }

// Source permits retiring unsupported peers.
func (logOnlyProtocolFilter) Source(config.SharedConfigProvider, *core.StoreInfo) *plan.Status {
	return statusOK
}

// Target checks software compatibility separately from hardware capability.
func (f logOnlyProtocolFilter) Target(_ config.SharedConfigProvider, store *core.StoreInfo) *plan.Status {
	minimum := f.required
	if f.isWitness && store.GetLabelValue(core.StoreCapabilityLabel) == core.LogOnlyStoreCapability {
		minimum = max(minimum, core.LogOnlyProtocolVersion)
	}
	if store.GetMeta().GetLogOnlyProtocolVersion() < minimum {
		return statusStoreNotMatchRule
	}
	return statusOK
}
