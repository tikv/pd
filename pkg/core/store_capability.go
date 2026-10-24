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

package core

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
)

const (
	// StoreCapabilityLabel transports TiKV's startup-fixed Store capability.
	// Ordinary placement labels do not authorize changing this declaration.
	StoreCapabilityLabel = "cse-store-capability"
	// FullStoreCapability admits only peers that maintain user data.
	FullStoreCapability = "full-v1"
	// LogOnlyStoreCapability admits only LogOnly peers, including learners.
	LogOnlyStoreCapability = "log-only-v1"
)

// ParseStoreCapability validates the versioned declaration. An absent label
// retains legacy Store semantics; unknown versions and duplicates fail closed.
func ParseStoreCapability(store *metapb.Store) (string, error) {
	capability, found := "", false
	for _, label := range store.GetLabels() {
		if !strings.EqualFold(label.GetKey(), StoreCapabilityLabel) {
			continue
		}
		if found {
			return "", errors.New("duplicate Store capability declaration")
		}
		found = true
		capability = label.GetValue()
		if capability != FullStoreCapability && capability != LogOnlyStoreCapability {
			return "", errors.Errorf("unsupported Store capability %q", capability)
		}
	}
	return capability, nil
}

// SupportsPeerCapability checks data capability independently of voter/learner
// membership. Unmarked Stores preserve the existing witness behavior.
func (s *StoreInfo) SupportsPeerCapability(isWitness bool) bool {
	if s == nil {
		return false
	}
	capability, err := ParseStoreCapability(s.GetMeta())
	if err != nil {
		return false
	}
	switch capability {
	case "":
		return true
	case FullStoreCapability:
		return !isWitness
	case LogOnlyStoreCapability:
		return isWitness
	default:
		return false
	}
}

// HasFixedPeerCapability reports whether in-place data capability changes are
// forbidden on this Store. Invalid declarations are also non-convertible.
func (s *StoreInfo) HasFixedPeerCapability() bool {
	if s == nil {
		return false
	}
	capability, err := ParseStoreCapability(s.GetMeta())
	return err != nil || capability != ""
}
