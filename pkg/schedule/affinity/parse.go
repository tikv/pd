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

import (
	"regexp"
	"strings"

	"github.com/tikv/pd/pkg/errs"
)

// CollectAllGroupStates returns all affinity group states mapped by ID.
func CollectAllGroupStates(manager *Manager) map[string]*GroupState {
	return manager.GetAffinityGroupStates(nil, "")
}

// ParseGroupIDs parses and validates affinity group IDs from query values.
// It trims whitespace, removes duplicates, and skips empty entries.
func ParseGroupIDs(rawIDs []string) ([]string, error) {
	if len(rawIDs) == 0 {
		return nil, nil
	}

	ids := make([]string, 0, len(rawIDs))
	seen := make(map[string]struct{}, len(rawIDs))
	for _, rawID := range rawIDs {
		id := strings.TrimSpace(rawID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		if err := ValidateGroupID(id); err != nil {
			return nil, err
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

// CollectGroupStates returns affinity group states for the specified ids.
// When rawIDs is empty, it returns all affinity group states.
func CollectGroupStates(manager *Manager, rawIDs []string) (map[string]*GroupState, error) {
	return CollectGroupStatesByQuery(manager, rawIDs, "")
}

// idPrefixRegexp matches a prefix of a valid group ID. An empty prefix selects every group.
var idPrefixRegexp = regexp.MustCompile("^[-A-Za-z0-9_]{0,64}$")

// ValidateGroupIDPrefix checks that prefix can start a valid group ID.
func ValidateGroupIDPrefix(prefix string) error {
	if !idPrefixRegexp.MatchString(prefix) {
		return errs.ErrAffinityGroupContent.FastGenByArgs("invalid group ID prefix " + prefix)
	}
	return nil
}

// CollectGroupStatesByQuery returns the affinity group states that a list
// request selects. rawIDs selects groups by ID and prefix selects the groups
// whose IDs start with it; when both are given, a group must match both. With
// neither, it returns all groups.
func CollectGroupStatesByQuery(manager *Manager, rawIDs []string, prefix string) (map[string]*GroupState, error) {
	if err := ValidateGroupIDPrefix(prefix); err != nil {
		return nil, err
	}
	ids, err := ParseGroupIDs(rawIDs)
	if err != nil {
		return nil, err
	}
	return manager.GetAffinityGroupStates(ids, prefix), nil
}

// GetAffinityGroupStates returns a snapshot of the groups matching ids and prefix.
// A nil ids slice selects all IDs; an empty non-nil slice selects none.
func (m *Manager) GetAffinityGroupStates(ids []string, prefix string) map[string]*GroupState {
	m.RLock()
	defer m.RUnlock()
	result := make(map[string]*GroupState, len(ids))
	if ids != nil {
		for _, id := range ids {
			if group, ok := m.groups[id]; ok && strings.HasPrefix(id, prefix) {
				result[id] = newGroupState(group)
			}
		}
		return result
	}
	for id, group := range m.groups {
		if strings.HasPrefix(id, prefix) {
			result[id] = newGroupState(group)
		}
	}
	return result
}
