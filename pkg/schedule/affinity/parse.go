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

import "strings"

type groupStateProvider interface {
	GetAllAffinityGroupStates() []*GroupState
	GetAffinityGroupState(id string) *GroupState
}

// CollectAllGroupStates returns all affinity group states mapped by ID.
func CollectAllGroupStates(provider groupStateProvider) map[string]*GroupState {
	allGroupStates := provider.GetAllAffinityGroupStates()
	result := make(map[string]*GroupState, len(allGroupStates))
	for _, state := range allGroupStates {
		result[state.ID] = state
	}
	return result
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
func CollectGroupStates(provider groupStateProvider, rawIDs []string) (map[string]*GroupState, error) {
	if len(rawIDs) == 0 {
		return CollectAllGroupStates(provider), nil
	}

	ids, err := ParseGroupIDs(rawIDs)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*GroupState, len(ids))
	for _, id := range ids {
		state := provider.GetAffinityGroupState(id)
		if state != nil {
			result[id] = state
		}
	}
	return result, nil
}
