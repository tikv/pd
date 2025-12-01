// Copyright 2025 TiKV Project Authors.
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

package apiutil

import (
	"regexp"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
)

// TODO: we need to ensure special characters in the id.
// idPattern is a regex that specifies acceptable characters of the id.
// Valid id must be non-empty and 64 characters or fewer and consist only of letters (a-z, A-Z),
// numbers (0-9), hyphens (-), and underscores (_).
const idPattern = "^[-A-Za-z0-9_]{1,64}$"

var idRegexp = regexp.MustCompile(idPattern)

// ValidateGroupID checks the ID format.
func ValidateGroupID(id string) error {
	if idRegexp.MatchString(id) {
		return nil
	}
	return errs.ErrInvalidGroupID.GenWithStackByArgs(id)
}

// GetGroupState returns the group state after validating the ID and presence.
func GetGroupState(manager *affinity.Manager, groupID string) (*affinity.GroupState, error) {
	if err := ValidateGroupID(groupID); err != nil {
		return nil, err
	}
	state := manager.GetAffinityGroupState(groupID)
	if state == nil {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}
	return state, nil
}
