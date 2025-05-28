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

package keypath

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseKeyspaceResourceGroupPath(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		expectedID    uint32
		expectedGroup string
		expectError   bool
	}{
		{
			name:          "valid path",
			path:          "123/group1",
			expectedID:    123,
			expectedGroup: "group1",
			expectError:   false,
		},
		{
			name:          "valid path with nested groups",
			path:          "456/group1/subgroup",
			expectedID:    456,
			expectedGroup: "group1/subgroup",
			expectError:   false,
		},
		{
			name:          "valid path with zero keyspace ID",
			path:          "0/group1",
			expectedID:    0,
			expectedGroup: "group1",
			expectError:   false,
		},
		{
			name:          "valid path with max uint32 keyspace ID",
			path:          "4294967295/group1",
			expectedID:    4294967295,
			expectedGroup: "group1",
			expectError:   false,
		},
		{
			name:        "invalid path - empty string",
			path:        "",
			expectError: true,
		},
		{
			name:        "invalid path - missing group",
			path:        "123",
			expectError: true,
		},
		{
			name:        "invalid path - invalid keyspace ID",
			path:        "abc/group1",
			expectError: true,
		},
		{
			name:        "invalid path - keyspace ID too large",
			path:        "4294967296/group1",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyspaceID, groupName, err := ParseKeyspaceResourceGroupPath(tt.path)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expectedID, keyspaceID)
			require.Equal(t, tt.expectedGroup, groupName)
		})
	}
}
