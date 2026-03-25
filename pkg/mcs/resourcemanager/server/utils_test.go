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

package server

import "testing"

func TestExtractKeyspaceID(t *testing.T) {
	testCases := []struct {
		name              string
		resourceGroupName string
		expected          uint32
	}{
		{name: "numeric", resourceGroupName: "123", expected: 123},
		{name: "empty", resourceGroupName: "", expected: 0},
		{name: "default", resourceGroupName: reservedDefaultGroupName, expected: 0},
		{name: "invalid", resourceGroupName: "foo", expected: 0},
		{name: "overflow", resourceGroupName: "4294967296", expected: 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if keyspaceID := ExtractKeyspaceID(tc.resourceGroupName); keyspaceID != tc.expected {
				t.Fatalf("expected %d, got %d", tc.expected, keyspaceID)
			}
		})
	}
}
