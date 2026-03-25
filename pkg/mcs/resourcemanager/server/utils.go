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

import "strconv"

// ExtractKeyspaceID extracts the keyspace ID from the resource group name in CSE.
func ExtractKeyspaceID(resourceGroupName string) uint32 {
	if resourceGroupName == "" || resourceGroupName == reservedDefaultGroupName {
		return 0
	}
	keyspaceID, err := strconv.ParseUint(resourceGroupName, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(keyspaceID)
}
