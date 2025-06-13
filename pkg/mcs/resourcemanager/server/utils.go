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

import (
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
)

// ExtractKeyspaceID extracts the keyspace ID from the keyspace ID value.
func ExtractKeyspaceID(keyspaceIDValue *rmpb.KeyspaceIDValue) uint32 {
	if keyspaceIDValue == nil {
		return constant.NullKeyspaceID
	}
	return keyspaceIDValue.GetValue()
}
