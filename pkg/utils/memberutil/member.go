// Copyright 2023 TiKV Project Authors.
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

package memberutil

import (
	"crypto/sha256"
	"encoding/binary"
)

// GenerateUniqueID generates a unique ID based on the given seed.
// This is used to generate a unique ID for a member.
func GenerateUniqueID(seed string) uint64 {
	hash := sha256.Sum256([]byte(seed))
	return binary.LittleEndian.Uint64(hash[:8])
}
