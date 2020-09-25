// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package keyutil

import (
	"encoding/hex"
	"fmt"
)

// BuildKeyRangeKey build key for a keyRange
func BuildKeyRangeKey(startKey, endKey []byte) string {
	return fmt.Sprintf("%s-%s", hex.EncodeToString(startKey), hex.EncodeToString(endKey))
}

// ParseRawKey parse hex format rawKey into []byte
func ParseRawKey(key string) ([]byte, string, error) {
	returned, err := hex.DecodeString(key)
	if err != nil {
		return nil, "", fmt.Errorf("raw key %s is not in hex format", key)
	}
	return returned, key, nil
}
