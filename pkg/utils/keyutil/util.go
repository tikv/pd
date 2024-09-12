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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package keyutil

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

// BuildKeyRangeKey build key for a keyRange
func BuildKeyRangeKey(startKey, endKey []byte) string {
	return fmt.Sprintf("%s-%s", hex.EncodeToString(startKey), hex.EncodeToString(endKey))
}

// MaxKey return the bigger key for the given keys.
func MaxKey(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

// MinKey returns the smaller key for the given keys.
func MinKey(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return b
	}
	return a
}

type boundary int

const (
	left boundary = iota
	right
)

// less returns true if a < b.
// If the key is empty and the boundary is right, the keys is infinite.
func less(a, b []byte, boundary boundary) bool {
	ret := bytes.Compare(a, b)
	if ret < 0 {
		return true
	}
	if boundary == right && len(b) == 0 && len(a) > 0 {
		return true
	}
	return false
}

// Between returns true if startKey < key < endKey.
// If the key is empty and the boundary is right, the keys is infinite.
func Between(startKey, endKey, key []byte) bool {
	return less(startKey, key, left) && less(key, endKey, right)
}
