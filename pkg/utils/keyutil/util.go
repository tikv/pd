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
func MaxKey(a, b []byte, boundary boundary) []byte {
	if Less(b, a, boundary) {
		return a
	}
	return b
}

// MinKey returns the smaller key for the given keys.
func MinKey(a, b []byte, boundary boundary) []byte {
	if Less(b, a, boundary) {
		return b
	}
	return a
}

type boundary int

const (
	// Left means that the empty key is the smallest key.
	Left boundary = iota
	// Right means that the empty key is the biggest key.
	Right
	// Mix means that the first empty key is the smallest key and the second empty key is biggest key.
	Mix
)

// Less returns true only if a < b.
func Less(a, b []byte, boundary boundary) bool {
	switch boundary {
	case Left:
		return bytes.Compare(a, b) < 0
	case Mix:
		return bytes.Compare(a, b) < 0 || len(b) == 0
	case Right:
		if len(a) == 0 {
			return false
		}
		if len(b) == 0 {
			return true
		}
		return bytes.Compare(a, b) < 0
	}
	return true
}

// Between returns true if startKey < key < endKey.
func Between(startKey, endKey, key []byte) bool {
	return Less(startKey, key, Left) && Less(key, endKey, Right)
}

// Contains returns true if startKey <= key < endKey.
func Contains(startKey, endKey, key []byte) bool {
	return Between(startKey, endKey, key) || bytes.Equal(startKey, key)
}
