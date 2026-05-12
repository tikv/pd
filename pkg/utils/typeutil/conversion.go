// Copyright 2016 TiKV Project Authors.
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

package typeutil

import (
	"encoding/binary"

	"github.com/tikv/pd/pkg/errs"
)

// BytesToUint64 converts a byte slice to uint64.
func BytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, errs.ErrBytesToUint64.FastGenByArgs(len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

// Uint64ToBytes converts uint64 to a byte slice.
func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// BoolToUint64 converts bool to uint64.
func BoolToUint64(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// BoolToInt converts bool to int.
func BoolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// JSONToUint64Slice converts JSON slice to uint64 slice.
func JSONToUint64Slice(from any) ([]uint64, bool) {
	items, ok := from.([]any)
	if !ok {
		return nil, false
	}
	to := make([]uint64, 0, len(items))
	for _, item := range items {
		id, ok := item.(float64)
		if !ok {
			return nil, false
		}
		to = append(to, uint64(id))
	}
	return to, true
}
