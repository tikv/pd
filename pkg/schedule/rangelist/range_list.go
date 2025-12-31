// Copyright 2021 TiKV Project Authors.
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

package rangelist

import (
	"bytes"
	"sort"
)

type compareFunc func(a, b any) int

type segment struct {
	startKey []byte
	data     []any
}

// List manages a list of key ranges.
type List struct {
	segments []segment
}

// Len returns count of all segments.
func (l List) Len() int {
	return len(l.segments)
}

// Get returns key and items at the position.
func (l List) Get(i int) ([]byte, []any) {
	return l.segments[i].startKey, l.segments[i].data
}

// GetDataByKey returns position and items by key.
func (l List) GetDataByKey(key []byte) (index int, data []any) {
	i := sort.Search(len(l.segments), func(i int) bool {
		return bytes.Compare(l.segments[i].startKey, key) > 0
	})
	if i == 0 {
		return -1, nil
	}
	return i - 1, l.segments[i-1].data
}

// GetData returns position and items by key range.
func (l List) GetData(start, end []byte) (index int, data []any) {
	i := sort.Search(len(l.segments), func(i int) bool {
		return bytes.Compare(l.segments[i].startKey, start) > 0
	})
	if i == 0 || i != len(l.segments) && (len(end) == 0 || bytes.Compare(end, l.segments[i].startKey) > 0) {
		return -1, nil
	}
	return i - 1, l.segments[i-1].data
}

// GetSplitKeys returns all split points in a range.
func (l List) GetSplitKeys(start, end []byte) [][]byte {
	var keys [][]byte
	i := sort.Search(len(l.segments), func(i int) bool {
		return bytes.Compare(l.segments[i].startKey, start) > 0
	})
	for ; i < len(l.segments) && (len(end) == 0 || bytes.Compare(l.segments[i].startKey, end) < 0); i++ {
		keys = append(keys, l.segments[i].startKey)
	}
	return keys
}
