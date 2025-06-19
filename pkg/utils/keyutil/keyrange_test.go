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

package keyutil

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCodecKeyRange(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		ks KeyRange
	}{
		{
			NewKeyRange(fmt.Sprintf("%20d", 0), fmt.Sprintf("%20d", 5)),
		},
		{
			NewKeyRange(fmt.Sprintf("%20d", 0), fmt.Sprintf("%20d", 10)),
		},
	}

	for _, tc := range testCases {
		data, err := tc.ks.MarshalJSON()
		re.NoError(err)
		var ks KeyRange
		re.NoError(ks.UnmarshalJSON(data))
		re.Equal(tc.ks, ks)
	}
}

func TestOverLap(t *testing.T) {
	re := require.New(t)
	for _, tc := range []struct {
		name       string
		a, b       KeyRange
		expect     bool
		isAdjacent bool
	}{
		{
			name:       "overlap",
			a:          NewKeyRange("a", "c"),
			b:          NewKeyRange("b", "d"),
			expect:     true,
			isAdjacent: false,
		},
		{
			name:       "no overlap",
			a:          NewKeyRange("a", "b"),
			b:          NewKeyRange("c", "d"),
			expect:     false,
			isAdjacent: false,
		},
		{
			name:       "continuous",
			a:          NewKeyRange("a", "b"),
			b:          NewKeyRange("b", "d"),
			expect:     true,
			isAdjacent: true,
		},
	} {
		re.Equal(tc.expect, tc.a.OverLapped(&tc.b))
		re.Equal(tc.expect, tc.b.OverLapped(&tc.a))
		re.Equal(tc.isAdjacent, tc.b.IsAdjacent(&tc.a))
		re.Equal(tc.isAdjacent, tc.a.IsAdjacent(&tc.b))
	}
}

func TestMergeKeyRanges(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		name   string
		input  []*KeyRange
		expect []*KeyRange
	}{
		{
			name:   "empty",
			input:  []*KeyRange{},
			expect: []*KeyRange{},
		},
		{
			name: "single",
			input: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
			},
			expect: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
			},
		},
		{
			name: "non-overlapping",
			input: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("c"), EndKey: []byte("d")},
			},
			expect: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("c"), EndKey: []byte("d")},
			},
		},
		{
			name: "continuous",
			input: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("b"), EndKey: []byte("c")},
			},
			expect: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("c")},
			},
		},
		{
			name: "boundless 1",
			input: []*KeyRange{
				{StartKey: nil, EndKey: []byte("b")},
				{StartKey: []byte("b"), EndKey: []byte("c")},
			},
			expect: []*KeyRange{
				{StartKey: nil, EndKey: []byte("c")},
			},
		},
		{
			name: "boundless 2",
			input: []*KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("b"), EndKey: nil},
			},
			expect: []*KeyRange{
				{StartKey: []byte("a"), EndKey: nil},
			},
		},
	}

	for _, tc := range testCases {
		rs := &KeyRanges{krs: tc.input}
		rs.Merge()
		re.Equal(tc.expect, rs.Ranges(), tc.name)
	}
}

func TestSortAndDeduce(t *testing.T) {
	testCases := []struct {
		name   string
		input  []*KeyRange
		expect []*KeyRange
	}{
		{
			name:   "empty",
			input:  []*KeyRange{},
			expect: []*KeyRange{},
		},
		{
			name:   "single",
			input:  []*KeyRange{newKeyRangePointer("a", "b")},
			expect: []*KeyRange{newKeyRangePointer("a", "b")},
		},
		{
			name:   "non-overlapping",
			input:  []*KeyRange{newKeyRangePointer("a", "b"), newKeyRangePointer("c", "d")},
			expect: []*KeyRange{newKeyRangePointer("a", "b"), newKeyRangePointer("c", "d")},
		},
		{
			name:   "overlapping",
			input:  []*KeyRange{newKeyRangePointer("a", "c"), newKeyRangePointer("c", "d")},
			expect: []*KeyRange{newKeyRangePointer("a", "d")},
		},
	}
	re := require.New(t)
	for _, tc := range testCases {
		rs := &KeyRanges{krs: tc.input}
		rs.SortAndDeduce()
		re.Equal(tc.expect, rs.Ranges(), tc.name)
	}
}

func TestSubtractKeyRanges(t *testing.T) {
	testCases := []struct {
		name   string
		ks     []*KeyRange
		base   *KeyRange
		expect []*KeyRange
	}{
		{
			name:   "empty",
			ks:     []*KeyRange{},
			base:   &KeyRange{},
			expect: []*KeyRange{},
		},
		{
			name:   "single",
			ks:     []*KeyRange{newKeyRangePointer("a", "d"), newKeyRangePointer("e", "f")},
			base:   &KeyRange{},
			expect: []*KeyRange{newKeyRangePointer("", "a"), newKeyRangePointer("d", "e"), newKeyRangePointer("f", "")},
		},
		{
			name:   "non-overlapping",
			ks:     []*KeyRange{newKeyRangePointer("a", "d"), newKeyRangePointer("e", "f")},
			base:   newKeyRangePointer("g", "h"),
			expect: []*KeyRange{newKeyRangePointer("g", "h")},
		},
		{
			name:   "overlapping",
			ks:     []*KeyRange{newKeyRangePointer("a", "d"), newKeyRangePointer("e", "f")},
			base:   newKeyRangePointer("c", "g"),
			expect: []*KeyRange{newKeyRangePointer("d", "e"), newKeyRangePointer("f", "g")},
		},
	}
	re := require.New(t)
	for _, tc := range testCases {
		rs := &KeyRanges{krs: tc.ks}
		res := rs.SubtractKeyRanges(tc.base)
		expectData, _ := json.Marshal(tc.expect)
		actualData, _ := json.Marshal(res)
		re.Equal(expectData, actualData, tc.name)
	}
}

func TestDeleteKeyRanges(t *testing.T) {
	testData := []struct {
		name   string
		input  []*KeyRange
		base   *KeyRange
		expect []*KeyRange
	}{
		{
			name:   "empty",
			input:  []*KeyRange{},
			base:   &KeyRange{},
			expect: []*KeyRange{},
		},
		{
			name:   "no-overlapping",
			input:  []*KeyRange{newKeyRangePointer("a", "b"), newKeyRangePointer("c", "d")},
			base:   &KeyRange{StartKey: []byte("f"), EndKey: []byte("g")},
			expect: []*KeyRange{newKeyRangePointer("a", "b"), newKeyRangePointer("c", "d")},
		},
		{
			name:   "overlapping",
			input:  []*KeyRange{newKeyRangePointer("", "c"), newKeyRangePointer("e", "")},
			base:   &KeyRange{StartKey: []byte("b"), EndKey: []byte("d")},
			expect: []*KeyRange{newKeyRangePointer("", "b"), newKeyRangePointer("e", "")},
		},
		{
			name:   "between",
			input:  []*KeyRange{newKeyRangePointer("", "")},
			base:   &KeyRange{StartKey: []byte("b"), EndKey: []byte("d")},
			expect: []*KeyRange{newKeyRangePointer("", "b"), newKeyRangePointer("d", "")},
		},
		{
			name:   "equal",
			input:  []*KeyRange{newKeyRangePointer("", "a"), newKeyRangePointer("b", "d")},
			base:   &KeyRange{StartKey: []byte("b"), EndKey: []byte("d")},
			expect: []*KeyRange{newKeyRangePointer("", "a")},
		},
	}
	re := require.New(t)
	for _, tc := range testData {
		rs := &KeyRanges{krs: tc.input}
		rs.Delete(tc.base)
		expectData, _ := json.Marshal(tc.expect)
		actualData, _ := json.Marshal(rs.krs)
		re.Equal(expectData, actualData, tc.name)
	}
}

func newKeyRangePointer(a, b string) *KeyRange {
	ks := NewKeyRange(a, b)
	return &ks
}
