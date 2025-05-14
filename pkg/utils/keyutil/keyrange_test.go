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
	for _, tc := range []struct {
		name   string
		a, b   KeyRange
		expect bool
	}{
		{
			name:   "overlap",
			a:      NewKeyRange("a", "c"),
			b:      NewKeyRange("b", "d"),
			expect: true,
		},
		{
			name:   "no overlap",
			a:      NewKeyRange("a", "b"),
			b:      NewKeyRange("c", "d"),
			expect: false,
		},
		{
			name:   "continuous",
			a:      NewKeyRange("a", "b"),
			b:      NewKeyRange("b", "d"),
			expect: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			re.Equal(tc.expect, tc.a.OverLapped(&tc.b))
			re.Equal(tc.expect, tc.b.OverLapped(&tc.a))
		})
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
	re := require.New(t)
	fn := func(a, b string) *KeyRange {
		ks := NewKeyRange(a, b)
		return &ks
	}
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
			input:  []*KeyRange{fn("a", "b")},
			expect: []*KeyRange{fn("a", "b")},
		},
		{
			name:   "non-overlapping",
			input:  []*KeyRange{fn("a", "b"), fn("c", "d")},
			expect: []*KeyRange{fn("a", "b"), fn("c", "d")},
		},
		{
			name:   "overlapping",
			input:  []*KeyRange{fn("a", "c"), fn("b", "d")},
			expect: []*KeyRange{fn("a", "d")},
		},
		{
			name:   "empty keys",
			input:  []*KeyRange{fn("", ""), fn("a", "b")},
			expect: []*KeyRange{fn("", "")},
		},
		{
			name:   "one empty key",
			input:  []*KeyRange{fn("d", ""), fn("", "b"), fn("b", "c")},
			expect: []*KeyRange{fn("", "c"), fn("d", "")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rs := &KeyRanges{krs: tc.input}
			rs.SortAndDeduce()
			re.Equal(tc.expect, rs.Ranges(), tc.name)
		})
	}
}

func TestSubtractKeyRanges(t *testing.T) {
	re := require.New(t)
	fn := func(a, b string) *KeyRange {
		ks := NewKeyRange(a, b)
		return &ks
	}
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
			ks:     []*KeyRange{fn("a", "d"), fn("e", "f")},
			base:   &KeyRange{},
			expect: []*KeyRange{fn("", "a"), fn("d", "e"), fn("f", "")},
		},
		{
			name:   "non-overlapping",
			ks:     []*KeyRange{fn("a", "d"), fn("e", "f")},
			base:   fn("g", "h"),
			expect: []*KeyRange{fn("g", "h")},
		},
		{
			name:   "overlapping",
			ks:     []*KeyRange{fn("a", "d"), fn("e", "f")},
			base:   fn("c", "g"),
			expect: []*KeyRange{fn("d", "e"), fn("f", "g")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rs := &KeyRanges{krs: tc.ks}
			res := rs.SubtractKeyRanges(tc.base)
			expectData, _ := json.Marshal(tc.expect)
			actualData, _ := json.Marshal(res)
			re.EqualValues(expectData, actualData, tc.name)
		})
	}
}
