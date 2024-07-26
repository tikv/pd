// Copyright 2024 TiKV Project Authors.
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

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
