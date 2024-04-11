// Copyright 2019 TiKV Project Authors.
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

package slice_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/slice"
)

func TestSlice(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		a      []int
		anyOf  bool
		noneOf bool
		allOf  bool
	}{
		{[]int{}, false, true, true},
		{[]int{1, 2, 3}, true, false, false},
		{[]int{1, 3}, false, true, false},
		{[]int{2, 2, 4}, true, false, true},
	}

	for _, testCase := range testCases {
		even := func(i int) bool { return testCase.a[i]%2 == 0 }
		re.Equal(testCase.anyOf, slice.AnyOf(testCase.a, even))
		re.Equal(testCase.noneOf, slice.NoneOf(testCase.a, even))
		re.Equal(testCase.allOf, slice.AllOf(testCase.a, even))
	}
}

func TestSliceContains(t *testing.T) {
	re := require.New(t)
	ss := []string{"a", "b", "c"}
	re.True(slice.Contains(ss, "a"))
	re.False(slice.Contains(ss, "d"))

	us := []uint64{1, 2, 3}
	re.True(slice.Contains(us, uint64(1)))
	re.False(slice.Contains(us, uint64(4)))

	is := []int64{1, 2, 3}
	re.True(slice.Contains(is, int64(1)))
	re.False(slice.Contains(is, int64(4)))
}

func TestSliceRemoveGenericTypes(t *testing.T) {
	re := require.New(t)
	ss := []string{"a", "b", "c"}
	ss = slice.Remove(ss, "a")
	re.Equal([]string{"b", "c"}, ss)

	us := []uint64{1, 2, 3}
	us = slice.Remove(us, 1)
	re.Equal([]uint64{2, 3}, us)

	is := []int64{1, 2, 3}
	is = slice.Remove(is, 1)
	re.Equal([]int64{2, 3}, is)
}

func TestSliceRemove(t *testing.T) {
	re := require.New(t)

	is := []int64{}
	is = slice.Remove(is, 1)
	re.Equal([]int64{}, is)

	is = []int64{1}
	is = slice.Remove(is, 2)
	re.Equal([]int64{1}, is)
	is = slice.Remove(is, 1)
	re.Equal([]int64{}, is)

	is = []int64{1, 2, 3}
	is = slice.Remove(is, 1)
	re.Equal([]int64{2, 3}, is)

	is = []int64{1, 1, 1}
	is = slice.Remove(is, 1)
	re.Equal([]int64{}, is)
}
