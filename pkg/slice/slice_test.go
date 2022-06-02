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

func Test(t *testing.T) {
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
	re.Contains(ss, "a")
	re.NotContains(ss, "d")

	us := []uint64{1, 2, 3}
	re.Contains(us, uint64(1))
	re.NotContains(us, uint64(4))

	is := []int64{1, 2, 3}
	re.Contains(is, int64(1))
	re.NotContains(is, int64(4))
}
