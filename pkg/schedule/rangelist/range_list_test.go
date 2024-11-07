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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeList(t *testing.T) {
	re := require.New(t)
	rl := NewBuilder().Build()
	re.Equal(0, rl.Len())
	i, data := rl.GetDataByKey([]byte("a"))
	re.Equal(-1, i)
	re.Nil(data)

	i, data = rl.GetData([]byte("a"), []byte("b"))
	re.Equal(-1, i)
	re.Nil(data)

	re.Nil(rl.GetSplitKeys(nil, []byte("foo")))

	b := NewBuilder()
	b.AddItem(nil, nil, 1)
	rl = b.Build()
	re.Equal(1, rl.Len())
	key, data := rl.Get(0)
	re.Nil(key)

	re.Equal([]any{1}, data)
	i, data = rl.GetDataByKey([]byte("foo"))
	re.Equal(0, i)
	re.Equal([]any{1}, data)
	i, data = rl.GetData([]byte("a"), []byte("b"))
	re.Equal(0, i)
	re.Equal([]any{1}, data)
	re.Nil(rl.GetSplitKeys(nil, []byte("foo")))
}

func TestRangeList2(t *testing.T) {
	re := require.New(t)
	b := NewBuilder()
	b.SetCompareFunc(func(a, b any) int {
		if a.(int) > b.(int) {
			return 1
		}
		if a.(int) < b.(int) {
			return -1
		}
		return 0
	})

	//       a   b   c   d   e   f   g   h   i
	// 1             |---|
	// 2 |-------|                   |-------|
	// 3 |-------|               |-------|
	// 4     |---------------|
	//   |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|
	b.AddItem([]byte("a"), []byte("e"), 4)
	b.AddItem([]byte("c"), []byte("d"), 1)
	b.AddItem([]byte("f"), []byte("h"), 3)
	b.AddItem([]byte("g"), []byte("i"), 2)
	b.AddItem([]byte(""), []byte("b"), 2)
	b.AddItem([]byte(""), []byte("b"), 3)

	expectKeys := [][]byte{
		{}, {'a'}, {'b'}, {'c'}, {'d'}, {'e'}, {'f'}, {'g'}, {'h'}, {'i'},
	}
	expectData := [][]any{
		{2, 3}, {2, 3, 4}, {4}, {1, 4}, {4}, {}, {3}, {2, 3}, {2}, {},
	}

	rl := b.Build()
	re.Len(expectKeys, rl.Len())
	for i := range rl.Len() {
		key, data := rl.Get(i)
		re.Equal(expectKeys[i], key)
		re.Equal(expectData[i], data)
	}

	getDataByKeyCases := []struct {
		key string
		pos int
	}{
		{"", 0}, {"a", 1}, {"abc", 1}, {"efg", 5}, {"z", 9},
	}
	for _, testCase := range getDataByKeyCases {
		i, data := rl.GetDataByKey([]byte(testCase.key))
		re.Equal(testCase.pos, i)
		re.Equal(expectData[i], data)
	}

	getDataCases := []struct {
		start, end string
		pos        int
	}{
		{"", "", -1}, {"", "a", 0}, {"", "aa", -1},
		{"b", "c", 2}, {"ef", "ex", 5}, {"e", "", -1},
	}
	for _, testCase := range getDataCases {
		i, data := rl.GetData([]byte(testCase.start), []byte(testCase.end))
		re.Equal(testCase.pos, i)
		if i >= 0 {
			re.Equal(expectData[i], data)
		}
	}

	getSplitKeysCases := []struct {
		start, end           string
		indexStart, indexEnd int
	}{
		{"", "", 1, 10},
		{"a", "c", 2, 3},
		{"cc", "fx", 4, 7},
	}
	for _, testCase := range getSplitKeysCases {
		re.Equal(expectKeys[testCase.indexStart:testCase.indexEnd], rl.GetSplitKeys([]byte(testCase.start), []byte(testCase.end)))
	}
}
