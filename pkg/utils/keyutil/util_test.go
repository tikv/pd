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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyUtil(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	startKey := []byte("a")
	endKey := []byte("b")
	key := BuildKeyRangeKey(startKey, endKey)
	re.Equal("61-62", key)
}

func TestCompare(t *testing.T) {
	re := require.New(t)
	testdata := []struct {
		a        []byte
		b        []byte
		min      []byte
		max      []byte
		boundary boundary
	}{
		{
			[]byte("a"),
			[]byte("b"),
			[]byte("a"),
			[]byte("b"),
			Left,
		}, {
			[]byte("a"),
			[]byte("b"),
			[]byte("a"),
			[]byte("b"),
			Right,
		},
		{
			[]byte("a"),
			[]byte(""),
			[]byte(""),
			[]byte("a"),
			Left,
		},
		{
			[]byte("a"),
			[]byte(""),
			[]byte("a"),
			[]byte(""),
			Right,
		},
	}

	for _, data := range testdata {
		re.Equal(data.min, MinKey(data.a, data.b, data.boundary))
		re.Equal(data.max, MaxKey(data.a, data.b, data.boundary))
	}
}

func TestLess(t *testing.T) {
	re := require.New(t)
	TestData := []struct {
		a        []byte
		b        []byte
		boundary boundary
		expect   bool
	}{
		{
			[]byte("a"),
			[]byte("b"),
			Left,
			true,
		},
		{
			[]byte("a"),
			[]byte("b"),
			Right,
			true,
		},
		{
			[]byte("a"),
			[]byte(""),
			Left,
			false,
		},
		{
			[]byte("a"),
			[]byte(""),
			Right,
			true,
		},
		{
			[]byte(""),
			[]byte("a"),
			Right,
			false,
		},
		{
			[]byte("a"),
			[]byte("a"),
			Right,
			false,
		},
		{
			[]byte(""),
			[]byte(""),
			Right,
			false,
		},
		{
			[]byte(""),
			[]byte(""),
			Left,
			false,
		},
		{
			[]byte(""),
			[]byte(""),
			Mix,
			true,
		},
		{
			[]byte("a"),
			[]byte(""),
			Mix,
			true,
		},
		{
			[]byte(""),
			[]byte("a"),
			Mix,
			true,
		},
	}
	for _, data := range TestData {
		re.Equal(data.expect, Less(data.a, data.b, data.boundary))
	}
}

func TestBetween(t *testing.T) {
	re := require.New(t)
	TestData := []struct {
		startKey []byte
		endKey   []byte
		key      []byte

		expect bool
	}{
		{
			[]byte("a"),
			[]byte("c"),
			[]byte("b"),
			true,
		},
		{
			[]byte("a"),
			[]byte("c"),
			[]byte("c"),
			false,
		},
		{
			[]byte("a"),
			[]byte(""),
			[]byte("b"),
			true,
		},
		{
			[]byte("a"),
			[]byte(""),
			[]byte(""),
			false,
		},
		{
			[]byte("a"),
			[]byte(""),
			[]byte("a"),
			false,
		},
	}
	for _, data := range TestData {
		re.Equal(data.expect, Between(data.startKey, data.endKey, data.key))
	}
}
