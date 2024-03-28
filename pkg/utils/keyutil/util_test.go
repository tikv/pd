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
	re := require.New(t)
	startKey := []byte("a")
	endKey := []byte("b")
	key := BuildKeyRangeKey(startKey, endKey)
	re.Equal("61-62", key)
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
			left,
			true,
		},
		{
			[]byte("a"),
			[]byte("b"),
			right,
			true,
		},
		{
			[]byte("a"),
			[]byte(""),
			left,
			false,
		},
		{
			[]byte("a"),
			[]byte(""),
			right,
			true,
		},
		{
			[]byte("a"),
			[]byte("a"),
			right,
			false,
		},
		{
			[]byte(""),
			[]byte(""),
			right,
			false,
		},
		{
			[]byte(""),
			[]byte(""),
			left,
			false,
		},
	}
	for _, data := range TestData {
		re.Equal(data.expect, less(data.a, data.b, data.boundary))
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
