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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckKey(t *testing.T) {
	re := require.New(t)
	testCases := []struct {
		key     []byte
		isValid bool
		tableID int64
	}{
		{[]byte("748000000000001dffb25f698000000000ff00000c0380000000ff23c1000603800000ff0000000001038000ff000067f52f2d0398ff00000000556d9500fe"), true, 7602}, // new mod
		{[]byte("748000000000001dffb25f698000000000ff00000c0380000000ff245c32c103800000ff0000000006038000ff00006818d60803d8ff00000008c904c300fe"), true, 7602}, // new mod
		{[]byte("7480000000000AE1FFAB5F72F800000000FF052EEA0100000000FB"), false, 713131}, // end with 0x01
		{[]byte("7480000000000ADEFF9E5F72F800000000FF024C9D0100000000FB"), false, 712350}, // end with 0x01
		{[]byte("7480000000000B01FFE75F72F800000000FF05613A0100000000FB"), false, 721383}, // end with 0x01
		{[]byte("7480000000000B01FFE75F72F800000000FF05613A0200000000FB"), false, 721383}, // end with 0x02
		{[]byte("7480000000000B01FFE75F72F800000000FF05613A0000000000FB"), true, 721383},  // end with 0x00
		{[]byte("7480000000000B01FFE75F720000000000FA"), true, 721383},
		{[]byte("7480000000000ADEFF9E5F720000000000FA"), true, 712350},
		{[]byte("7480000000000AE1FFAB5F720000000000FA"), true, 713131},
		// TODO: only consider the 9 bytes of the key and with non 0x00
		{[]byte("7480000000000001FFD75F728000000000FF0000140130FF0000FD"), true, 471}, // end with 0x0130FF
	}
	for _, tc := range testCases {
		rootNode := N("key", tc.key)
		rootNode.Expand()
		re.Equal(tc.isValid, !hasInvalidPatternRecursive(rootNode), string(tc.key))
		tableID, found, err := extractTableIDRecursive(rootNode)
		re.NoError(err)
		re.True(found)
		re.Equal(tc.tableID, tableID)
	}
}
