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

package command

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/codec"
)

func TestMakeKeyRanges(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		keyspaceID uint32
	}{
		{keyspaceID: 0},
		{keyspaceID: 1},
		{keyspaceID: 10},
		{keyspaceID: 100},
		{keyspaceID: 16777215}, // max valid keyspace ID (2^24 - 1)
	}

	for _, tc := range testCases {
		ranges := makeKeyRanges(tc.keyspaceID)
		re.Len(ranges, 2, "should have 2 ranges (raw and txn)")

		// Verify raw key range
		rawRange := ranges[0].(map[string]any)
		rawStart := rawRange["start_key"].(string)
		rawEnd := rawRange["end_key"].(string)
		re.NotEmpty(rawStart)
		re.NotEmpty(rawEnd)

		// Verify txn key range
		txnRange := ranges[1].(map[string]any)
		txnStart := txnRange["start_key"].(string)
		txnEnd := txnRange["end_key"].(string)
		re.NotEmpty(txnStart)
		re.NotEmpty(txnEnd)

		// Verify the encoding matches the expected format
		keyspaceIDBytes := make([]byte, 4)
		nextKeyspaceIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(keyspaceIDBytes, tc.keyspaceID)
		binary.BigEndian.PutUint32(nextKeyspaceIDBytes, tc.keyspaceID+1)

		expectedRawLeft := codec.EncodeBytes(append([]byte{'r'}, keyspaceIDBytes[1:]...))
		expectedRawRight := codec.EncodeBytes(append([]byte{'r'}, nextKeyspaceIDBytes[1:]...))
		expectedTxnLeft := codec.EncodeBytes(append([]byte{'x'}, keyspaceIDBytes[1:]...))
		expectedTxnRight := codec.EncodeBytes(append([]byte{'x'}, nextKeyspaceIDBytes[1:]...))

		re.Equal(hex.EncodeToString(expectedRawLeft), rawStart)
		re.Equal(hex.EncodeToString(expectedRawRight), rawEnd)
		re.Equal(hex.EncodeToString(expectedTxnLeft), txnStart)
		re.Equal(hex.EncodeToString(expectedTxnRight), txnEnd)
	}
}
