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

package command

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/keyspace"
)

func TestMakeKeyRanges(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		keyspaceID     uint32
		goldenRawStart string
		goldenRawEnd   string
		goldenTxnStart string
		goldenTxnEnd   string
	}{
		{0, "7200000000000000fb", "7200000100000000fb", "7800000000000000fb", "7800000100000000fb"},
		{1, "7200000100000000fb", "7200000200000000fb", "7800000100000000fb", "7800000200000000fb"},
		{10, "7200000a00000000fb", "7200000b00000000fb", "7800000a00000000fb", "7800000b00000000fb"},
		{100, "7200006400000000fb", "7200006500000000fb", "7800006400000000fb", "7800006500000000fb"},
		{
			keyspaceID:     16777215, // max valid keyspace ID (2^24 - 1)
			goldenRawStart: "72ffffff00000000fb",
			goldenRawEnd:   "7300000000000000fb",
			goldenTxnStart: "78ffffff00000000fb",
			goldenTxnEnd:   "7900000000000000fb",
		},
	}

	for _, tc := range testCases {
		ranges := keyspace.MakeKeyRanges(tc.keyspaceID)
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

		re.Equal(tc.goldenRawStart, rawStart)
		re.Equal(tc.goldenRawEnd, rawEnd)
		re.Equal(tc.goldenTxnStart, txnStart)
		re.Equal(tc.goldenTxnEnd, txnEnd)
	}
}
