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
	"encoding/hex"
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
		{keyspaceID: 0},
		{keyspaceID: 1},
		{keyspaceID: 10},
		{keyspaceID: 100},
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

		regionBound := keyspace.MakeRegionBound(tc.keyspaceID)
		re.Equal(hex.EncodeToString(regionBound.RawLeftBound), rawStart)
		re.Equal(hex.EncodeToString(regionBound.RawRightBound), rawEnd)
		re.Equal(hex.EncodeToString(regionBound.TxnLeftBound), txnStart)
		re.Equal(hex.EncodeToString(regionBound.TxnRightBound), txnEnd)
		if tc.goldenRawStart != "" {
			re.Equal(tc.goldenRawStart, rawStart)
			re.Equal(tc.goldenRawEnd, rawEnd)
			re.Equal(tc.goldenTxnStart, txnStart)
			re.Equal(tc.goldenTxnEnd, txnEnd)
		}
	}
}
