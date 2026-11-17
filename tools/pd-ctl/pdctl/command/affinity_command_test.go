// Copyright 2026 TiKV Project Authors.
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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/client/constants"
	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/codec"
)

func TestTiDBGroupKeyRange(t *testing.T) {
	re := require.New(t)
	// The expected keys are TiDB table prefixes t{id} encoded like Region keys,
	// as TiDB builds them for affinity groups. A keyspace adds the transaction
	// keyspace prefix x{keyspace ID in 3 bytes} before the encoding.
	for _, tc := range []struct {
		groupID    string
		start, end string
	}{
		{"_tidb_t_100", "7480000000000000ff6400000000000000f8", "7480000000000000ff6500000000000000f8"},
		{"_tidb_pt_100_p101", "7480000000000000ff6500000000000000f8", "7480000000000000ff6600000000000000f8"},
		{"_tidb_ks1_t_100", "7800000174800000ff0000000064000000fc", "7800000174800000ff0000000065000000fc"},
		{"_tidb_ks0_pt_100_p101", "7800000074800000ff0000000065000000fc", "7800000074800000ff0000000066000000fc"},
	} {
		keyRange, err := tidbGroupKeyRange(tc.groupID)
		re.NoError(err, tc.groupID)
		re.Equal(tc.start, hex.EncodeToString(keyRange.StartKey), tc.groupID)
		re.Equal(tc.end, hex.EncodeToString(keyRange.EndKey), tc.groupID)
	}

	for _, tc := range []struct {
		groupID, err string
	}{
		{"_tidb_tg_1_i0", "repair it with ADMIN REPAIR AFFINITY TABLE GROUP"},
		{"_tidb_ks1_tg_1_i0", "repair it with ADMIN REPAIR AFFINITY TABLE GROUP"},
		{"_tidb_pg1", "the _tidb_ prefix is reserved for TiDB"},
		{"_tidb_x_1", "the _tidb_ prefix is reserved for TiDB"},
		{"_tidb_t_0", "not a TiDB table or partition group"},
		{"_tidb_pt_99999999999999999999999_p1", "invalid table ID"},
		{"_tidb_ks1_pt_99999999999999999999999_p1", "invalid table ID"},
		{"_tidb_pt_1_p99999999999999999999999", "invalid physical ID"},
		{"_tidb_ks16777216_t_1", "invalid keyspace ID"},
		{"_tidb_t_9223372036854775807", "invalid physical ID"},
	} {
		_, err := tidbGroupKeyRange(tc.groupID)
		re.ErrorContains(err, tc.err, tc.groupID)
	}
}

func TestParseAffinityKeyRanges(t *testing.T) {
	re := require.New(t)
	ranges, err := parseAffinityKeyRanges([]string{"0a,0b", " 7480 , 7481 ", ","})
	re.NoError(err)
	re.Len(ranges, 3)
	re.Equal([]byte{0x0a}, ranges[0].StartKey)
	re.Equal([]byte{0x0b}, ranges[0].EndKey)
	re.Equal([]byte{0x74, 0x80}, ranges[1].StartKey)
	re.Empty(ranges[2].StartKey)
	re.Empty(ranges[2].EndKey)

	for _, raw := range []string{"0a", "0a,0b,0c", "zz,0b", "0a,zz", "0,1"} {
		_, err := parseAffinityKeyRanges([]string{raw})
		re.Error(err, raw)
	}
}

func TestCheckCustomGroupKeyspace(t *testing.T) {
	re := require.New(t)
	// key returns the Region key of a table in a transaction keyspace, or the
	// key where the keyspace starts for table 0.
	key := func(keyspaceID uint32, tableID int64) []byte {
		raw := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, keyspaceID)
		if tableID > 0 {
			raw = append(raw, codec.GenerateTableKey(tableID)...)
		}
		return codec.EncodeBytes(raw)
	}
	keyRange := func(start, end []byte) pd.AffinityGroupKeyRange {
		return pd.AffinityGroupKeyRange{StartKey: start, EndKey: end}
	}
	table := func(keyspaceID uint32, tableID int64) pd.AffinityGroupKeyRange {
		return keyRange(key(keyspaceID, tableID), key(keyspaceID, tableID+1))
	}
	ranges := func(r ...pd.AffinityGroupKeyRange) []pd.AffinityGroupKeyRange { return r }
	quote := func(r pd.AffinityGroupKeyRange) string { return strconv.Quote(formatAffinityKeyRange(r)) }
	afterLast := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, constants.MaxKeyspaceID+1)
	rawKeyspace := codec.EncodeBytes(codec.MakeKeyspacePrefix(codec.RawKeyspaceModePrefix, 1))
	for _, tc := range []struct {
		existing, added []pd.AffinityGroupKeyRange
		// problem is what the first range that breaks the rule does, or empty
		// if the ranges lie in one keyspace.
		problem string
	}{
		{added: ranges(table(1, 100), table(1, 200))},
		{existing: ranges(table(1, 100)), added: ranges(table(1, 200))},
		// A keyspace ends where the next one starts, and the last one at the
		// boundary after it.
		{added: ranges(keyRange(key(1, 0), key(2, 0)))},
		{added: ranges(keyRange(key(constants.MaxKeyspaceID, 0), afterLast[:]))},
		// As PD does, the keys are compared with the keyspace boundaries, so a
		// key between them is in the keyspace whatever its encoding.
		{added: ranges(keyRange([]byte{'x', 0, 0, 1, 't'}, []byte{'x', 0, 0, 1, 'u'}))},
		{added: ranges(keyRange(nil, nil)), problem: `key range "," covers the whole key space`},
		{
			added:   ranges(keyRange(nil, key(1, 100))),
			problem: `key range ",7800000174800000ff0000000064000000fc" has no start key`,
		},
		{
			added:   ranges(keyRange(key(1, 100), nil)),
			problem: `key range "7800000174800000ff0000000064000000fc," starts in keyspace 1 and has no end key`,
		},
		{added: ranges(keyRange([]byte{0x0a}, []byte{0x0b})), problem: `key range "0a,0b" does not start in a transaction keyspace`},
		{
			added:   ranges(keyRange(rawKeyspace, key(1, 0))),
			problem: "key range " + quote(keyRange(rawKeyspace, key(1, 0))) + " does not start in a transaction keyspace",
		},
		{
			added:   ranges(keyRange(afterLast[:], append(afterLast[:], 0))),
			problem: "key range " + quote(keyRange(afterLast[:], append(afterLast[:], 0))) + " does not start in a transaction keyspace",
		},
		{
			added:   ranges(keyRange(key(1, 100), key(3, 100))),
			problem: "key range " + quote(keyRange(key(1, 100), key(3, 100))) + " starts in keyspace 1 and ends after it",
		},
		{
			added:   ranges(keyRange(key(1, 0), append(key(2, 0), 0))),
			problem: "key range " + quote(keyRange(key(1, 0), append(key(2, 0), 0))) + " starts in keyspace 1 and ends after it",
		},
		{
			added:   ranges(table(1, 100), table(2, 100)),
			problem: "key range " + quote(table(2, 100)) + " lies in keyspace 2 and key range " + quote(table(1, 100)) + " in keyspace 1",
		},
		// The ranges that the group has are checked with the new ones.
		{
			existing: ranges(table(1, 100)),
			added:    ranges(table(2, 100)),
			problem:  "key range " + quote(table(2, 100)) + " lies in keyspace 2 and key range " + quote(table(1, 100)) + " in keyspace 1",
		},
		{existing: ranges(keyRange(nil, nil)), added: ranges(table(1, 100)), problem: `key range "," covers the whole key space`},
	} {
		err := checkCustomGroupKeyspace("poc", tc.existing, tc.added)
		if tc.problem == "" {
			re.NoError(err, "%v %v", tc.existing, tc.added)
			continue
		}
		re.EqualError(err, "nextgen requires the key ranges of custom group poc to lie in one keyspace, but "+tc.problem)
	}
}
