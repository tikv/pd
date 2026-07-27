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
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/pkg/keyspace/constant"
)

func TestParseGCStateKeyspaceID(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		input   string
		want    uint32
		wantErr bool
	}{
		{name: "zero", input: "0", want: 0},
		{name: "maximum-normal", input: "16777215", want: 16777215},
		{name: "null-keyspace", input: "4294967295", want: 4294967295},
		{name: "empty", input: "", wantErr: true},
		{name: "negative", input: "-1", wantErr: true},
		{name: "text", input: "tenant-a", wantErr: true},
		{name: "hexadecimal", input: "0xffffff", wantErr: true},
		{name: "first-invalid", input: "16777216", wantErr: true},
		{name: "null-synonym", input: "4294967294", wantErr: true},
		{name: "uint32-overflow", input: "4294967296", wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := parseGCStateKeyspaceID(testCase.input)
			if testCase.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.want, got)
		})
	}
}

func TestNewKeyspaceGCStateOutput(t *testing.T) {
	state := gc.NewGCStateWithGCBarriers(
		constant.NullKeyspaceID,
		100,
		90,
		[]*gc.GCBarrierInfo{
			gc.NewGCBarrierInfo("z-backup", 110, time.Hour, time.Time{}),
			gc.NewGCBarrierInfo("b-backup", 105, gc.TTLNeverExpire, time.Time{}),
			gc.NewGCBarrierInfo("a-backup", 110, 30*time.Second, time.Time{}),
		},
	)
	state.IsKeyspaceLevelGC = false

	got, err := newKeyspaceGCStateOutput(42, state)
	require.NoError(t, err)
	require.Equal(t, uint32(42), got.RequestedKeyspaceID)
	require.Equal(t, constant.NullKeyspaceID, got.EffectiveKeyspaceID)
	require.False(t, got.IsKeyspaceLevelGC)
	require.Equal(t, uint64(100), got.TxnSafePoint)
	require.Equal(t, uint64(90), got.GCSafePoint)
	require.Equal(t, []gcBarrierOutput{
		{BarrierID: "b-backup", BarrierTS: 105, TTLSeconds: math.MaxInt64},
		{BarrierID: "a-backup", BarrierTS: 110, TTLSeconds: 30},
		{BarrierID: "z-backup", BarrierTS: 110, TTLSeconds: 3600},
	}, got.GCBarriers)
}

func TestNewAllGCStatesOutputSortsAndKeepsEmptyArrays(t *testing.T) {
	empty := gc.NewGCStateWithGCBarriers(1, 20, 10, nil)
	empty.IsKeyspaceLevelGC = true
	nullState := gc.NewGCStateWithGCBarriers(
		constant.NullKeyspaceID,
		40,
		30,
		[]*gc.GCBarrierInfo{
			gc.NewGCBarrierInfo("local", 45, gc.TTLNeverExpire, time.Time{}),
		},
	)

	clusterState := gc.NewClusterGCStatesWithGlobalGCBarriers(
		map[uint32]gc.GCState{
			constant.NullKeyspaceID: nullState,
			1:                       empty,
		},
		[]*gc.GlobalGCBarrierInfo{
			gc.NewGlobalGCBarrierInfo("z-global", 60, time.Minute, time.Time{}),
			gc.NewGlobalGCBarrierInfo("a-global", 60, gc.TTLNeverExpire, time.Time{}),
			gc.NewGlobalGCBarrierInfo("first-global", 50, time.Second, time.Time{}),
		},
	)

	got, err := newAllGCStatesOutput(clusterState)
	require.NoError(t, err)
	require.Equal(t, []uint32{1, constant.NullKeyspaceID}, []uint32{
		got.GCStates[0].KeyspaceID,
		got.GCStates[1].KeyspaceID,
	})
	require.NotNil(t, got.GCStates[0].GCBarriers)
	require.Empty(t, got.GCStates[0].GCBarriers)
	require.Equal(t, []string{"first-global", "a-global", "z-global"}, []string{
		got.GlobalGCBarriers[0].BarrierID,
		got.GlobalGCBarriers[1].BarrierID,
		got.GlobalGCBarriers[2].BarrierID,
	})

	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"gc_barriers":[]`)
	require.Contains(t, string(encoded), `"global_gc_barriers":[`)
}

func TestGCStateOutputRejectsExcludedBarriers(t *testing.T) {
	state := gc.NewGCStateWithoutGCBarriers(42, 100, 90)
	_, err := newKeyspaceGCStateOutput(42, state)
	require.ErrorContains(t, err, "failed to read GC barriers for keyspace 42")

	clusterState := gc.NewClusterGCStatesWithoutGlobalGCBarriers(map[uint32]gc.GCState{})
	_, err = newAllGCStatesOutput(clusterState)
	require.ErrorContains(t, err, "failed to read global GC barriers")
}
