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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/pkg/keyspace/constant"
)

type fakeGCStateReader struct {
	state                   gc.GCState
	clusterState            gc.ClusterGCStates
	err                     error
	requestedID             uint32
	includeGlobalGCBarriers bool
	getStateCalls           int
	getAllCalls             int
	closed                  bool
}

func (r *fakeGCStateReader) getGCState(
	_ context.Context,
	keyspaceID uint32,
	includeGlobalGCBarriers bool,
) (gc.GCState, error) {
	r.requestedID = keyspaceID
	r.includeGlobalGCBarriers = includeGlobalGCBarriers
	r.getStateCalls++
	return r.state, r.err
}

func (r *fakeGCStateReader) getAllKeyspacesGCStates(
	_ context.Context,
	includeGlobalGCBarriers bool,
) (gc.ClusterGCStates, error) {
	r.includeGlobalGCBarriers = includeGlobalGCBarriers
	r.getAllCalls++
	return r.clusterState, r.err
}

func (r *fakeGCStateReader) close() {
	r.closed = true
}

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
	).WithGlobalGCBarriers(nil)
	state.IsKeyspaceLevelGC = false

	got, err := newKeyspaceGCStateOutput(42, state, false, true)
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
	require.NotNil(t, got.GlobalGCBarriers)
	require.Empty(t, *got.GlobalGCBarriers)
}

func TestNewKeyspaceGCStateOutputGlobalBarrierPresence(t *testing.T) {
	t.Run("requested-empty", func(t *testing.T) {
		state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil).
			WithGlobalGCBarriers(nil)
		got, err := newKeyspaceGCStateOutput(42, state, false, true)
		require.NoError(t, err)
		require.NotNil(t, got.GlobalGCBarriers)
		require.Empty(t, *got.GlobalGCBarriers)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"global_gc_barriers":[]`)
	})

	t.Run("excluded", func(t *testing.T) {
		state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil)
		got, err := newKeyspaceGCStateOutput(42, state, false, false)
		require.NoError(t, err)
		require.Nil(t, got.GlobalGCBarriers)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "global_gc_barriers")
	})

	t.Run("requested-missing", func(t *testing.T) {
		state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil)
		_, err := newKeyspaceGCStateOutput(42, state, false, true)
		require.ErrorContains(t, err,
			"retry with --exclude-global-barriers")
	})
}

func TestNewLocalGCBarrierOutputsSkipNilEntries(t *testing.T) {
	got := newLocalGCBarrierOutputs([]*gc.GCBarrierInfo{
		nil,
		gc.NewGCBarrierInfo("valid-local", 42, 30*time.Second, time.Time{}),
	}, false)

	require.Equal(t, []gcBarrierOutput{
		{BarrierID: "valid-local", BarrierTS: 42, TTLSeconds: 30},
	}, got)
}

func TestNewGlobalGCBarrierOutputsSkipNilEntries(t *testing.T) {
	got := newGlobalGCBarrierOutputs([]*gc.GlobalGCBarrierInfo{
		nil,
		gc.NewGlobalGCBarrierInfo("valid-global", 84, time.Minute, time.Time{}),
	}, false)

	require.Equal(t, []gcBarrierOutput{
		{BarrierID: "valid-global", BarrierTS: 84, TTLSeconds: 60},
	}, got)
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

	got, err := newAllGCStatesOutput(clusterState, false, true)
	require.NoError(t, err)
	require.Equal(t, []uint32{1, constant.NullKeyspaceID}, []uint32{
		got.GCStates[0].KeyspaceID,
		got.GCStates[1].KeyspaceID,
	})
	require.NotNil(t, got.GCStates[0].GCBarriers)
	require.Empty(t, got.GCStates[0].GCBarriers)
	require.NotNil(t, got.GlobalGCBarriers)
	require.Equal(t, []string{"first-global", "a-global", "z-global"}, []string{
		(*got.GlobalGCBarriers)[0].BarrierID,
		(*got.GlobalGCBarriers)[1].BarrierID,
		(*got.GlobalGCBarriers)[2].BarrierID,
	})

	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"gc_barriers":[]`)
	require.Contains(t, string(encoded), `"global_gc_barriers":[`)
}

func TestNewAllGCStatesOutputFiltersUnifiedGCPlaceholders(t *testing.T) {
	keyspaceLevelState := gc.NewGCStateWithGCBarriers(7, 70, 60, nil)
	keyspaceLevelState.IsKeyspaceLevelGC = true
	unifiedGCPlaceholder := gc.NewGCStateWithGCBarriers(
		8,
		800,
		700,
		[]*gc.GCBarrierInfo{
			gc.NewGCBarrierInfo("placeholder", 900, time.Hour, time.Time{}),
		},
	)
	// A non-Null state without keyspace-level GC only identifies a unified-GC keyspace.
	// Its safe points and barriers are not an effective GC scope.
	unifiedGCPlaceholder.IsKeyspaceLevelGC = false
	nullKeyspaceState := gc.NewGCStateWithGCBarriers(
		constant.NullKeyspaceID,
		100,
		90,
		[]*gc.GCBarrierInfo{
			gc.NewGCBarrierInfo("null", 110, gc.TTLNeverExpire, time.Time{}),
		},
	)
	nullKeyspaceState.IsKeyspaceLevelGC = false

	clusterState := gc.NewClusterGCStatesWithGlobalGCBarriers(
		map[uint32]gc.GCState{
			7:                       keyspaceLevelState,
			8:                       unifiedGCPlaceholder,
			constant.NullKeyspaceID: nullKeyspaceState,
		},
		nil,
	)

	got, err := newAllGCStatesOutput(clusterState, false, true)
	require.NoError(t, err)
	require.Equal(t, []gcStateOutput{
		{
			KeyspaceID:        7,
			IsKeyspaceLevelGC: true,
			TxnSafePoint:      70,
			GCSafePoint:       60,
			GCBarriers:        []gcBarrierOutput{},
		},
		{
			KeyspaceID:        constant.NullKeyspaceID,
			IsKeyspaceLevelGC: false,
			TxnSafePoint:      100,
			GCSafePoint:       90,
			GCBarriers: []gcBarrierOutput{
				{BarrierID: "null", BarrierTS: 110, TTLSeconds: math.MaxInt64},
			},
		},
	}, got.GCStates)
}

func TestNewAllGCStatesOutputKeepsEmptyGlobalBarrierArray(t *testing.T) {
	clusterState := gc.NewClusterGCStatesWithGlobalGCBarriers(
		map[uint32]gc.GCState{},
		nil,
	)

	got, err := newAllGCStatesOutput(clusterState, false, true)
	require.NoError(t, err)
	require.NotNil(t, got.GlobalGCBarriers)
	require.Empty(t, *got.GlobalGCBarriers)

	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"global_gc_barriers":[]`)
}

func TestNewAllGCStatesOutputGlobalBarrierPresence(t *testing.T) {
	t.Run("requested-empty", func(t *testing.T) {
		state := gc.NewClusterGCStatesWithGlobalGCBarriers(
			map[uint32]gc.GCState{},
			nil,
		)
		got, err := newAllGCStatesOutput(state, false, true)
		require.NoError(t, err)
		require.NotNil(t, got.GlobalGCBarriers)
		require.Empty(t, *got.GlobalGCBarriers)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"global_gc_barriers":[]`)
	})

	t.Run("excluded", func(t *testing.T) {
		state := gc.NewClusterGCStatesWithoutGlobalGCBarriers(
			map[uint32]gc.GCState{},
		)
		got, err := newAllGCStatesOutput(state, false, false)
		require.NoError(t, err)
		require.Nil(t, got.GlobalGCBarriers)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "global_gc_barriers")
	})

	t.Run("requested-missing", func(t *testing.T) {
		state := gc.NewClusterGCStatesWithoutGlobalGCBarriers(
			map[uint32]gc.GCState{},
		)
		_, err := newAllGCStatesOutput(state, false, true)
		require.ErrorContains(t, err,
			"retry with --exclude-global-barriers")
	})
}

func TestGCStateOutputRejectsExcludedBarriers(t *testing.T) {
	t.Run("missing-local", func(t *testing.T) {
		state := gc.NewGCStateWithoutGCBarriers(42, 100, 90)
		_, err := newKeyspaceGCStateOutput(42, state, false, false)
		require.ErrorContains(t, err, "failed to read GC barriers for keyspace 42")
	})

	t.Run("keyspace-requested-missing-global", func(t *testing.T) {
		state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil)
		_, err := newKeyspaceGCStateOutput(42, state, false, true)
		require.EqualError(t, err,
			"gc-state keyspace requires a PD server whose GetGCState supports global GC barriers; "+
				"retry with --exclude-global-barriers")
	})

	t.Run("keyspace-excluded-global", func(t *testing.T) {
		state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil)
		got, err := newKeyspaceGCStateOutput(42, state, false, false)
		require.NoError(t, err)
		require.Nil(t, got.GlobalGCBarriers)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "global_gc_barriers")
	})

	t.Run("all-requested-missing-global", func(t *testing.T) {
		clusterState := gc.NewClusterGCStatesWithoutGlobalGCBarriers(
			map[uint32]gc.GCState{},
		)
		_, err := newAllGCStatesOutput(clusterState, false, true)
		require.EqualError(t, err,
			"gc-state all response does not include global GC barriers; "+
				"retry with --exclude-global-barriers")
	})

	t.Run("all-excluded-global", func(t *testing.T) {
		clusterState := gc.NewClusterGCStatesWithoutGlobalGCBarriers(
			map[uint32]gc.GCState{},
		)
		got, err := newAllGCStatesOutput(clusterState, false, false)
		require.NoError(t, err)
		require.Nil(t, got.GlobalGCBarriers)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "global_gc_barriers")
	})

}

func TestGCStateAPIOptions(t *testing.T) {
	for _, includeGlobalGCBarriers := range []bool{false, true} {
		t.Run(strconv.FormatBool(includeGlobalGCBarriers), func(t *testing.T) {
			options := gc.DefaultGCStatesAPIOptions()
			for _, option := range gcStateAPIOptions(
				includeGlobalGCBarriers,
			) {
				option(&options)
			}
			require.False(t, options.ExcludeGCBarriers)
			require.Equal(t, !includeGlobalGCBarriers,
				options.ExcludeGlobalGCBarriers)
		})
	}
}

func TestGCStateCommandGlobalBarrierFlag(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		args        []string
		wantInclude bool
	}{
		{name: "keyspace-default", args: []string{"keyspace", "42"}, wantInclude: true},
		{name: "keyspace-excluded", args: []string{"keyspace", "42", "--exclude-global-barriers"}},
		{name: "all-default", args: []string{"all"}, wantInclude: true},
		{name: "all-excluded", args: []string{"all", "--exclude-global-barriers"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			reader := &fakeGCStateReader{
				state: gc.NewGCStateWithGCBarriers(42, 100, 90, nil).
					WithGlobalGCBarriers(nil),
				clusterState: gc.NewClusterGCStatesWithGlobalGCBarriers(
					map[uint32]gc.GCState{},
					nil,
				),
			}
			cmd := buildGCStateCommand(func(*cobra.Command) (gcStateReader, error) {
				return reader, nil
			})
			output := new(bytes.Buffer)
			cmd.SetOut(output)
			cmd.SetErr(output)
			cmd.SetArgs(testCase.args)

			require.NoError(t, cmd.Execute())
			require.Equal(t, testCase.wantInclude,
				reader.includeGlobalGCBarriers)
			if testCase.wantInclude {
				require.Contains(t, output.String(), "global_gc_barriers")
			} else {
				require.NotContains(t, output.String(), "global_gc_barriers")
			}
		})
	}
}

func TestGCStateGlobalCommandIsRemoved(t *testing.T) {
	factoryCalled := false
	cmd := buildGCStateCommand(func(*cobra.Command) (gcStateReader, error) {
		factoryCalled = true
		return nil, errors.New("factory must not run")
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"global"})

	err := cmd.Execute()
	require.ErrorContains(t, err, `unknown command "global"`)
	require.False(t, factoryCalled)
}

func TestGCStateKeyspaceCommand(t *testing.T) {
	state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil).
		WithGlobalGCBarriers(nil)
	state.IsKeyspaceLevelGC = true
	reader := &fakeGCStateReader{state: state}
	factoryCalls := 0
	cmd := buildGCStateCommand(func(*cobra.Command) (gcStateReader, error) {
		factoryCalls++
		return reader, nil
	})
	output := new(bytes.Buffer)
	cmd.SetOut(output)
	cmd.SetErr(output)
	cmd.SetArgs([]string{"keyspace", "42"})

	require.NoError(t, cmd.Execute())
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, 1, reader.getStateCalls)
	require.Zero(t, reader.getAllCalls)
	require.Equal(t, uint32(42), reader.requestedID)
	require.True(t, reader.closed)

	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
	require.Contains(t, decoded, "requested_keyspace_id")
	require.Contains(t, decoded, "effective_keyspace_id")
	require.Contains(t, decoded, "gc_barriers")
	require.Contains(t, decoded, "global_gc_barriers")
}

func TestGCStateAllCommand(t *testing.T) {
	reader := &fakeGCStateReader{
		clusterState: gc.NewClusterGCStatesWithGlobalGCBarriers(
			map[uint32]gc.GCState{},
			nil,
		),
	}
	cmd := buildGCStateCommand(func(*cobra.Command) (gcStateReader, error) {
		return reader, nil
	})
	output := new(bytes.Buffer)
	cmd.SetOut(output)
	cmd.SetErr(output)
	cmd.SetArgs([]string{"all"})

	require.NoError(t, cmd.Execute())
	require.Equal(t, 1, reader.getAllCalls)
	require.Zero(t, reader.getStateCalls)
	require.True(t, reader.closed)
	require.Contains(t, output.String(), `"gc_states": []`)
	require.Contains(t, output.String(), `"global_gc_barriers": []`)
}

func TestGCStateCommandExpiredBarrierVisibility(t *testing.T) {
	globalBarriers := []*gc.GlobalGCBarrierInfo{
		gc.NewGlobalGCBarrierInfo("active-global", 70, gc.TTLNeverExpire, time.Time{}),
		gc.NewGlobalGCBarrierInfo("expired-global", 60, 0, time.Time{}),
	}
	state := gc.NewGCStateWithGCBarriers(
		42,
		100,
		90,
		[]*gc.GCBarrierInfo{
			gc.NewGCBarrierInfo("active-local", 50, gc.TTLNeverExpire, time.Time{}),
			gc.NewGCBarrierInfo("expired-local", 40, 0, time.Time{}),
		},
	).WithGlobalGCBarriers(globalBarriers)
	state.IsKeyspaceLevelGC = true
	clusterState := gc.NewClusterGCStatesWithGlobalGCBarriers(
		map[uint32]gc.GCState{42: state},
		globalBarriers,
	)

	for _, testCase := range []struct {
		name       string
		args       []string
		wantLocal  []gcBarrierOutput
		wantGlobal []gcBarrierOutput
	}{
		{
			name: "keyspace-default",
			args: []string{"keyspace", "42"},
			wantLocal: []gcBarrierOutput{
				{BarrierID: "active-local", BarrierTS: 50, TTLSeconds: math.MaxInt64},
			},
			wantGlobal: []gcBarrierOutput{
				{BarrierID: "active-global", BarrierTS: 70, TTLSeconds: math.MaxInt64},
			},
		},
		{
			name: "keyspace-include-expired",
			args: []string{"keyspace", "42", "--include-expired"},
			wantLocal: []gcBarrierOutput{
				{BarrierID: "expired-local", BarrierTS: 40, TTLSeconds: 0},
				{BarrierID: "active-local", BarrierTS: 50, TTLSeconds: math.MaxInt64},
			},
			wantGlobal: []gcBarrierOutput{
				{BarrierID: "expired-global", BarrierTS: 60, TTLSeconds: 0},
				{BarrierID: "active-global", BarrierTS: 70, TTLSeconds: math.MaxInt64},
			},
		},
		{
			name: "all-default",
			args: []string{"all"},
			wantLocal: []gcBarrierOutput{
				{BarrierID: "active-local", BarrierTS: 50, TTLSeconds: math.MaxInt64},
			},
			wantGlobal: []gcBarrierOutput{
				{BarrierID: "active-global", BarrierTS: 70, TTLSeconds: math.MaxInt64},
			},
		},
		{
			name: "all-include-expired",
			args: []string{"all", "--include-expired"},
			wantLocal: []gcBarrierOutput{
				{BarrierID: "expired-local", BarrierTS: 40, TTLSeconds: 0},
				{BarrierID: "active-local", BarrierTS: 50, TTLSeconds: math.MaxInt64},
			},
			wantGlobal: []gcBarrierOutput{
				{BarrierID: "expired-global", BarrierTS: 60, TTLSeconds: 0},
				{BarrierID: "active-global", BarrierTS: 70, TTLSeconds: math.MaxInt64},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			reader := &fakeGCStateReader{state: state, clusterState: clusterState}
			cmd := buildGCStateCommand(func(*cobra.Command) (gcStateReader, error) {
				return reader, nil
			})
			output := new(bytes.Buffer)
			cmd.SetOut(output)
			cmd.SetErr(output)
			cmd.SetArgs(testCase.args)

			require.NoError(t, cmd.Execute())
			switch testCase.args[0] {
			case "keyspace":
				var decoded keyspaceGCStateOutput
				require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
				require.Equal(t, testCase.wantLocal, decoded.GCBarriers)
				require.NotNil(t, decoded.GlobalGCBarriers)
				require.Equal(t, testCase.wantGlobal, *decoded.GlobalGCBarriers)
			case "all":
				var decoded allGCStatesOutput
				require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
				require.Len(t, decoded.GCStates, 1)
				require.Equal(t, testCase.wantLocal, decoded.GCStates[0].GCBarriers)
				require.NotNil(t, decoded.GlobalGCBarriers)
				require.Equal(t, testCase.wantGlobal, *decoded.GlobalGCBarriers)
			default:
				require.Fail(t, "unexpected subcommand", testCase.args[0])
			}
		})
	}
}

func TestGCStateCommandValidatesBeforeCreatingClient(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"keyspace"},
		{"keyspace", "42", "extra"},
		{"keyspace", ""},
		{"keyspace", "-1"},
		{"keyspace", "tenant-a"},
		{"keyspace", "0xffffff"},
		{"keyspace", "16777216"},
		{"keyspace", "4294967294"},
		{"keyspace", "4294967296"},
		{"all", "extra"},
	} {
		t.Run(strings.Join(args, "-"), func(t *testing.T) {
			factoryCalled := false
			cmd := buildGCStateCommand(
				func(*cobra.Command) (gcStateReader, error) {
					factoryCalled = true
					return nil, errors.New("factory must not run")
				},
			)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs(args)
			err := cmd.Execute()
			if len(args) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.False(t, factoryCalled)
		})
	}
}

func TestGCStateCommandErrors(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		args        []string
		factory     gcStateReaderFactory
		wantMessage string
	}{
		{
			name: "client-creation",
			args: []string{"keyspace", "42"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return nil, errors.New("dial rejected")
			},
			wantMessage: "failed to create PD RPC client",
		},
		{
			name: "single-rpc-error",
			args: []string{"keyspace", "42"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{err: errors.New("rpc rejected")}, nil
			},
			wantMessage: "failed to get GC state for keyspace 42",
		},
		{
			name: "single-wrapped-unimplemented",
			args: []string{"keyspace", "42"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{
					err: fmt.Errorf("wrapped: %w",
						status.Error(codes.Unimplemented, "method unavailable")),
				}, nil
			},
			wantMessage: "gc-state requires a PD server that supports GetGCState",
		},
		{
			name: "single-missing-barriers",
			args: []string{"keyspace", "42"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{
					state: gc.NewGCStateWithoutGCBarriers(42, 100, 90),
				}, nil
			},
			wantMessage: "failed to read GC barriers for keyspace 42",
		},
		{
			name: "single-missing-global-barriers",
			args: []string{"keyspace", "42"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{
					state: gc.NewGCStateWithGCBarriers(42, 100, 90, nil),
				}, nil
			},
			wantMessage: "retry with --exclude-global-barriers",
		},
		{
			name: "all-rpc-error",
			args: []string{"all"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{err: errors.New("rpc rejected")}, nil
			},
			wantMessage: "failed to get all keyspaces GC states",
		},
		{
			name: "all-wrapped-unimplemented",
			args: []string{"all"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{
					err: fmt.Errorf("wrapped: %w",
						status.Error(codes.Unimplemented, "method unavailable")),
				}, nil
			},
			wantMessage: "gc-state all requires a PD server that supports " +
				"GetAllKeyspacesGCStates",
		},
		{
			name: "all-missing-global-barriers",
			args: []string{"all"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{
					clusterState: gc.NewClusterGCStatesWithoutGlobalGCBarriers(
						map[uint32]gc.GCState{},
					),
				}, nil
			},
			wantMessage: "retry with --exclude-global-barriers",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cmd := buildGCStateCommand(testCase.factory)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs(testCase.args)
			err := cmd.Execute()
			require.ErrorContains(t, err, testCase.wantMessage)
		})
	}
}

func TestGCStateCommandHelpContract(t *testing.T) {
	cmd := buildGCStateCommand(func(*cobra.Command) (gcStateReader, error) {
		return nil, errors.New("help must not create a reader")
	})
	require.Equal(t, "show keyspace and cluster-wide GC state", cmd.Short)
	require.Equal(t, "Show effective per-keyspace GC safe points and local and global barriers. Expired barriers awaiting lazy deletion are hidden by default; use --include-expired to include zero-TTL barriers returned by PD. Use keyspace for one effective GC scope or all for every effective GC scope.", cmd.Long)
	includeExpired := cmd.PersistentFlags().Lookup("include-expired")
	require.NotNil(t, includeExpired)
	require.Equal(t, "false", includeExpired.DefValue)
	require.Equal(t, "include zero-TTL barriers returned by PD, which normally represent expired barriers awaiting lazy deletion", includeExpired.Usage)
	excludeGlobalBarriers := cmd.PersistentFlags().Lookup("exclude-global-barriers")
	require.NotNil(t, excludeGlobalBarriers)
	require.Equal(t, "false", excludeGlobalBarriers.DefValue)
	require.Equal(t, "exclude global GC barriers from the PD request and JSON output", excludeGlobalBarriers.Usage)

	commands := cmd.Commands()
	require.Len(t, commands, 2)
	require.Equal(t, []string{"all", "keyspace"}, []string{
		commands[0].Name(),
		commands[1].Name(),
	})

	keyspace, _, err := cmd.Find([]string{"keyspace"})
	require.NoError(t, err)
	require.Equal(t, "keyspace <keyspace-id>", keyspace.Use)
	require.Equal(t, "show one keyspace's effective GC state", keyspace.Short)
	require.Equal(t, "Show one keyspace's effective GC safe points and local and global barriers. Use --exclude-global-barriers to omit cluster-wide barriers. Use gc-state all to inspect every effective GC scope. The decimal NullKeyspace ID is 4294967295.", keyspace.Long)
	require.Equal(t, "  pd-ctl gc-state keyspace 42\n  pd-ctl gc-state keyspace 4294967295", keyspace.Example)

	all, _, err := cmd.Find([]string{"all"})
	require.NoError(t, err)
	require.Equal(t, "all", all.Use)
	require.Equal(t, "show effective GC scopes and cluster-wide GC state", all.Short)
	require.Equal(t, "Show all effective GC scopes and local barriers, with global barriers once at the top level. Use --exclude-global-barriers to omit cluster-wide barriers.", all.Long)
	require.Equal(t, "  pd-ctl gc-state all", all.Example)
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("output rejected")
}

func TestGCStateCommandReturnsOutputError(t *testing.T) {
	state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil).
		WithGlobalGCBarriers(nil)
	reader := &fakeGCStateReader{state: state}
	cmd := buildGCStateCommand(func(*cobra.Command) (gcStateReader, error) {
		return reader, nil
	})
	cmd.SetOut(failingWriter{})
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"keyspace", "42"})

	err := cmd.Execute()
	require.ErrorContains(t, err, "failed to write GC state JSON")
	require.True(t, reader.closed)
}
