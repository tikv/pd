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
	"io"
	"math"
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
	state          gc.GCState
	clusterState   gc.ClusterGCStates
	err            error
	requestedID    uint32
	getStateCalls  int
	getAllCalls    int
	getGlobalCalls int
	closed         bool
}

func (r *fakeGCStateReader) getGlobalGCState(
	_ context.Context,
) (gc.ClusterGCStates, error) {
	r.getGlobalCalls++
	return r.clusterState, r.err
}

func (r *fakeGCStateReader) getGCState(
	_ context.Context,
	keyspaceID uint32,
) (gc.GCState, error) {
	r.requestedID = keyspaceID
	r.getStateCalls++
	return r.state, r.err
}

func (r *fakeGCStateReader) getAllKeyspacesGCStates(
	_ context.Context,
) (gc.ClusterGCStates, error) {
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

func TestNewAllGCStatesOutputKeepsEmptyGlobalBarrierArray(t *testing.T) {
	clusterState := gc.NewClusterGCStatesWithGlobalGCBarriers(
		map[uint32]gc.GCState{},
		nil,
	)

	got, err := newAllGCStatesOutput(clusterState)
	require.NoError(t, err)
	require.NotNil(t, got.GlobalGCBarriers)
	require.Empty(t, got.GlobalGCBarriers)

	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"global_gc_barriers":[]`)
}

func TestNewGlobalGCStateOutputSortsAndKeepsEmptyArray(t *testing.T) {
	t.Run("sorted", func(t *testing.T) {
		clusterState := gc.NewClusterGCStatesWithGlobalGCBarriers(
			map[uint32]gc.GCState{42: gc.NewGCStateWithoutGCBarriers(42, 100, 90)},
			[]*gc.GlobalGCBarrierInfo{
				gc.NewGlobalGCBarrierInfo("z-global", 60, time.Minute, time.Time{}),
				gc.NewGlobalGCBarrierInfo("a-global", 60, gc.TTLNeverExpire, time.Time{}),
				gc.NewGlobalGCBarrierInfo("first-global", 50, time.Second, time.Time{}),
			},
		)

		got, err := newGlobalGCStateOutput(clusterState)
		require.NoError(t, err)
		require.Equal(t, []gcBarrierOutput{
			{BarrierID: "first-global", BarrierTS: 50, TTLSeconds: 1},
			{BarrierID: "a-global", BarrierTS: 60, TTLSeconds: math.MaxInt64},
			{BarrierID: "z-global", BarrierTS: 60, TTLSeconds: 60},
		}, got.GlobalGCBarriers)

		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"global_gc_barriers": [
				{"barrier_id":"first-global","barrier_ts":50,"ttl_seconds":1},
				{"barrier_id":"a-global","barrier_ts":60,"ttl_seconds":9223372036854775807},
				{"barrier_id":"z-global","barrier_ts":60,"ttl_seconds":60}
			]
		}`, string(encoded))
	})

	t.Run("empty", func(t *testing.T) {
		clusterState := gc.NewClusterGCStatesWithGlobalGCBarriers(map[uint32]gc.GCState{}, nil)
		got, err := newGlobalGCStateOutput(clusterState)
		require.NoError(t, err)
		require.NotNil(t, got.GlobalGCBarriers)
		require.Empty(t, got.GlobalGCBarriers)

		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.JSONEq(t, `{"global_gc_barriers":[]}`, string(encoded))
	})
}

func TestGCStateOutputRejectsExcludedBarriers(t *testing.T) {
	state := gc.NewGCStateWithoutGCBarriers(42, 100, 90)
	_, err := newKeyspaceGCStateOutput(42, state)
	require.ErrorContains(t, err, "failed to read GC barriers for keyspace 42")

	clusterState := gc.NewClusterGCStatesWithoutGlobalGCBarriers(map[uint32]gc.GCState{})
	_, err = newAllGCStatesOutput(clusterState)
	require.ErrorContains(t, err, "failed to read global GC barriers")

	_, err = newGlobalGCStateOutput(clusterState)
	require.ErrorContains(t, err, "failed to read global GC barriers")
}

type fakeClusterGCStatesClient struct {
	options gc.GCStatesAPIOptions
	calls   int
}

func (c *fakeClusterGCStatesClient) GetAllKeyspacesGCStates(
	_ context.Context,
	opts ...gc.GCStatesAPIOption,
) (gc.ClusterGCStates, error) {
	c.options = gc.DefaultGCStatesAPIOptions()
	for _, opt := range opts {
		opt(&c.options)
	}
	c.calls++
	return gc.NewClusterGCStatesWithGlobalGCBarriers(map[uint32]gc.GCState{}, nil), nil
}

func TestReadClusterGCStatesOptions(t *testing.T) {
	for _, testCase := range []struct {
		name                  string
		excludeGCBarriers     bool
		wantExcludeGCBarriers bool
	}{
		{name: "all", excludeGCBarriers: false, wantExcludeGCBarriers: false},
		{name: "global", excludeGCBarriers: true, wantExcludeGCBarriers: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client := &fakeClusterGCStatesClient{}
			_, err := readClusterGCStates(t.Context(), client, testCase.excludeGCBarriers)
			require.NoError(t, err)
			require.Equal(t, 1, client.calls)
			require.Equal(t, testCase.wantExcludeGCBarriers, client.options.ExcludeGCBarriers)
			require.False(t, client.options.ExcludeGlobalGCBarriers)
		})
	}
}

func TestGCStateKeyspaceCommand(t *testing.T) {
	state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil)
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
	require.Equal(t, uint32(42), reader.requestedID)
	require.True(t, reader.closed)

	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
	require.Contains(t, decoded, "requested_keyspace_id")
	require.Contains(t, decoded, "effective_keyspace_id")
	require.Contains(t, decoded, "gc_barriers")
	require.NotContains(t, decoded, "global_gc_barriers")
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
	require.True(t, reader.closed)
	require.Contains(t, output.String(), `"gc_states": []`)
	require.Contains(t, output.String(), `"global_gc_barriers": []`)
}

func TestGCStateGlobalCommand(t *testing.T) {
	reader := &fakeGCStateReader{clusterState: gc.NewClusterGCStatesWithGlobalGCBarriers(
		map[uint32]gc.GCState{42: gc.NewGCStateWithoutGCBarriers(42, 100, 90)}, nil,
	)}
	cmd := buildGCStateCommand(func(*cobra.Command) (gcStateReader, error) { return reader, nil })
	output := new(bytes.Buffer)
	cmd.SetOut(output)
	cmd.SetErr(output)
	cmd.SetArgs([]string{"global"})

	require.NoError(t, cmd.Execute())
	require.Equal(t, 1, reader.getGlobalCalls)
	require.Zero(t, reader.getAllCalls)
	require.Zero(t, reader.getStateCalls)
	require.True(t, reader.closed)

	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
	require.Len(t, decoded, 1)
	require.Contains(t, decoded, "global_gc_barriers")
	require.NotContains(t, decoded, "gc_states")
	require.NotContains(t, decoded, "txn_safe_point")
	require.NotContains(t, decoded, "gc_safe_point")
	require.JSONEq(t, `{"global_gc_barriers":[]}`, output.String())
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
		{"global", "extra"},
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
			name: "single-unimplemented",
			args: []string{"keyspace", "42"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{
					err: status.Error(codes.Unimplemented, "method unavailable"),
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
			name: "all-rpc-error",
			args: []string{"all"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{err: errors.New("rpc rejected")}, nil
			},
			wantMessage: "failed to get all keyspaces GC states",
		},
		{
			name: "all-unimplemented",
			args: []string{"all"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{
					err: status.Error(codes.Unimplemented, "method unavailable"),
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
			wantMessage: "failed to read global GC barriers",
		},
		{
			name: "global-rpc-error",
			args: []string{"global"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{err: errors.New("rpc rejected")}, nil
			},
			wantMessage: "failed to get global GC state",
		},
		{
			name: "global-unimplemented",
			args: []string{"global"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{err: status.Error(codes.Unimplemented, "method unavailable")}, nil
			},
			wantMessage: "gc-state global requires a PD server that supports GetAllKeyspacesGCStates",
		},
		{
			name: "global-missing-global-barriers",
			args: []string{"global"},
			factory: func(*cobra.Command) (gcStateReader, error) {
				return &fakeGCStateReader{clusterState: gc.NewClusterGCStatesWithoutGlobalGCBarriers(map[uint32]gc.GCState{})}, nil
			},
			wantMessage: "failed to read global GC barriers",
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
	require.Equal(t, "Show effective per-keyspace GC safe points and local barriers, and cluster-wide GC state. Use keyspace for one effective GC scope, global for cluster-wide state, or all for a combined view.", cmd.Long)

	keyspace, _, err := cmd.Find([]string{"keyspace"})
	require.NoError(t, err)
	require.Equal(t, "keyspace <keyspace-id>", keyspace.Use)
	require.Equal(t, "show one keyspace's effective GC state", keyspace.Short)
	require.Equal(t, "Show one keyspace's effective GC safe points and local barriers. Use gc-state global to inspect only cluster-wide state, or gc-state all for a combined view. The decimal NullKeyspace ID is 4294967295.", keyspace.Long)
	require.Equal(t, "  pd-ctl gc-state keyspace 42\n  pd-ctl gc-state keyspace 4294967295", keyspace.Example)

	global, _, err := cmd.Find([]string{"global"})
	require.NoError(t, err)
	require.Equal(t, "global", global.Use)
	require.Equal(t, "show cluster-wide GC state", global.Short)
	require.Equal(t, "Show cluster-wide GC state without per-keyspace states. The current output contains global GC barriers.", global.Long)
	require.Equal(t, "  pd-ctl gc-state global", global.Example)

	all, _, err := cmd.Find([]string{"all"})
	require.NoError(t, err)
	require.Equal(t, "all", all.Use)
	require.Equal(t, "show combined keyspace and cluster-wide GC state", all.Short)
	require.Equal(t, "Show all active keyspace GC states and local barriers, with cluster-wide global barriers once at the top level. Use gc-state global to inspect only cluster-wide state.", all.Long)
	require.Equal(t, "  pd-ctl gc-state all", all.Example)
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("output rejected")
}

func TestGCStateCommandReturnsOutputError(t *testing.T) {
	state := gc.NewGCStateWithGCBarriers(42, 100, 90, nil)
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
