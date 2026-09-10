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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/client/clients/gc"
)

func TestTimestampInput(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  uint64
	}{
		{"262144001", 262144001},
		{"1970-01-01T00:00:01Z", 262144000},
		{"1970-01-01T08:00:01.001+08:00", 262406144},
		{"18446744073709551615", 18446744073709551615},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseTimestamp(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, input := range []string{"", "0", "-1", "18446744073709551616", "2026-09-10 12:00:00", "1969-12-31T23:59:59Z", "9999-01-01T00:00:00Z", "2026-09-10T00:00:00.000001Z"} {
		t.Run(input, func(t *testing.T) {
			_, err := parseTimestamp(input)
			require.Error(t, err)
		})
	}
}

// Incorrect input must fail before opening a connection, particularly IDs that
// PD would otherwise redirect to unified GC.
func TestInvalidArgumentsDoNotConnect(t *testing.T) {
	for _, args := range [][]string{
		{"show"},
		{"--pd=p:2379", "show"},
		{"--keyspace-id=1", "show"},
		{"--pd=p:2379", "--keyspace-id=16777216", "show"},
		{"--pd=p:2379", "--keyspace-id=4294967295", "delete", "old"},
		{"--pd=p:2379", "--keyspace-id=1", "--timeout=0s", "show"},
		{"--pd=p:2379", "--keyspace-id=1", "set", "temporary", "100"},
		{"--pd=p:2379", "--keyspace-id=1", "set", "temporary", "100", "--ttl=0s"},
		{"--pd=p:2379", "--keyspace-id=1", "set", "temporary", "100", "--ttl=-1s"},
		{"--pd=p:2379", "--keyspace-id=1", "set", "gc_worker", "100", "--ttl=never"},
		{"--pd=p:2379", "--keyspace-id=1", "delete", "gc_worker"},
		{"--pd=p:2379", "--keyspace-id=1", "delete", ""},
		{"--pd=p:2379", "--keyspace-id=1", "show", "extra"},
		{"--pd=p:2379", "--keyspace-id=1", "--cert=client.pem", "show"},
		{"--pd=p:2379", "--keyspace-id=1", "--cacert=ca.pem", "show"},
		{"--pd=https://p:2379", "--keyspace-id=1", "show"},
	} {
		t.Run(string(mustJSON(t, args)), func(t *testing.T) {
			connected := false
			cmd := newCommand(func(context.Context, options) (gc.GCStatesClient, func(), error) {
				connected = true
				return nil, nil, errors.New("unexpected connection")
			})
			cmd.SetArgs(args)
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			require.Error(t, cmd.Execute())
			require.False(t, connected)
		})
	}
}

func TestManualBarrierLifecycle(t *testing.T) {
	client := &memoryGCClient{state: gc.GCState{KeyspaceID: 42, TxnSafePoint: 100, GCSafePoint: 80}}
	client.state.GCBarriers = []*gc.GCBarrierInfo{gc.NewGCBarrierInfo("ticdc-old", 100, gc.TTLNeverExpire, time.Now())}

	out, err := executeTestCommand(t, client, "set", "recovery", "262144001", "--ttl=never")
	require.NoError(t, err)
	require.Contains(t, out, `"tso": "262144001"`)
	require.Contains(t, out, `"time": "1970-01-01T00:00:01Z"`)
	require.Contains(t, out, `"ttl": "never"`)
	require.Len(t, client.state.GCBarriers, 2)
	require.Equal(t, uint64(262144001), client.state.GCBarriers[1].BarrierTS)
	require.Equal(t, gc.TTLNeverExpire, client.state.GCBarriers[1].TTL)

	_, err = executeTestCommand(t, client, "delete", "ticdc-old")
	require.NoError(t, err)
	require.Len(t, client.state.GCBarriers, 1)
	require.Equal(t, "recovery", client.state.GCBarriers[0].BarrierID)

	_, err = executeTestCommand(t, client, "set", "recovery", "1970-01-01T00:00:02Z", "--ttl=2h")
	require.NoError(t, err)
	require.Len(t, client.state.GCBarriers, 1)
	require.Equal(t, uint64(524288000), client.state.GCBarriers[0].BarrierTS)
	require.Equal(t, 2*time.Hour, client.state.GCBarriers[0].TTL)

	out, err = executeTestCommand(t, client, "show")
	require.NoError(t, err)
	var state map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &state))
	require.Equal(t, float64(42), state["keyspace_id"])
	require.Contains(t, out, `"txn_safe_point"`)
	require.Contains(t, out, `"gc_safe_point"`)
	require.Contains(t, out, "recovery")

	_, err = executeTestCommand(t, client, "delete", "recovery")
	require.NoError(t, err)
	require.Empty(t, client.state.GCBarriers)
	out, err = executeTestCommand(t, client, "delete", "recovery")
	require.NoError(t, err)
	require.Contains(t, out, `"deleted_barrier": null`)
}

func TestPreflightAndRPCErrors(t *testing.T) {
	for _, action := range [][]string{{"show"}, {"set", "recovery", "200", "--ttl=never"}, {"delete", "old"}} {
		client := &memoryGCClient{state: gc.GCState{KeyspaceID: 4294967295}}
		_, err := executeTestCommand(t, client, action...)
		require.ErrorContains(t, err, "scope")
		require.Zero(t, client.mutations)
		client.state.KeyspaceID = 42
		client.readErr = errors.New("read unavailable")
		_, err = executeTestCommand(t, client, action...)
		require.ErrorContains(t, err, "read unavailable")
		require.Zero(t, client.mutations)
	}
	client := &memoryGCClient{state: gc.GCState{KeyspaceID: 42, TxnSafePoint: 300}}
	_, err := executeTestCommand(t, client, "set", "recovery", "200", "--ttl=never")
	require.Error(t, err)
	require.Zero(t, client.mutations)
	client.state.TxnSafePoint = 100
	client.writeErr = errors.New("write unavailable")
	for _, action := range [][]string{{"set", "recovery", "200", "--ttl=never"}, {"delete", "old"}} {
		out, err := executeTestCommand(t, client, action...)
		require.ErrorContains(t, err, "write unavailable")
		require.Empty(t, out)
	}
}

func executeTestCommand(t *testing.T, client gc.GCStatesClient, args ...string) (string, error) {
	t.Helper()
	closed := false
	cmd := newCommand(func(ctx context.Context, opts options) (gc.GCStatesClient, func(), error) {
		require.Equal(t, uint32(42), opts.keyspaceID)
		_, hasDeadline := ctx.Deadline()
		require.True(t, hasDeadline)
		return client, func() { closed = true }, nil
	})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs(append([]string{"--pd=p:2379", "--keyspace-id=42"}, args...))
	err := cmd.Execute()
	require.True(t, closed)
	return out.String(), err
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	require.NoError(t, err)
	return b
}

// Only the external GC service is substituted; command parsing, validation,
// scope checks, timestamp conversion and output encoding run unchanged.
type memoryGCClient struct {
	state     gc.GCState
	readErr   error
	writeErr  error
	mutations int
}

func (c *memoryGCClient) GetGCState(context.Context) (gc.GCState, error) {
	return c.state, c.readErr
}

func (c *memoryGCClient) SetGCBarrier(_ context.Context, id string, ts uint64, ttl time.Duration) (*gc.GCBarrierInfo, error) {
	c.mutations++
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	b := gc.NewGCBarrierInfo(id, ts, ttl, time.Now())
	for i, old := range c.state.GCBarriers {
		if old.BarrierID == id {
			c.state.GCBarriers[i] = b
			return b, nil
		}
	}
	c.state.GCBarriers = append(c.state.GCBarriers, b)
	return b, nil
}

func (c *memoryGCClient) DeleteGCBarrier(_ context.Context, id string) (*gc.GCBarrierInfo, error) {
	c.mutations++
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	for i, b := range c.state.GCBarriers {
		if b.BarrierID == id {
			c.state.GCBarriers = append(c.state.GCBarriers[:i], c.state.GCBarriers[i+1:]...)
			return b, nil
		}
	}
	return nil, nil
}
