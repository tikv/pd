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

package server

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/gc"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/grpcutil"
)

func TestGCStateChangeToProto(t *testing.T) {
	testCases := []struct {
		name    string
		change  gc.GCStateChange
		want    *pdpb.GCStateChange
		wantErr bool
	}{
		{
			name: "complete upsert",
			change: gc.NewGCStateUpsert(gc.GCState{
				KeyspaceID:      7,
				IsKeyspaceLevel: true,
				TxnSafePoint:    10,
				GCSafePoint:     5,
				GCBarriers: []*endpoint.GCBarrier{
					{BarrierID: "test-barrier", BarrierTS: 8},
				},
			}),
			want: &pdpb.GCStateChange{Change: &pdpb.GCStateChange_Upsert{Upsert: &pdpb.GCState{
				KeyspaceScope:     &pdpb.KeyspaceScope{Keyspace: &pdpb.KeyspaceScope_KeyspaceId{KeyspaceId: 7}},
				IsKeyspaceLevelGc: true,
				TxnSafePoint:      10,
				GcSafePoint:       5,
			}}},
		},
		{
			name:   "removed scope",
			change: gc.NewGCStateRemoved(9),
			want: &pdpb.GCStateChange{Change: &pdpb.GCStateChange_Removed{Removed: &pdpb.KeyspaceScope{
				Keyspace: &pdpb.KeyspaceScope_KeyspaceId{KeyspaceId: 9},
			}}},
		},
		{
			name:    "invalid zero value",
			wantErr: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := gcStateChangeToProto(testCase.change)
			if testCase.wantErr {
				require.Error(t, err)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.True(t, proto.Equal(testCase.want, got), "expected %s, got %s", testCase.want, got)
			if got.GetUpsert() != nil {
				require.Empty(t, got.GetUpsert().GetGcBarriers())
			}
		})
	}
}

func TestSplitWatchGCStatesResponses(t *testing.T) {
	change := &pdpb.GCStateChange{Change: &pdpb.GCStateChange_Upsert{Upsert: &pdpb.GCState{
		KeyspaceScope: &pdpb.KeyspaceScope{Keyspace: &pdpb.KeyspaceScope_KeyspaceId{KeyspaceId: 7}},
		TxnSafePoint:  10,
		GcSafePoint:   5,
	}}}
	base := proto.Size(&pdpb.WatchGCStatesResponse{Header: grpcutil.WrapHeader()})
	delta := proto.Size(&pdpb.WatchGCStatesResponse{Changes: []*pdpb.GCStateChange{change}})

	exact := splitWatchGCStatesResponses([]*pdpb.GCStateChange{change, change}, base+2*delta)
	require.Len(t, exact, 1)
	require.LessOrEqual(t, proto.Size(exact[0]), base+2*delta)

	split := splitWatchGCStatesResponses([]*pdpb.GCStateChange{change, change}, base+2*delta-1)
	require.Len(t, split, 2)
	for _, response := range split {
		require.NotNil(t, response.GetHeader())
		require.NotEmpty(t, response.GetChanges())
		require.LessOrEqual(t, proto.Size(response), base+2*delta-1)
	}

	oversized := splitWatchGCStatesResponses([]*pdpb.GCStateChange{change}, base+delta-1)
	require.Len(t, oversized, 1)
	require.Greater(t, proto.Size(oversized[0]), base+delta-1)
	require.Empty(t, splitWatchGCStatesResponses(nil, base+delta))
}

type fakeGCStateChangeReceiver struct {
	batches       [][]gc.GCStateChange
	receiveErr    error
	terminalErr   error
	receivedMaxes []int
}

func (r *fakeGCStateChangeReceiver) RecvBatch(maxChanges int) ([]gc.GCStateChange, error) {
	r.receivedMaxes = append(r.receivedMaxes, maxChanges)
	if len(r.batches) == 0 {
		return nil, r.receiveErr
	}
	batch := r.batches[0]
	r.batches = r.batches[1:]
	return batch, nil
}

func (r *fakeGCStateChangeReceiver) Err() error {
	return r.terminalErr
}

type fakeWatchGCStatesServer struct {
	ctx      context.Context
	sent     []*pdpb.WatchGCStatesResponse
	sendHook func(*pdpb.WatchGCStatesResponse) error
}

func (s *fakeWatchGCStatesServer) Send(response *pdpb.WatchGCStatesResponse) error {
	if s.sendHook != nil {
		if err := s.sendHook(response); err != nil {
			return err
		}
	}
	s.sent = append(s.sent, response)
	return nil
}

func (*fakeWatchGCStatesServer) SetHeader(metadata.MD) error  { return nil }
func (*fakeWatchGCStatesServer) SendHeader(metadata.MD) error { return nil }
func (*fakeWatchGCStatesServer) SetTrailer(metadata.MD)       {}

func (s *fakeWatchGCStatesServer) Context() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (*fakeWatchGCStatesServer) SendMsg(any) error { return nil }
func (*fakeWatchGCStatesServer) RecvMsg(any) error { return nil }

func TestServeWatchGCStatesRechecksTerminalCauseBeforeEverySend(t *testing.T) {
	state := gc.GCState{KeyspaceID: 7, TxnSafePoint: 10, GCSafePoint: 5}
	receiver := &fakeGCStateChangeReceiver{
		batches: [][]gc.GCStateChange{{gc.NewGCStateUpsert(state), gc.NewGCStateUpsert(state)}},
	}
	stream := &fakeWatchGCStatesServer{}
	stream.sendHook = func(*pdpb.WatchGCStatesResponse) error {
		receiver.terminalErr = errs.ErrNotLeader
		return nil
	}
	protoChange := &pdpb.GCStateChange{Change: &pdpb.GCStateChange_Upsert{Upsert: &pdpb.GCState{
		KeyspaceScope: &pdpb.KeyspaceScope{Keyspace: &pdpb.KeyspaceScope_KeyspaceId{KeyspaceId: 7}},
		TxnSafePoint:  10,
		GcSafePoint:   5,
	}}}
	maxSize := proto.Size(&pdpb.WatchGCStatesResponse{Header: grpcutil.WrapHeader()}) +
		proto.Size(&pdpb.WatchGCStatesResponse{Changes: []*pdpb.GCStateChange{protoChange}})

	err := serveWatchGCStates(receiver, stream, maxSize)
	require.ErrorIs(t, err, errs.ErrNotLeader)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Len(t, stream.sent, 1)
	require.Equal(t, []int{1024}, receiver.receivedMaxes)
}

func TestServeWatchGCStatesRejectsInvalidInternalChange(t *testing.T) {
	receiver := &fakeGCStateChangeReceiver{batches: [][]gc.GCStateChange{{{}}}}
	stream := &fakeWatchGCStatesServer{}

	err := serveWatchGCStates(receiver, stream, 1024)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Empty(t, stream.sent)
}

func TestServeWatchGCStatesReturnsRawSendError(t *testing.T) {
	sendErr := errors.New("send failed")
	receiver := &fakeGCStateChangeReceiver{
		batches: [][]gc.GCStateChange{{gc.NewGCStateRemoved(9)}},
	}
	stream := &fakeWatchGCStatesServer{sendHook: func(*pdpb.WatchGCStatesResponse) error {
		return sendErr
	}}

	err := serveWatchGCStates(receiver, stream, 1024)
	require.Same(t, sendErr, err)
}

func TestWatchGCStatesErrorToStatus(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "not leader", err: errs.ErrNotLeader, code: codes.Unavailable},
		{name: "initialization failure", err: errors.New("load initial GC states"), code: codes.Unavailable},
		{name: "slow consumer", err: errs.ErrGCStateWatcherSlowConsumer, code: codes.ResourceExhausted},
		{name: "canceled", err: context.Canceled, code: codes.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded, code: codes.DeadlineExceeded},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := watchGCStatesErrorToStatus(testCase.err)
			require.Equal(t, testCase.code, status.Code(err))
		})
	}
}
