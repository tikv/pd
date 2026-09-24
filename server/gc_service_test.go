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
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pingcap/failpoint"
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
			switch want := testCase.want.GetChange().(type) {
			case *pdpb.GCStateChange_Upsert:
				upsert := got.GetUpsert()
				require.NotNil(t, upsert)
				require.Equal(t, want.Upsert.GetKeyspaceScope().GetKeyspaceId(), upsert.GetKeyspaceScope().GetKeyspaceId())
				require.Equal(t, want.Upsert.GetIsKeyspaceLevelGc(), upsert.GetIsKeyspaceLevelGc())
				require.Equal(t, want.Upsert.GetTxnSafePoint(), upsert.GetTxnSafePoint())
				require.Equal(t, want.Upsert.GetGcSafePoint(), upsert.GetGcSafePoint())
				require.Empty(t, upsert.GetGcBarriers())
				require.Nil(t, got.GetRemoved())
			case *pdpb.GCStateChange_Removed:
				removed := got.GetRemoved()
				require.NotNil(t, removed)
				require.Equal(t, want.Removed.GetKeyspaceId(), removed.GetKeyspaceId())
				require.Nil(t, got.GetUpsert())
			default:
				require.FailNow(t, "unexpected expected GC state change type")
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
	base := (&pdpb.WatchGCStatesResponse{Header: grpcutil.WrapHeader()}).Size()
	delta := (&pdpb.WatchGCStatesResponse{Changes: []*pdpb.GCStateChange{change}}).Size()

	exact := splitWatchGCStatesResponses([]*pdpb.GCStateChange{change, change}, base+2*delta)
	require.Len(t, exact, 1)
	require.LessOrEqual(t, exact[0].Size(), base+2*delta)

	split := splitWatchGCStatesResponses([]*pdpb.GCStateChange{change, change}, base+2*delta-1)
	require.Len(t, split, 2)
	for _, response := range split {
		require.NotNil(t, response.GetHeader())
		require.NotEmpty(t, response.GetChanges())
		require.LessOrEqual(t, response.Size(), base+2*delta-1)
	}

	oversized := splitWatchGCStatesResponses([]*pdpb.GCStateChange{change}, base+delta-1)
	require.Len(t, oversized, 1)
	require.Greater(t, oversized[0].Size(), base+delta-1)
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

func (*fakeGCStateChangeReceiver) Done() <-chan struct{} { return nil }

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
	maxSize := (&pdpb.WatchGCStatesResponse{Header: grpcutil.WrapHeader()}).Size() +
		(&pdpb.WatchGCStatesResponse{Changes: []*pdpb.GCStateChange{protoChange}}).Size()

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

// cancelableGCStateReceiver keeps batch state in the sending worker and exposes
// the terminal cause through a context, which the supervisor can read safely.
type cancelableGCStateReceiver struct {
	ctx            context.Context
	changes        []gc.GCStateChange
	receiveStarted chan struct{}
	receiveExited  chan struct{}
}

func (r *cancelableGCStateReceiver) Done() <-chan struct{} { return r.ctx.Done() }
func (r *cancelableGCStateReceiver) Err() error            { return context.Cause(r.ctx) }
func (r *cancelableGCStateReceiver) RecvBatch(maxChanges int) ([]gc.GCStateChange, error) {
	if err := r.Err(); err != nil {
		return nil, err
	}
	if len(r.changes) == 0 {
		if r.receiveStarted != nil {
			close(r.receiveStarted)
			defer close(r.receiveExited)
		}
		<-r.Done()
		return nil, r.Err()
	}
	n := min(maxChanges, len(r.changes))
	batch := r.changes[:n]
	r.changes = r.changes[n:]
	return batch, nil
}

func waitWatchGCStatesSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		require.FailNow(t, message)
	}
}

func TestServeWatchGCStatesCancellationUnblocksHandler(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		code  codes.Code
	}{
		{"leader loss", errs.ErrNotLeader, codes.Unavailable},
		{"slow consumer", errs.ErrGCStateWatcherSlowConsumer, codes.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			streamCtx, cancelStream := context.WithCancel(context.Background())
			receiverCtx, cancelReceiver := context.WithCancelCause(streamCtx)
			receiver := &cancelableGCStateReceiver{ctx: receiverCtx, changes: []gc.GCStateChange{gc.NewGCStateUpsert(gc.GCState{KeyspaceID: 7, TxnSafePoint: 10})}}
			sendStarted, sendExited := make(chan struct{}), make(chan struct{})
			stream := &fakeWatchGCStatesServer{ctx: streamCtx, sendHook: func(*pdpb.WatchGCStatesResponse) error {
				close(sendStarted)
				defer close(sendExited)
				<-streamCtx.Done()
				return streamCtx.Err()
			}}
			handlerDone := make(chan struct{})
			var handlerErr error
			t.Cleanup(func() {
				cancelReceiver(context.Canceled)
				cancelStream()
				waitWatchGCStatesSignal(t, handlerDone, "handler did not clean up")
				select {
				case <-sendStarted:
					waitWatchGCStatesSignal(t, sendExited, "send did not clean up")
				default:
				}
			})
			go func() { defer close(handlerDone); handlerErr = serveWatchGCStates(receiver, stream, 1024) }()
			waitWatchGCStatesSignal(t, sendStarted, "send did not start")
			cancelReceiver(tc.cause)
			waitWatchGCStatesSignal(t, handlerDone, "handler did not return while send was blocked")
			require.Equal(t, tc.code, status.Code(handlerErr))
			require.NoError(t, streamCtx.Err())
			select {
			case <-sendExited:
				require.FailNow(t, "send exited before transport teardown")
			default:
			}
			cancelStream()
			waitWatchGCStatesSignal(t, sendExited, "send did not exit after transport teardown")
		})
	}
}

type observedWatchGCStatesStream struct {
	pdpb.PD_WatchGCStatesServer
	sentWireBytes      int
	observed           bool
	blockedSendStarted chan struct{}
	blockedSendExited  chan struct{}
}

func (s *observedWatchGCStatesStream) Send(response *pdpb.WatchGCStatesResponse) error {
	// grpc-go v1.82.1 starts with 64 KiB write quota. With the client's static
	// 64 KiB receive window and no Recv calls, this next send cannot regain quota.
	const receiveWindow = 64 << 10
	const writeQuota = 64 << 10
	if !s.observed && s.sentWireBytes >= receiveWindow+writeQuota {
		s.observed = true
		close(s.blockedSendStarted)
		defer close(s.blockedSendExited)
	}
	err := s.PD_WatchGCStatesServer.Send(response)
	if err == nil {
		s.sentWireBytes += response.Size() + 5
	}
	return err
}

type watchGCStatesTransportServer struct {
	pdpb.UnimplementedPDServer
	changes            []gc.GCStateChange
	cancelReceiver     chan context.CancelCauseFunc
	handlerResult      chan error
	blockedSendStarted chan struct{}
	blockedSendExited  chan struct{}
}

func (s *watchGCStatesTransportServer) WatchGCStates(_ *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
	ctx, cancel := context.WithCancelCause(stream.Context())
	defer cancel(context.Canceled)
	s.cancelReceiver <- cancel
	receiver := &cancelableGCStateReceiver{ctx: ctx, changes: s.changes}
	observed := &observedWatchGCStatesStream{PD_WatchGCStatesServer: stream, blockedSendStarted: s.blockedSendStarted, blockedSendExited: s.blockedSendExited}
	err := serveWatchGCStates(receiver, observed, maxWatchGCStatesResponseSize)
	s.handlerResult <- err
	return err
}

func TestWatchGCStatesTransportCancellationUnblocksSend(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		code  codes.Code
	}{
		{"leader loss", errs.ErrNotLeader, codes.Unavailable},
		{"slow consumer", errs.ErrGCStateWatcherSlowConsumer, codes.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &watchGCStatesTransportServer{
				cancelReceiver: make(chan context.CancelCauseFunc, 1), handlerResult: make(chan error, 1),
				blockedSendStarted: make(chan struct{}), blockedSendExited: make(chan struct{}),
			}
			for i := range 16 * 1024 {
				service.changes = append(service.changes, gc.NewGCStateUpsert(gc.GCState{
					KeyspaceID: uint32(i), IsKeyspaceLevel: true, TxnSafePoint: math.MaxUint64, GCSafePoint: math.MaxUint64 - 1,
				}))
			}
			listener := bufconn.Listen(1 << 20)
			transport := grpc.NewServer()
			pdpb.RegisterPDServer(transport, service)
			serveErr := make(chan error, 1)
			go func() { serveErr <- transport.Serve(listener) }()
			t.Cleanup(func() { transport.Stop(); require.NoError(t, <-serveErr) })
			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithStaticStreamWindowSize(64<<10), grpc.WithStaticConnWindowSize(64<<10),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(cancel)
			stream, err := pdpb.NewPDClient(conn).WatchGCStates(ctx, &pdpb.WatchGCStatesRequest{})
			require.NoError(t, err)
			waitWatchGCStatesSignal(t, service.blockedSendStarted, "transport send did not reach exhausted quota")
			select {
			case <-service.blockedSendExited:
				require.FailNow(t, "transport send unexpectedly completed")
			default:
			}
			cancelReceiver := <-service.cancelReceiver
			cancelReceiver(tc.cause)
			select {
			case err := <-service.handlerResult:
				require.Equal(t, tc.code, status.Code(err))
			case <-time.After(5 * time.Second):
				require.FailNow(t, "handler did not return while transport send was blocked")
			}
			waitWatchGCStatesSignal(t, service.blockedSendExited, "transport teardown did not unblock send")
			require.NoError(t, ctx.Err())
			for err == nil {
				_, err = stream.Recv()
			}
			require.Equal(t, tc.code, status.Code(err))
		})
	}
}

func TestServeWatchGCStatesCancellationBeforeServing(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errs.ErrNotLeader)
	receiver := &cancelableGCStateReceiver{ctx: ctx, changes: []gc.GCStateChange{gc.NewGCStateRemoved(7)}}
	stream := &fakeWatchGCStatesServer{}
	err := serveWatchGCStates(receiver, stream, 1024)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Empty(t, stream.sent)
}

func TestServeWatchGCStatesCancellationWhileReceiving(t *testing.T) {
	for _, parentCanceled := range []bool{false, true} {
		name := "watcher"
		if parentCanceled {
			name = "parent stream"
		}
		t.Run(name, func(t *testing.T) {
			streamCtx, cancelStream := context.WithCancel(context.Background())
			ctx, cancelReceiver := context.WithCancelCause(streamCtx)
			receiver := &cancelableGCStateReceiver{ctx: ctx, receiveStarted: make(chan struct{}), receiveExited: make(chan struct{})}
			stream := &fakeWatchGCStatesServer{ctx: streamCtx}
			handlerDone := make(chan struct{})
			var handlerErr error
			t.Cleanup(func() {
				cancelReceiver(context.Canceled)
				cancelStream()
				waitWatchGCStatesSignal(t, handlerDone, "handler did not clean up")
				select {
				case <-receiver.receiveStarted:
					waitWatchGCStatesSignal(t, receiver.receiveExited, "receive did not clean up")
				default:
				}
			})
			go func() { defer close(handlerDone); handlerErr = serveWatchGCStates(receiver, stream, 1024) }()
			waitWatchGCStatesSignal(t, receiver.receiveStarted, "receive did not start")
			want := codes.Unavailable
			if parentCanceled {
				cancelStream()
				want = codes.Canceled
			} else {
				cancelReceiver(errs.ErrNotLeader)
			}
			waitWatchGCStatesSignal(t, handlerDone, "handler did not return after cancellation")
			waitWatchGCStatesSignal(t, receiver.receiveExited, "receive did not return after cancellation")
			require.Equal(t, want, status.Code(handlerErr))
			require.Empty(t, stream.sent)
		})
	}
}

type watchGCStatesRPCServer struct {
	pdpb.UnimplementedPDServer
	watch func(*pdpb.WatchGCStatesRequest, pdpb.PD_WatchGCStatesServer) error
}

func (s *watchGCStatesRPCServer) WatchGCStates(request *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
	return s.watch(request, stream)
}

func newWatchGCStatesTestConn(t *testing.T, service pdpb.PDServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	transport := grpc.NewServer()
	pdpb.RegisterPDServer(transport, service)
	serveErr := make(chan error, 1)
	go func() { serveErr <- transport.Serve(listener) }()
	t.Cleanup(func() { transport.Stop(); require.NoError(t, <-serveErr) })
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStaticStreamWindowSize(64<<10), grpc.WithStaticConnWindowSize(64<<10),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func TestForwardWatchGCStatesRequestResponsesAndStatus(t *testing.T) {
	for _, code := range []codes.Code{codes.OK, codes.Unimplemented, codes.Unavailable, codes.ResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			t.Cleanup(cancel)
			request := &pdpb.WatchGCStatesRequest{
				Header: &pdpb.RequestHeader{ClusterId: 42, SenderId: 7}, SkipLoadingInitial: true,
			}
			responses := []*pdpb.WatchGCStatesResponse{
				{Header: &pdpb.ResponseHeader{ClusterId: 42}, Changes: []*pdpb.GCStateChange{{Change: &pdpb.GCStateChange_Upsert{Upsert: &pdpb.GCState{TxnSafePoint: 10}}}}},
				{Header: &pdpb.ResponseHeader{ClusterId: 42}, Changes: []*pdpb.GCStateChange{{Change: &pdpb.GCStateChange_Removed{Removed: &pdpb.KeyspaceScope{}}}}},
			}
			terminal := status.Error(code, "upstream watch ended")
			finish := make(chan struct{})
			seenRequest := make(chan *pdpb.WatchGCStatesRequest, 1)
			seenMetadata := make(chan metadata.MD, 1)
			upstream := newWatchGCStatesTestConn(t, &watchGCStatesRPCServer{watch: func(req *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
				seenRequest <- req
				md, _ := metadata.FromIncomingContext(stream.Context())
				seenMetadata <- md
				for _, response := range responses {
					if err := stream.Send(response); err != nil {
						return err
					}
				}
				if code == codes.OK {
					return nil
				}
				select {
				case <-finish:
					return terminal
				case <-stream.Context().Done():
					return stream.Context().Err()
				}
			}})
			proxy := &GrpcServer{Server: &Server{ctx: ctx, serverLoopCtx: ctx}}
			proxy.clientConns.Store("leader", upstream)
			downstream := newWatchGCStatesTestConn(t, &watchGCStatesRPCServer{watch: func(req *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
				return proxy.forwardWatchGCStates(req, stream, "leader")
			}})
			clientCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(grpcutil.ForwardMetadataKey, "leader", "test-metadata", "preserved"))
			stream, err := pdpb.NewPDClient(downstream).WatchGCStates(clientCtx, request)
			require.NoError(t, err)
			for _, expected := range responses {
				response, err := stream.Recv()
				require.NoError(t, err)
				require.Equal(t, expected, response)
			}
			require.Equal(t, request, <-seenRequest)
			md := <-seenMetadata
			require.Equal(t, []string{""}, md.Get(grpcutil.ForwardMetadataKey))
			require.Equal(t, []string{"preserved"}, md.Get("test-metadata"))
			close(finish)
			_, err = stream.Recv()
			if code == codes.OK {
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.Equal(t, status.Convert(terminal).Proto(), status.Convert(err).Proto())
			}
		})
	}
}

func TestForwardWatchGCStatesSendFailureCancelsUpstream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	upstreamExited := make(chan struct{})
	upstream := newWatchGCStatesTestConn(t, &watchGCStatesRPCServer{watch: func(_ *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
		defer close(upstreamExited)
		if err := stream.Send(&pdpb.WatchGCStatesResponse{}); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}})
	proxy := &GrpcServer{Server: &Server{ctx: ctx, serverLoopCtx: ctx}}
	proxy.clientConns.Store("leader", upstream)
	sendErr := errors.New("downstream send failed")
	stream := &fakeWatchGCStatesServer{
		ctx:      metadata.NewIncomingContext(ctx, metadata.Pairs(grpcutil.ForwardMetadataKey, "leader")),
		sendHook: func(*pdpb.WatchGCStatesResponse) error { return sendErr },
	}
	err := proxy.forwardWatchGCStates(&pdpb.WatchGCStatesRequest{}, stream, "leader")
	require.ErrorIs(t, err, sendErr)
	waitWatchGCStatesSignal(t, upstreamExited, "upstream subscription was not canceled")
	require.NoError(t, ctx.Err())
}

func TestForwardWatchGCStatesBlockedTransportCleanup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fullQueue bool
		action    string
		code      codes.Code
	}{
		{"upstream error", false, "upstream error", codes.Unavailable},
		{"proxy shutdown", false, "proxy shutdown", codes.Canceled},
		{"client cancel", false, "client cancel", codes.Canceled},
		{"stalled send", false, "timeout", codes.ResourceExhausted},
		{"stalled send after EOF", false, "upstream EOF", codes.ResourceExhausted},
		{"full queue stalled send", true, "timeout", codes.ResourceExhausted},
		{"full queue proxy shutdown", true, "proxy shutdown", codes.Canceled},
		{"full queue client cancel", true, "client cancel", codes.Canceled},
		{"full queue upstream error", true, "upstream error", codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			t.Cleanup(cancel)
			setWatchGCStatesForwardSendTimeout(t, time.Second)
			proxyCtx, stopProxy := context.WithCancel(ctx)
			t.Cleanup(stopProxy)
			queueFull := make(chan struct{})
			var fullOnce sync.Once
			const fullPoint = "github.com/tikv/pd/server/watchGCStatesForwardQueueFull"
			require.NoError(t, failpoint.EnableCall(fullPoint, func() { fullOnce.Do(func() { close(queueFull) }) }))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable(fullPoint)) })
			finishUpstream := make(chan struct{})
			upstreamExited := make(chan struct{})
			// Three 128 KiB responses exhaust the downstream's static receive
			// window and write quota. The client deliberately never calls Recv.
			response := &pdpb.WatchGCStatesResponse{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Message: strings.Repeat("x", 128<<10)}}}
			upstream := newWatchGCStatesTestConn(t, &watchGCStatesRPCServer{watch: func(_ *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
				defer close(upstreamExited)
				for i := 0; i < 3 || tc.fullQueue; i++ {
					select {
					case <-finishUpstream:
						return status.Error(codes.Unavailable, "leader changed")
					default:
					}
					if err := stream.Send(response); err != nil {
						return err
					}
				}
				select {
				case <-finishUpstream:
					if tc.action == "upstream EOF" {
						return nil
					}
					return status.Error(codes.Unavailable, "leader changed")
				case <-stream.Context().Done():
					return stream.Context().Err()
				}
			}})
			proxy := &GrpcServer{Server: &Server{ctx: ctx, serverLoopCtx: proxyCtx}}
			proxy.clientConns.Store("leader", upstream)
			started, exited := make(chan struct{}), make(chan struct{})
			handlerResult := make(chan error, 1)
			downstream := newWatchGCStatesTestConn(t, &watchGCStatesRPCServer{watch: func(req *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
				observed := &observedWatchGCStatesStream{PD_WatchGCStatesServer: stream, blockedSendStarted: started, blockedSendExited: exited}
				err := proxy.forwardWatchGCStates(req, observed, "leader")
				handlerResult <- err
				return err
			}})
			clientCtx, cancelClient := context.WithCancel(ctx)
			t.Cleanup(cancelClient)
			stream, err := pdpb.NewPDClient(downstream).WatchGCStates(grpcutil.BuildForwardContext(clientCtx, "leader"), &pdpb.WatchGCStatesRequest{})
			require.NoError(t, err)
			waitWatchGCStatesSignal(t, started, "downstream did not reach blocked Send")
			if tc.fullQueue {
				waitWatchGCStatesSignal(t, queueFull, "proxy queue did not fill")
			}
			select {
			case <-exited:
				require.FailNow(t, "send unexpectedly completed before termination")
			default:
			}
			switch tc.action {
			case "upstream error", "upstream EOF":
				close(finishUpstream)
			case "proxy shutdown":
				stopProxy()
			case "client cancel":
				cancelClient()
			}
			if tc.fullQueue && tc.action == "upstream error" {
				// Releasing backpressure lets Recv observe the terminal upstream
				// status; it must retain that status instead of reconnecting.
				for {
					_, err := stream.Recv()
					if err != nil {
						require.Equal(t, codes.Unavailable, status.Code(err))
						break
					}
				}
			}
			select {
			case err := <-handlerResult:
				require.Equal(t, tc.code, status.Code(err))
			case <-time.After(5 * time.Second):
				require.FailNow(t, "proxy handler remained blocked")
			}
			waitWatchGCStatesSignal(t, exited, "transport teardown did not unblock Send")
			waitWatchGCStatesSignal(t, upstreamExited, "upstream subscription remained active")
			require.NoError(t, ctx.Err())
		})
	}
}

func TestForwardWatchGCStatesSlowInitialConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	// More batches than the proxy can buffer, with a consumer that keeps
	// making progress but takes longer than one send timeout to load them all.
	const batchCount, batchSize = 256, 1024
	const sendTimeout = 2 * time.Second
	setWatchGCStatesForwardSendTimeout(t, sendTimeout)
	queueFull := make(chan struct{})
	var fullOnce sync.Once
	const fullPoint = "github.com/tikv/pd/server/watchGCStatesForwardQueueFull"
	require.NoError(t, failpoint.EnableCall(fullPoint, func() { fullOnce.Do(func() { close(queueFull) }) }))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(fullPoint)) })
	upstream := newWatchGCStatesTestConn(t, &watchGCStatesRPCServer{watch: func(_ *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
		for batch := range batchCount {
			response := &pdpb.WatchGCStatesResponse{Changes: make([]*pdpb.GCStateChange, batchSize)}
			for i := range batchSize {
				response.Changes[i] = &pdpb.GCStateChange{Change: &pdpb.GCStateChange_Upsert{Upsert: &pdpb.GCState{
					KeyspaceScope: &pdpb.KeyspaceScope{Keyspace: &pdpb.KeyspaceScope_KeyspaceId{KeyspaceId: uint32(batch*batchSize + i)}},
					TxnSafePoint:  100, GcSafePoint: 50,
				}}}
			}
			if err := stream.Send(response); err != nil {
				return err
			}
		}
		return nil
	}})
	proxy := &GrpcServer{Server: &Server{ctx: ctx, serverLoopCtx: ctx}}
	proxy.clientConns.Store("leader", upstream)
	downstream := newWatchGCStatesTestConn(t, &watchGCStatesRPCServer{watch: func(req *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
		return proxy.forwardWatchGCStates(req, stream, "leader")
	}})
	stream, err := pdpb.NewPDClient(downstream).WatchGCStates(grpcutil.BuildForwardContext(ctx, "leader"), &pdpb.WatchGCStatesRequest{})
	require.NoError(t, err)
	// Start consuming only after the queue fills, so this exercises
	// backpressure independently of how fast the test worker produces data.
	waitWatchGCStatesSignal(t, queueFull, "initial snapshot did not fill the proxy queue")
	start := time.Now()
	for batch := range batchCount {
		// Deliberately read slower than the upstream can produce the initial
		// snapshot, while continuing to release the transport receive window.
		time.Sleep(10 * time.Millisecond)
		response, err := stream.Recv()
		require.NoError(t, err)
		require.Len(t, response.GetChanges(), batchSize)
		for i, change := range response.GetChanges() {
			state := change.GetUpsert()
			require.Equal(t, uint32(batch*batchSize+i), state.GetKeyspaceScope().GetKeyspaceId())
			require.Equal(t, uint64(100), state.GetTxnSafePoint())
			require.Equal(t, uint64(50), state.GetGcSafePoint())
		}
	}
	require.Greater(t, time.Since(start), sendTimeout)
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func setWatchGCStatesForwardSendTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	const name = "github.com/tikv/pd/server/watchGCStatesForwardSendTimeout"
	require.NoError(t, failpoint.EnableCall(name, func(value *time.Duration) { *value = timeout }))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(name)) })
}

func TestForwardWatchGCStatesIdleStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	const sendTimeout = 500 * time.Millisecond
	setWatchGCStatesForwardSendTimeout(t, sendTimeout)
	resume := make(chan struct{})
	upstream := newWatchGCStatesTestConn(t, &watchGCStatesRPCServer{watch: func(_ *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
		if err := stream.Send(&pdpb.WatchGCStatesResponse{Header: &pdpb.ResponseHeader{ClusterId: 42}}); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-resume:
			return stream.Send(&pdpb.WatchGCStatesResponse{Header: &pdpb.ResponseHeader{ClusterId: 43}})
		}
	}})
	proxy := &GrpcServer{Server: &Server{ctx: ctx, serverLoopCtx: ctx}}
	proxy.clientConns.Store("leader", upstream)
	downstream := newWatchGCStatesTestConn(t, &watchGCStatesRPCServer{watch: func(req *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
		return proxy.forwardWatchGCStates(req, stream, "leader")
	}})
	stream, err := pdpb.NewPDClient(downstream).WatchGCStates(grpcutil.BuildForwardContext(ctx, "leader"), &pdpb.WatchGCStatesRequest{})
	require.NoError(t, err)
	response, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(42), response.GetHeader().GetClusterId())
	time.Sleep(2 * sendTimeout)
	close(resume)
	response, err = stream.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(43), response.GetHeader().GetClusterId())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
}
