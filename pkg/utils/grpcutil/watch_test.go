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

package grpcutil

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

func watchTestResponse(revision int64) clientv3.WatchResponse {
	return clientv3.WatchResponse{
		Header: etcdserverpb.ResponseHeader{Revision: revision},
		Events: []*clientv3.Event{{Kv: &mvccpb.KeyValue{Key: []byte("key"), Value: []byte("value"), ModRevision: revision}}},
	}
}

func receiveWatchTestResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not terminate")
		return nil
	}
}

func TestForwardEtcdWatchSlowConsumer(t *testing.T) {
	response := watchTestResponse(1)
	// A delete can retain a large previous value even when its current value is empty.
	response.Events[0].PrevKv = &mvccpb.KeyValue{Value: make([]byte, 4096)}
	for _, test := range []struct {
		name  string
		count int
		bytes int64
	}{
		{name: "response count includes in-flight send", count: 1, bytes: 1 << 20},
		{name: "byte budget includes previous value and in-flight send", count: 16, bytes: 2*etcdWatchResponseSize(response) - 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := make(chan clientv3.WatchResponse)
			started, released := make(chan struct{}), make(chan struct{})
			defer close(released)
			watchContext := make(chan context.Context, 1)
			result := make(chan error, 1)
			go func() {
				result <- forwardEtcdWatchWithConfig(ctx, func(ctx context.Context) clientv3.WatchChan {
					watchContext <- ctx
					return source
				}, func(clientv3.WatchResponse) error {
					close(started)
					<-released
					return nil
				}, watchForwardConfig{maxPendingResponses: test.count, maxPendingBytes: test.bytes, sendTimeout: time.Minute})
			}()
			source <- response
			<-started
			// The reader must keep consuming even though Send has not completed.
			source <- response
			re.Equal(codes.ResourceExhausted, status.Code(receiveWatchTestResult(t, result)))
			re.ErrorIs((<-watchContext).Err(), context.Canceled)
		})
	}
}

func TestForwardEtcdWatchOversizedResponse(t *testing.T) {
	response := watchTestResponse(1)
	source := make(chan clientv3.WatchResponse, 1)
	source <- response
	err := forwardEtcdWatchWithConfig(t.Context(), func(context.Context) clientv3.WatchChan { return source }, func(clientv3.WatchResponse) error {
		t.Error("an oversized response must not be sent")
		return nil
	}, watchForwardConfig{maxPendingResponses: 1, maxPendingBytes: etcdWatchResponseSize(response) - 1, sendTimeout: time.Second})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestForwardEtcdWatchPreviousValueBudget(t *testing.T) {
	response := watchTestResponse(1)
	response.Events[0].Type = mvccpb.DELETE
	response.Events[0].Kv.Value = nil
	response.Events[0].PrevKv = &mvccpb.KeyValue{Value: make([]byte, 4096)}
	source := make(chan clientv3.WatchResponse, 1)
	source <- response
	close(source)
	err := forwardEtcdWatchWithConfig(t.Context(), func(context.Context) clientv3.WatchChan { return source }, func(clientv3.WatchResponse) error {
		t.Error("the previous value exceeds the byte budget")
		return nil
	}, watchForwardConfig{maxPendingResponses: 16, maxPendingBytes: 1024, sendTimeout: time.Second})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestForwardEtcdWatchPreservesOrderAndCompaction(t *testing.T) {
	source := make(chan clientv3.WatchResponse, 3)
	first := watchTestResponse(11)
	second := watchTestResponse(12)
	second.Events = append(second.Events, &clientv3.Event{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("deleted"), ModRevision: 12}, PrevKv: first.Events[0].Kv})
	source <- first
	source <- second
	source <- clientv3.WatchResponse{Header: etcdserverpb.ResponseHeader{Revision: 20}, CompactRevision: 15, Canceled: true}
	close(source)
	var received []clientv3.WatchResponse
	err := ForwardEtcdWatch(t.Context(), func(context.Context) clientv3.WatchChan { return source }, func(response clientv3.WatchResponse) error {
		received = append(received, response)
		return response.Err()
	})
	require.ErrorIs(t, err, rpctypes.ErrCompacted)
	require.Len(t, received, 3)
	require.Equal(t, first, received[0])
	require.Equal(t, second, received[1])
	require.Equal(t, int64(15), received[2].CompactRevision)
}

func TestForwardEtcdWatchReleasesBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	source := make(chan clientv3.WatchResponse)
	received := make(chan int64)
	result := make(chan error, 1)
	response := watchTestResponse(1)
	go func() {
		result <- forwardEtcdWatchWithConfig(ctx, func(context.Context) clientv3.WatchChan { return source }, func(response clientv3.WatchResponse) error {
			received <- response.Header.Revision
			return nil
		}, watchForwardConfig{maxPendingResponses: 2, maxPendingBytes: 2 * etcdWatchResponseSize(response), sendTimeout: time.Second})
	}()
	for revision := int64(1); revision <= 20; revision++ {
		source <- watchTestResponse(revision)
		require.Equal(t, revision, <-received)
	}
	cancel()
	require.NoError(t, receiveWatchTestResult(t, result))
}

func TestForwardEtcdWatchSendTimeoutAndCancellation(t *testing.T) {
	for _, cancelDuringSend := range []bool{false, true} {
		t.Run(map[bool]string{false: "send timeout", true: "canceled send"}[cancelDuringSend], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := make(chan clientv3.WatchResponse)
			started, released := make(chan struct{}), make(chan struct{})
			defer close(released)
			result := make(chan error, 1)
			go func() {
				result <- forwardEtcdWatchWithConfig(ctx, func(context.Context) clientv3.WatchChan { return source }, func(clientv3.WatchResponse) error {
					close(started)
					<-released
					return nil
				}, watchForwardConfig{maxPendingResponses: 2, maxPendingBytes: 1 << 20, sendTimeout: 50 * time.Millisecond})
			}()
			// An idle watch must not expire before its first response.
			time.Sleep(100 * time.Millisecond)
			source <- watchTestResponse(1)
			<-started
			if cancelDuringSend {
				cancel()
				require.NoError(t, receiveWatchTestResult(t, result))
			} else {
				require.Equal(t, codes.ResourceExhausted, status.Code(receiveWatchTestResult(t, result)))
			}
		})
	}
}

func TestForwardEtcdWatchSendError(t *testing.T) {
	source := make(chan clientv3.WatchResponse, 1)
	source <- watchTestResponse(1)
	expected := errors.New("send failed")
	err := ForwardEtcdWatch(t.Context(), func(context.Context) clientv3.WatchChan { return source }, func(clientv3.WatchResponse) error { return expected })
	require.ErrorIs(t, err, expected)
}

type blockedWatchTestServer struct {
	pdpb.UnimplementedPDServer
	result     chan error
	senderDone chan struct{}
}

func (s *blockedWatchTestServer) WatchGlobalConfig(_ *pdpb.WatchGlobalConfigRequest, stream pdpb.PD_WatchGlobalConfigServer) error {
	source := make(chan clientv3.WatchResponse, 1)
	source <- watchTestResponse(1)
	err := forwardEtcdWatchWithConfig(stream.Context(), func(context.Context) clientv3.WatchChan { return source }, func(clientv3.WatchResponse) error {
		defer close(s.senderDone)
		// Exceed the transport window while the connected client never reads.
		response := &pdpb.WatchGlobalConfigResponse{Changes: []*pdpb.GlobalConfigItem{{Payload: make([]byte, 1<<20)}}}
		for {
			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}, watchForwardConfig{maxPendingResponses: 2, maxPendingBytes: 1 << 20, sendTimeout: 100 * time.Millisecond})
	s.result <- err
	return err
}

func TestForwardEtcdWatchGRPCReleasesBlockedSend(t *testing.T) {
	re := require.New(t)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	service := &blockedWatchTestServer{result: make(chan error, 1), senderDone: make(chan struct{})}
	pdpb.RegisterPDServer(server, service)
	defer server.Stop()
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///watch-test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	re.NoError(err)
	defer conn.Close()
	_, err = pdpb.NewPDClient(conn).WatchGlobalConfig(t.Context(), &pdpb.WatchGlobalConfigRequest{})
	re.NoError(err)
	re.Equal(codes.ResourceExhausted, status.Code(receiveWatchTestResult(t, service.result)))
	select {
	case <-service.senderDone:
	case <-time.After(5 * time.Second):
		t.Fatal("returning from the handler did not release the blocked Send")
	}
}
