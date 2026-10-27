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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

type watchTestClient struct {
	watch func(context.Context) (etcdserverpb.Watch_WatchClient, error)
}

func (c *watchTestClient) Watch(ctx context.Context, _ ...grpc.CallOption) (etcdserverpb.Watch_WatchClient, error) {
	return c.watch(ctx)
}

type watchTestStream struct {
	grpc.ClientStream
	send func(*etcdserverpb.WatchRequest) error
	recv func() (*etcdserverpb.WatchResponse, error)
}

func (s *watchTestStream) Send(request *etcdserverpb.WatchRequest) error { return s.send(request) }
func (s *watchTestStream) Recv() (*etcdserverpb.WatchResponse, error)    { return s.recv() }

func watchTestResponse(revision int64) *etcdserverpb.WatchResponse {
	return &etcdserverpb.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: revision},
		Events: []*mvccpb.Event{{Kv: &mvccpb.KeyValue{Key: []byte("key"), Value: []byte("value"), ModRevision: revision}}},
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

func TestForwardEtcdWatchResumes(t *testing.T) {
	history := watchTestResponse(6)
	history.Header.Revision = 100
	history.Events = append([]*mvccpb.Event{watchTestResponse(5).Events[0]}, history.Events...)
	for _, test := range []struct {
		name      string
		start     int64
		responses []*etcdserverpb.WatchResponse
		resume    int64
	}{
		{name: "current revision before first event", resume: 101},
		{name: "history before first event", start: 5, resume: 5},
		{name: "last event instead of header", start: 5, responses: []*etcdserverpb.WatchResponse{history}, resume: 7},
		{name: "progress", start: 5, responses: []*etcdserverpb.WatchResponse{{Header: &etcdserverpb.ResponseHeader{Revision: 40}}}, resume: 41},
		{name: "future revision", start: 200, resume: 200},
	} {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			completed := errors.New("received resumed event")
			var requests []int64
			var contexts []context.Context
			client := &watchTestClient{watch: func(ctx context.Context) (etcdserverpb.Watch_WatchClient, error) {
				contexts = append(contexts, ctx)
				responses := append([]*etcdserverpb.WatchResponse{{Header: &etcdserverpb.ResponseHeader{Revision: 100}, Created: true}}, test.responses...)
				if len(contexts) > 1 {
					responses = []*etcdserverpb.WatchResponse{watchTestResponse(test.resume)}
				}
				return &watchTestStream{
					send: func(request *etcdserverpb.WatchRequest) error {
						requests = append(requests, request.GetCreateRequest().StartRevision)
						return nil
					},
					recv: func() (*etcdserverpb.WatchResponse, error) {
						if len(responses) == 0 {
							return nil, status.Error(codes.Unavailable, "backend connection lost")
						}
						response := responses[0]
						responses = responses[1:]
						return response, nil
					},
				}, nil
			}}
			var delivered []int64
			err := forwardEtcdWatchWithRetry(t.Context(), client, etcdserverpb.WatchCreateRequest{Key: []byte("key"), StartRevision: test.start}, func(response *etcdserverpb.WatchResponse, err error) error {
				re.NoError(err)
				for _, event := range response.Events {
					delivered = append(delivered, event.Kv.ModRevision)
				}
				if len(requests) == 2 {
					return completed
				}
				return nil
			})
			re.ErrorIs(err, completed)
			re.Equal([]int64{test.start, test.resume}, requests)
			expected := []int64{test.resume}
			if len(test.responses) > 0 && len(test.responses[0].Events) > 0 {
				expected = []int64{5, 6, test.resume}
			}
			re.Equal(expected, delivered)
			for _, ctx := range contexts {
				re.ErrorIs(ctx.Err(), context.Canceled)
			}
		})
	}
}

func TestForwardEtcdWatchTerminalErrors(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *etcdserverpb.WatchResponse
		sendErr  error
		err      error
	}{
		{name: "compacted", response: &etcdserverpb.WatchResponse{Canceled: true, CompactRevision: 20}, err: rpctypes.ErrCompacted},
		{name: "compacted after send EOF", response: &etcdserverpb.WatchResponse{Canceled: true, CompactRevision: 20}, sendErr: io.EOF, err: rpctypes.ErrCompacted},
		{name: "future revision", response: &etcdserverpb.WatchResponse{Canceled: true}, err: rpctypes.ErrFutureRev},
		{name: "cancel reason", response: &etcdserverpb.WatchResponse{Canceled: true, CancelReason: "permission denied"}, err: status.Error(codes.FailedPrecondition, "permission denied")},
	} {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			var opens, sends int
			client := &watchTestClient{watch: func(context.Context) (etcdserverpb.Watch_WatchClient, error) {
				opens++
				return &watchTestStream{
					send: func(*etcdserverpb.WatchRequest) error { return test.sendErr },
					recv: func() (*etcdserverpb.WatchResponse, error) { return test.response, nil },
				}, nil
			}}
			err := forwardEtcdWatchWithRetry(t.Context(), client, etcdserverpb.WatchCreateRequest{}, func(response *etcdserverpb.WatchResponse, err error) error {
				sends++
				re.Same(test.response, response)
				re.Equal(test.err.Error(), err.Error())
				return nil
			})
			re.Equal(test.err.Error(), err.Error())
			re.Equal(1, opens)
			re.Equal(1, sends)
		})
	}
}

func TestForwardEtcdWatchSendError(t *testing.T) {
	re := require.New(t)
	expected := status.Error(codes.Internal, "downstream send failed")
	var opens int
	client := &watchTestClient{watch: func(context.Context) (etcdserverpb.Watch_WatchClient, error) {
		opens++
		return &watchTestStream{
			send: func(*etcdserverpb.WatchRequest) error { return nil },
			recv: func() (*etcdserverpb.WatchResponse, error) { return watchTestResponse(1), nil },
		}, nil
	}}
	err := forwardEtcdWatchWithRetry(t.Context(), client, etcdserverpb.WatchCreateRequest{}, func(*etcdserverpb.WatchResponse, error) error { return expected })
	re.ErrorIs(err, expected)
	re.Equal(1, opens)
}

func TestForwardEtcdWatchSendEOF(t *testing.T) {
	re := require.New(t)
	expected := status.Error(codes.PermissionDenied, "backend permission denied")
	client := &watchTestClient{watch: func(context.Context) (etcdserverpb.Watch_WatchClient, error) {
		return &watchTestStream{
			send: func(*etcdserverpb.WatchRequest) error { return io.EOF },
			recv: func() (*etcdserverpb.WatchResponse, error) { return nil, expected },
		}, nil
	}}
	var reported error
	err := forwardEtcdWatchWithRetry(t.Context(), client, etcdserverpb.WatchCreateRequest{}, func(response *etcdserverpb.WatchResponse, err error) error {
		re.True(response.Canceled)
		reported = err
		return nil
	})
	re.Equal(codes.PermissionDenied, status.Code(err))
	re.Equal(codes.PermissionDenied, status.Code(reported))
}

func TestForwardEtcdWatchCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	client := &watchTestClient{watch: func(ctx context.Context) (etcdserverpb.Watch_WatchClient, error) {
		return &watchTestStream{
			send: func(*etcdserverpb.WatchRequest) error { return nil },
			recv: func() (*etcdserverpb.WatchResponse, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}, nil
	}}
	result := make(chan error, 1)
	go func() {
		result <- forwardEtcdWatchWithRetry(ctx, client, etcdserverpb.WatchCreateRequest{}, func(*etcdserverpb.WatchResponse, error) error { return nil })
	}()
	<-started
	cancel()
	require.NoError(t, receiveWatchTestResult(t, result))
}

type blockedWatchTestServer struct {
	pdpb.UnimplementedPDServer
	result      chan error
	sending     chan int64
	upstreamCtx chan context.Context
	received    atomic.Int64
}

func (s *blockedWatchTestServer) WatchGlobalConfig(_ *pdpb.WatchGlobalConfigRequest, stream pdpb.PD_WatchGlobalConfigServer) error {
	payload := make([]byte, 1<<20)
	client := &watchTestClient{watch: func(ctx context.Context) (etcdserverpb.Watch_WatchClient, error) {
		s.upstreamCtx <- ctx
		return &watchTestStream{
			send: func(*etcdserverpb.WatchRequest) error { return nil },
			recv: func() (*etcdserverpb.WatchResponse, error) {
				response := watchTestResponse(s.received.Add(1))
				response.Events[0].Kv.Value = payload
				return response, nil
			},
		}, nil
	}}
	err := forwardEtcdWatchWithRetry(stream.Context(), client, etcdserverpb.WatchCreateRequest{}, func(response *etcdserverpb.WatchResponse, _ error) error {
		s.sending <- response.Header.Revision
		return stream.Send(&pdpb.WatchGlobalConfigResponse{Changes: []*pdpb.GlobalConfigItem{{Payload: response.Events[0].Kv.Value}}})
	})
	s.result <- err
	return err
}

func TestForwardEtcdWatchTransportBackpressure(t *testing.T) {
	for _, tlsHandler := range []bool{false, true} {
		name := "native gRPC"
		if tlsHandler {
			name = "TLS ServeHTTP"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			shutdownCtx, shutdown := context.WithCancel(t.Context())
			defer shutdown()
			server := grpc.NewServer()
			service := &blockedWatchTestServer{result: make(chan error, 1), sending: make(chan int64, 128), upstreamCtx: make(chan context.Context, 1)}
			pdpb.RegisterPDServer(server, service)
			t.Cleanup(server.Stop)
			var conn *grpc.ClientConn
			var err error
			if tlsHandler {
				https := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ServeHTTPWithContext(shutdownCtx, server, w, r)
				}))
				https.EnableHTTP2 = true
				https.StartTLS()
				t.Cleanup(https.Close)
				roots := x509.NewCertPool()
				roots.AddCert(https.Certificate())
				conn, err = grpc.NewClient(https.Listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
			} else {
				listener := bufconn.Listen(1 << 20)
				t.Cleanup(func() { _ = listener.Close() })
				go func() { _ = server.Serve(listener) }()
				conn, err = grpc.NewClient("passthrough:///watch-test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
			}
			re.NoError(err)
			t.Cleanup(func() { _ = conn.Close() })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			_, err = pdpb.NewPDClient(conn).WatchGlobalConfig(ctx, &pdpb.WatchGlobalConfigRequest{})
			re.NoError(err)
			// Two large sends exhaust the stream window while the client never reads.
			for revision := int64(1); revision <= 2; revision++ {
				select {
				case actual := <-service.sending:
					re.Equal(revision, actual)
				case <-time.After(5 * time.Second):
					t.Fatal("watch did not reach transport flow control")
				}
			}
			select {
			case <-service.sending:
				t.Fatal("blocked client did not apply backpressure to the upstream watch")
			case <-time.After(100 * time.Millisecond):
			}
			re.Equal(int64(2), service.received.Load())
			if tlsHandler {
				// The client remains connected and does not read or cancel its RPC.
				shutdown()
			} else {
				cancel()
			}
			_ = receiveWatchTestResult(t, service.result)
			re.ErrorIs((<-service.upstreamCtx).Err(), context.Canceled)
		})
	}
}
