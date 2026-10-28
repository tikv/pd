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
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

type watchServingServer struct{ bs.Server }

func (*watchServingServer) IsServing() bool { return true }

type metaWatchStream struct {
	grpc.ServerStream
	ctx  context.Context
	send func(*meta_storagepb.WatchResponse) error
}

func (s *metaWatchStream) Context() context.Context { return s.ctx }

func (s *metaWatchStream) Send(response *meta_storagepb.WatchResponse) error {
	return s.send(response)
}

// watchRelay forwards real etcd responses and holds the first stream at the
// selected boundary, keeping later responses off the connection being cut.
type watchRelay struct {
	etcdserverpb.UnimplementedWatchServer
	client   etcdserverpb.WatchClient
	created  bool
	resume   chan struct{}
	requests chan int64
	attempts atomic.Int64
}

func (s *watchRelay) Watch(stream etcdserverpb.Watch_WatchServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	first := s.attempts.Add(1) == 1
	select {
	case s.requests <- request.GetCreateRequest().StartRevision:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	if !first {
		select {
		case <-s.resume:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	upstream, err := s.client.Watch(stream.Context())
	if err != nil {
		return err
	}
	if err := upstream.Send(request); err != nil {
		return err
	}
	for {
		response, err := upstream.Recv()
		if err != nil {
			return err
		}
		if err := stream.Send(response); err != nil {
			return err
		}
		if first && ((s.created && response.Created) || (!s.created && len(response.Events) > 0)) {
			<-stream.Context().Done()
			return stream.Context().Err()
		}
	}
}

type observedWatchStream struct {
	grpc.ClientStream
	observe func(*etcdserverpb.WatchResponse)
	sent    func(*etcdserverpb.WatchRequest)
}

func (s *observedWatchStream) RecvMsg(message any) error {
	if err := s.ClientStream.RecvMsg(message); err != nil {
		return err
	}
	if s.observe != nil {
		s.observe(message.(*etcdserverpb.WatchResponse))
	}
	return nil
}

func (s *observedWatchStream) SendMsg(message any) error {
	if err := s.ClientStream.SendMsg(message); err != nil {
		return err
	}
	if s.sent != nil {
		s.sent(message.(*etcdserverpb.WatchRequest))
	}
	return nil
}

func TestWatchResumesAfterConnectionLoss(t *testing.T) {
	for _, name := range []string{"before first event", "between historical batches", "compacted while disconnected"} {
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
			defer cleanup()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			var revisions []int64
			write := func() {
				value := strconv.Itoa(len(revisions))
				response, err := client.Txn(ctx).Then(clientv3.OpPut("watch/reconnect/a", value), clientv3.OpPut("watch/reconnect/b", value)).Commit()
				re.NoError(err)
				revisions = append(revisions, response.Header.Revision)
			}
			var startRevision int64
			if name == "between historical batches" {
				// etcd sends at most 1000 historical revisions in one batch.
				for range 1002 {
					write()
				}
				startRevision = revisions[0]
			}
			relay := &watchRelay{
				client: etcdserverpb.NewWatchClient(client.ActiveConnection()), created: startRevision == 0,
				resume: make(chan struct{}), requests: make(chan int64, 4),
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			re.NoError(err)
			server := grpc.NewServer()
			etcdserverpb.RegisterWatchServer(server, relay)
			defer server.Stop()
			go func() { _ = server.Serve(listener) }()
			observed := make(chan *etcdserverpb.WatchResponse, 1)
			var connection atomic.Pointer[net.TCPConn]
			var attempts atomic.Int64
			watchClient, err := clientv3.New(clientv3.Config{
				Endpoints: []string{listener.Addr().String()}, DialTimeout: 5 * time.Second,
				DialOptions: []grpc.DialOption{
					grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
						conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
						if err == nil {
							connection.Store(conn.(*net.TCPConn))
						}
						return conn, err
					}),
					grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
						stream, err := streamer(ctx, desc, conn, method, opts...)
						if err != nil || attempts.Add(1) != 1 {
							return stream, err
						}
						return &observedWatchStream{ClientStream: stream, observe: func(response *etcdserverpb.WatchResponse) {
							if (relay.created && response.Created) || (!relay.created && len(response.Events) > 0) {
								observed <- response
							}
						}}, nil
					}),
				},
			})
			re.NoError(err)
			defer watchClient.Close()
			service := &Service{ctx: ctx, manager: &Manager{srv: &watchServingServer{}, client: watchClient}}
			completed := errors.New("received all revisions")
			var events []*meta_storagepb.Event
			var compacted *meta_storagepb.WatchResponse
			expectedEvents := (len(revisions) + 1) * 2
			result := make(chan error, 1)
			go func() {
				result <- service.Watch(&meta_storagepb.WatchRequest{
					Key: []byte("watch/reconnect/"), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("watch/reconnect/")), StartRevision: startRevision, PrevKv: true,
				}, &metaWatchStream{ctx: ctx, send: func(response *meta_storagepb.WatchResponse) error {
					if response.Header.GetError() != nil {
						compacted = response
					}
					events = append(events, response.Events...)
					if len(events) == expectedEvents {
						return completed
					}
					return nil
				}})
			}()
			var boundary *etcdserverpb.WatchResponse
			select {
			case boundary = <-observed:
			case <-ctx.Done():
				t.Fatal("watch did not reach the disconnect boundary")
			}
			nextRevision := boundary.Header.Revision + 1
			if len(boundary.Events) > 0 {
				nextRevision = boundary.Events[len(boundary.Events)-1].Kv.ModRevision + 1
				re.Less(nextRevision, boundary.Header.Revision)
			}
			re.NoError(connection.Load().Close())
			write()
			if name == "compacted while disconnected" {
				write()
				_, err := client.Compact(ctx, revisions[len(revisions)-1])
				re.NoError(err)
			}
			close(relay.resume)
			for _, revision := range []int64{startRevision, nextRevision} {
				select {
				case requested := <-relay.requests:
					re.Equal(revision, requested)
				case <-ctx.Done():
					t.Fatal("watch did not issue its expected revision request")
				}
			}
			select {
			case err := <-result:
				if name == "compacted while disconnected" {
					re.ErrorIs(err, rpctypes.ErrCompacted)
				} else {
					re.ErrorIs(err, completed)
				}
			case <-ctx.Done():
				t.Fatal("watch did not recover after its TCP connection closed")
			}
			re.GreaterOrEqual(attempts.Load(), int64(2))
			if name == "compacted while disconnected" {
				re.Empty(events)
				re.NotNil(compacted)
				re.Equal(meta_storagepb.ErrorType_DATA_COMPACTED, compacted.Header.Error.Type)
				re.Equal(revisions[len(revisions)-1], compacted.CompactRevision)
				return
			}
			re.Len(events, len(revisions)*2)
			for i, revision := range revisions {
				for j, key := range []string{"watch/reconnect/a", "watch/reconnect/b"} {
					event := events[i*2+j]
					re.Equal(meta_storagepb.Event_PUT, event.Type)
					re.Equal(key, string(event.Kv.Key))
					re.Equal(strconv.Itoa(i), string(event.Kv.Value))
					re.Equal(revision, event.Kv.ModRevision)
					if i > 0 {
						re.Equal(strconv.Itoa(i-1), string(event.PrevKv.Value))
					}
				}
			}
		})
	}
}

func TestWatchPreservesRevisionAndPreviousValue(t *testing.T) {
	re := require.New(t)
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	put, err := client.Put(ctx, "watch/key", "previous")
	re.NoError(err)
	deleted, err := client.Delete(ctx, "watch/key")
	re.NoError(err)
	service := &Service{ctx: ctx, manager: &Manager{srv: &watchServingServer{}, client: client}}
	request := &meta_storagepb.WatchRequest{
		Key: []byte("watch/"), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("watch/")), StartRevision: put.Header.Revision, PrevKv: true,
	}
	completed := errors.New("received deletion")
	var events []*meta_storagepb.Event
	err = service.Watch(request, &metaWatchStream{ctx: ctx, send: func(response *meta_storagepb.WatchResponse) error {
		events = append(events, response.Events...)
		if response.Header.Revision == deleted.Header.Revision {
			return completed
		}
		return nil
	}})
	re.ErrorIs(err, completed)
	re.Len(events, 2)
	re.Equal(put.Header.Revision, events[0].Kv.ModRevision)
	re.Equal(deleted.Header.Revision, events[1].Kv.ModRevision)
	re.Equal(meta_storagepb.Event_DELETE, events[1].Type)
	re.Equal([]byte("previous"), events[1].PrevKv.Value)

	_, err = client.Compact(ctx, deleted.Header.Revision)
	re.NoError(err)
	var compacted *meta_storagepb.WatchResponse
	err = service.Watch(request, &metaWatchStream{ctx: ctx, send: func(response *meta_storagepb.WatchResponse) error {
		compacted = response
		return nil
	}})
	re.ErrorIs(err, rpctypes.ErrCompacted)
	re.NotNil(compacted)
	re.Equal(meta_storagepb.ErrorType_DATA_COMPACTED, compacted.Header.Error.Type)
	re.Equal(deleted.Header.Revision, compacted.CompactRevision)
}

func TestWatchSlowConsumerPreservesEvents(t *testing.T) {
	for _, compacted := range []bool{false, true} {
		t.Run("compacted="+strconv.FormatBool(compacted), func(t *testing.T) {
			re := require.New(t)
			_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
			defer cleanup()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			var streams atomic.Int64
			paused := make(chan struct{})
			var pauseOnce sync.Once
			watchClient, err := clientv3.New(clientv3.Config{
				Endpoints: client.Endpoints(), DialTimeout: 5 * time.Second,
				DialOptions: []grpc.DialOption{grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					stream, err := streamer(ctx, desc, conn, method, opts...)
					if err != nil || method != "/etcdserverpb.Watch/Watch" {
						return stream, err
					}
					streams.Add(1)
					return &observedWatchStream{ClientStream: stream, sent: func(request *etcdserverpb.WatchRequest) {
						if request.GetCancelRequest().GetWatchId() == 1 {
							pauseOnce.Do(func() { close(paused) })
						}
					}}, nil
				})},
			})
			re.NoError(err)
			defer watchClient.Close()
			put, err := client.Put(ctx, "watch/slow", "0")
			re.NoError(err)
			service := &Service{ctx: ctx, manager: &Manager{srv: &watchServingServer{}, client: watchClient}}
			const watchers, slowWatchers, updates = 32, 16, 256
			started, caughtUp := make(chan struct{}, watchers), make(chan int, watchers)
			released := make(chan struct{})
			var release sync.Once
			defer release.Do(func() { close(released) })
			completed := errors.New("received all updates")
			compactedResponses := make([]*meta_storagepb.WatchResponse, slowWatchers)
			events := make([][]*meta_storagepb.Event, watchers)
			results := make(chan error, watchers)
			start := func(index int) {
				go func() {
					results <- service.Watch(&meta_storagepb.WatchRequest{Key: []byte("watch/slow"), StartRevision: put.Header.Revision, PrevKv: true}, &metaWatchStream{ctx: ctx, send: func(response *meta_storagepb.WatchResponse) error {
						if index < slowWatchers && response.Header.GetError() != nil {
							compactedResponses[index] = response
						}
						if len(events[index]) == 0 {
							started <- struct{}{}
							if index < slowWatchers {
								select {
								case <-released:
								case <-ctx.Done():
									return ctx.Err()
								}
							}
						}
						events[index] = append(events[index], response.Events...)
						for _, event := range response.Events {
							if string(event.Kv.Value) == strconv.Itoa(updates-1) {
								caughtUp <- index
							}
						}
						if len(events[index]) >= updates+1 {
							return completed
						}
						return nil
					}})
				}()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("initial watch response was not sent")
				}
			}
			// Register the blocked sender first so its upstream WatchId is known.
			for i := range watchers {
				start(i)
			}
			re.Equal(int64(1), streams.Load(), "concurrent watches must share their upstream RPC")
			for i := 1; i < updates; i++ {
				_, err := client.Put(ctx, "watch/slow", strconv.Itoa(i))
				re.NoError(err)
			}
			select {
			case <-paused:
			case <-ctx.Done():
				t.Fatal("blocked consumer was not paused upstream")
			}
			for range watchers - slowWatchers {
				select {
				case index := <-caughtUp:
					re.GreaterOrEqual(index, slowWatchers, "healthy senders must catch up while slow senders are blocked")
				case <-ctx.Done():
					t.Fatal("slow consumer blocked a healthy consumer")
				}
			}
			re.Equal(int64(1), streams.Load(), "pausing one watch must not close the shared RPC")
			remaining := watchers
			if compacted {
				current, err := client.Get(ctx, "watch/slow")
				re.NoError(err)
				_, err = client.Compact(ctx, current.Header.Revision)
				re.NoError(err)
			}
			release.Do(func() { close(released) })
			if compacted {
				for range slowWatchers {
					select {
					case err := <-results:
						re.ErrorIs(err, rpctypes.ErrCompacted)
					case <-ctx.Done():
						t.Fatal("paused watch did not report compacted history")
					}
				}
				for index, response := range compactedResponses {
					re.NotNil(response)
					re.Equal(meta_storagepb.ErrorType_DATA_COMPACTED, response.Header.GetError().GetType())
					re.Len(events[index], 1)
				}
				remaining -= slowWatchers
			} else {
				for range slowWatchers {
					select {
					case index := <-caughtUp:
						re.Less(index, slowWatchers)
					case <-ctx.Done():
						t.Fatal("slow consumer did not replay its backlog")
					}
				}
			}
			_, err = client.Put(ctx, "watch/slow", strconv.Itoa(updates))
			re.NoError(err)
			for range remaining {
				select {
				case err := <-results:
					re.ErrorIs(err, completed)
				case <-ctx.Done():
					t.Fatal("watch did not receive all updates")
				}
			}
			for index, received := range events {
				if compacted && index < slowWatchers {
					continue
				}
				re.Len(received, updates+1)
				for i, event := range received {
					re.Equal(strconv.Itoa(i), string(event.Kv.Value))
					if i > 0 {
						re.Greater(event.Kv.ModRevision, received[i-1].Kv.ModRevision)
						re.Equal(strconv.Itoa(i-1), string(event.PrevKv.Value))
					}
				}
			}
		})
	}
}

func TestWatchPermissionFailureDoesNotInterruptOtherWatches(t *testing.T) {
	re := require.New(t)
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := client.UserAdd(ctx, "root", "root-password")
	re.NoError(err)
	_, err = client.UserGrantRole(ctx, "root", "root")
	re.NoError(err)
	_, err = client.UserAdd(ctx, "watcher", "watch-password")
	re.NoError(err)
	_, err = client.RoleAdd(ctx, "watch-reader")
	re.NoError(err)
	_, err = client.RoleGrantPermission(ctx, "watch-reader", "watch/allowed", "", clientv3.PermissionType(clientv3.PermRead))
	re.NoError(err)
	_, err = client.UserGrantRole(ctx, "watcher", "watch-reader")
	re.NoError(err)
	put, err := client.Put(ctx, "watch/allowed", "before")
	re.NoError(err)
	_, err = client.AuthEnable(ctx)
	re.NoError(err)
	root, err := clientv3.New(clientv3.Config{Endpoints: client.Endpoints(), Username: "root", Password: "root-password", DialTimeout: 5 * time.Second})
	re.NoError(err)
	defer root.Close()
	watchClient, err := clientv3.New(clientv3.Config{Endpoints: client.Endpoints(), Username: "watcher", Password: "watch-password", DialTimeout: 5 * time.Second})
	re.NoError(err)
	defer watchClient.Close()
	service := &Service{ctx: ctx, manager: &Manager{srv: &watchServingServer{}, client: watchClient}}
	started := make(chan struct{})
	result := make(chan error, 1)
	completed := errors.New("received authorized update")
	var events []*meta_storagepb.Event
	go func() {
		result <- service.Watch(&meta_storagepb.WatchRequest{Key: []byte("watch/allowed"), StartRevision: put.Header.Revision}, &metaWatchStream{ctx: ctx, send: func(response *meta_storagepb.WatchResponse) error {
			events = append(events, response.Events...)
			if len(events) == 1 {
				close(started)
			}
			if len(events) == 2 {
				return completed
			}
			return nil
		}})
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("authorized watch did not start")
	}
	var denied *meta_storagepb.WatchResponse
	err = service.Watch(&meta_storagepb.WatchRequest{Key: []byte("watch/denied")}, &metaWatchStream{ctx: ctx, send: func(response *meta_storagepb.WatchResponse) error {
		denied = response
		return nil
	}})
	re.ErrorContains(err, "permission denied")
	re.NotNil(denied)
	re.Equal(meta_storagepb.ErrorType_UNKNOWN, denied.Header.GetError().GetType())
	_, err = root.Put(ctx, "watch/allowed", "after")
	re.NoError(err)
	select {
	case err := <-result:
		re.ErrorIs(err, completed)
	case <-ctx.Done():
		t.Fatal("permission failure interrupted another watch")
	}
	re.Len(events, 2)
	re.Equal("before", string(events[0].Kv.Value))
	re.Equal("after", string(events[1].Kv.Value))
}

func TestWatchLargeHistoricalResponse(t *testing.T) {
	re := require.New(t)
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	const count = 20
	value := string(make([]byte, 1<<20))
	var firstRevision int64
	for i := range count {
		put, err := client.Put(ctx, "watch/history/"+strconv.Itoa(i), value)
		re.NoError(err)
		if i == 0 {
			firstRevision = put.Header.Revision
		}
	}
	service := &Service{ctx: ctx, manager: &Manager{srv: &watchServingServer{}, client: client}}
	completed := errors.New("received historical batch")
	var events []*meta_storagepb.Event
	err := service.Watch(&meta_storagepb.WatchRequest{
		Key: []byte("watch/history/"), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("watch/history/")), StartRevision: firstRevision,
	}, &metaWatchStream{ctx: ctx, send: func(response *meta_storagepb.WatchResponse) error {
		events = response.Events
		return completed
	}})
	re.ErrorIs(err, completed)
	re.Len(events, count)
	for i, event := range events {
		re.Equal("watch/history/"+strconv.Itoa(i), string(event.Kv.Key))
		re.Equal(value, string(event.Kv.Value))
		re.Equal(firstRevision+int64(i), event.Kv.ModRevision)
	}
}
