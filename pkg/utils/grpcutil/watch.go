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
	"io"
	"math"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/log"
)

// EtcdWatchForwarder shares an upstream RPC between watches. Each subscription
// buffers one complete response. A subscription that falls behind is canceled
// upstream and resumes after its last delivered revision when the sender is ready.
// The zero value is ready to use and must not be copied after first use.
type EtcdWatchForwarder struct {
	mu     sync.Mutex
	stream *sharedEtcdWatch
}

// Forward sends complete revisions in order, using the calling goroutine for
// send. The caller must bind ctx to both client and server lifetimes.
func (f *EtcdWatchForwarder) Forward(ctx context.Context, client *clientv3.Client, request etcdserverpb.WatchCreateRequest, send func(*etcdserverpb.WatchResponse, error) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(client.Ctx(), cancel)
	defer stop()
	return forwardEtcdWatchWithRetry(ctx, func(ctx context.Context) (etcdWatchStream, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.stream == nil || f.stream.client != client || watchDone(f.stream.done) {
			f.stream = newSharedEtcdWatch(client)
			go f.stream.run()
		}
		return &watchSubscription{
			stream: f.stream, ctx: ctx, created: make(chan *etcdserverpb.WatchResponse, 1),
			responses: make(chan *etcdserverpb.WatchResponse, 1), sent: make(chan error, 1), done: make(chan struct{}),
		}, nil
	}, request, send)
}

type etcdWatchStream interface {
	Send(*etcdserverpb.WatchRequest) error
	Recv() (*etcdserverpb.WatchResponse, error)
}

func openEtcdWatchStream(ctx context.Context, client etcdserverpb.WatchClient) (etcdWatchStream, error) {
	// Match clientv3.Watcher's receive limit, including large historical batches.
	return client.Watch(ctx, grpc.WaitForReady(true), grpc.MaxCallRecvMsgSize(math.MaxInt32))
}

func forwardEtcdWatchWithRetry(ctx context.Context, open func(context.Context) (etcdWatchStream, error), request etcdserverpb.WatchCreateRequest, send func(*etcdserverpb.WatchResponse, error) error) error {
	// A complete revision must be delivered before advancing the resume cursor.
	request.Fragment = false
	backoff := time.Millisecond
	for {
		watchCtx, cancel := context.WithCancel(ctx)
		stream, err := open(watchCtx)
		if err == nil {
			err = stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &request}})
			if err == io.EOF {
				// Recv exposes the RPC status when Send only reports a closed stream.
				err = nil
			}
		}
		if err == nil {
			var terminal bool
			terminal, err = forwardEtcdWatchStream(stream, &request, send)
			if terminal {
				cancel()
				return err
			}
		}
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err != io.EOF && (rpctypes.Error(err) == rpctypes.ErrNoLeader || (status.Code(err) != codes.Unavailable && status.Code(err) != codes.Internal)) {
			watchErr := rpctypes.Error(err)
			if err := send(&etcdserverpb.WatchResponse{Canceled: true}, watchErr); err != nil {
				return err
			}
			return watchErr
		}
		if err == errWatchPaused {
			// Backpressure has ended when send returns. Re-register without the
			// connection-failure backoff, which would penalize healthy burst traffic.
			continue
		}
		log.Debug("resuming etcd watch", zap.Int64("next-revision", request.StartRevision), zap.Error(err))
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff = min(100*time.Millisecond, backoff*2)
	}
}

func forwardEtcdWatchStream(stream etcdWatchStream, request *etcdserverpb.WatchCreateRequest, send func(*etcdserverpb.WatchResponse, error) error) (bool, error) {
	for {
		response, err := stream.Recv()
		if err != nil {
			return false, err
		}
		var watchErr error
		switch {
		case response.CompactRevision != 0:
			watchErr = rpctypes.ErrCompacted
		case response.Canceled:
			watchErr = rpctypes.ErrFutureRev
			if response.CancelReason != "" {
				watchErr = rpctypes.Error(status.Error(codes.FailedPrecondition, response.CancelReason))
			}
		case response.Created:
			if request.StartRevision == 0 {
				// Pin a current-revision watch before its first event so that a
				// reconnect cannot skip writes made after the creation response.
				request.StartRevision = response.GetHeader().GetRevision() + 1
			}
			continue
		}
		if err := send(response, watchErr); err != nil {
			return true, err
		}
		if watchErr != nil {
			return true, watchErr
		}
		if events := response.Events; len(events) > 0 {
			// A historical batch's header can be newer than its last event.
			request.StartRevision = events[len(events)-1].Kv.ModRevision + 1
		} else {
			request.StartRevision = max(request.StartRevision, response.GetHeader().GetRevision()+1)
		}
	}
}

var (
	errWatchPaused       = status.Error(codes.Unavailable, "etcd watch paused for a slow consumer")
	errWatchStreamClosed = status.Error(codes.Unavailable, "etcd watch stream closed")
)

func watchDone(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

type watchSubscription struct {
	stream          *sharedEtcdWatch
	ctx             context.Context
	request         etcdserverpb.WatchCreateRequest
	id              int64
	created         chan *etcdserverpb.WatchResponse
	responses       chan *etcdserverpb.WatchResponse
	sent            chan error
	done            chan struct{}
	createdReceived bool
	// Protected by stream.mu; err is read only after done closes.
	finished bool
	err      error
	stop     func() bool
}

// Send registers this subscription on the shared RPC.
func (s *watchSubscription) Send(request *etcdserverpb.WatchRequest) error {
	s.request = *request.GetCreateRequest()
	select {
	case s.stream.requests <- s:
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.stream.done:
		return s.stream.err
	}
	select {
	case err := <-s.sent:
		return err
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.stream.done:
		// A completed Send must still allow Recv to observe its creation cursor.
		select {
		case err := <-s.sent:
			return err
		default:
			return s.stream.err
		}
	}
}

// Recv preserves the creation cursor before reporting a paused subscription.
func (s *watchSubscription) Recv() (*etcdserverpb.WatchResponse, error) {
	if !s.createdReceived {
		select {
		case response := <-s.created:
			s.createdReceived = true
			return response, nil
		case <-s.done:
			// Preserve an observed creation response even if this subscription
			// overflowed or the connection failed before its caller received it.
			select {
			case response := <-s.created:
				s.createdReceived = true
				return response, nil
			default:
				return nil, s.err
			}
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		}
	}
	select {
	case response := <-s.responses:
		// Closing can discard earlier queued responses. Do not deliver a later
		// response across that boundary; replay from the caller's cursor instead.
		select {
		case <-s.done:
			return nil, s.err
		default:
			return response, nil
		}
	case <-s.done:
		return nil, s.err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

type sharedEtcdWatch struct {
	client   *clientv3.Client
	ctx      context.Context
	cancel   context.CancelFunc
	requests chan *watchSubscription
	wake     chan struct{}
	done     chan struct{}

	mu            sync.Mutex
	subscriptions map[int64]*watchSubscription
	canceled      map[*watchSubscription]struct{}
	// Serial creation also identifies auth failures whose WatchId is -1.
	creating *watchSubscription
	nextID   int64
	closed   bool
	err      error
}

func newSharedEtcdWatch(client *clientv3.Client) *sharedEtcdWatch {
	ctx, cancel := context.WithCancel(client.Ctx())
	return &sharedEtcdWatch{
		client: client, ctx: ctx, cancel: cancel, requests: make(chan *watchSubscription),
		wake: make(chan struct{}, 1), done: make(chan struct{}),
		subscriptions: make(map[int64]*watchSubscription), canceled: make(map[*watchSubscription]struct{}),
	}
}

func (s *sharedEtcdWatch) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *sharedEtcdWatch) fail(err error) {
	s.mu.Lock()
	if s.err == nil && !s.closed {
		s.err = err
	}
	s.mu.Unlock()
	s.cancel()
}

// finishWatchSubscription is called with the stream mutex held. Only the creation response survives closing;
// queued events are replayed from the caller's last delivered revision.
func finishWatchSubscription(sub *watchSubscription, err error) {
	if sub.finished {
		return
	}
	sub.finished, sub.err = true, err
	if sub.stop != nil {
		sub.stop()
	}
	close(sub.done)
	for {
		select {
		case <-sub.responses:
		default:
			return
		}
	}
}

func (s *sharedEtcdWatch) cancelSubscription(sub *watchSubscription) {
	s.mu.Lock()
	if !s.closed && !sub.finished {
		s.canceled[sub] = struct{}{}
		s.signal()
	}
	s.mu.Unlock()
}

func (s *sharedEtcdWatch) dispatch(response *etcdserverpb.WatchResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if response.Created {
		sub := s.creating
		if sub == nil || (response.WatchId != sub.id && response.WatchId != clientv3.InvalidWatchID) {
			return status.Error(codes.Internal, "unexpected etcd watch creation response")
		}
		s.creating = nil
		sub.created <- response
		s.signal()
		return nil
	}
	sub := s.subscriptions[response.WatchId]
	if sub == nil || sub.finished {
		return nil
	}
	select {
	case sub.responses <- response:
	default:
		s.canceled[sub] = struct{}{}
		finishWatchSubscription(sub, errWatchPaused)
		s.signal()
	}
	return nil
}

func (s *sharedEtcdWatch) register(stream etcdWatchStream, sub *watchSubscription) error {
	s.mu.Lock()
	if err := sub.ctx.Err(); err != nil {
		finishWatchSubscription(sub, err)
		s.mu.Unlock()
		sub.sent <- err
		return nil
	}
	s.nextID++
	sub.id, sub.request.WatchId = s.nextID, s.nextID
	s.subscriptions[sub.id], s.creating = sub, sub
	sub.stop = context.AfterFunc(sub.ctx, func() { s.cancelSubscription(sub) })
	s.mu.Unlock()
	err := stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &sub.request}})
	sub.sent <- err
	return err
}

func (s *sharedEtcdWatch) run() {
	var readerDone chan struct{}
	defer func() {
		s.mu.Lock()
		if s.err == nil {
			s.err = errWatchStreamClosed
		}
		s.mu.Unlock()
		s.cancel()
		if readerDone != nil {
			<-readerDone
		}
		s.mu.Lock()
		s.closed = true
		for _, sub := range s.subscriptions {
			finishWatchSubscription(sub, s.err)
		}
		s.subscriptions, s.canceled, s.creating = nil, nil, nil
		s.mu.Unlock()
		close(s.done)
	}()
	var first *watchSubscription
	select {
	case first = <-s.requests:
	case <-s.ctx.Done():
		return
	}
	// No other watch has been registered yet; a canceled first caller must not
	// leave an upstream connection attempt running for the client's lifetime.
	stopOpening := context.AfterFunc(first.ctx, func() { s.fail(errWatchStreamClosed) })
	defer stopOpening()
	stream, err := openEtcdWatchStream(s.ctx, etcdserverpb.NewWatchClient(s.client.ActiveConnection()))
	if err != nil {
		first.sent <- err
		s.fail(err)
		return
	}
	readerDone = make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			response, err := stream.Recv()
			if err == nil {
				err = s.dispatch(response)
			}
			if err != nil {
				s.fail(err)
				return
			}
		}
	}()
	err = s.register(stream, first)
	stopOpening()
	canSend := err != io.EOF
	if err != nil && err != io.EOF {
		s.fail(err)
		return
	}
	for {
		s.mu.Lock()
		var canceled []int64
		for sub := range s.canceled {
			delete(s.subscriptions, sub.id)
			finishWatchSubscription(sub, sub.ctx.Err())
			canceled = append(canceled, sub.id)
		}
		clear(s.canceled)
		empty := len(s.subscriptions) == 0
		requests := s.requests
		if s.creating != nil || !canSend {
			requests = nil
		}
		s.mu.Unlock()
		if empty {
			return
		}
		for _, id := range canceled {
			if !canSend {
				break
			}
			err = stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: id}}})
			if err == io.EOF {
				canSend = false
				requests = nil
			} else if err != nil {
				s.fail(err)
				return
			}
		}
		select {
		case sub := <-requests:
			err = s.register(stream, sub)
			if err == io.EOF {
				canSend = false
			} else if err != nil {
				s.fail(err)
				return
			}
		case <-s.wake:
		case <-s.ctx.Done():
			return
		}
	}
}
