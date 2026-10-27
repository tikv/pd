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

// ForwardEtcdWatch forwards complete etcd watch responses in order. It reads the
// gRPC watch stream directly so that a blocked send applies transport backpressure
// instead of accumulating responses in the unbounded clientv3.Watcher buffer.
// Recoverable upstream failures resume after the last successfully sent revision.
// The caller must bind ctx to both client and server lifetimes; send runs in the
// calling goroutine and receives terminal watch errors alongside their response.
func ForwardEtcdWatch(ctx context.Context, client *clientv3.Client, request etcdserverpb.WatchCreateRequest, send func(*etcdserverpb.WatchResponse, error) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(client.Ctx(), cancel)
	defer stop()
	return forwardEtcdWatchWithRetry(ctx, etcdserverpb.NewWatchClient(client.ActiveConnection()), request, send)
}

func forwardEtcdWatchWithRetry(ctx context.Context, client etcdserverpb.WatchClient, request etcdserverpb.WatchCreateRequest, send func(*etcdserverpb.WatchResponse, error) error) error {
	// A complete revision must be delivered before advancing the resume cursor.
	request.Fragment = false
	backoff := time.Millisecond
	for {
		watchCtx, cancel := context.WithCancel(ctx)
		// Match clientv3.Watcher's default receive limit. Historical watch responses
		// can be larger than a single etcd write or gRPC's default 4 MiB limit.
		stream, err := client.Watch(watchCtx, grpc.WaitForReady(true), grpc.MaxCallRecvMsgSize(math.MaxInt32))
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

func forwardEtcdWatchStream(stream etcdserverpb.Watch_WatchClient, request *etcdserverpb.WatchCreateRequest, send func(*etcdserverpb.WatchResponse, error) error) (bool, error) {
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
