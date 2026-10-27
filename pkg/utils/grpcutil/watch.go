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
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/logutil"
)

type watchForwardConfig struct {
	maxPendingResponses int
	maxPendingBytes     int64
	sendTimeout         time.Duration
}

type pendingWatchResponse struct {
	response clientv3.WatchResponse
	size     int64
}

type watchSendState struct {
	sending bool
	size    int64
}

// ForwardEtcdWatch forwards ordered etcd responses from watch to a gRPC stream.
// It keeps draining the etcd client while send is blocked, and terminates watches
// that exceed the pending response, byte, or send-time budgets. The byte budget
// includes the response being sent, previous values, and per-event overhead.
// Limits are 128 pending responses, 16 MiB of estimated response memory, and
// 30 seconds per send. Exceeding a limit returns ResourceExhausted; clients must
// resume from their last received revision or reload state.
// The caller must return its error from the gRPC handler to allow transport
// teardown. It must also bind ctx to both client and server lifetimes.
func ForwardEtcdWatch(ctx context.Context, watch func(context.Context) clientv3.WatchChan, send func(clientv3.WatchResponse) error) error {
	return forwardEtcdWatchWithConfig(ctx, watch, send, watchForwardConfig{
		maxPendingResponses: 128,
		maxPendingBytes:     16 << 20,
		sendTimeout:         30 * time.Second,
	})
}

func forwardEtcdWatchWithConfig(ctx context.Context, watch func(context.Context) clientv3.WatchChan, send func(clientv3.WatchResponse) error, cfg watchForwardConfig) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchCh := watch(ctx)
	pending := make(chan pendingWatchResponse, cfg.maxPendingResponses)
	sendState := make(chan watchSendState)
	result := make(chan error, 1)
	go func() {
		defer logutil.LogPanic()
		result <- sendEtcdWatchResponses(ctx, pending, sendState, send)
	}()
	// Native gRPC releases a Send blocked on flow control after the handler
	// returns. Do not join the sender here; cancel the etcd watch even when
	// transport teardown is delayed.
	timer := time.NewTimer(cfg.sendTimeout)
	timer.Stop()
	defer timer.Stop()
	var timeout <-chan time.Time
	var pendingBytes int64
	var pendingResponses int
	exceededLimit := func(reason string) error {
		log.Warn("etcd watch forwarding limit exceeded",
			zap.String("reason", reason),
			zap.Int("pending-responses", pendingResponses),
			zap.Int64("pending-bytes", pendingBytes))
		return status.Error(codes.ResourceExhausted, "etcd watch pending response or send-time limit exceeded")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-result:
			return err
		case state := <-sendState:
			if state.sending {
				timer.Reset(cfg.sendTimeout)
				timeout = timer.C
			} else {
				timer.Stop()
				timeout = nil
				pendingBytes -= state.size
				pendingResponses--
			}
		case <-timeout:
			return exceededLimit("send timeout")
		case response, ok := <-watchCh:
			if !ok {
				// Deliver queued terminal responses (notably compaction errors)
				// before reporting an unexpected watch-channel closure.
				close(pending)
				watchCh = nil
				continue
			}
			size := etcdWatchResponseSize(response)
			if pendingResponses >= cfg.maxPendingResponses || size > cfg.maxPendingBytes-pendingBytes {
				return exceededLimit("pending response budget exceeded")
			}
			pendingBytes += size
			pendingResponses++
			// Only this goroutine enqueues, and pendingResponses also counts
			// the in-flight response, so the capacity check above cannot block.
			pending <- pendingWatchResponse{response: response, size: size}
		}
	}
}

func sendEtcdWatchResponses(ctx context.Context, pending <-chan pendingWatchResponse, state chan<- watchSendState, send func(clientv3.WatchResponse) error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case item, ok := <-pending:
			if !ok {
				return errors.New("watch channel closed unexpectedly")
			}
			select {
			case <-ctx.Done():
				return nil
			case state <- watchSendState{sending: true}:
			}
			if err := send(item.response); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return nil
			case state <- watchSendState{size: item.size}:
			}
		}
	}
}

func etcdWatchResponseSize(response clientv3.WatchResponse) int64 {
	// Account for the response, event pointers, event structs, and protobuf
	// payloads. A response-count limit separately bounds empty responses.
	size := int64(128 + response.Header.Size())
	for _, event := range response.Events {
		size += 128
		if event.Kv != nil {
			size += int64(event.Kv.Size())
		}
		if event.PrevKv != nil {
			size += int64(event.PrevKv.Size())
		}
	}
	return size
}
