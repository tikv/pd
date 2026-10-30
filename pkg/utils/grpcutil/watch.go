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

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ForwardEtcdWatch forwards complete responses synchronously. A slow sender
// pauses its native etcd subscription and resumes after the last delivered
// revision, while the client continues serving other subscriptions.
func ForwardEtcdWatch(ctx context.Context, client *clientv3.Client, key string, revision int64, send func(clientv3.WatchResponse, error) error, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(client.Ctx(), cancel)
	defer stop()
	for ctx.Err() == nil {
		paused, err := forwardEtcdWatchAttempt(ctx, client, key, &revision, send, opts)
		if err != nil || !paused {
			return err
		}
	}
	return nil
}

func forwardEtcdWatchAttempt(ctx context.Context, client *clientv3.Client, key string, revision *int64, send func(clientv3.WatchResponse, error) error, opts []clientv3.OpOption) (bool, error) {
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchOpts := append([]clientv3.OpOption(nil), opts...)
	watchOpts = append(watchOpts, clientv3.WithRev(*revision), clientv3.WithCreatedNotify())
	watch := client.Watch(watchCtx, key, watchOpts...)
	created, ok := <-watch
	if !ok {
		return false, nil
	}
	if watchErr := created.Err(); watchErr != nil {
		if err := send(created, watchErr); err != nil {
			return false, err
		}
		return false, watchErr
	}
	if *revision == 0 {
		// Retain the creation cursor before any event can pause the pump.
		*revision = created.Header.Revision + 1
	}
	responses := make(chan clientv3.WatchResponse, 1)
	// Read paused only after responses closes, which joins the pump.
	var paused bool
	go func() {
		defer close(responses)
		defer cancel()
		for response := range watch {
			select {
			case responses <- response:
			case <-watchCtx.Done():
				return
			default:
				paused = true
				return
			}
		}
	}()
	defer func() {
		cancel()
		// Join even if Send failed, without retaining the queued response.
		for range responses {
		}
	}()
	for response := range responses {
		watchErr := response.Err()
		if response.CompactRevision != 0 && *revision > response.CompactRevision {
			return true, nil
		}
		// The native client can replay an earlier revision after reconnecting.
		// Only revisions not yet delivered belong on the downstream stream.
		events := response.Events
		for len(response.Events) > 0 && response.Events[0].Kv.ModRevision < *revision {
			response.Events = response.Events[1:]
		}
		if len(events) > 0 && len(response.Events) == 0 {
			continue
		}
		if err := send(response, watchErr); err != nil {
			return false, err
		}
		if watchErr != nil {
			return false, watchErr
		}
		if events := response.Events; len(events) > 0 {
			// A historical batch's header can be newer than its last event.
			*revision = events[len(events)-1].Kv.ModRevision + 1
		} else {
			*revision = max(*revision, response.Header.Revision+1)
		}
	}
	return paused, nil
}
