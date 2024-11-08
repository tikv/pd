// Copyright 2023 TiKV Project Authors.
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

package pd

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type batchController[T any] struct {
	maxBatchSize int
	// bestBatchSize is a dynamic size that changed based on the current batch effect.
	bestBatchSize int

	collectedRequests     []T
	collectedRequestCount int

	// The finisher function to cancel collected requests when an internal error occurs.
	cancelFinisher func(int, T)
	// The observer to record the best batch size.
	bestBatchObserver prometheus.Histogram
	// The time after getting the first request and the token, and before performing extra batching.
	extraBatchingStartTime time.Time
}

func newBatchController[T any](maxBatchSize int, cancelFinisher func(int, T), bestBatchObserver prometheus.Histogram) *batchController[T] {
	return &batchController[T]{
		maxBatchSize:          maxBatchSize,
		bestBatchSize:         8, /* Starting from a low value is necessary because we need to make sure it will be converged to (current_batch_size - 4) */
		collectedRequests:     make([]T, maxBatchSize+1),
		collectedRequestCount: 0,
		cancelFinisher:        cancelFinisher,
		bestBatchObserver:     bestBatchObserver,
	}
}

// fetchPendingRequests will start a new round of the batch collecting from the channel.
// It returns nil error if everything goes well, otherwise a non-nil error which means we should stop the service.
// It's guaranteed that if this function failed after collecting some requests, then these requests will be cancelled
// when the function returns, so the caller don't need to clear them manually.
func (bc *batchController[T]) fetchPendingRequests(ctx context.Context, requestCh <-chan T, tokenCh chan struct{}, maxBatchWaitInterval time.Duration) (errRet error) {
	var tokenAcquired bool
	defer func() {
		if errRet != nil {
			// Something went wrong when collecting a batch of requests. Release the token and cancel collected requests
			// if any.
			if tokenAcquired {
				tokenCh <- struct{}{}
			}
			bc.finishCollectedRequests(bc.cancelFinisher)
		}
	}()

	// Wait until BOTH the first request and the token have arrived.
	// TODO: `bc.collectedRequestCount` should never be non-empty here. Consider do assertion here.
	bc.collectedRequestCount = 0
	for {
		// If the batch size reaches the maxBatchSize limit but the token haven't arrived yet, don't receive more
		// requests, and return when token is ready.
		if bc.collectedRequestCount >= bc.maxBatchSize && !tokenAcquired {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tokenCh:
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case req := <-requestCh:
			// Start to batch when the first request arrives.
			bc.pushRequest(req)
			// A request arrives but the token is not ready yet. Continue waiting, and also allowing collecting the next
			// request if it arrives.
			continue
		case <-tokenCh:
			tokenAcquired = true
		}

		// The token is ready. If the first request didn't arrive, wait for it.
		if bc.collectedRequestCount == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case firstRequest := <-requestCh:
				bc.pushRequest(firstRequest)
			}
		}

		// Both token and the first request have arrived.
		break
	}

	bc.extraBatchingStartTime = time.Now()

	// This loop is for trying best to collect more requests, so we use `bc.maxBatchSize` here.
fetchPendingRequestsLoop:
	for bc.collectedRequestCount < bc.maxBatchSize {
		select {
		case req := <-requestCh:
			bc.pushRequest(req)
		case <-ctx.Done():
			return ctx.Err()
		default:
			break fetchPendingRequestsLoop
		}
	}

	// Check whether we should fetch more pending requests from the channel.
	if bc.collectedRequestCount >= bc.maxBatchSize || maxBatchWaitInterval <= 0 {
		return nil
	}

	// Fetches more pending requests from the channel.
	// Try to collect `bc.bestBatchSize` requests, or wait `maxBatchWaitInterval`
	// when `bc.collectedRequestCount` is less than the `bc.bestBatchSize`.
	if bc.collectedRequestCount < bc.bestBatchSize {
		after := time.NewTimer(maxBatchWaitInterval)
		defer after.Stop()
		for bc.collectedRequestCount < bc.bestBatchSize {
			select {
			case req := <-requestCh:
				bc.pushRequest(req)
			case <-ctx.Done():
				return ctx.Err()
			case <-after.C:
				return nil
			}
		}
	}

	// Do an additional non-block try. Here we test the length with `bc.maxBatchSize` instead
	// of `bc.bestBatchSize` because trying best to fetch more requests is necessary so that
	// we can adjust the `bc.bestBatchSize` dynamically later.
	for bc.collectedRequestCount < bc.maxBatchSize {
		select {
		case req := <-requestCh:
			bc.pushRequest(req)
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	return nil
}

// fetchRequestsWithTimer tries to fetch requests until the given timer ticks. The caller must set the timer properly
// before calling this function.
func (bc *batchController[T]) fetchRequestsWithTimer(ctx context.Context, requestCh <-chan T, timer *time.Timer) error {
batchingLoop:
	for bc.collectedRequestCount < bc.maxBatchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req := <-requestCh:
			bc.pushRequest(req)
		case <-timer.C:
			break batchingLoop
		}
	}

	// Try to collect more requests in non-blocking way.
nonWaitingBatchLoop:
	for bc.collectedRequestCount < bc.maxBatchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req := <-requestCh:
			bc.pushRequest(req)
		default:
			break nonWaitingBatchLoop
		}
	}

	return nil
}

func (bc *batchController[T]) pushRequest(req T) {
	bc.collectedRequests[bc.collectedRequestCount] = req
	bc.collectedRequestCount++
}

func (bc *batchController[T]) getCollectedRequests() []T {
	return bc.collectedRequests[:bc.collectedRequestCount]
}

// adjustBestBatchSize stabilizes the latency with the AIAD algorithm.
func (bc *batchController[T]) adjustBestBatchSize() {
	bc.bestBatchObserver.Observe(float64(bc.bestBatchSize))
	length := bc.collectedRequestCount
	if length < bc.bestBatchSize && bc.bestBatchSize > 1 {
		// Waits too long to collect requests, reduce the target batch size.
		bc.bestBatchSize--
	} else if length > bc.bestBatchSize+4 /* Hard-coded number, in order to make `bc.bestBatchSize` stable */ &&
		bc.bestBatchSize < bc.maxBatchSize {
		bc.bestBatchSize++
	}
}

func (bc *batchController[T]) finishCollectedRequests(finisher func(int, T)) {
	if finisher == nil {
		finisher = bc.cancelFinisher
	}
	for i := range bc.collectedRequestCount {
		finisher(i, bc.collectedRequests[i])
	}
	// Prevent the finished requests from being processed again.
	bc.collectedRequestCount = 0
}
