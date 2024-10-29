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
	"runtime/trace"
	"time"

	"github.com/tikv/pd/client/tsoutil"
)

type tsoBatchController struct {
	maxBatchSize int
	// bestBatchSize is a dynamic size that changed based on the current batch effect.
	bestBatchSize int

	collectedRequests     []*tsoRequest
	collectedRequestCount int

	// The time after getting the first request and the token, and before performing extra batching.
	extraBatchingStartTime time.Time
}

func newTSOBatchController(maxBatchSize int) *tsoBatchController {
	return &tsoBatchController{
		maxBatchSize:          maxBatchSize,
		bestBatchSize:         8, /* Starting from a low value is necessary because we need to make sure it will be converged to (current_batch_size - 4) */
		collectedRequests:     make([]*tsoRequest, maxBatchSize+1),
		collectedRequestCount: 0,
	}
}

// fetchPendingRequests will start a new round of the batch collecting from the channel.
// It returns nil error if everything goes well, otherwise a non-nil error which means we should stop the service.
// It's guaranteed that if this function failed after collecting some requests, then these requests will be cancelled
// when the function returns, so the caller don't need to clear them manually.
func (tbc *tsoBatchController) fetchPendingRequests(ctx context.Context, tsoRequestCh <-chan *tsoRequest, tokenCh chan struct{}, maxBatchWaitInterval time.Duration) (errRet error) {
	var tokenAcquired bool
	defer func() {
		if errRet != nil {
			// Something went wrong when collecting a batch of requests. Release the token and cancel collected requests
			// if any.
			if tokenAcquired {
				tokenCh <- struct{}{}
			}
			tbc.finishCollectedRequests(0, 0, 0, invalidStreamID, errRet)
		}
	}()

	// Wait until BOTH the first request and the token have arrived.
	// TODO: `tbc.collectedRequestCount` should never be non-empty here. Consider do assertion here.
	tbc.collectedRequestCount = 0
	for {
		// If the batch size reaches the maxBatchSize limit but the token haven't arrived yet, don't receive more
		// requests, and return when token is ready.
		if tbc.collectedRequestCount >= tbc.maxBatchSize && !tokenAcquired {
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
		case req := <-tsoRequestCh:
			// Start to batch when the first TSO request arrives.
			tbc.pushRequest(req)
			// A request arrives but the token is not ready yet. Continue waiting, and also allowing collecting the next
			// request if it arrives.
			continue
		case <-tokenCh:
			tokenAcquired = true
		}

		// The token is ready. If the first request didn't arrive, wait for it.
		if tbc.collectedRequestCount == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case firstRequest := <-tsoRequestCh:
				tbc.pushRequest(firstRequest)
			}
		}

		// Both token and the first request have arrived.
		break
	}

	tbc.extraBatchingStartTime = time.Now()

	// This loop is for trying best to collect more requests, so we use `tbc.maxBatchSize` here.
fetchPendingRequestsLoop:
	for tbc.collectedRequestCount < tbc.maxBatchSize {
		select {
		case tsoReq := <-tsoRequestCh:
			tbc.pushRequest(tsoReq)
		case <-ctx.Done():
			return ctx.Err()
		default:
			break fetchPendingRequestsLoop
		}
	}

	// Check whether we should fetch more pending TSO requests from the channel.
	// TODO: maybe consider the actual load that returns through a TSO response from PD server.
	if tbc.collectedRequestCount >= tbc.maxBatchSize || maxBatchWaitInterval <= 0 {
		return nil
	}

	// Fetches more pending TSO requests from the channel.
	// Try to collect `tbc.bestBatchSize` requests, or wait `maxBatchWaitInterval`
	// when `tbc.collectedRequestCount` is less than the `tbc.bestBatchSize`.
	if tbc.collectedRequestCount < tbc.bestBatchSize {
		after := time.NewTimer(maxBatchWaitInterval)
		defer after.Stop()
		for tbc.collectedRequestCount < tbc.bestBatchSize {
			select {
			case tsoReq := <-tsoRequestCh:
				tbc.pushRequest(tsoReq)
			case <-ctx.Done():
				return ctx.Err()
			case <-after.C:
				return nil
			}
		}
	}

	// Do an additional non-block try. Here we test the length with `tbc.maxBatchSize` instead
	// of `tbc.bestBatchSize` because trying best to fetch more requests is necessary so that
	// we can adjust the `tbc.bestBatchSize` dynamically later.
	for tbc.collectedRequestCount < tbc.maxBatchSize {
		select {
		case tsoReq := <-tsoRequestCh:
			tbc.pushRequest(tsoReq)
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
func (tbc *tsoBatchController) fetchRequestsWithTimer(ctx context.Context, tsoRequestCh <-chan *tsoRequest, timer *time.Timer) error {
batchingLoop:
	for tbc.collectedRequestCount < tbc.maxBatchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req := <-tsoRequestCh:
			tbc.pushRequest(req)
		case <-timer.C:
			break batchingLoop
		}
	}

	// Try to collect more requests in non-blocking way.
nonWaitingBatchLoop:
	for tbc.collectedRequestCount < tbc.maxBatchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req := <-tsoRequestCh:
			tbc.pushRequest(req)
		default:
			break nonWaitingBatchLoop
		}
	}

	return nil
}

func (tbc *tsoBatchController) pushRequest(tsoReq *tsoRequest) {
	tbc.collectedRequests[tbc.collectedRequestCount] = tsoReq
	tbc.collectedRequestCount++
}

func (tbc *tsoBatchController) getCollectedRequests() []*tsoRequest {
	return tbc.collectedRequests[:tbc.collectedRequestCount]
}

// adjustBestBatchSize stabilizes the latency with the AIAD algorithm.
func (tbc *tsoBatchController) adjustBestBatchSize() {
	tsoBestBatchSize.Observe(float64(tbc.bestBatchSize))
	length := tbc.collectedRequestCount
	if length < tbc.bestBatchSize && tbc.bestBatchSize > 1 {
		// Waits too long to collect requests, reduce the target batch size.
		tbc.bestBatchSize--
	} else if length > tbc.bestBatchSize+4 /* Hard-coded number, in order to make `tbc.bestBatchSize` stable */ &&
		tbc.bestBatchSize < tbc.maxBatchSize {
		tbc.bestBatchSize++
	}
}

func (tbc *tsoBatchController) finishCollectedRequests(physical, firstLogical int64, suffixBits uint32, streamID string, err error) {
	for i := range tbc.collectedRequestCount {
		tsoReq := tbc.collectedRequests[i]
		// Retrieve the request context before the request is done to trace without race.
		requestCtx := tsoReq.requestCtx
		tsoReq.physical, tsoReq.logical = physical, tsoutil.AddLogical(firstLogical, int64(i), suffixBits)
		tsoReq.streamID = streamID
		tsoReq.tryDone(err)
		trace.StartRegion(requestCtx, "pdclient.tsoReqDequeue").End()
	}
	// Prevent the finished requests from being processed again.
	tbc.collectedRequestCount = 0
}
