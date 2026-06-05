// Copyright 2024 TiKV Project Authors.
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

package batch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdjustBestBatchSize(t *testing.T) {
	re := require.New(t)
	bc := NewController[int](20, nil, nil)
	re.Equal(defaultBestBatchSize, bc.bestBatchSize)
	bc.AdjustBestBatchSize()
	re.Equal(defaultBestBatchSize-1, bc.bestBatchSize)
	// Clear the collected requests.
	bc.FinishCollectedRequests(nil, nil)
	// Push 10 requests - do not increase the best batch size.
	for i := range 10 {
		bc.pushRequest(i)
	}
	bc.AdjustBestBatchSize()
	re.Equal(defaultBestBatchSize-1, bc.bestBatchSize)
	bc.FinishCollectedRequests(nil, nil)
	// Push 15 requests, increase the best batch size.
	for i := range 15 {
		bc.pushRequest(i)
	}
	bc.AdjustBestBatchSize()
	re.Equal(defaultBestBatchSize, bc.bestBatchSize)
	bc.FinishCollectedRequests(nil, nil)
}

type testRequest struct {
	idx int
	err error
}

func TestFinishCollectedRequests(t *testing.T) {
	re := require.New(t)
	bc := NewController[*testRequest](20, nil, nil)
	// Finish with zero request count.
	re.Zero(bc.collectedRequestCount)
	bc.FinishCollectedRequests(nil, nil)
	re.Zero(bc.collectedRequestCount)
	// Finish with non-zero request count.
	requests := make([]*testRequest, 10)
	for i := range 10 {
		requests[i] = &testRequest{}
		bc.pushRequest(requests[i])
	}
	re.Equal(10, bc.collectedRequestCount)
	bc.FinishCollectedRequests(nil, nil)
	re.Zero(bc.collectedRequestCount)
	// Finish with custom finisher.
	for i := range 10 {
		requests[i] = &testRequest{}
		bc.pushRequest(requests[i])
	}
	bc.FinishCollectedRequests(func(idx int, tr *testRequest, err error) {
		tr.idx = idx
		tr.err = err
	}, context.Canceled)
	re.Zero(bc.collectedRequestCount)
	for i := range 10 {
		re.Equal(i, requests[i].idx)
		re.Equal(context.Canceled, requests[i].err)
	}
}
