// Copyright 2025 TiKV Project Authors.
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

package router

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func newMockRegionResponse(id uint64) *pdpb.RegionResponse {
	return &pdpb.RegionResponse{
		Region:  &metapb.Region{Id: id, StartKey: make([]byte, 1)},
		Leader:  &metapb.Peer{Id: id},
		Buckets: &metapb.Buckets{},
	}
}

// newTestRequest builds a *Request directly for finisher tests, mirroring the
// invariants that the production newRequest guarantees: a non-nil options and a
// buffered done channel. Callers set key/prevKey/id afterwards.
func newTestRequest(ctx context.Context, opts ...opt.GetRegionOption) *Request {
	req := &Request{
		requestCtx: ctx,
		options:    &opt.GetRegionOp{},
		done:       make(chan error, 1),
	}
	for _, o := range opts {
		o(req.options)
	}
	return req
}

func TestRequestFinisherNoDataRace(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	// Create a mock QueryRegionResponse.
	resp := &pdpb.QueryRegionResponse{
		KeyIdMap:     []uint64{1, 2},
		PrevKeyIdMap: []uint64{1, 2},
		RegionsById: map[uint64]*pdpb.RegionResponse{
			1: newMockRegionResponse(1),
			2: newMockRegionResponse(2),
		},
	}

	// Build a batch of mock requests:
	// • Two requests with key set (will use KeyIdMap).
	// • Two requests with prevKey set (will use PrevKeyIdMap).
	// • Two requests with neither key nor prevKey (so the id branch is used).
	var requests []*Request

	// Requests that use `key`.
	for range 2 {
		req := newTestRequest(ctx)
		req.key = []byte("dummy-key")
		requests = append(requests, req)
	}

	// Requests that use `prevKey`.
	for range 2 {
		req := newTestRequest(ctx)
		req.prevKey = []byte("dummy-prev-key")
		requests = append(requests, req)
	}

	// Requests that use `id`.
	for _, id := range []uint64{1, 2} {
		req := newTestRequest(ctx)
		req.id = id
		requests = append(requests, req)
	}

	// Get the finisher function.
	finisher := requestFinisher(resp)

	// Simulate finishing the batch – call the finisher for each request.
	for idx, req := range requests {
		finisher(idx, req, nil)
		re.NoError(<-req.done)
		// Modify the region key range in place.
		req.region.Meta.StartKey[0] += byte(idx + 1)
	}

	// Verify that each request got the correct cloned region.
	for idx, req := range requests {
		re.Equal([]byte{byte(idx + 1)}, req.region.Meta.StartKey)
	}
}

// TestRequestFinisherClearsUnrequestedBuckets verifies that buckets are only
// returned to requests that actually asked for them. `NeedBuckets` is a
// batch-wide flag in the QueryRegion request, so when any request in a batch
// sets it, the response carries buckets for every region in the batch. The
// finisher must drop those buckets for the requests that did not ask, matching
// the per-request semantics of the unary GetRegion path.
func TestRequestFinisherClearsUnrequestedBuckets(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()

	// The response carries buckets for every region, simulating a batch where
	// at least one request set NeedBuckets.
	resp := &pdpb.QueryRegionResponse{
		RegionsById: map[uint64]*pdpb.RegionResponse{
			1: newMockRegionResponse(1),
			2: newMockRegionResponse(2),
		},
	}

	reqWithBuckets := newTestRequest(ctx, opt.WithBuckets())
	reqWithBuckets.id = 1
	reqWithoutBuckets := newTestRequest(ctx)
	reqWithoutBuckets.id = 2

	finisher := requestFinisher(resp)
	finisher(0, reqWithBuckets, nil)
	re.NoError(<-reqWithBuckets.done)
	finisher(1, reqWithoutBuckets, nil)
	re.NoError(<-reqWithoutBuckets.done)

	// The request that asked for buckets keeps them.
	re.NotNil(reqWithBuckets.region.Buckets)
	// The request that did not ask for buckets must not receive them.
	re.Nil(reqWithoutBuckets.region.Buckets)
}
