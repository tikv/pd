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
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/batch"
	"github.com/tikv/pd/client/pkg/caller"
	sd "github.com/tikv/pd/client/servicediscovery"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
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
// buffered done channel. Callers set key, prevKey, or id afterwards.
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

func TestRequestFinisherWithZeroRegionID(t *testing.T) {
	re := require.New(t)
	req := newTestRequest(context.Background())

	finisher := requestFinisher(&pdpb.QueryRegionResponse{})
	finisher(0, req, nil)

	re.NoError(<-req.done)
	re.Nil(req.region)
}

func TestBuildQueryRegionRequest(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	keyReq := newTestRequest(ctx)
	keyReq.key = []byte{}
	prevKeyReq := newTestRequest(ctx, opt.WithBuckets())
	prevKeyReq.prevKey = []byte{}
	zeroIDReq := newTestRequest(ctx)
	zeroIDReq.id = 0
	maxIDReq := newTestRequest(ctx)
	maxIDReq.id = math.MaxUint64

	queryReq := buildQueryRegionRequest(42, []*Request{
		keyReq,
		prevKeyReq,
		zeroIDReq,
		maxIDReq,
	})

	re.Equal(uint64(42), queryReq.GetHeader().GetClusterId())
	re.Len(queryReq.GetKeys(), 1)
	re.NotNil(queryReq.GetKeys()[0])
	re.Empty(queryReq.GetKeys()[0])
	re.Len(queryReq.GetPrevKeys(), 1)
	re.NotNil(queryReq.GetPrevKeys()[0])
	re.Empty(queryReq.GetPrevKeys()[0])
	re.Equal([]uint64{0, math.MaxUint64}, queryReq.GetIds())
	re.True(queryReq.GetNeedBuckets())
}

func TestProcessRequestsCallerAttribution(t *testing.T) {
	for _, failure := range []string{"none", "same-component", "send", "recv", "header"} {
		t.Run(failure, func(t *testing.T) {
			re := require.New(t)
			ctx := context.Background()
			c := &Cli{
				ctx:             ctx,
				svcDiscovery:    sd.NewMockServiceDiscovery(nil, nil),
				reqPool:         &sync.Pool{New: func() any { return &Request{done: make(chan error, 1)} }},
				batchController: batch.NewController(20, requestFinisher(nil), nil),
			}
			// Interleave components and query kinds; attribution must preserve
			// one batch and the original response mapping.
			components := []caller.Component{"b", "", "c", "", "b", "c", "b", "", "c"}
			if failure == "same-component" {
				for i := range components {
					components[i] = "a"
				}
			}
			shouldFail := failure != "none" && failure != "same-component"
			requests := make([]*Request, 0, len(components))
			requestCh := make(chan *Request, len(components))
			for i, component := range components {
				req := c.newRequest(caller.WithComponent(ctx, component))
				id := uint64(i + 1)
				switch i / 3 {
				case 0:
					req.key = []byte{byte(id)}
				case 1:
					req.prevKey = []byte{byte(id)}
				default:
					req.id = id
				}
				req.options.NeedBuckets = i%2 == 0
				requests = append(requests, req)
				requestCh <- req
			}
			re.NoError(c.batchController.FetchPendingRequests(ctx, requestCh, nil, 0))
			injected := errors.New("batch failed")
			var current *pdpb.QueryRegionRequest
			var sent []string
			send := func(req *pdpb.QueryRegionRequest) error {
				current = req
				component := req.GetHeader().GetCallerComponent()
				sent = append(sent, component)
				re.Equal(string(caller.GetCallerID()), req.GetHeader().GetCallerId())
				re.Equal(uint64(0), req.GetHeader().GetClusterId())
				ids := append([]uint64{}, req.GetIds()...)
				for _, key := range req.GetKeys() {
					ids = append(ids, uint64(key[0]))
				}
				for _, key := range req.GetPrevKeys() {
					ids = append(ids, uint64(key[0]))
				}
				re.Len(ids, len(components))
				if failure == "same-component" {
					re.Equal("a", component)
					re.Empty(req.GetKeyCallerComponents())
					re.Empty(req.GetPrevKeyCallerComponents())
					re.Empty(req.GetIdCallerComponents())
				} else {
					re.Empty(component)
					re.Equal([]string{"b", "", "c"}, req.GetKeyCallerComponents())
					re.Equal([]string{"", "b", "c"}, req.GetPrevKeyCallerComponents())
					re.Equal([]string{"b", "", "c"}, req.GetIdCallerComponents())
				}
				if failure == "send" {
					return injected
				}
				return nil
			}
			recv := func() (*pdpb.QueryRegionResponse, error) {
				switch failure {
				case "recv":
					return nil, injected
				case "header":
					return &pdpb.QueryRegionResponse{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Type: pdpb.ErrorType_NOT_BOOTSTRAPPED}}}, nil
				}
				resp := &pdpb.QueryRegionResponse{RegionsById: make(map[uint64]*pdpb.RegionResponse)}
				for _, key := range current.GetKeys() {
					resp.KeyIdMap = append(resp.KeyIdMap, uint64(key[0]))
				}
				for _, key := range current.GetPrevKeys() {
					resp.PrevKeyIdMap = append(resp.PrevKeyIdMap, uint64(key[0]))
				}
				for i := range components {
					resp.RegionsById[uint64(i+1)] = newMockRegionResponse(uint64(i + 1))
				}
				return resp, nil
			}
			// The dispatcher cancels the entire batch after Send/Recv/header errors.
			err := c.processRequestsInner(send, recv)
			re.Len(sent, 1)
			if shouldFail {
				re.Error(err)
				c.cancelCollectedRequests(err)
			} else {
				re.NoError(err)
			}
			re.Zero(c.batchController.GetCollectedRequestCount())
			c.cancelCollectedRequests(injected)
			for i, req := range requests {
				result, waitErr := req.wait()
				if shouldFail {
					re.Error(waitErr)
					re.Nil(result)
				} else {
					re.NoError(waitErr)
					re.Equal(uint64(i+1), result.Meta.GetId())
					re.Equal(i%2 == 0, result.Buckets != nil)
				}
			}
			// Pooled requests must not retain a previous caller component.
			re.Empty(c.newRequest(ctx).callerComponent)
		})
	}
}
