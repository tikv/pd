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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/routerpb"

	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/batch"
	cctx "github.com/tikv/pd/client/pkg/connectionctx"
	"github.com/tikv/pd/client/pkg/utils/testutil"
	sd "github.com/tikv/pd/client/servicediscovery"
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

type queryRegionTestStream struct {
	grpc.ClientStream
	response   *pdpb.QueryRegionResponse
	requests   []*pdpb.QueryRegionRequest
	beforeRecv func()
	sendErr    error
	recvErr    error
}

func (s *queryRegionTestStream) Send(req *pdpb.QueryRegionRequest) error {
	s.requests = append(s.requests, req)
	return s.sendErr
}

func (s *queryRegionTestStream) Recv() (*pdpb.QueryRegionResponse, error) {
	if s.beforeRecv != nil {
		s.beforeRecv()
	}
	return s.response, s.recvErr
}

const queryRegionTestLeaderURL = "leader"

func newQueryRegionTestClient(
	t *testing.T,
	requests []*Request,
) *Cli {
	t.Helper()
	controller := batch.NewController[*Request](len(requests), requestFinisher(nil), nil)
	requestCh := make(chan *Request, len(requests))
	for _, req := range requests {
		requestCh <- req
	}
	require.NoError(t, controller.FetchPendingRequests(context.Background(), requestCh, nil, 0))

	client := &Cli{
		svcDiscovery:    sd.NewMockServiceDiscovery(nil, nil),
		batchController: controller,
	}
	return client
}

func collectQueryRegionTestRequests(
	t *testing.T,
	client *Cli,
	requests ...*Request,
) {
	t.Helper()
	requestCh := make(chan *Request, len(requests))
	for _, req := range requests {
		requestCh <- req
	}
	require.NoError(t, client.batchController.FetchPendingRequests(
		context.Background(),
		requestCh,
		nil,
		0,
	))
}

func TestProcessRequestsRetriesOnlyMissingRegions(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	keyFound := newTestRequest(ctx)
	keyFound.key = []byte("key-found")
	idFound := newTestRequest(ctx)
	idFound.id = 3
	prevKeyFound := newTestRequest(ctx)
	prevKeyFound.prevKey = []byte("prev-key-found")
	keyMissing := newTestRequest(ctx)
	keyMissing.key = []byte("key-missing")
	idMissing := newTestRequest(ctx)
	idMissing.id = 4
	prevKeyMissing := newTestRequest(ctx)
	prevKeyMissing.prevKey = []byte("prev-key-missing")
	requests := []*Request{
		keyFound,
		idFound,
		prevKeyFound,
		keyMissing,
		idMissing,
		prevKeyMissing,
	}

	followerStream := &queryRegionTestStream{
		response: &pdpb.QueryRegionResponse{
			Header:       &pdpb.ResponseHeader{},
			KeyIdMap:     []uint64{1, 0},
			PrevKeyIdMap: []uint64{2, 0},
			RegionsById: map[uint64]*pdpb.RegionResponse{
				1: newMockRegionResponse(1),
				2: newMockRegionResponse(2),
				3: newMockRegionResponse(3),
				4: nil,
			},
		},
	}
	leaderStream := &queryRegionTestStream{
		response: &pdpb.QueryRegionResponse{
			Header:       &pdpb.ResponseHeader{},
			KeyIdMap:     []uint64{5},
			PrevKeyIdMap: []uint64{6},
			RegionsById: map[uint64]*pdpb.RegionResponse{
				4: newMockRegionResponse(4),
				5: newMockRegionResponse(5),
				6: newMockRegionResponse(6),
			},
		},
		beforeRecv: func() {
			re.Len(keyFound.done, 1)
			re.Len(idFound.done, 1)
			re.Len(prevKeyFound.done, 1)
			re.Empty(keyMissing.done)
			re.Empty(idMissing.done)
			re.Empty(prevKeyMissing.done)
		},
	}
	client := newQueryRegionTestClient(t, requests)

	retryRequests, err := client.processRequestsInner(
		followerStream.Send,
		followerStream.Recv,
		true,
		false,
	)
	re.NoError(err)

	re.Len(followerStream.requests, 1)
	re.Len(followerStream.requests[0].GetKeys(), 2)
	re.Len(followerStream.requests[0].GetPrevKeys(), 2)
	re.Equal([]uint64{3, 4}, followerStream.requests[0].GetIds())
	re.Equal([]*Request{keyMissing, idMissing, prevKeyMissing}, retryRequests)
	re.Empty(leaderStream.requests)

	collectQueryRegionTestRequests(t, client, retryRequests...)
	retryRequests, err = client.processRequestsInner(
		leaderStream.Send,
		leaderStream.Recv,
		false,
		true,
	)
	re.NoError(err)
	re.Empty(retryRequests)
	re.Len(leaderStream.requests, 1)
	re.Equal([][]byte{keyMissing.key}, leaderStream.requests[0].GetKeys())
	re.Equal([][]byte{prevKeyMissing.prevKey}, leaderStream.requests[0].GetPrevKeys())
	re.Equal([]uint64{4}, leaderStream.requests[0].GetIds())

	for req, expectedID := range map[*Request]uint64{
		keyFound:       1,
		idFound:        3,
		prevKeyFound:   2,
		keyMissing:     5,
		idMissing:      4,
		prevKeyMissing: 6,
	} {
		re.NoError(<-req.done)
		re.Equal(expectedID, req.region.Meta.GetId())
	}
}

func TestProcessRequestsRetriesInvalidResponseOnLeader(t *testing.T) {
	testCases := []struct {
		name       string
		headerErr  *pdpb.Error
		isFollower bool
	}{
		{
			name:      "region not found",
			headerErr: &pdpb.Error{Type: pdpb.ErrorType_REGION_NOT_FOUND},
		},
		{
			name:       "other follower error",
			headerErr:  &pdpb.Error{Type: pdpb.ErrorType_NOT_BOOTSTRAPPED},
			isFollower: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			req := newTestRequest(context.Background())
			req.id = 1
			followerStream := &queryRegionTestStream{
				response: &pdpb.QueryRegionResponse{
					Header: &pdpb.ResponseHeader{Error: testCase.headerErr},
				},
			}
			leaderStream := &queryRegionTestStream{
				response: &pdpb.QueryRegionResponse{
					Header: &pdpb.ResponseHeader{},
					RegionsById: map[uint64]*pdpb.RegionResponse{
						1: newMockRegionResponse(1),
					},
				},
			}
			client := newQueryRegionTestClient(t, []*Request{req})

			retryRequests, err := client.processRequestsInner(
				followerStream.Send,
				followerStream.Recv,
				testCase.isFollower,
				false,
			)
			re.NoError(err)
			re.Equal([]*Request{req}, retryRequests)
			re.Empty(leaderStream.requests)

			collectQueryRegionTestRequests(t, client, retryRequests...)
			retryRequests, err = client.processRequestsInner(
				leaderStream.Send,
				leaderStream.Recv,
				false,
				true,
			)
			re.NoError(err)
			re.Empty(retryRequests)
			re.Len(leaderStream.requests, 1)
			re.Equal([]uint64{1}, leaderStream.requests[0].GetIds())
			re.NoError(<-req.done)
			re.Equal(uint64(1), req.region.Meta.GetId())
		})
	}
}

func TestLeaderRetryBatchDoesNotRetryAgain(t *testing.T) {
	re := require.New(t)
	req := newTestRequest(context.Background())
	req.id = 1
	leaderStream := &queryRegionTestStream{
		response: &pdpb.QueryRegionResponse{Header: &pdpb.ResponseHeader{}},
	}
	client := newQueryRegionTestClient(t, []*Request{req})

	retryRequests, err := client.processRequestsInner(
		leaderStream.Send,
		leaderStream.Recv,
		false,
		true,
	)
	re.NoError(err)
	re.Empty(retryRequests)
	re.NoError(<-req.done)
	re.Nil(req.region)
	re.Len(leaderStream.requests, 1)
}

func TestDispatcherIsolatesLeaderRetryBatch(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())

	followerHit := newTestRequest(ctx, opt.WithAllowFollowerHandle())
	followerHit.id = 1
	followerMiss := newTestRequest(ctx, opt.WithAllowFollowerHandle())
	followerMiss.id = 2
	routerRequest := newTestRequest(ctx, opt.WithAllowRouterServiceHandle())
	routerRequest.id = 3

	leaderStreamErr := errors.New("leader stream failed")
	leaderStream := &queryRegionTestStream{
		sendErr: leaderStreamErr,
	}
	routerStream := &queryRegionTestStream{
		response: &pdpb.QueryRegionResponse{
			Header: &pdpb.ResponseHeader{},
			RegionsById: map[uint64]*pdpb.RegionResponse{
				3: newMockRegionResponse(3),
			},
		},
	}
	option := opt.NewOption()
	option.SetEnableFollowerHandle(true)
	client := &Cli{
		ctx:             ctx,
		cancel:          cancel,
		option:          option,
		svcDiscovery:    sd.NewMockServiceDiscovery(nil, nil),
		conCtxMgr:       cctx.NewManager[pdpb.PD_QueryRegionClient](),
		msConCtxMgr:     cctx.NewManager[routerpb.Router_QueryRegionClient](),
		requestCh:       make(chan *Request, 3),
		batchController: batch.NewController[*Request](3, requestFinisher(nil), nil),
	}
	client.leaderURL.Store(queryRegionTestLeaderURL)

	followerStream := &queryRegionTestStream{
		response: &pdpb.QueryRegionResponse{
			Header: &pdpb.ResponseHeader{},
			RegionsById: map[uint64]*pdpb.RegionResponse{
				1: newMockRegionResponse(1),
				2: nil,
			},
		},
		beforeRecv: func() {
			leaderCtx, leaderCancel := context.WithCancel(ctx)
			if !client.conCtxMgr.CleanAllAndStore(
				leaderCtx,
				leaderCancel,
				queryRegionTestLeaderURL,
				leaderStream,
			) {
				leaderCancel()
			}
			routerCtx, routerCancel := context.WithCancel(ctx)
			if !client.msConCtxMgr.Store(
				routerCtx,
				routerCancel,
				"router-service",
				routerStream,
			) {
				routerCancel()
			}
			// Queue a fresh Router Service request while the follower batch is in flight.
			client.requestCh <- routerRequest
		},
	}
	followerCtx, followerCancel := context.WithCancel(ctx)
	re.True(client.conCtxMgr.Store(
		followerCtx,
		followerCancel,
		"follower",
		followerStream,
	))

	client.requestCh <- followerHit
	client.requestCh <- followerMiss
	client.wg.Add(1)
	go client.dispatcher()
	t.Cleanup(func() {
		cancel()
		client.wg.Wait()
		client.conCtxMgr.ReleaseAll()
		client.msConCtxMgr.ReleaseAll()
	})

	waitRequest := func(name string, req *Request) error {
		t.Helper()
		select {
		case err := <-req.done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s", name)
			return nil
		}
	}
	re.NoError(waitRequest("follower hit", followerHit))
	re.Equal(uint64(1), followerHit.region.Meta.GetId())
	re.ErrorIs(waitRequest("leader retry", followerMiss), leaderStreamErr)
	re.Nil(followerMiss.region)
	re.NoError(waitRequest("queued router request", routerRequest))
	re.Equal(uint64(3), routerRequest.region.Meta.GetId())
	cancel()
	client.wg.Wait()

	re.Len(followerStream.requests, 1)
	re.Equal([]uint64{1, 2}, followerStream.requests[0].GetIds())
	re.Len(leaderStream.requests, 1)
	re.Equal([]uint64{2}, leaderStream.requests[0].GetIds())
	re.Len(routerStream.requests, 1)
	re.Equal([]uint64{3}, routerStream.requests[0].GetIds())
}
