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

package pd

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/pkg/batch"
)

func newMockQueryRegionResponse(id uint64) *pdpb.RegionResponse {
	return &pdpb.RegionResponse{
		Region:  &metapb.Region{Id: id, StartKey: []byte{byte(id)}},
		Leader:  &metapb.Peer{Id: id},
		Buckets: &metapb.Buckets{},
	}
}

func TestRegionRequestFinisherMapsMixedBatchAndClonesRegions(t *testing.T) {
	re := require.New(t)
	resp := &pdpb.QueryRegionResponse{
		KeyIdMap:     []uint64{101, 102},
		PrevKeyIdMap: []uint64{201},
		RegionsById: map[uint64]*pdpb.RegionResponse{
			101: newMockQueryRegionResponse(101),
			102: newMockQueryRegionResponse(102),
			201: newMockQueryRegionResponse(201),
			301: newMockQueryRegionResponse(301),
		},
	}
	requests := []*regionRequest{
		{requestCtx: context.Background(), key: []byte("k1"), done: make(chan error, 1)},
		{requestCtx: context.Background(), prevKey: []byte("pk"), done: make(chan error, 1)},
		{requestCtx: context.Background(), id: 301, done: make(chan error, 1)},
		{requestCtx: context.Background(), key: []byte("k2"), done: make(chan error, 1)},
	}

	finisher := regionRequestFinisher(resp)
	for i, req := range requests {
		finisher(i, req, nil)
		re.NoError(<-req.done)
	}
	re.Equal(uint64(101), requests[0].region.Meta.GetId())
	re.Equal(uint64(201), requests[1].region.Meta.GetId())
	re.Equal(uint64(301), requests[2].region.Meta.GetId())
	re.Equal(uint64(102), requests[3].region.Meta.GetId())

	requests[0].region.Meta.StartKey[0] = 0
	re.Equal([]byte{byte(101)}, resp.RegionsById[101].Region.StartKey)
}

func TestRegionRequestFinisherNoDataRace(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	resp := &pdpb.QueryRegionResponse{
		KeyIdMap:     []uint64{1, 2},
		PrevKeyIdMap: []uint64{1, 2},
		RegionsById: map[uint64]*pdpb.RegionResponse{
			1: newMockQueryRegionResponse(1),
			2: newMockQueryRegionResponse(2),
		},
	}
	requests := []*regionRequest{
		{requestCtx: ctx, key: []byte("dummy-key"), done: make(chan error, 1)},
		{requestCtx: ctx, key: []byte("dummy-key"), done: make(chan error, 1)},
		{requestCtx: ctx, prevKey: []byte("dummy-prev-key"), done: make(chan error, 1)},
		{requestCtx: ctx, prevKey: []byte("dummy-prev-key"), done: make(chan error, 1)},
		{requestCtx: ctx, id: 1, done: make(chan error, 1)},
		{requestCtx: ctx, id: 2, done: make(chan error, 1)},
	}

	finisher := regionRequestFinisher(resp)
	for idx, req := range requests {
		finisher(idx, req, nil)
		re.NoError(<-req.done)
		req.region.Meta.StartKey[0] += byte(idx + 1)
	}
	for idx, req := range requests {
		re.Equal([]byte{byte(req.region.Meta.GetId()) + byte(idx+1)}, req.region.Meta.StartKey)
	}
}

func TestRegionRequestFinisherReturnsTimeoutWhenResponseIsNil(t *testing.T) {
	re := require.New(t)
	req := &regionRequest{
		requestCtx: context.Background(),
		key:        []byte("k"),
		done:       make(chan error, 1),
	}

	regionRequestFinisher(nil)(0, req, nil)
	re.ErrorIs(<-req.done, errs.ErrClientRouterConnectionTimeout)
}

func TestRegionClientProcessRequestsBuildsMixedQueryRegionRequest(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	client := &regionClient{
		svcDiscovery: &pdServiceDiscovery{clusterID: 123},
		batchController: batch.NewController(
			defaultMaxRegionRequestBatchSize,
			regionRequestFinisher(nil),
			metrics.QueryRegionBestBatchSize,
		),
	}
	requestCh := make(chan *regionRequest, 3)
	requests := []*regionRequest{
		{requestCtx: ctx, key: []byte("k"), options: &GetRegionOp{needBuckets: true}, done: make(chan error, 1)},
		{requestCtx: ctx, prevKey: []byte("pk"), options: &GetRegionOp{}, done: make(chan error, 1)},
		{requestCtx: ctx, id: 301, options: &GetRegionOp{}, done: make(chan error, 1)},
	}
	for _, req := range requests {
		requestCh <- req
	}
	re.NoError(client.batchController.FetchPendingRequests(ctx, requestCh, nil, 0))

	var gotReq *pdpb.QueryRegionRequest
	err := client.processRequestsInner(
		func(req *pdpb.QueryRegionRequest) error {
			gotReq = req
			return nil
		},
		func() (*pdpb.QueryRegionResponse, error) {
			return &pdpb.QueryRegionResponse{
				Header:       &pdpb.ResponseHeader{},
				KeyIdMap:     []uint64{101},
				PrevKeyIdMap: []uint64{201},
				RegionsById: map[uint64]*pdpb.RegionResponse{
					101: newMockQueryRegionResponse(101),
					201: newMockQueryRegionResponse(201),
					301: newMockQueryRegionResponse(301),
				},
			}, nil
		},
	)
	re.NoError(err)
	re.Equal(uint64(123), gotReq.GetHeader().GetClusterId())
	re.True(gotReq.GetNeedBuckets())
	re.Equal([][]byte{[]byte("k")}, gotReq.GetKeys())
	re.Equal([][]byte{[]byte("pk")}, gotReq.GetPrevKeys())
	re.Equal([]uint64{301}, gotReq.GetIds())
	re.NoError(<-requests[0].done)
	re.NoError(<-requests[1].done)
	re.NoError(<-requests[2].done)
	re.Equal(uint64(101), requests[0].region.Meta.GetId())
	re.Equal(uint64(201), requests[1].region.Meta.GetId())
	re.Equal(uint64(301), requests[2].region.Meta.GetId())
}

func TestGetRegionFallsBackToUnaryWhenQueryRegionStreamUnavailable(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	option := newOption()
	option.timeout = time.Millisecond
	option.setEnableQueryRegion(true)
	client := &client{
		ctx:            ctx,
		cancel:         cancel,
		option:         option,
		pdSvcDiscovery: &pdServiceDiscovery{option: option, checkMembershipCh: make(chan struct{}, 1)},
		serviceModeKeeper: serviceModeKeeper{
			serviceMode: pdpb.ServiceMode_UNKNOWN_SVC_MODE,
		},
	}
	defer client.Close()

	_, err := client.GetRegion(context.Background(), []byte("k"))
	re.ErrorIs(errors.Cause(err), errs.ErrClientGetProtoClient)
}
