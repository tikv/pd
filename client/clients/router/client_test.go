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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

func newMockRegionResponse(id uint64) *pdpb.RegionResponse {
	return &pdpb.RegionResponse{
		Region:  &metapb.Region{Id: id, StartKey: make([]byte, 1)},
		Leader:  &metapb.Peer{Id: id},
		Buckets: &metapb.Buckets{},
	}
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
		req := &Request{
			requestCtx: ctx,
			key:        []byte("dummy-key"),
			done:       make(chan error, 1),
		}
		requests = append(requests, req)
	}

	// Requests that use `prevKey`.
	for range 2 {
		req := &Request{
			requestCtx: ctx,
			prevKey:    []byte("dummy-prev-key"),
			done:       make(chan error, 1),
		}
		requests = append(requests, req)
	}

	// Requests that use `id`.
	for _, id := range []uint64{1, 2} {
		req := &Request{
			requestCtx: ctx,
			id:         id,
			done:       make(chan error, 1),
		}
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
