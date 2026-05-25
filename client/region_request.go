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
	"runtime/trace"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/pkg/batch"
)

type regionRequest struct {
	requestCtx context.Context
	clientCtx  context.Context

	key     []byte
	prevKey []byte
	id      uint64

	options *GetRegionOp

	done   chan error
	region *Region

	start time.Time
	pool  *sync.Pool
}

func (req *regionRequest) tryDone(err error) {
	select {
	case req.done <- err:
	default:
	}
}

func (req *regionRequest) wait() (*Region, error) {
	start := time.Now()
	metrics.CmdDurationQueryRegionAsyncWait.Observe(start.Sub(req.start).Seconds())
	select {
	case err := <-req.done:
		defer req.pool.Put(req)
		defer trace.StartRegion(req.requestCtx, "pdclient.regionReqDone").End()
		now := time.Now()
		if err != nil {
			metrics.CmdFailedDurationQueryRegionWait.Observe(now.Sub(start).Seconds())
			metrics.CmdFailedDurationQueryRegion.Observe(now.Sub(req.start).Seconds())
			return nil, errors.WithStack(err)
		}
		metrics.CmdDurationQueryRegionWait.Observe(now.Sub(start).Seconds())
		metrics.CmdDurationQueryRegion.Observe(now.Sub(req.start).Seconds())
		return req.region, nil
	case <-req.requestCtx.Done():
		return nil, errors.WithStack(req.requestCtx.Err())
	case <-req.clientCtx.Done():
		return nil, errors.WithStack(req.clientCtx.Err())
	}
}

func convertToRegionCopy(res *pdpb.RegionResponse) *Region {
	if res.GetRegion() == nil {
		return nil
	}

	r := &Region{
		Meta:         proto.Clone(res.GetRegion()).(*metapb.Region),
		Leader:       proto.Clone(res.GetLeader()).(*metapb.Peer),
		PendingPeers: make([]*metapb.Peer, 0, len(res.GetPendingPeers())),
		Buckets:      proto.Clone(res.GetBuckets()).(*metapb.Buckets),
	}
	for _, peer := range res.GetPendingPeers() {
		r.PendingPeers = append(r.PendingPeers, proto.Clone(peer).(*metapb.Peer))
	}
	r.DownPeers = make([]*metapb.Peer, 0, len(res.GetDownPeers()))
	for _, peerStats := range res.GetDownPeers() {
		r.DownPeers = append(r.DownPeers, proto.Clone(peerStats.GetPeer()).(*metapb.Peer))
	}
	return r
}

func regionRequestFinisher(resp *pdpb.QueryRegionResponse) batch.FinisherFunc[*regionRequest] {
	var keyIdx, prevKeyIdx int
	return func(_ int, req *regionRequest, err error) {
		requestCtx := req.requestCtx
		defer trace.StartRegion(requestCtx, "pdclient.regionReqDone").End()

		if err != nil {
			req.tryDone(err)
			return
		}

		if resp == nil {
			req.tryDone(errs.ErrClientRouterConnectionTimeout)
			return
		}

		var id uint64
		if req.key != nil {
			id = resp.KeyIdMap[keyIdx]
			keyIdx++
		} else if req.prevKey != nil {
			id = resp.PrevKeyIdMap[prevKeyIdx]
			prevKeyIdx++
		} else if req.id != 0 {
			id = req.id
		}
		if regionResp, ok := resp.RegionsById[id]; ok {
			req.region = convertToRegionCopy(regionResp)
		}
		req.tryDone(nil)
	}
}
