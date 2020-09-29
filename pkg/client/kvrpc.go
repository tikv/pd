// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/retry"
	"github.com/tikv/client-go/rpc"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RegionRequestSender struct {
	client   rpc.Client
	rpcError error
}

// NewRegionRequestSender creates a new sender without regionCache.
func NewRegionRequestSender(conf *config.RPC) *RegionRequestSender {
	return &RegionRequestSender{
		client: rpc.NewRPCClient(conf),
	}
}

// RegionRequest wrap the request to the region.
type RegionRequest struct {
	bo          *retry.Backoffer
	req         *rpc.Request
	region      *core.RegionInfo
	leaderStore *core.StoreInfo
	timeout     time.Duration
}

// NewRegionRequest create a new RegionRequest
func NewRegionRequest(bo *retry.Backoffer,
	req *rpc.Request, region *core.RegionInfo,
	leaderStore *core.StoreInfo,
	timeout time.Duration) *RegionRequest {
	return &RegionRequest{
		bo:          bo,
		req:         req,
		region:      region,
		leaderStore: leaderStore,
		timeout:     timeout,
	}
}

// SendReq sends a request to tikv server.
func (s *RegionRequestSender) SendReq(regionRequest *RegionRequest) (*rpc.Response, error) {
	for {
		resp, retry, err := s.sendReqToRegion(regionRequest)
		if err != nil {
			return nil, err
		}
		if retry {
			continue
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, err
		}
		// If the error is the regionErr, the error should be directly returned.
		if regionErr != nil {
			return nil, errors.Errorf("regionErr, err: %v", regionErr.Message)
		}
		return resp, nil
	}
}

func (s *RegionRequestSender) sendReqToRegion(request *RegionRequest) (resp *rpc.Response, retry bool, err error) {
	var peer *metapb.Peer
	for _, p := range request.region.GetPeers() {
		if p.StoreId == request.leaderStore.GetID() {
			peer = p
		}
	}
	if e := rpc.SetContext(request.req, request.region.GetMeta(), peer); e != nil {
		return nil, false, err
	}
	resp, err = s.client.SendRequest(request.bo.GetContext(), request.leaderStore.GetAddress(), request.req, request.timeout)
	if err != nil {
		s.rpcError = err
		if e := s.onSendFail(request.bo, err); e != nil {
			return nil, false, err
		}
		return nil, true, nil
	}
	return
}

func (s *RegionRequestSender) onSendFail(bo *retry.Backoffer, err error) error {
	// If it failed because the context is cancelled by ourself, don't retry.
	if errors.Cause(err) == context.Canceled {
		return err
	}
	code := codes.Unknown
	if s, ok := status.FromError(errors.Cause(err)); ok {
		code = s.Code()
	}
	if code == codes.Canceled {
		select {
		case <-bo.GetContext().Done():
			return err
		default:
			// If we don't cancel, but the error code is Canceled, it must be from grpc remote.
			// This may happen when tikv is killed and exiting.
			// Backoff and retry in this case.
			log.Warn("receive a grpc cancel signal from remote:", zap.Error(err))
		}
	}
	// Retry on send request failure when it's not canceled.
	// When a store is not available, the leader of related region should be elected quickly.
	// TODO: the number of retry time should be limited:since region may be unavailable
	// when some unrecoverable disaster happened.
	return bo.Backoff(retry.BoTiKVRPC, errors.Errorf("send tikv request error: %v", err))
}
