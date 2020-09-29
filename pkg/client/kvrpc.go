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
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rpc"
	"github.com/tikv/pd/server/core"
)

// RegionRequestSender send the region request to Store.
type RegionRequestSender struct {
	client rpc.Client
}

// NewRegionRequestSender creates a new RegionRequestSender.
func NewRegionRequestSender(conf *config.RPC) *RegionRequestSender {
	return &RegionRequestSender{
		client: rpc.NewRPCClient(conf),
	}
}

// RegionRequest wrap the request to the region.
type RegionRequest struct {
	req         *rpc.Request
	region      *core.RegionInfo
	leaderStore *core.StoreInfo
	timeout     time.Duration
}

// NewRegionRequest create a new RegionRequest
func NewRegionRequest(
	req *rpc.Request,
	region *core.RegionInfo,
	leaderStore *core.StoreInfo,
	timeout time.Duration) *RegionRequest {
	return &RegionRequest{
		req:         req,
		region:      region,
		leaderStore: leaderStore,
		timeout:     timeout,
	}
}

// SendReq sends a request to tikv server.
func (s *RegionRequestSender) SendReq(ctx context.Context, regionRequest *RegionRequest) (*rpc.Response, error) {
	resp, err := s.sendReqToRegion(ctx, regionRequest)
	if err != nil {
		return nil, err
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return nil, err
	}
	// If the error is the regionErr, the error should be directly returned.
	if regionErr != nil {
		return resp, errors.Errorf("regionErr, err: %v", regionErr.Message)
	}
	return resp, nil
}

func (s *RegionRequestSender) sendReqToRegion(ctx context.Context, request *RegionRequest) (resp *rpc.Response, err error) {
	var peer *metapb.Peer
	for _, p := range request.region.GetPeers() {
		if p.StoreId == request.leaderStore.GetID() {
			peer = p
		}
	}
	if e := rpc.SetContext(request.req, request.region.GetMeta(), peer); e != nil {
		return nil, err
	}
	resp, err = s.client.SendRequest(ctx, request.leaderStore.GetAddress(), request.req, request.timeout)
	if err != nil {
		return nil, err
	}
	return
}
