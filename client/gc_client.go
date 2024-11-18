// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pd

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"go.uber.org/zap"
)

// GCClient is a client for doing GC
type GCClient interface {
	UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error)
	UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error)
	WatchGCSafePointV2(ctx context.Context, revision int64) (chan []*pdpb.SafePointEvent, error)
}

// UpdateGCSafePointV2 update gc safe point for the given keyspace.
func (c *client) UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.UpdateGCSafePointV2", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateGCSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.Timeout)
	req := &pdpb.UpdateGCSafePointV2Request{
		Header:     c.requestHeader(),
		KeyspaceId: keyspaceID,
		SafePoint:  safePoint,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return 0, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.UpdateGCSafePointV2(ctx, req)
	cancel()

	if err = c.respForErr(cmdFailedDurationUpdateGCSafePointV2, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetNewSafePoint(), nil
}

// UpdateServiceSafePointV2 update service safe point for the given keyspace.
func (c *client) UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.UpdateServiceSafePointV2", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateServiceSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.Timeout)
	req := &pdpb.UpdateServiceSafePointV2Request{
		Header:     c.requestHeader(),
		KeyspaceId: keyspaceID,
		ServiceId:  []byte(serviceID),
		SafePoint:  safePoint,
		Ttl:        ttl,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return 0, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.UpdateServiceSafePointV2(ctx, req)
	cancel()
	if err = c.respForErr(cmdFailedDurationUpdateServiceSafePointV2, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetMinSafePoint(), nil
}

// WatchGCSafePointV2 watch gc safe point change.
func (c *client) WatchGCSafePointV2(ctx context.Context, revision int64) (chan []*pdpb.SafePointEvent, error) {
	SafePointEventsChan := make(chan []*pdpb.SafePointEvent)
	req := &pdpb.WatchGCSafePointV2Request{
		Header:   c.requestHeader(),
		Revision: revision,
	}

	ctx, cancel := context.WithTimeout(ctx, c.option.Timeout)
	defer cancel()
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	stream, err := protoClient.WatchGCSafePointV2(ctx, req)
	if err != nil {
		close(SafePointEventsChan)
		return nil, err
	}
	go func() {
		defer func() {
			close(SafePointEventsChan)
			if r := recover(); r != nil {
				log.Error("[pd] panic in gc client `WatchGCSafePointV2`", zap.Any("error", r))
				return
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				resp, err := stream.Recv()
				if err != nil {
					log.Error("watch gc safe point v2 error", errs.ZapError(errs.ErrClientWatchGCSafePointV2Stream, err))
					return
				}
				SafePointEventsChan <- resp.GetEvents()
			}
		}
	}()
	return SafePointEventsChan, err
}
