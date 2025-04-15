// Copyright 2023 TiKV Project Authors.
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
	"math"
	"math/bits"
	"time"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
)

// UpdateGCSafePointV2 update gc safe point for the given keyspace.
// Deprecated: These v2 APIs are deprecated. Consider using GetGCStates instead.
func (c *client) UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.UpdateGCSafePointV2", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationUpdateGCSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
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

	if err = c.respForErr(metrics.CmdFailedDurationUpdateGCSafePointV2, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetNewSafePoint(), nil
}

// UpdateServiceSafePointV2 update service safe point for the given keyspace.
// Deprecated: These v2 APIs are deprecated. Consider using SetGCBarrier & DeleteGCBarrier instead.
func (c *client) UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.UpdateServiceSafePointV2", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationUpdateServiceSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
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
	if err = c.respForErr(metrics.CmdFailedDurationUpdateServiceSafePointV2, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetMinSafePoint(), nil
}

// WatchGCSafePointV2 watch gc safe point change.
// Deprecated: These v2 APIs are deprecated. Consider polling GetAllKeyspacesGCStates instead.
func (c *client) WatchGCSafePointV2(ctx context.Context, revision int64) (chan []*pdpb.SafePointEvent, error) {
	SafePointEventsChan := make(chan []*pdpb.SafePointEvent)
	req := &pdpb.WatchGCSafePointV2Request{
		Header:   c.requestHeader(),
		Revision: revision,
	}

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
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

// gcInternalController is a stateless wrapper over the client and implements gc.InternalController interface.
type gcInternalController struct {
	client     *client
	keyspaceID uint32
}

func newGCInternalController(client *client, keyspaceID uint32) *gcInternalController {
	return &gcInternalController{
		client:     client,
		keyspaceID: keyspaceID,
	}
}

func wrapKeyspaceScope(keyspaceID uint32) *pdpb.KeyspaceScope {
	return &pdpb.KeyspaceScope{
		KeyspaceId: keyspaceID,
	}
}

func (c gcInternalController) AdvanceTxnSafePoint(ctx context.Context, target uint64) (gc.AdvanceTxnSafePointResult, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.AdvanceTxnSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationAdvanceTxnSafePoint.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.client.inner.option.Timeout)
	defer cancel()
	req := &pdpb.AdvanceTxnSafePointRequest{
		Header:        c.client.requestHeader(),
		KeyspaceScope: wrapKeyspaceScope(c.keyspaceID),
		Target:        target,
	}
	protoClient, ctx := c.client.getClientAndContext(ctx)
	if protoClient == nil {
		return gc.AdvanceTxnSafePointResult{}, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.AdvanceTxnSafePoint(ctx, req)
	if err = c.client.respForErr(metrics.CmdFailedDurationAdvanceTxnSafePoint, start, err, resp.GetHeader()); err != nil {
		return gc.AdvanceTxnSafePointResult{}, err
	}
	return gc.AdvanceTxnSafePointResult{
		OldTxnSafePoint:    resp.GetOldTxnSafePoint(),
		Target:             target,
		NewTxnSafePoint:    resp.GetNewTxnSafePoint(),
		BlockerDescription: resp.GetBlockerDescription(),
	}, nil
}

func (c gcInternalController) AdvanceGCSafePoint(ctx context.Context, target uint64) (gc.AdvanceGCSafePointResult, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.AdvanceGCSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationAdvanceGCSafePoint.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.client.inner.option.Timeout)
	defer cancel()
	req := &pdpb.AdvanceGCSafePointRequest{
		Header:        c.client.requestHeader(),
		KeyspaceScope: wrapKeyspaceScope(c.keyspaceID),
		Target:        target,
	}
	protoClient, ctx := c.client.getClientAndContext(ctx)
	if protoClient == nil {
		return gc.AdvanceGCSafePointResult{}, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.AdvanceGCSafePoint(ctx, req)
	if err = c.client.respForErr(metrics.CmdFailedDurationAdvanceGCSafePoint, start, err, resp.GetHeader()); err != nil {
		return gc.AdvanceGCSafePointResult{}, err
	}
	return gc.AdvanceGCSafePointResult{
		OldGCSafePoint: resp.GetOldGcSafePoint(),
		Target:         target,
		NewGCSafePoint: resp.GetNewGcSafePoint(),
	}, nil
}

// gcStatesClient is a stateless wrapper over the client and implements gc.GCStatesClient interface.
type gcStatesClient struct {
	client     *client
	keyspaceID uint32
}

func newGCStatesClient(client *client, keyspaceID uint32) *gcStatesClient {
	return &gcStatesClient{
		client:     client,
		keyspaceID: keyspaceID,
	}
}

func roundUpDurationToSeconds(d time.Duration) int64 {
	if d == time.Duration(math.MaxInt64) {
		return math.MaxInt64
	}
	var result = int64(d / time.Second)
	if d%time.Second != 0 {
		result++
	}
	return result
}

// saturatingStdDurationFromSeconds returns a time.Duration representing the given seconds, truncated within the range
// [0, math.MaxInt64] to avoid overflowing that may happen on plain multiplication.
func saturatingStdDurationFromSeconds(seconds int64) time.Duration {
	if seconds < 0 {
		return 0
	}
	h, l := bits.Mul64(uint64(seconds), uint64(time.Second))
	if h != 0 || l > uint64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(l)
}

func pbToGCBarrierInfo(pb *pdpb.GCBarrierInfo, reqStartTime time.Time) *gc.GCBarrierInfo {
	if pb == nil {
		return nil
	}
	ttl := saturatingStdDurationFromSeconds(pb.GetTtlSeconds())
	return gc.NewGCBarrierInfo(
		pb.GetBarrierId(),
		pb.GetBarrierTs(),
		ttl,
		reqStartTime,
	)
}

func (c gcStatesClient) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*gc.GCBarrierInfo, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.SetGCBarrier", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationSetGCBarrier.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.client.inner.option.Timeout)
	defer cancel()
	req := &pdpb.SetGCBarrierRequest{
		Header:        c.client.requestHeader(),
		KeyspaceScope: wrapKeyspaceScope(c.keyspaceID),
		BarrierId:     barrierID,
		BarrierTs:     barrierTS,
		TtlSeconds:    0,
	}
	protoClient, ctx := c.client.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.SetGCBarrier(ctx, req)
	if err = c.client.respForErr(metrics.CmdFailedDurationSetGCBarrier, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return pbToGCBarrierInfo(resp.GetNewBarrierInfo(), start), nil
}

func (c gcStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*gc.GCBarrierInfo, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.DeleteGCBarrier", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationDeleteGCBarrier.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.client.inner.option.Timeout)
	defer cancel()
	req := &pdpb.DeleteGCBarrierRequest{
		Header:        c.client.requestHeader(),
		KeyspaceScope: wrapKeyspaceScope(c.keyspaceID),
		BarrierId:     barrierID,
	}
	protoClient, ctx := c.client.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.DeleteGCBarrier(ctx, req)
	if err = c.client.respForErr(metrics.CmdFailedDurationDeleteGCBarrier, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return pbToGCBarrierInfo(resp.GetDeletedBarrierInfo(), start), nil
}

func (c gcStatesClient) GetGCState(ctx context.Context) (gc.GCStateInfo, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.GetGCState", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationGetGCState.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.client.inner.option.Timeout)
	defer cancel()
	req := &pdpb.GetGCStateRequest{
		Header:        c.client.requestHeader(),
		KeyspaceScope: wrapKeyspaceScope(c.keyspaceID),
	}
	protoClient, ctx := c.client.getClientAndContext(ctx)
	if protoClient == nil {
		return gc.GCStateInfo{}, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetGCState(ctx, req)
	if err = c.client.respForErr(metrics.CmdFailedDurationGetGCState, start, err, resp.GetHeader()); err != nil {
		return gc.GCStateInfo{}, err
	}

	gcState := resp.GetGcState()
	keyspaceID := constants.NullKeyspaceID
	if gcState.KeyspaceScope != nil {
		keyspaceID = gcState.KeyspaceScope.KeyspaceId
	}
	gcBarriers := make([]*gc.GCBarrierInfo, 0, len(gcState.GetGcBarriers()))
	for _, b := range gcState.GetGcBarriers() {
		gcBarriers = append(gcBarriers, pbToGCBarrierInfo(b, start))
	}
	return gc.GCStateInfo{
		KeyspaceID:   keyspaceID,
		TxnSafePoint: gcState.GetTxnSafePoint(),
		GCSafePoint:  gcState.GetGcSafePoint(),
		GCBarriers:   gcBarriers,
	}, nil
}
