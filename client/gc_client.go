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

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
)

// updateGCSafePointV2 update gc safe point for the given keyspace.
// Only used for handling `UpdateGCSafePoint` in keyspace context, which is a deprecated usage.
func (c *client) updateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.UpdateGCSafePointV2", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationUpdateGCSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	//nolint:staticcheck
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
	resp, err := protoClient.UpdateGCSafePointV2(ctx, req) //nolint:staticcheck
	cancel()

	if err = c.respForErr(metrics.CmdFailedDurationUpdateGCSafePointV2, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetNewSafePoint(), nil
}

// updateServiceSafePointV2 update service safe point for the given keyspace.
// Only used for handling `UpdateServiceGCSafePoint` in keyspace context, which is a deprecated usage.
func (c *client) updateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.UpdateServiceSafePointV2", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationUpdateServiceSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	//nolint:staticcheck
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
	resp, err := protoClient.UpdateServiceSafePointV2(ctx, req) //nolint:staticcheck
	cancel()
	if err = c.respForErr(metrics.CmdFailedDurationUpdateServiceSafePointV2, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetMinSafePoint(), nil
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

// AdvanceTxnSafePoint tries to advance the transaction safe point to the target value.
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

// AdvanceGCSafePoint tries to advance the GC safe point to the target value.
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

func pbToGlobalGCBarrierInfo(pb *pdpb.GlobalGCBarrierInfo, reqStartTime time.Time) *gc.GlobalGCBarrierInfo {
	if pb == nil {
		return nil
	}
	ttl := saturatingStdDurationFromSeconds(pb.GetTtlSeconds())
	return gc.NewGlobalGCBarrierInfo(
		pb.GetBarrierId(),
		pb.GetBarrierTs(),
		ttl,
		reqStartTime,
	)
}

// SetGCBarrier sets (creates or updates) a GC barrier.
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
		TtlSeconds:    roundUpDurationToSeconds(ttl),
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

// DeleteGCBarrier deletes a GC barrier.
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

// GetGCState gets the current GC state.
func (c gcStatesClient) GetGCState(ctx context.Context) (gc.GCState, error) {
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
		return gc.GCState{}, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetGCState(ctx, req)
	if err = c.client.respForErr(metrics.CmdFailedDurationGetGCState, start, err, resp.GetHeader()); err != nil {
		return gc.GCState{}, err
	}

	gcState := resp.GetGcState()
	return pbToGCState(gcState, start), nil
}

func pbToGCState(pb *pdpb.GCState, reqStartTime time.Time) gc.GCState {
	keyspaceID := constants.NullKeyspaceID
	if pb.KeyspaceScope != nil {
		keyspaceID = pb.KeyspaceScope.KeyspaceId
	}
	gcBarriers := make([]*gc.GCBarrierInfo, 0, len(pb.GetGcBarriers()))
	for _, b := range pb.GetGcBarriers() {
		gcBarriers = append(gcBarriers, pbToGCBarrierInfo(b, reqStartTime))
	}
	return gc.GCState{
		KeyspaceID:   keyspaceID,
		TxnSafePoint: pb.GetTxnSafePoint(),
		GCSafePoint:  pb.GetGcSafePoint(),
		GCBarriers:   gcBarriers,
	}
}

// SetGlobalGCBarrier sets (creates or updates) a global GC barrier.
func (c gcStatesClient) SetGlobalGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*gc.GlobalGCBarrierInfo, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.SetGlobalGCBarrier", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationSetGlobalGCBarrier.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.client.inner.option.Timeout)
	defer cancel()
	req := &pdpb.SetGlobalGCBarrierRequest{
		Header:     c.client.requestHeader(),
		BarrierId:  barrierID,
		BarrierTs:  barrierTS,
		TtlSeconds: roundUpDurationToSeconds(ttl),
	}
	protoClient, ctx := c.client.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.SetGlobalGCBarrier(ctx, req)
	if err = c.client.respForErr(metrics.CmdFailedDurationSetGlobalGCBarrier, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return pbToGlobalGCBarrierInfo(resp.GetNewBarrierInfo(), start), nil
}

// DeleteGlobalGCBarrier deletes a GC barrier.
func (c gcStatesClient) DeleteGlobalGCBarrier(ctx context.Context, barrierID string) (*gc.GlobalGCBarrierInfo, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.DeleteGlobalGCBarrier", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationDeleteGlobalGCBarrier.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.client.inner.option.Timeout)
	defer cancel()
	req := &pdpb.DeleteGlobalGCBarrierRequest{
		Header:    c.client.requestHeader(),
		BarrierId: barrierID,
	}
	protoClient, ctx := c.client.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.DeleteGlobalGCBarrier(ctx, req)
	if err = c.client.respForErr(metrics.CmdFailedDurationDeleteGlobalGCBarrier, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return pbToGlobalGCBarrierInfo(resp.GetDeletedBarrierInfo(), start), nil
}

// GetAllKeyspacesGCStates gets the GC states from all keyspaces.
func (c gcStatesClient) GetAllKeyspacesGCStates(ctx context.Context) (gc.ClusterGCStates, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.GetAllKeyspacesGCState", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationGetAllKeyspacesGCStates.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.client.inner.option.Timeout)
	defer cancel()
	req := &pdpb.GetAllKeyspacesGCStatesRequest{
		Header: c.client.requestHeader(),
	}
	protoClient, ctx := c.client.getClientAndContext(ctx)
	if protoClient == nil {
		return gc.ClusterGCStates{}, errs.ErrClientGetProtoClient
	}

	resp, err := protoClient.GetAllKeyspacesGCStates(ctx, req)
	if err = c.client.respForErr(metrics.CmdFailedDurationGetAllKeyspacesGCStates, start, err, resp.GetHeader()); err != nil {
		return gc.ClusterGCStates{}, err
	}

	var ret gc.ClusterGCStates
	ret.GCStates = make(map[uint32]gc.GCState, len(resp.GetGcStates()))
	for _, state := range resp.GetGcStates() {
		var keyspaceID uint32
		if state.KeyspaceScope == nil {
			keyspaceID = constants.NullKeyspaceID
		} else {
			keyspaceID = state.KeyspaceScope.KeyspaceId
		}
		ret.GCStates[keyspaceID] = pbToGCState(state, start)
	}
	for _, barrier := range resp.GetGlobalGcBarriers() {
		ret.GlobalGCBarriers = append(ret.GlobalGCBarriers, pbToGlobalGCBarrierInfo(barrier, start))
	}
	return ret, nil
}
