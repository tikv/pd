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
<<<<<<< HEAD
=======

	"github.com/pingcap/errors"
>>>>>>> 9f1c47b1e8 (client: Expose service safe point v2 interfaces for legacy PD migration (#10749))
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
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
	defer func() { metrics.CmdDurationUpdateGCSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
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
func (c *client) UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.UpdateServiceSafePointV2", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationUpdateServiceSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
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
func (c *client) WatchGCSafePointV2(ctx context.Context, revision int64) (chan []*pdpb.SafePointEvent, error) {
	SafePointEventsChan := make(chan []*pdpb.SafePointEvent)
	req := &pdpb.WatchGCSafePointV2Request{
		Header:   c.requestHeader(),
		Revision: revision,
	}

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
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

const reservedGetMinServiceSafePointServiceID = "_reserved_get_min_ssp"

// GetMinServiceSafePointV2 returns the current minimum service GC safe point for the given keyspace.
//
// The deprecated V2 API has no read-only "get minimum" RPC, so it reuses updateServiceSafePointV2.
//   - serviceID = reservedGetMinServiceSafePointServiceID: Just a placeholder. Must be non-empty to pass the argument validation.
//   - ttl = 1: to reach the GCStateManager.setGCBarrierImpl.
//   - safePoint = 0: to make GCStateManager.setGCBarrierImpl return "ErrGCBarrierTSBehindTxnSafePoint", then GCStateManager.CompatibleUpdateServiceGCSafePoint return the TxnSafePoint.
//     There is a corner case that if current TxnSafePoint is 0, this API will set a GC barrier with safe point 0 and ttl 1, which is low impact (last for 1 second) and do not break correctness.
func (c *client) GetMinServiceSafePointV2(ctx context.Context, keyspaceID uint32) (uint64, error) {
	return c.updateServiceSafePointV2(ctx, keyspaceID, reservedGetMinServiceSafePointServiceID, 1, 0)
}

// SetServiceSafePointV2 updates a service GC safe point for the given keyspace and returns the new minimum safe point.
//
// NOTE: MUST check the returned new `MinSafePoint`.
// When `MinSafePoint > input safe point`, it means the update is rejected because of the TxnSafePoint (or MinServiceSafePoint from legacy PD servers) already exceeds the input safe point.
func (c *client) SetServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if ttl <= 0 {
		return 0, errors.Errorf("[gc] invalid ttl %d. It should be positive", ttl)
	}
	if serviceID == reservedGetMinServiceSafePointServiceID {
		return 0, errors.Errorf("[gc] invalid serviceID %q. It is reserved for GetMinServiceSafePointV2", serviceID)
	}
	return c.updateServiceSafePointV2(ctx, keyspaceID, serviceID, ttl, safePoint)
}

// DeleteServiceSafePointV2 deletes a service GC safe point for the given keyspace and returns the new minimum safe point.
// Missing safe point with `serviceID` are no-op success, and return is still the current minimum safe point.
//
// * ttl = -1: to reach the GCStateManager.deleteGCBarrierImpl.
func (c *client) DeleteServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string) (uint64, error) {
	return c.updateServiceSafePointV2(ctx, keyspaceID, serviceID, -1, 0)
}
