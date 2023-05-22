package pd

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/grpcutil"
	"go.uber.org/zap"
)

// GCClient is a client for doing GC
type GCClient interface {
	UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error)
	UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error)
	WatchGCSafePointV2(ctx context.Context) (chan []*pdpb.SafePointEvent, error)
	GetGCSafePointV2(ctx context.Context, keyspaceID uint32) (uint64, error)
}

// UpdateGCSafePointV2 update gc safe point for the given keyspace.
func (c *client) UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateGCSafePointV2", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateGCSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.UpdateGCSafePointV2Request{
		Header:     c.requestHeader(),
		KeyspaceId: keyspaceID,
		SafePoint:  safePoint,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().UpdateGCSafePointV2(ctx, req)
	cancel()

	if err = c.respForErr(cmdFailedDurationUpdateGCSafePointV2, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetNewSafePoint(), nil
}

// UpdateServiceSafePointV2 update service safe point for the given keyspace.
func (c *client) UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateServiceSafePointV2", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateServiceSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.UpdateServiceSafePointV2Request{
		Header:     c.requestHeader(),
		KeyspaceId: keyspaceID,
		ServiceId:  []byte(serviceID),
		SafePoint:  safePoint,
		Ttl:        ttl,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().UpdateServiceSafePointV2(ctx, req)
	cancel()
	if err = c.respForErr(cmdFailedDurationUpdateServiceSafePointV2, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetMinSafePoint(), nil
}

// WatchGCSafePointV2 watch gc safe point change.
func (c *client) WatchGCSafePointV2(ctx context.Context) (chan []*pdpb.SafePointEvent, error) {
	SafePointEventsChan := make(chan []*pdpb.SafePointEvent)
	req := &pdpb.WatchGCSafePointV2Request{
		Header: c.requestHeader(),
	}
	stream, err := c.getClient().WatchGCSafePointV2(ctx, req)
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
					return
				}
				SafePointEventsChan <- resp.GetEvents()
			}
		}
	}()
	return SafePointEventsChan, err
}

// GetGCSafePointV2 get gc safe point for the given keyspace.
func (c *client) GetGCSafePointV2(ctx context.Context, keyspaceID uint32) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateGCSafePointV2", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetGCSafePointV2.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.GetGCSafePointV2Request{
		Header:     c.requestHeader(),
		KeyspaceId: keyspaceID,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.getClient().GetGCSafePointV2(ctx, req)
	cancel()

	if err = c.respForErr(cmdFailedDurationUpdateGCSafePointV2, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetSafePoint(), nil
}
