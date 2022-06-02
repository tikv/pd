package pd

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/gcpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/grpcutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type GCClient interface {
	// ListKeySpaces returns all key spaces that has gc safe point.
	// If withGCSafePoint set to true, it will also return their corresponding gc safe points, otherwise they will be 0.
	ListKeySpaces(ctx context.Context, withGCSafePoint bool) ([]*gcpb.KeySpace, error)
	// GetMinServiceSafePoint returns the minimum of all service safe point of the given key space.
	// It also returns the current revision of the pd storage, within which the returned min is valid.
	// If no service safe point exist for the given key space, it will return 0 as safe point and revision.
	GetMinServiceSafePoint(ctx context.Context, spaceID string) (safePoint uint64, revision int64, err error)
	// UpdateKeySpaceGCSafePoint update the target safe point, previously obtained revision is required.
	// If failed, caller should retry from GetMinServiceSafePoint.
	UpdateKeySpaceGCSafePoint(ctx context.Context, spaceID string, safePoint uint64, revision int64) (succeeded bool, newSafePoint uint64, err error)
	// UpdateServiceSafePoint update the given service's safe point
	// Pass in a negative ttl to remove it
	// If failed, caller should retry with higher safe point
	UpdateServiceSafePoint(ctx context.Context, spaceID, serviceID string, ttl int64, safePoint uint64) (succeeded bool, gcSafePoint, oldSafePoint, newSafePoint uint64, err error)
}

var _ GCClient = (*client)(nil)

func (c *client) gcHeader() *gcpb.RequestHeader {
	return &gcpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}

func (c *client) leaderGCClient() gcpb.GCClient {
	if cc, ok := c.clientConns.Load(c.GetLeaderAddr()); ok {
		return gcpb.NewGCClient(cc.(*grpc.ClientConn))
	}
	return nil
}

// followerGCClient gets a gcClient of the currently reachable and healthy PD follower randomly.
func (c *client) followerGCClient() (gcpb.GCClient, string) {
	cc, addr := c.healthyFollower()
	if cc == nil {
		return nil, ""
	}
	return gcpb.NewGCClient(cc), addr
}

func (c *client) gcClient() gcpb.GCClient {
	if c.option.enableForwarding && atomic.LoadInt32(&c.leaderNetworkFailure) == 1 {
		followerClient, addr := c.followerGCClient()
		if followerClient != nil {
			log.Debug("[pd] use follower Client", zap.String("addr", addr))
			return followerClient
		}
	}
	return c.leaderGCClient()
}

func (c *client) ListKeySpaces(ctx context.Context, withGCSafePoint bool) ([]*gcpb.KeySpace, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.ListKeySpaces", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationListKeySpaces.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &gcpb.ListKeySpacesRequest{
		Header:          c.gcHeader(),
		WithGcSafePoint: withGCSafePoint,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.gcClient().ListKeySpaces(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationListKeySpaces.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}

	return resp.KeySpaces, nil
}

func (c *client) GetMinServiceSafePoint(ctx context.Context, spaceID string) (safePoint uint64, revision int64, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetMinServiceSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetMinServiceSafePoint.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &gcpb.GetMinServiceSafePointRequest{
		Header:  c.gcHeader(),
		SpaceId: []byte(spaceID),
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.gcClient().GetMinServiceSafePoint(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationGetMinServiceSafePoint.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return 0, 0, errors.WithStack(err)
	}

	return resp.SafePoint, resp.Revision, nil
}

func (c *client) UpdateKeySpaceGCSafePoint(ctx context.Context, spaceID string, safePoint uint64, revision int64) (succeeded bool, newSafePoint uint64, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateKeySpaceGCSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateKeySpaceGCSafePoint.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &gcpb.UpdateGCSafePointRequest{
		Header:    c.gcHeader(),
		SpaceId:   []byte(spaceID),
		SafePoint: safePoint,
		Revision:  revision,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.gcClient().UpdateGCSafePoint(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationUpdateKeySpaceGCSafePoint.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return false, 0, errors.WithStack(err)
	}
	return resp.Succeeded, resp.NewSafePoint, nil
}

func (c *client) UpdateServiceSafePoint(ctx context.Context, spaceID, serviceID string, ttl int64, safePoint uint64) (succeeded bool, gcSafePoint, oldSafePoint, newSafePoint uint64, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateServiceSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateServiceSafePoint.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &gcpb.UpdateServiceSafePointRequest{
		Header:    c.gcHeader(),
		SpaceId:   []byte(spaceID),
		ServiceId: []byte(serviceID),
		TTL:       ttl,
		SafePoint: safePoint,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.gcClient().UpdateServiceSafePoint(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationUpdateServiceSafePoint.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return false, 0, 0, 0, errors.WithStack(err)
	}

	return resp.Succeeded, resp.GcSafePoint, resp.OldSafePoint, resp.NewSafePoint, nil
}
