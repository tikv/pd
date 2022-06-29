// Copyright 2022 TiKV Project Authors.
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

// KeyspaceGCClient provides gc related functionalities at keyspace level.
type KeyspaceGCClient interface {
	// ListGCSafePoints returns gc safe point for each key spaces.
	ListGCSafePoints(ctx context.Context) ([]*gcpb.GCSafePoint, error)
	// UpdateKeyspaceGCSafePoint updates given keyspace's gc safe point, which
	// is checked by TiKV to do GC if it's necessary.
	// If the given safe point is less than the current one, it will not be updated.
	// Returns the new safe point after the update.
	UpdateKeyspaceGCSafePoint(ctx context.Context, spaceID uint32, safePoint uint64) (newSafePoint uint64, err error)
	// UpdateServiceSafePoint updates given keyspace's target service's service safe point.
	// Note that this does not trigger a GC job.
	// Returns the minimum service safe point for all services under the same keyspace.
	UpdateServiceSafePoint(ctx context.Context, spaceID uint32, serviceID string, ttl int64, safePoint uint64) (minSafePoint uint64, err error)
}

var _ KeyspaceGCClient = (*client)(nil)

func (c *client) leaderGCClient() gcpb.GCClient {
	if cc, ok := c.clientConns.Load(c.GetLeaderAddr()); ok {
		return gcpb.NewGCClient(cc.(*grpc.ClientConn))
	}
	return nil
}

// followerGCClient gets a gcClient of the currently reachable and healthy PD follower randomly.
func (c *client) followerGCClient() (gcpb.GCClient, string) {
	_, addr := c.followerClient()
	if addr == "" {
		return nil, ""
	}
	cc, err := c.getOrCreateGRPCConn(addr)
	if err != nil {
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

func (c *client) ListGCSafePoints(ctx context.Context) ([]*gcpb.GCSafePoint, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.ListGCSafePoints", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationListGCSafePoints.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &gcpb.ListGCSafePointsRequest{
		Header: c.requestHeader(),
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.gcClient().ListGCSafePoints(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationListGCSafePoints.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return nil, errors.WithStack(err)
	}

	return resp.GetSafePoints(), nil
}

func (c *client) UpdateKeyspaceGCSafePoint(ctx context.Context, spaceID uint32, safePoint uint64) (newSafePoint uint64, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateKeyspaceGCSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateKeyspaceGCSafePoint.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &gcpb.UpdateGCSafePointRequest{
		Header:    c.requestHeader(),
		SpaceId:   spaceID,
		SafePoint: safePoint,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.gcClient().UpdateGCSafePoint(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationUpdateKeyspaceGCSafePoint.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return 0, errors.WithStack(err)
	}
	return resp.GetNewSafePoint(), nil
}

func (c *client) UpdateServiceSafePoint(ctx context.Context, spaceID uint32, serviceID string, ttl int64, safePoint uint64) (minSafePoint uint64, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateServiceSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateServiceSafePoint.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &gcpb.UpdateServiceSafePointRequest{
		Header:    c.requestHeader(),
		SpaceId:   spaceID,
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
		return 0, errors.WithStack(err)
	}

	return resp.GetMinSafePoint(), err
}
