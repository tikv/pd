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
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/tikv/pd/client/grpcutil"
	"google.golang.org/grpc"
)

// TsoClient manages TSO data.
type TsoClient interface {
	// LoadTimestamp loads and returns timestamp.
	LoadTimestamp(ctx context.Context, key string) (uint64, error)
	// SaveTimestamp saves the timestamp.
	SaveTimestamp(ctx context.Context, key string, timestamp uint64, skipCheck bool, lastTimestamp ...uint64) error
}

// tsoClient returns the TsoClient from current PD leader.
func (c *client) tsoClient() tsopb.TsoClient {
	if cc, ok := c.clientConns.Load(c.GetLeaderAddr()); ok {
		return tsopb.NewTsoClient(cc.(*grpc.ClientConn))
	}
	return nil
}

// LoadTimestamp loads and returns timestamp.
func (c *client) LoadTimestamp(ctx context.Context, key string) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("tsoClient.LoadTimestamp", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationLoadTimestamp.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &tsopb.LoadTimestampRequest{
		Header: c.requestHeader(),
		Key:    key,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.tsoClient().LoadTimestamp(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationLoadTimestamp.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return 0, err
	}

	if resp.Header.GetError() != nil {
		cmdFailedDurationLoadTimestamp.Observe(time.Since(start).Seconds())
		return 0, errors.Errorf("Load timestamp %s failed: %s", key, resp.Header.GetError().String())
	}

	return resp.Timestamp, nil
}

// SaveTimestamp saves the timestamp.
func (c *client) SaveTimestamp(ctx context.Context, key string, timestamp uint64, skipCheck bool, lastTimestamp ...uint64) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("tsoClient.SaveTimestamp", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationSaveTimestamp.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &tsopb.SaveTimestampRequest{
		Header:    c.requestHeader(),
		Key:       key,
		Timestamp: timestamp,
		SkipCheck: skipCheck,
	}
	if !skipCheck && len(lastTimestamp) != 0 {
		req.LastTimestamp = lastTimestamp[0]
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
	resp, err := c.tsoClient().SaveTimestamp(ctx, req)
	cancel()

	if err != nil {
		cmdFailedDurationSaveTimestamp.Observe(time.Since(start).Seconds())
		c.ScheduleCheckLeader()
		return err
	}

	if resp.Header.GetError() != nil {
		cmdFailedDurationSaveTimestamp.Observe(time.Since(start).Seconds())
		return errors.Errorf("Save timestamp %d to %s failed: %s", timestamp, key, resp.Header.GetError().String())
	}

	return nil
}
