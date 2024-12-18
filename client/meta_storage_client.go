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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/utils/grpcutil"
)

// metaStorageClient gets the meta storage client from current PD leader.
func (c *innerClient) metaStorageClient() meta_storagepb.MetaStorageClient {
	if client := c.pdSvcDiscovery.GetServingEndpointClientConn(); client != nil {
		return meta_storagepb.NewMetaStorageClient(client)
	}
	return nil
}

// See https://github.com/etcd-io/etcd/blob/da4bf0f76fb708e0b57763edb46ba523447b9510/client/v3/op.go#L372-L385
func getPrefix(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			end = end[:i+1]
			return end
		}
	}
	return []byte{0}
}

// Put implements the MetaStorageClient interface.
func (c *innerClient) Put(ctx context.Context, key, value []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	options := &opt.MetaStorageOp{}
	for _, opt := range opts {
		opt(options)
	}

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.Put", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationPut.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.Timeout)
	req := &meta_storagepb.PutRequest{
		Key:    key,
		Value:  value,
		Lease:  options.Lease,
		PrevKv: options.PrevKv,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.pdSvcDiscovery.GetServingURL())
	cli := c.metaStorageClient()
	if cli == nil {
		cancel()
		return nil, errs.ErrClientGetMetaStorageClient
	}
	resp, err := cli.Put(ctx, req)
	cancel()

	if err = c.respForMetaStorageErr(metrics.CmdFailedDurationPut, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp, nil
}

// Get implements the MetaStorageClient interface.
func (c *innerClient) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	options := &opt.MetaStorageOp{}
	for _, opt := range opts {
		opt(options)
	}
	if options.IsOptsWithPrefix {
		options.RangeEnd = getPrefix(key)
	}

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.Get", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationGet.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.Timeout)
	req := &meta_storagepb.GetRequest{
		Key:      key,
		RangeEnd: options.RangeEnd,
		Limit:    options.Limit,
		Revision: options.Revision,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.pdSvcDiscovery.GetServingURL())
	cli := c.metaStorageClient()
	if cli == nil {
		cancel()
		return nil, errs.ErrClientGetMetaStorageClient
	}
	resp, err := cli.Get(ctx, req)
	cancel()

	if err = c.respForMetaStorageErr(metrics.CmdFailedDurationGet, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp, nil
}

// Watch implements the MetaStorageClient interface.
func (c *innerClient) Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error) {
	eventCh := make(chan []*meta_storagepb.Event, 100)
	options := &opt.MetaStorageOp{}
	for _, opt := range opts {
		opt(options)
	}
	if options.IsOptsWithPrefix {
		options.RangeEnd = getPrefix(key)
	}

	cli := c.metaStorageClient()
	if cli == nil {
		return nil, errs.ErrClientGetMetaStorageClient
	}
	res, err := cli.Watch(ctx, &meta_storagepb.WatchRequest{
		Key:           key,
		RangeEnd:      options.RangeEnd,
		StartRevision: options.Revision,
		PrevKv:        options.PrevKv,
	})
	if err != nil {
		close(eventCh)
		return nil, err
	}
	go func() {
		defer func() {
			close(eventCh)
		}()
		for {
			resp, err := res.Recv()
			failpoint.Inject("watchStreamError", func() {
				err = errors.Errorf("fake error")
			})
			if err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case eventCh <- resp.GetEvents():
			}
		}
	}()
	return eventCh, err
}

func (c *innerClient) respForMetaStorageErr(observer prometheus.Observer, start time.Time, err error, header *meta_storagepb.ResponseHeader) error {
	if err != nil || header.GetError() != nil {
		observer.Observe(time.Since(start).Seconds())
		if err != nil {
			c.pdSvcDiscovery.ScheduleCheckMemberChanged()
			return errors.WithStack(err)
		}
		return errors.WithStack(errors.New(header.GetError().String()))
	}
	return nil
}

// Put implements the MetaStorageClient interface.
func (c *client) Put(ctx context.Context, key, value []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	return c.inner.Put(ctx, key, value, opts...)
}

// Get implements the MetaStorageClient interface.
func (c *client) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	return c.inner.Get(ctx, key, opts...)
}

// Watch implements the MetaStorageClient interface.
func (c *client) Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error) {
	return c.inner.Watch(ctx, key, opts...)
}
