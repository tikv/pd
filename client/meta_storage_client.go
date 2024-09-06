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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
)

// MetaStorageClient is the interface for meta storage client.
type MetaStorageClient interface {
	// Watch watches on a key or prefix.
	Watch(ctx context.Context, key []byte, opts ...OpOption) (chan []*meta_storagepb.Event, error)
	// Get gets the value for a key.
	Get(ctx context.Context, key []byte, opts ...OpOption) (*meta_storagepb.GetResponse, error)
	// Put puts a key-value pair into meta storage.
	Put(ctx context.Context, key []byte, value []byte, opts ...OpOption) (*meta_storagepb.PutResponse, error)
}

// metaStorageClient gets the meta storage client from current PD leader.
func (c *client) metaStorageClient() meta_storagepb.MetaStorageClient {
	if client := c.pdSvcDiscovery.GetServingEndpointClientConn(); client != nil {
		return meta_storagepb.NewMetaStorageClient(client)
	}
	return nil
}

// Op represents available options when using meta storage client.
type Op struct {
	rangeEnd         []byte
	revision         int64
	prevKv           bool
	lease            int64
	limit            int64
	isOptsWithPrefix bool
}

// OpOption configures etcd Op.
type OpOption func(*Op)

// WithLimit specifies the limit of the key.
func WithLimit(limit int64) OpOption {
	return func(op *Op) { op.limit = limit }
}

// WithRangeEnd specifies the range end of the key.
func WithRangeEnd(rangeEnd []byte) OpOption {
	return func(op *Op) { op.rangeEnd = rangeEnd }
}

// WithRev specifies the start revision of the key.
func WithRev(revision int64) OpOption {
	return func(op *Op) { op.revision = revision }
}

// WithPrevKV specifies the previous key-value pair of the key.
func WithPrevKV() OpOption {
	return func(op *Op) { op.prevKv = true }
}

// WithLease specifies the lease of the key.
func WithLease(lease int64) OpOption {
	return func(op *Op) { op.lease = lease }
}

// WithPrefix specifies the prefix of the key.
func WithPrefix() OpOption {
	return func(op *Op) {
		op.isOptsWithPrefix = true
	}
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
func (c *client) Put(ctx context.Context, key, value []byte, opts ...OpOption) (*meta_storagepb.PutResponse, error) {
	options := &Op{}
	for _, opt := range opts {
		opt(options)
	}

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.Put", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationPut.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &meta_storagepb.PutRequest{
		Key:    key,
		Value:  value,
		Lease:  options.lease,
		PrevKv: options.prevKv,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderURL())
	cli := c.metaStorageClient()
	if cli == nil {
		cancel()
		return nil, errs.ErrClientGetMetaStorageClient
	}
	resp, err := cli.Put(ctx, req)
	cancel()

	if err = c.respForMetaStorageErr(cmdFailedDurationPut, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp, nil
}

// Get implements the MetaStorageClient interface.
func (c *client) Get(ctx context.Context, key []byte, opts ...OpOption) (*meta_storagepb.GetResponse, error) {
	options := &Op{}
	for _, opt := range opts {
		opt(options)
	}
	if options.isOptsWithPrefix {
		options.rangeEnd = getPrefix(key)
	}

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.Get", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGet.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &meta_storagepb.GetRequest{
		Key:      key,
		RangeEnd: options.rangeEnd,
		Limit:    options.limit,
		Revision: options.revision,
	}
	ctx = grpcutil.BuildForwardContext(ctx, c.GetLeaderURL())
	cli := c.metaStorageClient()
	if cli == nil {
		cancel()
		return nil, errs.ErrClientGetMetaStorageClient
	}
	resp, err := cli.Get(ctx, req)
	cancel()

	if err = c.respForMetaStorageErr(cmdFailedDurationGet, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp, nil
}

// Watch implements the MetaStorageClient interface.
func (c *client) Watch(ctx context.Context, key []byte, opts ...OpOption) (chan []*meta_storagepb.Event, error) {
	eventCh := make(chan []*meta_storagepb.Event, 100)
	options := &Op{}
	for _, opt := range opts {
		opt(options)
	}
	if options.isOptsWithPrefix {
		options.rangeEnd = getPrefix(key)
	}

	cli := c.metaStorageClient()
	if cli == nil {
		return nil, errs.ErrClientGetMetaStorageClient
	}
	res, err := cli.Watch(ctx, &meta_storagepb.WatchRequest{
		Key:           key,
		RangeEnd:      options.rangeEnd,
		StartRevision: options.revision,
		PrevKv:        options.prevKv,
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

func (c *client) respForMetaStorageErr(observer prometheus.Observer, start time.Time, err error, header *meta_storagepb.ResponseHeader) error {
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
