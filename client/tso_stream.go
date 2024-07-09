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
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/tikv/pd/client/errs"
	"google.golang.org/grpc"
)

// TSO Stream Builder Factory

type tsoStreamBuilderFactory interface {
	makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder
}

type pdTSOStreamBuilderFactory struct{}

func (*pdTSOStreamBuilderFactory) makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder {
	return &pdTSOStreamBuilder{client: pdpb.NewPDClient(cc), serverURL: cc.Target()}
}

type tsoTSOStreamBuilderFactory struct{}

func (*tsoTSOStreamBuilderFactory) makeBuilder(cc *grpc.ClientConn) tsoStreamBuilder {
	return &tsoTSOStreamBuilder{client: tsopb.NewTSOClient(cc), serverURL: cc.Target()}
}

// TSO Stream Builder

type tsoStreamBuilder interface {
	build(context.Context, context.CancelFunc, time.Duration) (tsoStream, error)
}

type pdTSOStreamBuilder struct {
	serverURL string
	client    pdpb.PDClient
}

func (b *pdTSOStreamBuilder) build(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) (tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Tso(ctx)
	done <- struct{}{}
	if err == nil {
		return &pdTSOStream{stream: stream, serverURL: b.serverURL}, nil
	}
	return nil, err
}

type tsoTSOStreamBuilder struct {
	serverURL string
	client    tsopb.TSOClient
}

func (b *tsoTSOStreamBuilder) build(
	ctx context.Context, cancel context.CancelFunc, timeout time.Duration,
) (tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Tso(ctx)
	done <- struct{}{}
	if err == nil {
		return &tsoTSOStream{stream: stream, serverURL: b.serverURL}, nil
	}
	return nil, err
}

func checkStreamTimeout(ctx context.Context, cancel context.CancelFunc, done chan struct{}, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
		cancel()
	case <-ctx.Done():
	}
	<-done
}

// TSO Stream

type tsoStream interface {
	getServerURL() string
	// processRequests processes TSO requests in streaming mode to get timestamps
	processRequests(
		clusterID uint64, keyspaceID, keyspaceGroupID uint32, dcLocation string,
		count int64, batchStartTime time.Time,
	) (respKeyspaceGroupID uint32, physical, logical int64, suffixBits uint32, err error)
}

type pdTSOStream struct {
	serverURL string
	stream    pdpb.PD_TsoClient
}

func (s *pdTSOStream) getServerURL() string {
	return s.serverURL
}

func (s *pdTSOStream) processRequests(
	clusterID uint64, _, _ uint32, dcLocation string, count int64, batchStartTime time.Time,
) (respKeyspaceGroupID uint32, physical, logical int64, suffixBits uint32, err error) {
	start := time.Now()
	req := &pdpb.TsoRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Count:      uint32(count),
		DcLocation: dcLocation,
	}

	if err = s.stream.Send(req); err != nil {
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return
	}
	tsoBatchSendLatency.Observe(time.Since(batchStartTime).Seconds())
	resp, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return
	}
	requestDurationTSO.Observe(time.Since(start).Seconds())
	tsoBatchSize.Observe(float64(count))

	if resp.GetCount() != uint32(count) {
		err = errors.WithStack(errTSOLength)
		return
	}

	ts := resp.GetTimestamp()
	respKeyspaceGroupID = defaultKeySpaceGroupID
	physical, logical, suffixBits = ts.GetPhysical(), ts.GetLogical(), ts.GetSuffixBits()
	return
}

type tsoTSOStream struct {
	serverURL string
	stream    tsopb.TSO_TsoClient
}

func (s *tsoTSOStream) getServerURL() string {
	return s.serverURL
}

func (s *tsoTSOStream) processRequests(
	clusterID uint64, keyspaceID, keyspaceGroupID uint32, dcLocation string,
	count int64, batchStartTime time.Time,
) (respKeyspaceGroupID uint32, physical, logical int64, suffixBits uint32, err error) {
	start := time.Now()
	req := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId:       clusterID,
			KeyspaceId:      keyspaceID,
			KeyspaceGroupId: keyspaceGroupID,
		},
		Count:      uint32(count),
		DcLocation: dcLocation,
	}

	if err = s.stream.Send(req); err != nil {
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return
	}
	tsoBatchSendLatency.Observe(time.Since(batchStartTime).Seconds())
	resp, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return
	}
	requestDurationTSO.Observe(time.Since(start).Seconds())
	tsoBatchSize.Observe(float64(count))

	if resp.GetCount() != uint32(count) {
		err = errors.WithStack(errTSOLength)
		return
	}

	ts := resp.GetTimestamp()
	respKeyspaceGroupID = resp.GetHeader().GetKeyspaceGroupId()
	physical, logical, suffixBits = ts.GetPhysical(), ts.GetLogical(), ts.GetSuffixBits()
	return
}
