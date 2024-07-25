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
	build(context.Context, context.CancelFunc, time.Duration) (*tsoStream, error)
}

type pdTSOStreamBuilder struct {
	serverURL string
	client    pdpb.PDClient
}

func (b *pdTSOStreamBuilder) build(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) (*tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Tso(ctx)
	done <- struct{}{}
	if err == nil {
		return &tsoStream{stream: pdTSOStreamAdapter{stream}, serverURL: b.serverURL}, nil
	}
	return nil, err
}

type tsoTSOStreamBuilder struct {
	serverURL string
	client    tsopb.TSOClient
}

func (b *tsoTSOStreamBuilder) build(
	ctx context.Context, cancel context.CancelFunc, timeout time.Duration,
) (*tsoStream, error) {
	done := make(chan struct{})
	// TODO: we need to handle a conner case that this goroutine is timeout while the stream is successfully created.
	go checkStreamTimeout(ctx, cancel, done, timeout)
	stream, err := b.client.Tso(ctx)
	done <- struct{}{}
	if err == nil {
		return &tsoStream{stream: tsoTSOStreamAdapter{stream}, serverURL: b.serverURL}, nil
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

type tsoRequestResult struct {
	physical, logical   int64
	count               uint32
	suffixBits          uint32
	respKeyspaceGroupID uint32
}

type grpcTSOStreamAdapter interface {
	Send(clusterID uint64, keyspaceID, keyspaceGroupID uint32, dcLocation string,
		count int64) error
	Recv() (tsoRequestResult, error)
}

type pdTSOStreamAdapter struct {
	stream pdpb.PD_TsoClient
}

func (s pdTSOStreamAdapter) Send(clusterID uint64, _, _ uint32, dcLocation string, count int64) error {
	req := &pdpb.TsoRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: clusterID,
		},
		Count:      uint32(count),
		DcLocation: dcLocation,
	}
	return s.stream.Send(req)
}

func (s pdTSOStreamAdapter) Recv() (tsoRequestResult, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return tsoRequestResult{}, err
	}
	return tsoRequestResult{
		physical:            resp.GetTimestamp().GetPhysical(),
		logical:             resp.GetTimestamp().GetLogical(),
		count:               resp.GetCount(),
		suffixBits:          resp.GetTimestamp().GetSuffixBits(),
		respKeyspaceGroupID: defaultKeySpaceGroupID,
	}, nil
}

type tsoTSOStreamAdapter struct {
	stream tsopb.TSO_TsoClient
}

func (s tsoTSOStreamAdapter) Send(clusterID uint64, keyspaceID, keyspaceGroupID uint32, dcLocation string, count int64) error {
	req := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId:       clusterID,
			KeyspaceId:      keyspaceID,
			KeyspaceGroupId: keyspaceGroupID,
		},
		Count:      uint32(count),
		DcLocation: dcLocation,
	}
	return s.stream.Send(req)
}

func (s tsoTSOStreamAdapter) Recv() (tsoRequestResult, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return tsoRequestResult{}, err
	}
	return tsoRequestResult{
		physical:            resp.GetTimestamp().GetPhysical(),
		logical:             resp.GetTimestamp().GetLogical(),
		count:               resp.GetCount(),
		suffixBits:          resp.GetTimestamp().GetSuffixBits(),
		respKeyspaceGroupID: resp.GetHeader().GetKeyspaceGroupId(),
	}, nil
}

type tsoStream struct {
	serverURL string
	// The internal gRPC stream.
	//   - `pdpb.PD_TsoClient` for a leader/follower in the PD cluster.
	//   - `tsopb.TSO_TsoClient` for a primary/secondary in the TSO cluster.
	stream grpcTSOStreamAdapter
}

func (s *tsoStream) getServerURL() string {
	return s.serverURL
}

func (s *tsoStream) processRequests(
	clusterID uint64, keyspaceID, keyspaceGroupID uint32, dcLocation string, count int64, batchStartTime time.Time,
) (respKeyspaceGroupID uint32, physical, logical int64, suffixBits uint32, err error) {
	start := time.Now()
	if err = s.stream.Send(clusterID, keyspaceID, keyspaceGroupID, dcLocation, count); err != nil {
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return
	}
	tsoBatchSendLatency.Observe(time.Since(batchStartTime).Seconds())
	res, err := s.stream.Recv()
	duration := time.Since(start).Seconds()
	if err != nil {
		requestFailedDurationTSO.Observe(duration)
		if err == io.EOF {
			err = errs.ErrClientTSOStreamClosed
		} else {
			err = errors.WithStack(err)
		}
		return
	}
	requestDurationTSO.Observe(duration)
	tsoBatchSize.Observe(float64(count))

	if res.count != uint32(count) {
		err = errors.WithStack(errTSOLength)
		return
	}

	respKeyspaceGroupID = res.respKeyspaceGroupID
	physical, logical, suffixBits = res.physical, res.logical, res.suffixBits
	return
}
