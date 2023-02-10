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

package tso

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxMergeTSORequests    = 10000
	defaultTSOProxyTimeout = 3 * time.Second
)

// GrpcServer wraps Server to provide TSO grpc service.
type GrpcServer interface {
	ClusterID() uint64
	IsClosed() bool
	IsLocalRequest(forwardedHost string) bool
	GetTSOAllocatorManager() *AllocatorManager
	GetTSODispatcher() *sync.Map
	CreateTsoForwardStream(client *grpc.ClientConn) (pdpb.PD_TsoClient, context.CancelFunc, error)
	GetDelegateClient(ctx context.Context, forwardedHost string) (*grpc.ClientConn, error)
}

// Tso implements gRPC PDServer.
func Tso(s GrpcServer, stream pdpb.PD_TsoServer) error {
	var (
		doneCh chan struct{}
		errCh  chan error
	)
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	for {
		// Prevent unnecessary performance overhead of the channel.
		if errCh != nil {
			select {
			case err := <-errCh:
				return errors.WithStack(err)
			default:
			}
		}
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		streamCtx := stream.Context()
		forwardedHost := grpcutil.GetForwardedHost(streamCtx)
		if !s.IsLocalRequest(forwardedHost) {
			if errCh == nil {
				doneCh = make(chan struct{})
				defer close(doneCh)
				errCh = make(chan error)
			}
			dispatchTSORequest(ctx, s, &tsoRequest{
				forwardedHost,
				request,
				stream,
			}, forwardedHost, doneCh, errCh)
			continue
		}

		start := time.Now()
		// TSO uses leader lease to determine validity. No need to check leader here.
		if s.IsClosed() {
			return status.Errorf(codes.Unknown, "server not started")
		}
		if request.GetHeader().GetClusterId() != s.ClusterID() {
			return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.ClusterID(), request.GetHeader().GetClusterId())
		}
		count := request.GetCount()
		ts, err := s.GetTSOAllocatorManager().HandleTSORequest(request.GetDcLocation(), count)
		if err != nil {
			return status.Errorf(codes.Unknown, err.Error())
		}
		tsoHandleDuration.Observe(time.Since(start).Seconds())
		response := &pdpb.TsoResponse{
			Header:    Header(s.ClusterID()),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

// Header is a helper function to wrapper the response header for the given cluster id
func Header(clusterID uint64) *pdpb.ResponseHeader {
	if clusterID == 0 {
		return WrapErrorToHeader(clusterID, pdpb.ErrorType_NOT_BOOTSTRAPPED, "cluster id is not ready")
	}
	return &pdpb.ResponseHeader{ClusterId: clusterID}
}

// WrapErrorToHeader is a helper function to wrapper the response header for the given cluster id and the error info
func WrapErrorToHeader(clusterID uint64, errorType pdpb.ErrorType, message string) *pdpb.ResponseHeader {
	return ErrorHeader(
		clusterID,
		&pdpb.Error{
			Type:    errorType,
			Message: message,
		})
}

// ErrorHeader is a helper function to wrapper the response header for the given cluster id and the error
func ErrorHeader(clusterID uint64, err *pdpb.Error) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: clusterID,
		Error:     err,
	}
}

type tsoRequest struct {
	forwardedHost string
	request       *pdpb.TsoRequest
	stream        pdpb.PD_TsoServer
}

func dispatchTSORequest(ctx context.Context, s GrpcServer, request *tsoRequest, forwardedHost string, doneCh <-chan struct{}, errCh chan<- error) {
	tsoRequestChInterface, loaded := s.GetTSODispatcher().LoadOrStore(forwardedHost, make(chan *tsoRequest, maxMergeTSORequests))
	if !loaded {
		tsDeadlineCh := make(chan deadline, 1)
		go handleDispatcher(ctx, s, forwardedHost, tsoRequestChInterface.(chan *tsoRequest), tsDeadlineCh, doneCh, errCh)
		go watchTSDeadline(ctx, tsDeadlineCh)
	}
	tsoRequestChInterface.(chan *tsoRequest) <- request
}

func handleDispatcher(ctx context.Context, s GrpcServer, forwardedHost string, tsoRequestCh <-chan *tsoRequest, tsDeadlineCh chan<- deadline, doneCh <-chan struct{}, errCh chan<- error) {
	dispatcherCtx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()
	defer s.GetTSODispatcher().Delete(forwardedHost)

	var (
		forwardStream pdpb.PD_TsoClient
		cancel        context.CancelFunc
	)
	client, err := s.GetDelegateClient(ctx, forwardedHost)
	if err != nil {
		goto errHandling
	}
	log.Info("create tso forward stream", zap.String("forwarded-host", forwardedHost))
	forwardStream, cancel, err = s.CreateTsoForwardStream(client)
errHandling:
	if err != nil || forwardStream == nil {
		log.Error("create tso forwarding stream error", zap.String("forwarded-host", forwardedHost), errs.ZapError(errs.ErrGRPCCreateStream, err))
		select {
		case <-dispatcherCtx.Done():
			return
		case _, ok := <-doneCh:
			if !ok {
				return
			}
		case errCh <- err:
			close(errCh)
			return
		}
	}
	defer cancel()

	requests := make([]*tsoRequest, maxMergeTSORequests+1)
	for {
		select {
		case first := <-tsoRequestCh:
			pendingTSOReqCount := len(tsoRequestCh) + 1
			requests[0] = first
			for i := 1; i < pendingTSOReqCount; i++ {
				requests[i] = <-tsoRequestCh
			}
			done := make(chan struct{})
			dl := deadline{
				timer:  time.After(defaultTSOProxyTimeout),
				done:   done,
				cancel: cancel,
			}
			select {
			case tsDeadlineCh <- dl:
			case <-dispatcherCtx.Done():
				return
			}
			err = processTSORequests(s, forwardStream, requests[:pendingTSOReqCount])
			close(done)
			if err != nil {
				log.Error("proxy forward tso error", zap.String("forwarded-host", forwardedHost), errs.ZapError(errs.ErrGRPCSend, err))
				select {
				case <-dispatcherCtx.Done():
					return
				case _, ok := <-doneCh:
					if !ok {
						return
					}
				case errCh <- err:
					close(errCh)
					return
				}
			}
		case <-dispatcherCtx.Done():
			return
		}
	}
}

func processTSORequests(s GrpcServer, forwardStream pdpb.PD_TsoClient, requests []*tsoRequest) error {
	start := time.Now()
	// Merge the requests
	count := uint32(0)
	for _, request := range requests {
		count += request.request.GetCount()
	}
	req := &pdpb.TsoRequest{
		Header: requests[0].request.GetHeader(),
		Count:  count,
		// TODO: support Local TSO proxy forwarding.
		DcLocation: requests[0].request.GetDcLocation(),
	}
	// Send to the leader stream.
	if err := forwardStream.Send(req); err != nil {
		return err
	}
	resp, err := forwardStream.Recv()
	if err != nil {
		return err
	}
	tsoProxyHandleDuration.Observe(time.Since(start).Seconds())
	tsoProxyBatchSize.Observe(float64(count))
	// Split the response
	physical, logical, suffixBits := resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical(), resp.GetTimestamp().GetSuffixBits()
	// `logical` is the largest ts's logical part here, we need to do the subtracting before we finish each TSO request.
	// This is different from the logic of client batch, for example, if we have a largest ts whose logical part is 10,
	// count is 5, then the splitting results should be 5 and 10.
	firstLogical := addLogical(logical, -int64(count), suffixBits)
	return finishTSORequest(s, requests, physical, firstLogical, suffixBits)
}

// Because of the suffix, we need to shift the count before we add it to the logical part.
func addLogical(logical, count int64, suffixBits uint32) int64 {
	return logical + count<<suffixBits
}

func finishTSORequest(s GrpcServer, requests []*tsoRequest, physical, firstLogical int64, suffixBits uint32) error {
	countSum := int64(0)
	for i := 0; i < len(requests); i++ {
		count := requests[i].request.GetCount()
		countSum += int64(count)
		response := &pdpb.TsoResponse{
			Header: Header(s.ClusterID()),
			Count:  count,
			Timestamp: &pdpb.Timestamp{
				Physical:   physical,
				Logical:    addLogical(firstLogical, countSum, suffixBits),
				SuffixBits: suffixBits,
			},
		}
		// Send back to the client.
		if err := requests[i].stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

type deadline struct {
	timer  <-chan time.Time
	done   chan struct{}
	cancel context.CancelFunc
}

func watchTSDeadline(ctx context.Context, tsDeadlineCh <-chan deadline) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		select {
		case d := <-tsDeadlineCh:
			select {
			case <-d.timer:
				log.Error("tso proxy request processing is canceled due to timeout", errs.ZapError(errs.ErrProxyTSOTimeout))
				d.cancel()
			case <-d.done:
				continue
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
