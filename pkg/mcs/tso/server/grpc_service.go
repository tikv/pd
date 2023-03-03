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

package server

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// tso
	maxMergeTSORequests    = 10000
	defaultTSOProxyTimeout = 3 * time.Second
)

// gRPC errors
var (
	ErrNotStarted = status.Errorf(codes.Unavailable, "server not started")
)

var _ tsopb.TSOServer = (*Service)(nil)

// SetUpRestHandler is a hook to sets up the REST service.
var SetUpRestHandler = func(srv *Service) (http.Handler, apiutil.APIServiceGroup) {
	return dummyRestService{}, apiutil.APIServiceGroup{}
}

type dummyRestService struct{}

func (d dummyRestService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("not implemented"))
}

// Service is the TSO grpc service.
type Service struct {
	*Server
}

// NewService creates a new TSO service.
func NewService(svr bs.Server) registry.RegistrableService {
	server, ok := svr.(*Server)
	if !ok {
		log.Fatal("create tso server failed")
	}
	return &Service{
		Server: server,
	}
}

// RegisterGRPCService registers the service to gRPC server.
func (s *Service) RegisterGRPCService(g *grpc.Server) {
	tsopb.RegisterTSOServer(g, s)
}

// RegisterRESTHandler registers the service to REST server.
func (s *Service) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) {
	handler, group := SetUpRestHandler(s)
	apiutil.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}

// Tso returns a stream of timestamps
func (s *Service) Tso(stream tsopb.TSO_TsoServer) error {
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
			s.dispatchTSORequest(ctx, &tsoRequest{
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
		if request.GetHeader().GetClusterId() != s.clusterID {
			return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, request.GetHeader().GetClusterId())
		}
		count := request.GetCount()
		ts, err := s.tsoAllocatorManager.HandleTSORequest(request.GetDcLocation(), count)
		if err != nil {
			return status.Errorf(codes.Unknown, err.Error())
		}
		tsoHandleDuration.Observe(time.Since(start).Seconds())
		response := &tsopb.TsoResponse{
			Header:    s.header(),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

func (s *Service) header() *tsopb.ResponseHeader {
	if s.clusterID == 0 {
		return s.wrapErrorToHeader(tsopb.ErrorType_NOT_BOOTSTRAPPED, "cluster id is not ready")
	}
	return &tsopb.ResponseHeader{ClusterId: s.clusterID}
}

func (s *Service) wrapErrorToHeader(errorType tsopb.ErrorType, message string) *tsopb.ResponseHeader {
	return s.errorHeader(&tsopb.Error{
		Type:    errorType,
		Message: message,
	})
}

func (s *Service) errorHeader(err *tsopb.Error) *tsopb.ResponseHeader {
	return &tsopb.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

type tsoRequest struct {
	forwardedHost string
	request       *tsopb.TsoRequest
	stream        tsopb.TSO_TsoServer
}

func (s *Service) dispatchTSORequest(ctx context.Context, request *tsoRequest, forwardedHost string, doneCh <-chan struct{}, errCh chan<- error) {
	tsoRequestChInterface, loaded := s.tsoDispatcher.LoadOrStore(forwardedHost, make(chan *tsoRequest, maxMergeTSORequests))
	if !loaded {
		tsDeadlineCh := make(chan deadline, 1)
		go s.handleDispatcher(ctx, forwardedHost, tsoRequestChInterface.(chan *tsoRequest), tsDeadlineCh, doneCh, errCh)
		go watchTSDeadline(ctx, tsDeadlineCh)
	}
	tsoRequestChInterface.(chan *tsoRequest) <- request
}

func (s *Service) handleDispatcher(ctx context.Context, forwardedHost string, tsoRequestCh <-chan *tsoRequest, tsDeadlineCh chan<- deadline, doneCh <-chan struct{}, errCh chan<- error) {
	dispatcherCtx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()
	defer s.tsoDispatcher.Delete(forwardedHost)

	var (
		forwardStream tsopb.TSO_TsoClient
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
			err = s.processTSORequests(forwardStream, requests[:pendingTSOReqCount])
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

func (s *Service) processTSORequests(forwardStream tsopb.TSO_TsoClient, requests []*tsoRequest) error {
	start := time.Now()
	// Merge the requests
	count := uint32(0)
	for _, request := range requests {
		count += request.request.GetCount()
	}
	req := &tsopb.TsoRequest{
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
	return s.finishTSORequest(requests, physical, firstLogical, suffixBits)
}

// Because of the suffix, we need to shift the count before we add it to the logical part.
func addLogical(logical, count int64, suffixBits uint32) int64 {
	return logical + count<<suffixBits
}

func (s *Service) finishTSORequest(requests []*tsoRequest, physical, firstLogical int64, suffixBits uint32) error {
	countSum := int64(0)
	for i := 0; i < len(requests); i++ {
		count := requests[i].request.GetCount()
		countSum += int64(count)
		response := &tsopb.TsoResponse{
			Header: s.header(),
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
