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
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/cluster"
)

// forwardToTSOService forwards the TSO requests to the TSO service.
func (s *GrpcServer) forwardToTSOService(stream pdpb.PD_TsoServer) error {
	var (
		server       = &tsoServer{stream: stream}
		forwarder    = newTSOForwarder(server)
		tsoStreamErr error
	)
	defer func() {
		s.concurrentTSOProxyStreamings.Add(-1)
		forwarder.cancel()
		if grpcutil.NeedRebuildConnection(tsoStreamErr) {
			s.closeDelegateClient(forwarder.host)
		}
	}()

	maxConcurrentTSOProxyStreamings := int32(s.GetMaxConcurrentTSOProxyStreamings())
	if maxConcurrentTSOProxyStreamings >= 0 {
		if newCount := s.concurrentTSOProxyStreamings.Add(1); newCount > maxConcurrentTSOProxyStreamings {
			return errors.WithStack(errs.ErrMaxCountTSOProxyRoutinesExceeded)
		}
	}

	tsDeadlineCh := make(chan *tsoutil.TSDeadline, 1)
	go tsoutil.WatchTSDeadline(stream.Context(), tsDeadlineCh)

	for {
		select {
		case <-s.ctx.Done():
			return errors.WithStack(s.ctx.Err())
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}

		request, err := server.recv(s.GetTSOProxyRecvFromClientTimeout())
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		if request.GetCount() == 0 {
			err = errs.ErrGenerateTimestamp.FastGenByArgs("tso count should be positive")
			return errs.ErrUnknown(err)
		}
		tsoStreamErr, err = s.handleTSOForwarding(stream.Context(), forwarder, request, tsDeadlineCh)
		if tsoStreamErr != nil {
			return tsoStreamErr
		}
		if err != nil {
			return err
		}
	}
}

type tsoForwarder struct {
	// The original source that we need to send the response back to.
	responser interface{ Send(*pdpb.TsoResponse) error }
	// The context for the forwarding stream.
	ctx context.Context
	// The cancel function for the forwarding stream.
	canceller context.CancelFunc
	// The current forwarding stream.
	stream tsopb.TSO_TsoClient
	// The current host of the forwarding stream.
	host string
}

func newTSOForwarder(responser interface{ Send(*pdpb.TsoResponse) error }) *tsoForwarder {
	return &tsoForwarder{
		responser: responser,
	}
}

func (f *tsoForwarder) cancel() {
	if f != nil && f.canceller != nil {
		f.canceller()
	}
}

// forwardTSORequest sends the TSO request with the current forward stream.
func (f *tsoForwarder) forwardTSORequest(
	request *pdpb.TsoRequest,
) (*tsopb.TsoResponse, error) {
	tsopbReq := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId:       request.GetHeader().GetClusterId(),
			SenderId:        request.GetHeader().GetSenderId(),
			KeyspaceId:      constant.DefaultKeyspaceID,
			KeyspaceGroupId: constant.DefaultKeyspaceGroupID,
		},
		Count: request.GetCount(),
	}

	failpoint.Inject("tsoProxySendToTSOTimeout", func() {
		// block until watchDeadline routine cancels the context.
		<-f.ctx.Done()
	})

	select {
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	default:
	}

	if err := f.stream.Send(tsopbReq); err != nil {
		return nil, err
	}

	failpoint.Inject("tsoProxyRecvFromTSOTimeout", func() {
		// block until watchDeadline routine cancels the context.
		<-f.ctx.Done()
	})

	select {
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	default:
	}

	return f.stream.Recv()
}

func (s *GrpcServer) handleTSOForwarding(
	ctx context.Context,
	forwarder *tsoForwarder,
	request *pdpb.TsoRequest,
	tsDeadlineCh chan<- *tsoutil.TSDeadline,
) (tsoStreamErr, sendErr error) {
	// Get the latest TSO primary address.
	targetHost, ok := s.GetServicePrimaryAddr(ctx, constant.TSOServiceName)
	if !ok || len(targetHost) == 0 {
		return errors.WithStack(errs.ErrNotFoundTSOAddr), nil
	}
	// Check if the forwarder is already built with the target host.
	if forwarder.stream == nil || forwarder.host != targetHost {
		// Cancel the old forwarder.
		forwarder.cancel()
		// Build a new forward stream.
		clientConn, err := s.getDelegateClient(s.ctx, targetHost)
		if err != nil {
			return errors.WithStack(err), nil
		}
		forwarder.stream, forwarder.ctx, forwarder.canceller, err = createTSOForwardStream(ctx, clientConn)
		if err != nil {
			return errors.WithStack(err), nil
		}
		forwarder.host = targetHost
	}

	// Forward the TSO request with the deadline.
	tsopbResp, err := s.forwardTSORequestWithDeadLine(forwarder, request, tsDeadlineCh)
	if err != nil {
		return errors.WithStack(err), nil
	}

	// The error types defined for tsopb and pdpb are different, so we need to convert them.
	var pdpbErr *pdpb.Error
	tsopbErr := tsopbResp.GetHeader().GetError()
	if tsopbErr != nil {
		if tsopbErr.Type == tsopb.ErrorType_OK {
			pdpbErr = &pdpb.Error{
				Type:    pdpb.ErrorType_OK,
				Message: tsopbErr.GetMessage(),
			}
		} else {
			// TODO: specify FORWARD FAILURE error type instead of UNKNOWN.
			pdpbErr = &pdpb.Error{
				Type:    pdpb.ErrorType_UNKNOWN,
				Message: tsopbErr.GetMessage(),
			}
		}
	}
	// Send the TSO response back to the original source.
	sendErr = forwarder.responser.Send(&pdpb.TsoResponse{
		Header: &pdpb.ResponseHeader{
			ClusterId: tsopbResp.GetHeader().GetClusterId(),
			Error:     pdpbErr,
		},
		Count:     tsopbResp.GetCount(),
		Timestamp: tsopbResp.GetTimestamp(),
	})

	return nil, errors.WithStack(sendErr)
}

func (s *GrpcServer) forwardTSORequestWithDeadLine(
	forwarder *tsoForwarder,
	request *pdpb.TsoRequest,
	tsDeadlineCh chan<- *tsoutil.TSDeadline,
) (*tsopb.TsoResponse, error) {
	var (
		forwardCtx    = forwarder.ctx
		forwardCancel = forwarder.canceller
		done          = make(chan struct{})
		dl            = tsoutil.NewTSDeadline(tsoutil.DefaultTSOProxyTimeout, done, forwardCancel)
	)
	select {
	case tsDeadlineCh <- dl:
	case <-forwardCtx.Done():
		return nil, forwardCtx.Err()
	}

	start := time.Now()
	resp, err := forwarder.forwardTSORequest(request)
	close(done)
	if err != nil {
		if strings.Contains(err.Error(), errs.NotLeaderErr) {
			s.tsoPrimaryWatcher.ForceLoad()
		}
		return nil, err
	}
	tsoProxyBatchSize.Observe(float64(request.GetCount()))
	tsoProxyHandleDuration.Observe(time.Since(start).Seconds())
	return resp, nil
}

func createTSOForwardStream(ctx context.Context, client *grpc.ClientConn) (tsopb.TSO_TsoClient, context.Context, context.CancelFunc, error) {
	done := make(chan struct{})
	forwardCtx, cancelForward := context.WithCancel(ctx)
	go grpcutil.CheckStream(forwardCtx, cancelForward, done)
	forwardStream, err := tsopb.NewTSOClient(client).Tso(forwardCtx)
	done <- struct{}{}
	return forwardStream, forwardCtx, cancelForward, err
}

func (s *GrpcServer) createRegionHeartbeatForwardStream(client *grpc.ClientConn) (pdpb.PD_RegionHeartbeatClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go grpcutil.CheckStream(ctx, cancel, done)
	forwardStream, err := pdpb.NewPDClient(client).RegionHeartbeat(ctx)
	done <- struct{}{}
	return forwardStream, cancel, err
}

func createRegionHeartbeatSchedulingStream(ctx context.Context, client *grpc.ClientConn) (schedulingpb.Scheduling_RegionHeartbeatClient, context.Context, context.CancelFunc, error) {
	done := make(chan struct{})
	forwardCtx, cancelForward := context.WithCancel(ctx)
	go grpcutil.CheckStream(forwardCtx, cancelForward, done)
	forwardStream, err := schedulingpb.NewSchedulingClient(client).RegionHeartbeat(forwardCtx)
	done <- struct{}{}
	return forwardStream, forwardCtx, cancelForward, err
}

func forwardRegionHeartbeatToScheduling(rc *cluster.RaftCluster, forwardStream schedulingpb.Scheduling_RegionHeartbeatClient, server *heartbeatServer, errCh chan error) {
	defer logutil.LogPanic()
	defer close(errCh)
	for {
		resp, err := forwardStream.Recv()
		if err == io.EOF {
			errCh <- errors.WithStack(err)
			return
		}
		if err != nil {
			errCh <- errors.WithStack(err)
			return
		}
		// TODO: find a better way to halt scheduling immediately.
		if rc.IsSchedulingHalted() {
			continue
		}
		// The error types defined for schedulingpb and pdpb are different, so we need to convert them.
		var pdpbErr *pdpb.Error
		schedulingpbErr := resp.GetHeader().GetError()
		if schedulingpbErr != nil {
			if schedulingpbErr.Type == schedulingpb.ErrorType_OK {
				pdpbErr = &pdpb.Error{
					Type:    pdpb.ErrorType_OK,
					Message: schedulingpbErr.GetMessage(),
				}
			} else {
				// TODO: specify FORWARD FAILURE error type instead of UNKNOWN.
				pdpbErr = &pdpb.Error{
					Type:    pdpb.ErrorType_UNKNOWN,
					Message: schedulingpbErr.GetMessage(),
				}
			}
		}
		response := &pdpb.RegionHeartbeatResponse{
			Header: &pdpb.ResponseHeader{
				ClusterId: resp.GetHeader().GetClusterId(),
				Error:     pdpbErr,
			},
			ChangePeer:      resp.GetChangePeer(),
			TransferLeader:  resp.GetTransferLeader(),
			RegionId:        resp.GetRegionId(),
			RegionEpoch:     resp.GetRegionEpoch(),
			TargetPeer:      resp.GetTargetPeer(),
			Merge:           resp.GetMerge(),
			SplitRegion:     resp.GetSplitRegion(),
			ChangePeerV2:    resp.GetChangePeerV2(),
			SwitchWitnesses: resp.GetSwitchWitnesses(),
		}

		if err := server.Send(response); err != nil {
			errCh <- errors.WithStack(err)
			return
		}
	}
}

func forwardRegionHeartbeatClientToServer(forwardStream pdpb.PD_RegionHeartbeatClient, server *heartbeatServer, errCh chan error) {
	defer logutil.LogPanic()
	defer close(errCh)
	for {
		resp, err := forwardStream.Recv()
		if err != nil {
			errCh <- errors.WithStack(err)
			return
		}
		if err := server.Send(resp); err != nil {
			errCh <- errors.WithStack(err)
			return
		}
	}
}

func forwardReportBucketClientToServer(forwardStream pdpb.PD_ReportBucketsClient, server *bucketHeartbeatServer, errCh chan error) {
	defer logutil.LogPanic()
	defer close(errCh)
	for {
		resp, err := forwardStream.CloseAndRecv()
		if err != nil {
			errCh <- errors.WithStack(err)
			return
		}
		if err := server.send(resp); err != nil {
			errCh <- errors.WithStack(err)
			return
		}
	}
}

func (s *GrpcServer) createReportBucketsForwardStream(client *grpc.ClientConn) (pdpb.PD_ReportBucketsClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go grpcutil.CheckStream(ctx, cancel, done)
	forwardStream, err := pdpb.NewPDClient(client).ReportBuckets(ctx)
	done <- struct{}{}
	return forwardStream, cancel, err
}

func (s *GrpcServer) getDelegateClient(ctx context.Context, forwardedHost string) (*grpc.ClientConn, error) {
	client, ok := s.clientConns.Load(forwardedHost)
	if ok {
		// Mostly, the connection is already established, and return it directly.
		return client.(*grpc.ClientConn), nil
	}

	tlsConfig, err := s.GetTLSConfig().ToTLSConfig()
	if err != nil {
		return nil, err
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, defaultGRPCDialTimeout)
	defer cancel()
	newConn, err := grpcutil.GetClientConn(ctxTimeout, forwardedHost, tlsConfig)
	if err != nil {
		return nil, err
	}
	conn, loaded := s.clientConns.LoadOrStore(forwardedHost, newConn)
	if !loaded {
		// Successfully stored the connection we created.
		return newConn, nil
	}
	// Loaded a connection created/stored by another goroutine, so close the one we created
	// and return the one we loaded.
	newConn.Close()
	return conn.(*grpc.ClientConn), nil
}

func (s *GrpcServer) closeDelegateClient(forwardedHost string) {
	client, ok := s.clientConns.LoadAndDelete(forwardedHost)
	if !ok {
		return
	}
	client.(*grpc.ClientConn).Close()
	log.Debug("close delegate client connection", zap.String("forwarded-host", forwardedHost))
}

func (s *GrpcServer) isLocalRequest(host string) bool {
	failpoint.Inject("useForwardRequest", func() {
		failpoint.Return(false)
	})
	if host == "" {
		return true
	}
	memberAddrs := s.GetMember().Member().GetClientUrls()
	for _, addr := range memberAddrs {
		if addr == host {
			return true
		}
	}
	return false
}

func (s *GrpcServer) getGlobalTSO(ctx context.Context) (pdpb.Timestamp, error) {
	if !s.IsServiceIndependent(constant.TSOServiceName) {
		return s.tsoAllocatorManager.HandleRequest(ctx, 1)
	}
	request := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId:       keypath.ClusterID(),
			KeyspaceId:      constant.DefaultKeyspaceID,
			KeyspaceGroupId: constant.DefaultKeyspaceGroupID,
		},
		Count: 1,
	}
	var (
		forwardedHost string
		forwardStream *streamWrapper
		ts            *tsopb.TsoResponse
		err           error
		ok            bool
	)
	handleStreamError := func(err error) (needRetry bool) {
		if strings.Contains(err.Error(), errs.NotLeaderErr) {
			s.tsoPrimaryWatcher.ForceLoad()
			log.Warn("force to load tso primary address due to error", zap.Error(err), zap.String("tso-addr", forwardedHost))
			return true
		}
		if grpcutil.NeedRebuildConnection(err) {
			s.tsoClientPool.Lock()
			delete(s.tsoClientPool.clients, forwardedHost)
			s.tsoClientPool.Unlock()
			log.Warn("client connection removed due to error", zap.Error(err), zap.String("tso-addr", forwardedHost))
			return true
		}
		return false
	}
	for i := range maxRetryTimesRequestTSOServer {
		if i > 0 {
			time.Sleep(retryIntervalRequestTSOServer)
		}
		forwardedHost, ok = s.GetServicePrimaryAddr(ctx, constant.TSOServiceName)
		if !ok || forwardedHost == "" {
			return pdpb.Timestamp{}, errs.ErrNotFoundTSOAddr
		}
		forwardStream, err = s.getTSOForwardStream(forwardedHost)
		if err != nil {
			return pdpb.Timestamp{}, err
		}
		start := time.Now()
		forwardStream.Lock()
		err = forwardStream.Send(request)
		if err != nil {
			if needRetry := handleStreamError(err); needRetry {
				forwardStream.Unlock()
				continue
			}
			log.Error("send request to tso primary server failed", zap.Error(err), zap.String("tso-addr", forwardedHost))
			forwardStream.Unlock()
			return pdpb.Timestamp{}, err
		}
		ts, err = forwardStream.Recv()
		forwardStream.Unlock()
		forwardTsoDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			if needRetry := handleStreamError(err); needRetry {
				continue
			}
			log.Error("receive response from tso primary server failed", zap.Error(err), zap.String("tso-addr", forwardedHost))
			return pdpb.Timestamp{}, err
		}
		return *ts.GetTimestamp(), nil
	}
	log.Error("get global tso from tso primary server failed after retry", zap.Error(err), zap.String("tso-addr", forwardedHost))
	return pdpb.Timestamp{}, err
}

func (s *GrpcServer) getTSOForwardStream(forwardedHost string) (*streamWrapper, error) {
	s.tsoClientPool.RLock()
	forwardStream, ok := s.tsoClientPool.clients[forwardedHost]
	s.tsoClientPool.RUnlock()
	if ok {
		// This is the common case to return here
		return forwardStream, nil
	}

	s.tsoClientPool.Lock()
	defer s.tsoClientPool.Unlock()

	// Double check after entering the critical section
	forwardStream, ok = s.tsoClientPool.clients[forwardedHost]
	if ok {
		return forwardStream, nil
	}

	// Now let's create the client connection and the forward stream
	client, err := s.getDelegateClient(s.ctx, forwardedHost)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go grpcutil.CheckStream(ctx, cancel, done)
	tsoClient, err := tsopb.NewTSOClient(client).Tso(ctx)
	done <- struct{}{}
	if err != nil {
		return nil, err
	}
	forwardStream = &streamWrapper{
		TSO_TsoClient: tsoClient,
	}
	s.tsoClientPool.clients[forwardedHost] = forwardStream
	return forwardStream, nil
}
