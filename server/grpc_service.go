// Copyright 2017 TiKV Project Authors.
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
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"runtime"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/cluster"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	heartbeatSendTimeout          = 5 * time.Second
	maxRetryTimesRequestTSOServer = 6
	retryIntervalRequestTSOServer = 500 * time.Millisecond
	getMinTSFromTSOServerTimeout  = 1 * time.Second
	defaultGRPCDialTimeout        = 3 * time.Second

	gRPCServiceName = "pdpb.PD"
)

// gRPC errors
var (
	// ErrNotLeader is returned when current server is not the leader and not possible to process request.
	// TODO: work as proxy.
	ErrNotLeader                        = status.Errorf(codes.Unavailable, "not leader")
	ErrNotStarted                       = status.Errorf(codes.Unavailable, "server not started")
	ErrSendHeartbeatTimeout             = status.Errorf(codes.DeadlineExceeded, "send heartbeat timeout")
	ErrNotFoundTSOAddr                  = status.Errorf(codes.NotFound, "not found tso address")
	ErrNotFoundSchedulingAddr           = status.Errorf(codes.NotFound, "not found scheduling address")
	ErrNotFoundService                  = status.Errorf(codes.NotFound, "not found service")
	ErrForwardTSOTimeout                = status.Errorf(codes.DeadlineExceeded, "forward tso request timeout")
	ErrMaxCountTSOProxyRoutinesExceeded = status.Errorf(codes.ResourceExhausted, "max count of concurrent tso proxy routines exceeded")
	ErrTSOProxyRecvFromClientTimeout    = status.Errorf(codes.DeadlineExceeded, "tso proxy timeout when receiving from client; stream closed by server")
	ErrEtcdNotStarted                   = status.Errorf(codes.Unavailable, "server is started, but etcd not started")
	ErrFollowerHandlingNotAllowed       = status.Errorf(codes.Unavailable, "not leader and follower handling not allowed")
)

var (
	errRegionHeartbeatSend   = forwardFailCounter.WithLabelValues("region_heartbeat", "send")
	errRegionHeartbeatClient = forwardFailCounter.WithLabelValues("region_heartbeat", "client")
	errRegionHeartbeatStream = forwardFailCounter.WithLabelValues("region_heartbeat", "stream")
	errRegionHeartbeatRecv   = forwardFailCounter.WithLabelValues("region_heartbeat", "recv")
	errScatterRegionSend     = forwardFailCounter.WithLabelValues("scatter_region", "send")
	errSplitRegionsSend      = forwardFailCounter.WithLabelValues("split_regions", "send")
	errStoreHeartbeatSend    = forwardFailCounter.WithLabelValues("store_heartbeat", "send")
	errGetOperatorSend       = forwardFailCounter.WithLabelValues("get_operator", "send")
)

// GrpcServer wraps Server to provide grpc service.
type GrpcServer struct {
	*Server
	schedulingClient             atomic.Value
	concurrentTSOProxyStreamings atomic.Int32
}

// tsoServer wraps PD_TsoServer to ensure when any error
// occurs on Send() or Recv(), both endpoints will be closed.
type tsoServer struct {
	stream pdpb.PD_TsoServer
	closed int32
}

type pdpbTSORequest struct {
	request *pdpb.TsoRequest
	err     error
}

func (s *tsoServer) send(m *pdpb.TsoResponse) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.EOF
	}
	done := make(chan error, 1)
	go func() {
		defer logutil.LogPanic()
		failpoint.Inject("tsoProxyFailToSendToClient", func() {
			done <- errors.New("injected error")
			failpoint.Return()
		})
		done <- s.stream.Send(m)
	}()
	timer := time.NewTimer(tsoutil.DefaultTSOProxyTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			atomic.StoreInt32(&s.closed, 1)
		}
		return errors.WithStack(err)
	case <-timer.C:
		atomic.StoreInt32(&s.closed, 1)
		return ErrForwardTSOTimeout
	}
}

func (s *tsoServer) recv(timeout time.Duration) (*pdpb.TsoRequest, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, io.EOF
	}
	failpoint.Inject("tsoProxyRecvFromClientTimeout", func(val failpoint.Value) {
		if customTimeoutInSeconds, ok := val.(int); ok {
			timeout = time.Duration(customTimeoutInSeconds) * time.Second
		}
	})
	requestCh := make(chan *pdpbTSORequest, 1)
	go func() {
		defer logutil.LogPanic()
		request, err := s.stream.Recv()
		requestCh <- &pdpbTSORequest{request: request, err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case req := <-requestCh:
		if req.err != nil {
			atomic.StoreInt32(&s.closed, 1)
			return nil, errors.WithStack(req.err)
		}
		return req.request, nil
	case <-timer.C:
		atomic.StoreInt32(&s.closed, 1)
		return nil, ErrTSOProxyRecvFromClientTimeout
	}
}

// heartbeatServer wraps PD_RegionHeartbeatServer to ensure when any error
// occurs on Send() or Recv(), both endpoints will be closed.
type heartbeatServer struct {
	stream pdpb.PD_RegionHeartbeatServer
	closed int32
}

// Send wraps Send() of PD_RegionHeartbeatServer.
func (s *heartbeatServer) Send(m core.RegionHeartbeatResponse) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.EOF
	}
	done := make(chan error, 1)
	go func() {
		defer logutil.LogPanic()
		done <- s.stream.Send(m.(*pdpb.RegionHeartbeatResponse))
	}()
	timer := time.NewTimer(heartbeatSendTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			atomic.StoreInt32(&s.closed, 1)
		}
		return errors.WithStack(err)
	case <-timer.C:
		atomic.StoreInt32(&s.closed, 1)
		return ErrSendHeartbeatTimeout
	}
}

// Recv wraps Recv() of PD_RegionHeartbeatServer.
func (s *heartbeatServer) Recv() (*pdpb.RegionHeartbeatRequest, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, io.EOF
	}
	req, err := s.stream.Recv()
	if err != nil {
		atomic.StoreInt32(&s.closed, 1)
		return nil, errors.WithStack(err)
	}
	return req, nil
}

type schedulingClient struct {
	client  schedulingpb.SchedulingClient
	primary string
}

func (s *schedulingClient) getClient() schedulingpb.SchedulingClient {
	if s == nil {
		return nil
	}
	return s.client
}

func (s *schedulingClient) getPrimaryAddr() string {
	if s == nil {
		return ""
	}
	return s.primary
}

type request interface {
	GetHeader() *pdpb.RequestHeader
}

type forwardFn func(ctx context.Context, client *grpc.ClientConn) (any, error)

func (s *GrpcServer) unaryMiddleware(ctx context.Context, req request, fn forwardFn) (rsp any, err error) {
	return s.unaryFollowerMiddleware(ctx, req, fn, nil)
}

// unaryFollowerMiddleware adds the check of followers enable compared to unaryMiddleware.
func (s *GrpcServer) unaryFollowerMiddleware(ctx context.Context, req request, fn forwardFn, allowFollower *bool) (rsp any, err error) {
	failpoint.Inject("customTimeout", func() {
		time.Sleep(5 * time.Second)
	})
	forwardedHost := grpcutil.GetForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return fn(ctx, client)
	}
	if err := s.validateRoleInRequest(ctx, req.GetHeader(), allowFollower); err != nil {
		return nil, err
	}
	return nil, nil
}

// GetClusterInfo implements gRPC PDServer.
func (s *GrpcServer) GetClusterInfo(context.Context, *pdpb.GetClusterInfoRequest) (*pdpb.GetClusterInfoResponse, error) {
	// Here we purposely do not check the cluster ID because the client does not know the correct cluster ID
	// at startup and needs to get the cluster ID with the first request (i.e. GetMembers).
	if s.IsClosed() {
		return &pdpb.GetClusterInfoResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, errs.ErrServerNotStarted.FastGenByArgs().Error()),
		}, nil
	}

	var tsoServiceAddrs []string
	svcModes := make([]pdpb.ServiceMode, 0)
	if s.IsServiceIndependent(constant.TSOServiceName) {
		svcModes = append(svcModes, pdpb.ServiceMode_API_SVC_MODE)
		tsoServiceAddrs = s.keyspaceGroupManager.GetTSOServiceAddrs()
	} else {
		svcModes = append(svcModes, pdpb.ServiceMode_PD_SVC_MODE)
	}

	return &pdpb.GetClusterInfoResponse{
		Header:       wrapHeader(),
		ServiceModes: svcModes,
		TsoUrls:      tsoServiceAddrs,
	}, nil
}

// GetMinTS implements gRPC PDServer. In PD service mode, it simply returns a timestamp.
// In API service mode, it queries all tso servers and gets the minimum timestamp across
// all keyspace groups.
func (s *GrpcServer) GetMinTS(
	ctx context.Context, request *pdpb.GetMinTSRequest,
) (*pdpb.GetMinTSResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetMinTSResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetMinTS(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetMinTSResponse), nil
	}

	var (
		minTS *pdpb.Timestamp
		err   error
	)
	if s.IsServiceIndependent(constant.TSOServiceName) {
		minTS, err = s.GetMinTSFromTSOService(tso.GlobalDCLocation)
	} else {
		start := time.Now()
		ts, internalErr := s.tsoAllocatorManager.HandleRequest(ctx, tso.GlobalDCLocation, 1)
		if internalErr == nil {
			tsoHandleDuration.Observe(time.Since(start).Seconds())
		}
		minTS = &ts
	}
	if err != nil {
		return &pdpb.GetMinTSResponse{
			Header:    wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			Timestamp: minTS,
		}, nil
	}

	return &pdpb.GetMinTSResponse{
		Header:    wrapHeader(),
		Timestamp: minTS,
	}, nil
}

// GetMinTSFromTSOService queries all tso servers and gets the minimum timestamp across
// all keyspace groups.
func (s *GrpcServer) GetMinTSFromTSOService(dcLocation string) (*pdpb.Timestamp, error) {
	addrs := s.keyspaceGroupManager.GetTSOServiceAddrs()
	if len(addrs) == 0 {
		return &pdpb.Timestamp{}, errs.ErrGetMinTS.FastGenByArgs("no tso servers/pods discovered")
	}

	// Get the minimal timestamp from the TSO servers/pods
	var mutex syncutil.Mutex
	resps := make([]*tsopb.GetMinTSResponse, 0)
	wg := sync.WaitGroup{}
	wg.Add(len(addrs))
	for _, addr := range addrs {
		go func(addr string) {
			defer wg.Done()
			resp, err := s.getMinTSFromSingleServer(s.ctx, dcLocation, addr)
			if err != nil || resp == nil {
				log.Warn("failed to get min ts from tso server",
					zap.String("address", addr), zap.Error(err))
				return
			}
			mutex.Lock()
			defer mutex.Unlock()
			resps = append(resps, resp)
		}(addr)
	}
	wg.Wait()

	// Check the results. The returned minimal timestamp is valid if all the conditions are met:
	// 1. The number of responses is equal to the number of TSO servers/pods.
	// 2. The number of keyspace groups asked is equal to the number of TSO servers/pods.
	// 3. The minimal timestamp is not zero.
	var (
		minTS               *pdpb.Timestamp
		keyspaceGroupsAsked uint32
	)
	if len(resps) == 0 {
		return &pdpb.Timestamp{}, errs.ErrGetMinTS.FastGenByArgs("none of tso server/pod responded")
	}
	emptyTS := &pdpb.Timestamp{}
	keyspaceGroupsTotal := resps[0].KeyspaceGroupsTotal
	for _, resp := range resps {
		if resp.KeyspaceGroupsTotal == 0 {
			return &pdpb.Timestamp{}, errs.ErrGetMinTS.FastGenByArgs("the tso service has no keyspace group")
		}
		if resp.KeyspaceGroupsTotal != keyspaceGroupsTotal {
			return &pdpb.Timestamp{}, errs.ErrGetMinTS.FastGenByArgs(
				"the tso service has inconsistent keyspace group total count")
		}
		keyspaceGroupsAsked += resp.KeyspaceGroupsServing
		if tsoutil.CompareTimestamp(resp.Timestamp, emptyTS) > 0 &&
			(minTS == nil || tsoutil.CompareTimestamp(resp.Timestamp, minTS) < 0) {
			minTS = resp.Timestamp
		}
	}

	if keyspaceGroupsAsked != keyspaceGroupsTotal {
		return &pdpb.Timestamp{}, errs.ErrGetMinTS.FastGenByArgs(
			fmt.Sprintf("can't query all the tso keyspace groups. Asked %d, expected %d",
				keyspaceGroupsAsked, keyspaceGroupsTotal))
	}

	if minTS == nil {
		return &pdpb.Timestamp{}, errs.ErrGetMinTS.FastGenByArgs("the tso service is not ready")
	}

	return minTS, nil
}

func (s *GrpcServer) getMinTSFromSingleServer(
	ctx context.Context, dcLocation, tsoSrvAddr string,
) (*tsopb.GetMinTSResponse, error) {
	cc, err := s.getDelegateClient(s.ctx, tsoSrvAddr)
	if err != nil {
		return nil, errs.ErrClientGetMinTSO.FastGenByArgs(
			fmt.Sprintf("can't connect to tso server %s", tsoSrvAddr))
	}

	cctx, cancel := context.WithTimeout(ctx, getMinTSFromTSOServerTimeout)
	defer cancel()

	resp, err := tsopb.NewTSOClient(cc).GetMinTS(
		cctx, &tsopb.GetMinTSRequest{
			Header: &tsopb.RequestHeader{
				ClusterId: keypath.ClusterID(),
			},
			DcLocation: dcLocation,
		})
	if err != nil {
		attachErr := errors.Errorf("error:%s target:%s status:%s",
			err, cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientGetMinTSO.Wrap(attachErr).GenWithStackByCause()
	}
	if resp == nil {
		attachErr := errors.Errorf("error:%s target:%s status:%s",
			"no min ts info collected", cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientGetMinTSO.Wrap(attachErr).GenWithStackByCause()
	}
	if resp.GetHeader().GetError() != nil {
		attachErr := errors.Errorf("error:%s target:%s status:%s",
			resp.GetHeader().GetError().String(), cc.Target(), cc.GetState().String())
		return nil, errs.ErrClientGetMinTSO.Wrap(attachErr).GenWithStackByCause()
	}

	return resp, nil
}

// GetMembers implements gRPC PDServer.
func (s *GrpcServer) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetMembersResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	// Here we purposely do not check the cluster ID because the client does not know the correct cluster ID
	// at startup and needs to get the cluster ID with the first request (i.e. GetMembers).
	if s.IsClosed() {
		return &pdpb.GetMembersResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, errs.ErrServerNotStarted.FastGenByArgs().Error()),
		}, nil
	}
	members, err := cluster.GetMembers(s.GetClient())
	if err != nil {
		return &pdpb.GetMembersResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	var etcdLeader, pdLeader *pdpb.Member
	leaderID := s.member.GetEtcdLeader()
	for _, m := range members {
		if m.MemberId == leaderID {
			etcdLeader = m
			break
		}
	}

	tsoAllocatorLeaders := make(map[string]*pdpb.Member)
	if !s.IsServiceIndependent(constant.TSOServiceName) {
		tsoAllocatorManager := s.GetTSOAllocatorManager()
		tsoAllocatorLeaders, err = tsoAllocatorManager.GetLocalAllocatorLeaders()
	}
	if err != nil {
		return &pdpb.GetMembersResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	leader := s.member.GetLeader()
	for _, m := range members {
		if m.MemberId == leader.GetMemberId() {
			pdLeader = m
			break
		}
	}

	return &pdpb.GetMembersResponse{
		Header:              wrapHeader(),
		Members:             members,
		Leader:              pdLeader,
		EtcdLeader:          etcdLeader,
		TsoAllocatorLeaders: tsoAllocatorLeaders,
	}, nil
}

// Tso implements gRPC PDServer.
func (s *GrpcServer) Tso(stream pdpb.PD_TsoServer) error {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return err
		}
	}
	if s.IsServiceIndependent(constant.TSOServiceName) {
		return s.forwardTSO(stream)
	}

	tsDeadlineCh := make(chan *tsoutil.TSDeadline, 1)
	go tsoutil.WatchTSDeadline(stream.Context(), tsDeadlineCh)

	var (
		doneCh chan struct{}
		errCh  chan error
		// The following are tso forward stream related variables.
		forwardStream     tsopb.TSO_TsoClient
		cancelForward     context.CancelFunc
		forwardCtx        context.Context
		tsoStreamErr      error
		lastForwardedHost string
	)

	defer func() {
		if cancelForward != nil {
			cancelForward()
		}
		if grpcutil.NeedRebuildConnection(tsoStreamErr) {
			s.closeDelegateClient(lastForwardedHost)
		}
	}()

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

		forwardedHost := grpcutil.GetForwardedHost(stream.Context())
		if !s.isLocalRequest(forwardedHost) {
			clientConn, err := s.getDelegateClient(s.ctx, forwardedHost)
			if err != nil {
				return errors.WithStack(err)
			}

			if errCh == nil {
				doneCh = make(chan struct{})
				defer close(doneCh) // nolint
				errCh = make(chan error)
			}

			tsoRequest := tsoutil.NewPDProtoRequest(forwardedHost, clientConn, request, stream)
			s.tsoDispatcher.DispatchRequest(ctx, tsoRequest, s.pdProtoFactory, doneCh, errCh, s.tsoPrimaryWatcher)
			continue
		}

		if s.IsServiceIndependent(constant.TSOServiceName) {
			if request.GetCount() == 0 {
				err = errs.ErrGenerateTimestamp.FastGenByArgs("tso count should be positive")
				return status.Error(codes.Unknown, err.Error())
			}
			forwardCtx, cancelForward, forwardStream, lastForwardedHost, tsoStreamErr, err = s.handleTSOForwarding(forwardCtx, forwardStream, stream, nil, request, tsDeadlineCh, lastForwardedHost, cancelForward)
			if tsoStreamErr != nil {
				return tsoStreamErr
			}
			if err != nil {
				return err
			}
			continue
		}

		start := time.Now()
		// TSO uses leader lease to determine validity. No need to check leader here.
		if s.IsClosed() {
			return status.Errorf(codes.Unknown, "server not started")
		}
		if clusterID := keypath.ClusterID(); request.GetHeader().GetClusterId() != clusterID {
			return status.Errorf(codes.FailedPrecondition,
				"mismatch cluster id, need %d but got %d", clusterID, request.GetHeader().GetClusterId())
		}
		count := request.GetCount()
		ctx, task := trace.NewTask(ctx, "tso")
		ts, err := s.tsoAllocatorManager.HandleRequest(ctx, request.GetDcLocation(), count)
		task.End()
		tsoHandleDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			return status.Error(codes.Unknown, err.Error())
		}
		response := &pdpb.TsoResponse{
			Header:    wrapHeader(),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

// Bootstrap implements gRPC PDServer.
func (s *GrpcServer) Bootstrap(ctx context.Context, request *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.BootstrapResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).Bootstrap(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.BootstrapResponse), nil
	}

	rc := s.GetRaftCluster()
	if rc != nil {
		err := &pdpb.Error{
			Type:    pdpb.ErrorType_ALREADY_BOOTSTRAPPED,
			Message: "cluster is already bootstrapped",
		}
		return &pdpb.BootstrapResponse{
			Header: errorHeader(err),
		}, nil
	}

	res, err := s.bootstrapCluster(request)
	if err != nil {
		return &pdpb.BootstrapResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	res.Header = wrapHeader()
	return res, nil
}

// IsBootstrapped implements gRPC PDServer.
func (s *GrpcServer) IsBootstrapped(ctx context.Context, request *pdpb.IsBootstrappedRequest) (*pdpb.IsBootstrappedResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.IsBootstrappedResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).IsBootstrapped(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.IsBootstrappedResponse), err
	}

	rc := s.GetRaftCluster()
	return &pdpb.IsBootstrappedResponse{
		Header:       wrapHeader(),
		Bootstrapped: rc != nil,
	}, nil
}

// AllocID implements gRPC PDServer.
func (s *GrpcServer) AllocID(ctx context.Context, request *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.AllocIDResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).AllocID(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.AllocIDResponse), err
	}

	// We can use an allocator for all types ID allocation.
	id, err := s.idAllocator.Alloc()
	if err != nil {
		return &pdpb.AllocIDResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.AllocIDResponse{
		Header: wrapHeader(),
		Id:     id,
	}, nil
}

// IsSnapshotRecovering implements gRPC PDServer.
func (s *GrpcServer) IsSnapshotRecovering(ctx context.Context, _ *pdpb.IsSnapshotRecoveringRequest) (*pdpb.IsSnapshotRecoveringResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.IsSnapshotRecoveringResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	// recovering mark is stored in etcd directly, there's no need to forward.
	marked, err := s.Server.IsSnapshotRecovering(ctx)
	if err != nil {
		return &pdpb.IsSnapshotRecoveringResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}
	return &pdpb.IsSnapshotRecoveringResponse{
		Header: wrapHeader(),
		Marked: marked,
	}, nil
}

// GetStore implements gRPC PDServer.
func (s *GrpcServer) GetStore(ctx context.Context, request *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetStoreResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetStore(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetStoreResponse), err
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetStoreResponse{Header: notBootstrappedHeader()}, nil
	}

	storeID := request.GetStoreId()
	store := rc.GetStore(storeID)
	if store == nil {
		return &pdpb.GetStoreResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				fmt.Sprintf("invalid store ID %d, not found", storeID)),
		}, nil
	}
	return &pdpb.GetStoreResponse{
		Header: wrapHeader(),
		Store:  store.GetMeta(),
		Stats:  store.GetStoreStats(),
	}, nil
}

// checkStore returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
func checkStore(rc *cluster.RaftCluster, storeID uint64) *pdpb.Error {
	store := rc.GetStore(storeID)
	if store != nil {
		if store.IsRemoved() {
			return &pdpb.Error{
				Type:    pdpb.ErrorType_STORE_TOMBSTONE,
				Message: "store is tombstone",
			}
		}
	}
	return nil
}

// PutStore implements gRPC PDServer.
func (s *GrpcServer) PutStore(ctx context.Context, request *pdpb.PutStoreRequest) (*pdpb.PutStoreResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.PutStoreResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).PutStore(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.PutStoreResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.PutStoreResponse{Header: notBootstrappedHeader()}, nil
	}

	store := request.GetStore()
	if pberr := checkStore(rc, store.GetId()); pberr != nil {
		return &pdpb.PutStoreResponse{
			Header: errorHeader(pberr),
		}, nil
	}

	// NOTE: can be removed when placement rules feature is enabled by default.
	if !s.GetConfig().Replication.EnablePlacementRules && core.IsStoreContainLabel(store, core.EngineKey, core.EngineTiFlash) {
		return &pdpb.PutStoreResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				"placement rules is disabled"),
		}, nil
	}

	if err := rc.PutMetaStore(store); err != nil {
		return &pdpb.PutStoreResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	log.Info("put store ok", zap.Stringer("store", store))
	CheckPDVersionWithClusterVersion(s.persistOptions)

	return &pdpb.PutStoreResponse{
		Header:            wrapHeader(),
		ReplicationStatus: rc.GetReplicationMode().GetReplicationStatus(),
	}, nil
}

// GetAllStores implements gRPC PDServer.
func (s *GrpcServer) GetAllStores(ctx context.Context, request *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetAllStoresResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetAllStores(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetAllStoresResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetAllStoresResponse{Header: notBootstrappedHeader()}, nil
	}

	// Don't return tombstone stores.
	var stores []*metapb.Store
	if request.GetExcludeTombstoneStores() {
		for _, store := range rc.GetMetaStores() {
			if store.GetNodeState() != metapb.NodeState_Removed {
				stores = append(stores, store)
			}
		}
	} else {
		stores = rc.GetMetaStores()
	}

	return &pdpb.GetAllStoresResponse{
		Header: wrapHeader(),
		Stores: stores,
	}, nil
}

// StoreHeartbeat implements gRPC PDServer.
func (s *GrpcServer) StoreHeartbeat(ctx context.Context, request *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.StoreHeartbeatResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, errs.ErrRateLimitExceeded.FastGenByArgs().Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).StoreHeartbeat(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.StoreHeartbeatResponse), err
	}

	if request.GetStats() == nil {
		return nil, errors.Errorf("invalid store heartbeat command, but %v", request)
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.StoreHeartbeatResponse{Header: notBootstrappedHeader()}, nil
	}

	if pberr := checkStore(rc, request.GetStats().GetStoreId()); pberr != nil {
		return &pdpb.StoreHeartbeatResponse{
			Header: errorHeader(pberr),
		}, nil
	}
	storeID := request.GetStats().GetStoreId()
	store := rc.GetStore(storeID)
	if store == nil {
		return &pdpb.StoreHeartbeatResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				fmt.Sprintf("store %v not found", storeID)),
		}, nil
	}

	resp := &pdpb.StoreHeartbeatResponse{Header: wrapHeader()}
	// Bypass stats handling if the store report for unsafe recover is not empty.
	if request.GetStoreReport() == nil {
		storeAddress := store.GetAddress()
		storeLabel := strconv.FormatUint(storeID, 10)
		start := time.Now()

		err := rc.HandleStoreHeartbeat(request, resp)
		if err != nil {
			return &pdpb.StoreHeartbeatResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
					err.Error()),
			}, nil
		}

		s.handleDamagedStore(request.GetStats())
		storeHeartbeatHandleDuration.WithLabelValues(storeAddress, storeLabel).Observe(time.Since(start).Seconds())
		if rc.IsServiceIndependent(constant.SchedulingServiceName) {
			forwardCli, _ := s.updateSchedulingClient(ctx)
			cli := forwardCli.getClient()
			if cli != nil {
				req := &schedulingpb.StoreHeartbeatRequest{
					Header: &schedulingpb.RequestHeader{
						ClusterId: request.GetHeader().GetClusterId(),
						SenderId:  request.GetHeader().GetSenderId(),
					},
					Stats: request.GetStats(),
				}
				if _, err := cli.StoreHeartbeat(ctx, req); err != nil {
					errStoreHeartbeatSend.Inc()
					log.Debug("forward store heartbeat failed", zap.Error(err))
					// reset to let it be updated in the next request
					s.schedulingClient.CompareAndSwap(forwardCli, &schedulingClient{})
				}
			}
		}
	}

	if status := request.GetDrAutosyncStatus(); status != nil {
		rc.GetReplicationMode().UpdateStoreDRStatus(request.GetStats().GetStoreId(), status)
	}

	resp.ReplicationStatus = rc.GetReplicationMode().GetReplicationStatus()
	resp.ClusterVersion = rc.GetClusterVersion()
	rc.GetUnsafeRecoveryController().HandleStoreHeartbeat(request, resp)

	return resp, nil
}

// 1. forwardedHost is empty, return nil
// 2. forwardedHost is not empty and forwardedHost is equal to pre, return pre
// 3. the rest of cases, update forwardedHost and return new client
func (s *GrpcServer) updateSchedulingClient(ctx context.Context) (*schedulingClient, error) {
	forwardedHost, _ := s.GetServicePrimaryAddr(ctx, constant.SchedulingServiceName)
	if forwardedHost == "" {
		return nil, ErrNotFoundSchedulingAddr
	}

	pre := s.schedulingClient.Load()
	if pre != nil && forwardedHost == pre.(*schedulingClient).getPrimaryAddr() {
		return pre.(*schedulingClient), nil
	}

	client, err := s.getDelegateClient(ctx, forwardedHost)
	if err != nil {
		log.Error("get delegate client failed", zap.Error(err))
		return nil, err
	}
	forwardCli := &schedulingClient{
		client:  schedulingpb.NewSchedulingClient(client),
		primary: forwardedHost,
	}
	swapped := s.schedulingClient.CompareAndSwap(pre, forwardCli)
	if swapped {
		oldForwardedHost := ""
		if pre != nil {
			oldForwardedHost = pre.(*schedulingClient).getPrimaryAddr()
		}
		log.Info("update scheduling client", zap.String("old-forwarded-host", oldForwardedHost), zap.String("new-forwarded-host", forwardedHost))
	}
	return forwardCli, nil
}

// bucketHeartbeatServer wraps PD_ReportBucketsServer to ensure when any error
// occurs on SendAndClose() or Recv(), both endpoints will be closed.
type bucketHeartbeatServer struct {
	stream pdpb.PD_ReportBucketsServer
	closed int32
}

func (b *bucketHeartbeatServer) send(bucket *pdpb.ReportBucketsResponse) error {
	if atomic.LoadInt32(&b.closed) == 1 {
		return status.Errorf(codes.Canceled, "stream is closed")
	}
	done := make(chan error, 1)
	go func() {
		defer logutil.LogPanic()
		done <- b.stream.SendAndClose(bucket)
	}()
	timer := time.NewTimer(heartbeatSendTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			atomic.StoreInt32(&b.closed, 1)
		}
		return err
	case <-timer.C:
		atomic.StoreInt32(&b.closed, 1)
		return ErrSendHeartbeatTimeout
	}
}

func (b *bucketHeartbeatServer) recv() (*pdpb.ReportBucketsRequest, error) {
	if atomic.LoadInt32(&b.closed) == 1 {
		return nil, io.EOF
	}
	req, err := b.stream.Recv()
	if err != nil {
		atomic.StoreInt32(&b.closed, 1)
		return nil, errors.WithStack(err)
	}
	return req, nil
}

// ReportBuckets implements gRPC PDServer
func (s *GrpcServer) ReportBuckets(stream pdpb.PD_ReportBucketsServer) error {
	var (
		server            = &bucketHeartbeatServer{stream: stream}
		forwardStream     pdpb.PD_ReportBucketsClient
		cancel            context.CancelFunc
		lastForwardedHost string
		errCh             chan error
	)
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return err
		}
	}
	for {
		request, err := server.recv()
		failpoint.Inject("grpcClientClosed", func() {
			err = status.Error(codes.Canceled, "grpc client closed")
			request = nil
		})
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		forwardedHost := grpcutil.GetForwardedHost(stream.Context())
		failpoint.Inject("grpcClientClosed", func() {
			forwardedHost = s.GetMember().Member().GetClientUrls()[0]
		})
		if !s.isLocalRequest(forwardedHost) {
			if forwardStream == nil || lastForwardedHost != forwardedHost {
				if cancel != nil {
					cancel()
				}
				client, err := s.getDelegateClient(s.ctx, forwardedHost)
				if err != nil {
					return err
				}
				log.Info("create bucket report forward stream", zap.String("forwarded-host", forwardedHost))
				forwardStream, cancel, err = s.createReportBucketsForwardStream(client)
				if err != nil {
					return err
				}
				lastForwardedHost = forwardedHost
				errCh = make(chan error, 1)
				go forwardReportBucketClientToServer(forwardStream, server, errCh)
			}
			if err := forwardStream.Send(request); err != nil {
				return errors.WithStack(err)
			}

			select {
			case err := <-errCh:
				return err
			default:
			}
			continue
		}
		rc := s.GetRaftCluster()
		if rc == nil {
			resp := &pdpb.ReportBucketsResponse{
				Header: notBootstrappedHeader(),
			}
			err := server.send(resp)
			return errors.WithStack(err)
		}
		if err := s.validateRequest(request.GetHeader()); err != nil {
			return err
		}
		buckets := request.GetBuckets()
		if buckets == nil || len(buckets.Keys) == 0 {
			continue
		}
		var (
			storeLabel   string
			storeAddress string
		)
		store := rc.GetLeaderStoreByRegionID(buckets.GetRegionId())
		if store == nil {
			// As TiKV report buckets just after the region heartbeat, for new created region, PD may receive buckets report before the first region heartbeat is handled.
			// So we should not return error here.
			log.Warn("the store of the bucket in region is not found ", zap.Uint64("region-id", buckets.GetRegionId()))
		} else {
			storeLabel = strconv.FormatUint(store.GetID(), 10)
			storeAddress = store.GetAddress()
		}
		bucketReportCounter.WithLabelValues(storeAddress, storeLabel, "report", "recv").Inc()

		start := time.Now()
		err = rc.HandleReportBuckets(buckets)
		if err != nil {
			bucketReportCounter.WithLabelValues(storeAddress, storeLabel, "report", "err").Inc()
			continue
		}
		bucketReportInterval.WithLabelValues(storeAddress, storeLabel).Observe(float64(buckets.GetPeriodInMs() / 1000))
		bucketReportLatency.WithLabelValues(storeAddress, storeLabel).Observe(time.Since(start).Seconds())
		bucketReportCounter.WithLabelValues(storeAddress, storeLabel, "report", "ok").Inc()
	}
}

// RegionHeartbeat implements gRPC PDServer.
func (s *GrpcServer) RegionHeartbeat(stream pdpb.PD_RegionHeartbeatServer) error {
	var (
		server                      = &heartbeatServer{stream: stream}
		flowRoundDivisor            = s.persistOptions.GetPDServerConfig().FlowRoundByDigit
		cancel                      context.CancelFunc
		lastBind                    time.Time
		errCh                       chan error
		forwardStream               pdpb.PD_RegionHeartbeatClient
		lastForwardedHost           string
		forwardErrCh                chan error
		forwardSchedulingStream     schedulingpb.Scheduling_RegionHeartbeatClient
		lastForwardedSchedulingHost string
	)
	defer func() {
		// cancel the forward stream
		if cancel != nil {
			cancel()
		}
	}()
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return err
		}
	}
	for {
		request, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		forwardedHost := grpcutil.GetForwardedHost(stream.Context())
		failpoint.Inject("grpcClientClosed", func() {
			forwardedHost = s.GetMember().Member().GetClientUrls()[0]
		})
		if !s.isLocalRequest(forwardedHost) {
			if forwardStream == nil || lastForwardedHost != forwardedHost {
				if cancel != nil {
					cancel()
				}
				client, err := s.getDelegateClient(s.ctx, forwardedHost)
				if err != nil {
					return err
				}
				log.Info("create region heartbeat forward stream", zap.String("forwarded-host", forwardedHost))
				forwardStream, cancel, err = s.createRegionHeartbeatForwardStream(client)
				if err != nil {
					return err
				}
				lastForwardedHost = forwardedHost
				errCh = make(chan error, 1)
				go forwardRegionHeartbeatClientToServer(forwardStream, server, errCh)
			}
			if err := forwardStream.Send(request); err != nil {
				return errors.WithStack(err)
			}

			select {
			case err := <-errCh:
				return err
			default:
			}
			continue
		}

		rc := s.GetRaftCluster()
		if rc == nil {
			resp := &pdpb.RegionHeartbeatResponse{
				Header: notBootstrappedHeader(),
			}
			err := server.Send(resp)
			return errors.WithStack(err)
		}

		if err = s.validateRequest(request.GetHeader()); err != nil {
			return err
		}

		storeID := request.GetLeader().GetStoreId()
		storeLabel := strconv.FormatUint(storeID, 10)
		store := rc.GetStore(storeID)
		if store == nil {
			return errors.Errorf("invalid store ID %d, not found", storeID)
		}
		storeAddress := store.GetAddress()

		regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "recv").Inc()
		regionHeartbeatLatency.WithLabelValues(storeAddress, storeLabel).Observe(float64(time.Now().Unix()) - float64(request.GetInterval().GetEndTimestamp()))

		if time.Since(lastBind) > s.cfg.HeartbeatStreamBindInterval.Duration {
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "bind").Inc()
			s.hbStreams.BindStream(storeID, server)
			// refresh FlowRoundByDigit
			flowRoundDivisor = s.persistOptions.GetPDServerConfig().FlowRoundByDigit
			lastBind = time.Now()
		}

		region := core.RegionFromHeartbeat(request, flowRoundDivisor)
		if region.GetLeader() == nil {
			log.Error("invalid request, the leader is nil", zap.Reflect("request", request), errs.ZapError(errs.ErrLeaderNil))
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "invalid-leader").Inc()
			msg := fmt.Sprintf("invalid request leader, %v", request)
			s.hbStreams.SendErr(pdpb.ErrorType_UNKNOWN, msg, request.GetLeader())
			continue
		}
		if region.GetID() == 0 {
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "invalid-region").Inc()
			msg := fmt.Sprintf("invalid request region, %v", request)
			s.hbStreams.SendErr(pdpb.ErrorType_UNKNOWN, msg, request.GetLeader())
			continue
		}

		// If the region peer count is 0, then we should not handle this.
		if len(region.GetPeers()) == 0 {
			log.Warn("invalid region, zero region peer count",
				logutil.ZapRedactStringer("region-meta", core.RegionToHexMeta(region.GetMeta())))
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "no-peer").Inc()
			msg := fmt.Sprintf("invalid region, zero region peer count: %v", logutil.RedactStringer(core.RegionToHexMeta(region.GetMeta())))
			s.hbStreams.SendErr(pdpb.ErrorType_UNKNOWN, msg, request.GetLeader())
			continue
		}
		start := time.Now()
		err = rc.HandleRegionHeartbeat(region)
		if err != nil {
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "err").Inc()
			msg := err.Error()
			s.hbStreams.SendErr(pdpb.ErrorType_UNKNOWN, msg, request.GetLeader())
			continue
		}
		regionHeartbeatHandleDuration.WithLabelValues(storeAddress, storeLabel).Observe(time.Since(start).Seconds())
		regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "ok").Inc()

		if rc.IsServiceIndependent(constant.SchedulingServiceName) {
			if forwardErrCh != nil {
				select {
				case err, ok := <-forwardErrCh:
					if ok {
						if cancel != nil {
							cancel()
						}
						forwardSchedulingStream = nil
						log.Error("meet error and need to re-establish the stream", zap.Error(err))
					}
				default:
				}
			}
			forwardedSchedulingHost, ok := s.GetServicePrimaryAddr(stream.Context(), constant.SchedulingServiceName)
			if !ok || len(forwardedSchedulingHost) == 0 {
				log.Debug("failed to find scheduling service primary address")
				if cancel != nil {
					cancel()
				}
				continue
			}
			if forwardSchedulingStream == nil || lastForwardedSchedulingHost != forwardedSchedulingHost {
				if cancel != nil {
					cancel()
				}

				client, err := s.getDelegateClient(s.ctx, forwardedSchedulingHost)
				if err != nil {
					errRegionHeartbeatClient.Inc()
					log.Error("failed to get client", zap.Error(err))
					continue
				}
				log.Debug("create scheduling forwarding stream", zap.String("forwarded-host", forwardedSchedulingHost))
				forwardSchedulingStream, _, cancel, err = createRegionHeartbeatSchedulingStream(stream.Context(), client)
				if err != nil {
					errRegionHeartbeatStream.Inc()
					log.Debug("failed to create stream", zap.Error(err))
					continue
				}
				lastForwardedSchedulingHost = forwardedSchedulingHost
				forwardErrCh = make(chan error, 1)
				go forwardRegionHeartbeatToScheduling(rc, forwardSchedulingStream, server, forwardErrCh)
			}
			schedulingpbReq := &schedulingpb.RegionHeartbeatRequest{
				Header: &schedulingpb.RequestHeader{
					ClusterId: request.GetHeader().GetClusterId(),
					SenderId:  request.GetHeader().GetSenderId(),
				},
				Region:          request.GetRegion(),
				Leader:          request.GetLeader(),
				DownPeers:       request.GetDownPeers(),
				PendingPeers:    request.GetPendingPeers(),
				BytesWritten:    request.GetBytesWritten(),
				BytesRead:       request.GetBytesRead(),
				KeysWritten:     request.GetKeysWritten(),
				KeysRead:        request.GetKeysRead(),
				ApproximateSize: request.GetApproximateSize(),
				ApproximateKeys: request.GetApproximateKeys(),
				Interval:        request.GetInterval(),
				Term:            request.GetTerm(),
				QueryStats:      request.GetQueryStats(),
			}
			if err := forwardSchedulingStream.Send(schedulingpbReq); err != nil {
				forwardSchedulingStream = nil
				if grpcutil.NeedRebuildConnection(err) {
					s.closeDelegateClient(lastForwardedSchedulingHost)
				}
				errRegionHeartbeatSend.Inc()
				log.Error("failed to send request to scheduling service", zap.Error(err))
			}

			select {
			case err, ok := <-forwardErrCh:
				if ok {
					forwardSchedulingStream = nil
					errRegionHeartbeatRecv.Inc()
					log.Error("failed to send response", zap.Error(err))
				}
			default:
			}
		}
	}
}

// GetRegion implements gRPC PDServer.
func (s *GrpcServer) GetRegion(ctx context.Context, request *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetRegionResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetRegion(ctx, request)
	}
	followerHandle := new(bool)
	if rsp, err := s.unaryFollowerMiddleware(ctx, request, fn, followerHandle); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetRegionResponse), nil
	}
	failpoint.Inject("delayProcess", nil)
	var (
		rc     *cluster.RaftCluster
		region *core.RegionInfo
	)
	if *followerHandle {
		rc = s.cluster
		if !rc.GetRegionSyncer().IsRunning() {
			return &pdpb.GetRegionResponse{Header: regionNotFound()}, nil
		}
		region = rc.GetRegionByKey(request.GetRegionKey())
		if region == nil {
			log.Warn("follower get region nil", zap.String("key", string(request.GetRegionKey())))
			return &pdpb.GetRegionResponse{Header: regionNotFound()}, nil
		}
	} else {
		rc = s.GetRaftCluster()
		if rc == nil {
			return &pdpb.GetRegionResponse{Header: notBootstrappedHeader()}, nil
		}
		region = rc.GetRegionByKey(request.GetRegionKey())
		if region == nil {
			log.Warn("leader get region nil", zap.String("key", string(request.GetRegionKey())))
			return &pdpb.GetRegionResponse{Header: wrapHeader()}, nil
		}
	}

	var buckets *metapb.Buckets
	// FIXME: If the bucket is disabled dynamically, the bucket information is returned unexpectedly
	if !*followerHandle && rc.GetStoreConfig().IsEnableRegionBucket() && request.GetNeedBuckets() {
		buckets = region.GetBuckets()
	}
	return &pdpb.GetRegionResponse{
		Header:       wrapHeader(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
		Buckets:      buckets,
	}, nil
}

// GetPrevRegion implements gRPC PDServer
func (s *GrpcServer) GetPrevRegion(ctx context.Context, request *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetRegionResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetPrevRegion(ctx, request)
	}
	followerHandle := new(bool)
	if rsp, err := s.unaryFollowerMiddleware(ctx, request, fn, followerHandle); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetRegionResponse), err
	}

	var rc *cluster.RaftCluster
	if *followerHandle {
		// no need to check running status
		rc = s.cluster
		if !rc.GetRegionSyncer().IsRunning() {
			return &pdpb.GetRegionResponse{Header: regionNotFound()}, nil
		}
	} else {
		rc = s.GetRaftCluster()
		if rc == nil {
			return &pdpb.GetRegionResponse{Header: notBootstrappedHeader()}, nil
		}
	}

	region := rc.GetPrevRegionByKey(request.GetRegionKey())
	if region == nil {
		if *followerHandle {
			return &pdpb.GetRegionResponse{Header: regionNotFound()}, nil
		}
		return &pdpb.GetRegionResponse{Header: wrapHeader()}, nil
	}
	var buckets *metapb.Buckets
	// FIXME: If the bucket is disabled dynamically, the bucket information is returned unexpectedly
	if !*followerHandle && rc.GetStoreConfig().IsEnableRegionBucket() && request.GetNeedBuckets() {
		buckets = region.GetBuckets()
	}
	return &pdpb.GetRegionResponse{
		Header:       wrapHeader(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
		Buckets:      buckets,
	}, nil
}

// GetRegionByID implements gRPC PDServer.
func (s *GrpcServer) GetRegionByID(ctx context.Context, request *pdpb.GetRegionByIDRequest) (*pdpb.GetRegionResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetRegionResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetRegionByID(ctx, request)
	}
	followerHandle := new(bool)
	if rsp, err := s.unaryFollowerMiddleware(ctx, request, fn, followerHandle); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetRegionResponse), err
	}

	var rc *cluster.RaftCluster
	if *followerHandle {
		rc = s.cluster
		if !rc.GetRegionSyncer().IsRunning() {
			return &pdpb.GetRegionResponse{Header: regionNotFound()}, nil
		}
	} else {
		rc = s.GetRaftCluster()
		if rc == nil {
			return &pdpb.GetRegionResponse{Header: regionNotFound()}, nil
		}
	}
	region := rc.GetRegion(request.GetRegionId())
	failpoint.Inject("followerHandleError", func() {
		if *followerHandle {
			region = nil
		}
	})
	if region == nil {
		if *followerHandle {
			return &pdpb.GetRegionResponse{Header: regionNotFound()}, nil
		}
		return &pdpb.GetRegionResponse{Header: wrapHeader()}, nil
	}
	var buckets *metapb.Buckets
	if !*followerHandle && rc.GetStoreConfig().IsEnableRegionBucket() && request.GetNeedBuckets() {
		buckets = region.GetBuckets()
	}
	return &pdpb.GetRegionResponse{
		Header:       wrapHeader(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
		Buckets:      buckets,
	}, nil
}

// Deprecated: use BatchScanRegions instead.
// ScanRegions implements gRPC PDServer.
func (s *GrpcServer) ScanRegions(ctx context.Context, request *pdpb.ScanRegionsRequest) (*pdpb.ScanRegionsResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.ScanRegionsResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).ScanRegions(ctx, request)
	}
	followerHandle := new(bool)
	if rsp, err := s.unaryFollowerMiddleware(ctx, request, fn, followerHandle); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.ScanRegionsResponse), nil
	}

	var rc *cluster.RaftCluster
	if *followerHandle {
		rc = s.cluster
		if !rc.GetRegionSyncer().IsRunning() {
			return &pdpb.ScanRegionsResponse{Header: regionNotFound()}, nil
		}
	} else {
		rc = s.GetRaftCluster()
		if rc == nil {
			return &pdpb.ScanRegionsResponse{Header: notBootstrappedHeader()}, nil
		}
	}
	regions := rc.ScanRegions(request.GetStartKey(), request.GetEndKey(), int(request.GetLimit()))
	if *followerHandle && len(regions) == 0 {
		return &pdpb.ScanRegionsResponse{Header: regionNotFound()}, nil
	}
	resp := &pdpb.ScanRegionsResponse{Header: wrapHeader()}
	for _, r := range regions {
		leader := r.GetLeader()
		if leader == nil {
			leader = &metapb.Peer{}
		}
		// Set RegionMetas and Leaders to make it compatible with old client.
		resp.RegionMetas = append(resp.RegionMetas, r.GetMeta())
		resp.Leaders = append(resp.Leaders, leader)
		resp.Regions = append(resp.Regions, &pdpb.Region{
			Region:       r.GetMeta(),
			Leader:       leader,
			DownPeers:    r.GetDownPeers(),
			PendingPeers: r.GetPendingPeers(),
		})
	}
	return resp, nil
}

// BatchScanRegions implements gRPC PDServer.
func (s *GrpcServer) BatchScanRegions(ctx context.Context, request *pdpb.BatchScanRegionsRequest) (*pdpb.BatchScanRegionsResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.BatchScanRegionsResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).BatchScanRegions(ctx, request)
	}
	followerHandle := new(bool)
	if rsp, err := s.unaryFollowerMiddleware(ctx, request, fn, followerHandle); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.BatchScanRegionsResponse), nil
	}

	var rc *cluster.RaftCluster
	if *followerHandle {
		rc = s.cluster
		if !rc.GetRegionSyncer().IsRunning() {
			return &pdpb.BatchScanRegionsResponse{Header: regionNotFound()}, nil
		}
	} else {
		rc = s.GetRaftCluster()
		if rc == nil {
			return &pdpb.BatchScanRegionsResponse{Header: notBootstrappedHeader()}, nil
		}
	}
	needBucket := request.GetNeedBuckets() && !*followerHandle && rc.GetStoreConfig().IsEnableRegionBucket()
	limit := request.GetLimit()
	// cast to core.KeyRanges and check the validation.
	keyRanges := core.NewKeyRangesWithSize(len(request.GetRanges()))
	reqRanges := request.GetRanges()
	for i, reqRange := range reqRanges {
		if i > 0 {
			if bytes.Compare(reqRange.StartKey, reqRanges[i-1].EndKey) < 0 {
				return &pdpb.BatchScanRegionsResponse{Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, "invalid key range, ranges overlapped")}, nil
			}
		}
		if len(reqRange.EndKey) > 0 && bytes.Compare(reqRange.StartKey, reqRange.EndKey) > 0 {
			return &pdpb.BatchScanRegionsResponse{Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, "invalid key range, start key > end key")}, nil
		}
		keyRanges.Append(reqRange.StartKey, reqRange.EndKey)
	}

	scanOptions := []core.BatchScanRegionsOptionFunc{core.WithLimit(int(limit))}
	if request.ContainAllKeyRange {
		scanOptions = append(scanOptions, core.WithOutputMustContainAllKeyRange())
	}
	res, err := rc.BatchScanRegions(keyRanges, scanOptions...)
	if err != nil {
		if errs.ErrRegionNotAdjacent.Equal(multierr.Errors(err)[0]) {
			return &pdpb.BatchScanRegionsResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_REGIONS_NOT_CONTAIN_ALL_KEY_RANGE, err.Error()),
			}, nil
		}
		return &pdpb.BatchScanRegionsResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}
	regions := make([]*pdpb.Region, 0, len(res))
	for _, r := range res {
		leader := r.GetLeader()
		if leader == nil {
			leader = &metapb.Peer{}
		}
		var buckets *metapb.Buckets
		if needBucket {
			buckets = r.GetBuckets()
		}
		regions = append(regions, &pdpb.Region{
			Region:       r.GetMeta(),
			Leader:       leader,
			DownPeers:    r.GetDownPeers(),
			PendingPeers: r.GetPendingPeers(),
			Buckets:      buckets,
		})
	}
	if *followerHandle && len(regions) == 0 {
		return &pdpb.BatchScanRegionsResponse{Header: regionNotFound()}, nil
	}
	resp := &pdpb.BatchScanRegionsResponse{Header: wrapHeader(), Regions: regions}
	return resp, nil
}

// AskSplit implements gRPC PDServer.
func (s *GrpcServer) AskSplit(ctx context.Context, request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.AskSplitResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).AskSplit(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.AskSplitResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.AskSplitResponse{Header: notBootstrappedHeader()}, nil
	}
	if request.GetRegion() == nil {
		return &pdpb.AskSplitResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_REGION_NOT_FOUND,
				"missing region for split"),
		}, nil
	}
	split, err := rc.HandleAskSplit(request)
	if err != nil {
		return &pdpb.AskSplitResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.AskSplitResponse{
		Header:      wrapHeader(),
		NewRegionId: split.NewRegionId,
		NewPeerIds:  split.NewPeerIds,
	}, nil
}

// AskBatchSplit implements gRPC PDServer.
func (s *GrpcServer) AskBatchSplit(ctx context.Context, request *pdpb.AskBatchSplitRequest) (*pdpb.AskBatchSplitResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.AskBatchSplitResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.AskBatchSplitResponse{Header: notBootstrappedHeader()}, nil
	}

	if rc.IsServiceIndependent(constant.SchedulingServiceName) {
		forwardCli, err := s.updateSchedulingClient(ctx)
		if err != nil {
			return &pdpb.AskBatchSplitResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
		cli := forwardCli.getClient()
		if cli != nil {
			req := &schedulingpb.AskBatchSplitRequest{
				Header: &schedulingpb.RequestHeader{
					ClusterId: request.GetHeader().GetClusterId(),
					SenderId:  request.GetHeader().GetSenderId(),
				},
				Region:     request.GetRegion(),
				SplitCount: request.GetSplitCount(),
			}
			resp, err := cli.AskBatchSplit(ctx, req)
			if err != nil {
				// reset to let it be updated in the next request
				s.schedulingClient.CompareAndSwap(forwardCli, &schedulingClient{})
				return convertAskSplitResponse(resp), err
			}
			return convertAskSplitResponse(resp), nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).AskBatchSplit(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.AskBatchSplitResponse), err
	}

	if !versioninfo.IsFeatureSupported(rc.GetOpts().GetClusterVersion(), versioninfo.BatchSplit) {
		return &pdpb.AskBatchSplitResponse{Header: s.incompatibleVersion("batch_split")}, nil
	}
	if request.GetRegion() == nil {
		return &pdpb.AskBatchSplitResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_REGION_NOT_FOUND,
				"missing region for split"),
		}, nil
	}
	split, err := rc.HandleAskBatchSplit(request)
	if err != nil {
		return &pdpb.AskBatchSplitResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.AskBatchSplitResponse{
		Header: wrapHeader(),
		Ids:    split.Ids,
	}, nil
}

// ReportSplit implements gRPC PDServer.
func (s *GrpcServer) ReportSplit(ctx context.Context, request *pdpb.ReportSplitRequest) (*pdpb.ReportSplitResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.ReportSplitResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).ReportSplit(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.ReportSplitResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ReportSplitResponse{Header: notBootstrappedHeader()}, nil
	}
	_, err := rc.HandleReportSplit(request)
	if err != nil {
		return &pdpb.ReportSplitResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.ReportSplitResponse{
		Header: wrapHeader(),
	}, nil
}

// ReportBatchSplit implements gRPC PDServer.
func (s *GrpcServer) ReportBatchSplit(ctx context.Context, request *pdpb.ReportBatchSplitRequest) (*pdpb.ReportBatchSplitResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.ReportBatchSplitResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).ReportBatchSplit(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.ReportBatchSplitResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ReportBatchSplitResponse{Header: notBootstrappedHeader()}, nil
	}
	_, err := rc.HandleBatchReportSplit(request)
	if err != nil {
		return &pdpb.ReportBatchSplitResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				err.Error()),
		}, nil
	}

	return &pdpb.ReportBatchSplitResponse{
		Header: wrapHeader(),
	}, nil
}

// GetClusterConfig implements gRPC PDServer.
func (s *GrpcServer) GetClusterConfig(ctx context.Context, request *pdpb.GetClusterConfigRequest) (*pdpb.GetClusterConfigResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetClusterConfigResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetClusterConfig(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetClusterConfigResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetClusterConfigResponse{Header: notBootstrappedHeader()}, nil
	}
	return &pdpb.GetClusterConfigResponse{
		Header:  wrapHeader(),
		Cluster: rc.GetMetaCluster(),
	}, nil
}

// PutClusterConfig implements gRPC PDServer.
func (s *GrpcServer) PutClusterConfig(ctx context.Context, request *pdpb.PutClusterConfigRequest) (*pdpb.PutClusterConfigResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.PutClusterConfigResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).PutClusterConfig(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.PutClusterConfigResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.PutClusterConfigResponse{Header: notBootstrappedHeader()}, nil
	}
	conf := request.GetCluster()
	if err := rc.PutMetaCluster(conf); err != nil {
		return &pdpb.PutClusterConfigResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				err.Error()),
		}, nil
	}

	log.Info("put cluster config ok", zap.Reflect("config", conf))

	return &pdpb.PutClusterConfigResponse{
		Header: wrapHeader(),
	}, nil
}

// ScatterRegion implements gRPC PDServer.
func (s *GrpcServer) ScatterRegion(ctx context.Context, request *pdpb.ScatterRegionRequest) (*pdpb.ScatterRegionResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.ScatterRegionResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ScatterRegionResponse{Header: notBootstrappedHeader()}, nil
	}

	if rc.IsServiceIndependent(constant.SchedulingServiceName) {
		forwardCli, err := s.updateSchedulingClient(ctx)
		if err != nil {
			return &pdpb.ScatterRegionResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
		cli := forwardCli.getClient()
		if cli != nil {
			var regionsID []uint64
			// nolint:staticcheck
			if request.GetRegionId() != 0 {
				// nolint:staticcheck
				regionsID = []uint64{request.GetRegionId()}
			} else {
				regionsID = request.GetRegionsId()
			}
			if len(regionsID) == 0 {
				return &pdpb.ScatterRegionResponse{
					Header: invalidValue("regions id is required"),
				}, nil
			}
			req := &schedulingpb.ScatterRegionsRequest{
				Header: &schedulingpb.RequestHeader{
					ClusterId: request.GetHeader().GetClusterId(),
					SenderId:  request.GetHeader().GetSenderId(),
				},
				RegionsId:      regionsID,
				Group:          request.GetGroup(),
				RetryLimit:     request.GetRetryLimit(),
				SkipStoreLimit: request.GetSkipStoreLimit(),
			}
			resp, err := cli.ScatterRegions(ctx, req)
			if err != nil {
				errScatterRegionSend.Inc()
				// reset to let it be updated in the next request
				s.schedulingClient.CompareAndSwap(forwardCli, &schedulingClient{})
				return convertScatterResponse(resp), err
			}
			return convertScatterResponse(resp), nil
		}
	}

	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).ScatterRegion(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.ScatterRegionResponse), err
	}

	if len(request.GetRegionsId()) > 0 {
		percentage, err := scatterRegions(rc, request.GetRegionsId(), request.GetGroup(), int(request.GetRetryLimit()), request.GetSkipStoreLimit())
		if err != nil {
			return nil, err
		}
		return &pdpb.ScatterRegionResponse{
			Header:             wrapHeader(),
			FinishedPercentage: uint64(percentage),
		}, nil
	}
	// TODO: Deprecate it use `request.GetRegionsID`.
	// nolint:staticcheck
	region := rc.GetRegion(request.GetRegionId())
	if region == nil {
		if request.GetRegion() == nil {
			return &pdpb.ScatterRegionResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_REGION_NOT_FOUND,
					"region %d not found"),
			}, nil
		}
		region = core.NewRegionInfo(request.GetRegion(), request.GetLeader())
	}

	op, err := rc.GetRegionScatterer().Scatter(region, request.GetGroup(), request.GetSkipStoreLimit())
	if err != nil {
		return nil, err
	}

	if op != nil {
		if !rc.GetOperatorController().AddOperator(op) {
			return &pdpb.ScatterRegionResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
					"operator canceled because cannot add an operator to the execute queue"),
			}, nil
		}
	}

	return &pdpb.ScatterRegionResponse{
		Header:             wrapHeader(),
		FinishedPercentage: 100,
	}, nil
}

// GetGCSafePoint implements gRPC PDServer.
func (s *GrpcServer) GetGCSafePoint(ctx context.Context, request *pdpb.GetGCSafePointRequest) (*pdpb.GetGCSafePointResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetGCSafePointResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetGCSafePoint(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetGCSafePointResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetGCSafePointResponse{Header: notBootstrappedHeader()}, nil
	}

	safePoint, err := s.gcSafePointManager.LoadGCSafePoint()
	if err != nil {
		return nil, err
	}

	return &pdpb.GetGCSafePointResponse{
		Header:    wrapHeader(),
		SafePoint: safePoint,
	}, nil
}

// SyncRegions syncs the regions.
func (s *GrpcServer) SyncRegions(stream pdpb.PD_SyncRegionsServer) error {
	if s.IsClosed() || s.cluster == nil {
		return ErrNotStarted
	}
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return err
		}
	}
	ctx := s.cluster.Context()
	if ctx == nil {
		return ErrNotStarted
	}
	return s.cluster.GetRegionSyncer().Sync(ctx, stream)
}

// UpdateGCSafePoint implements gRPC PDServer.
func (s *GrpcServer) UpdateGCSafePoint(ctx context.Context, request *pdpb.UpdateGCSafePointRequest) (*pdpb.UpdateGCSafePointResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.UpdateGCSafePointResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).UpdateGCSafePoint(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.UpdateGCSafePointResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.UpdateGCSafePointResponse{Header: notBootstrappedHeader()}, nil
	}

	newSafePoint := request.GetSafePoint()
	oldSafePoint, err := s.gcSafePointManager.UpdateGCSafePoint(newSafePoint)
	if err != nil {
		return nil, err
	}

	if newSafePoint > oldSafePoint {
		log.Info("updated gc safe point",
			zap.Uint64("safe-point", newSafePoint))
	} else if newSafePoint < oldSafePoint {
		log.Warn("trying to update gc safe point",
			zap.Uint64("old-safe-point", oldSafePoint),
			zap.Uint64("new-safe-point", newSafePoint))
		newSafePoint = oldSafePoint
	}

	return &pdpb.UpdateGCSafePointResponse{
		Header:       wrapHeader(),
		NewSafePoint: newSafePoint,
	}, nil
}

// UpdateServiceGCSafePoint update the safepoint for specific service
func (s *GrpcServer) UpdateServiceGCSafePoint(ctx context.Context, request *pdpb.UpdateServiceGCSafePointRequest) (*pdpb.UpdateServiceGCSafePointResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.UpdateServiceGCSafePointResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).UpdateServiceGCSafePoint(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.UpdateServiceGCSafePointResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.UpdateServiceGCSafePointResponse{Header: notBootstrappedHeader()}, nil
	}
	var storage endpoint.GCSafePointStorage = s.storage
	if request.TTL <= 0 {
		if err := storage.RemoveServiceGCSafePoint(string(request.ServiceId)); err != nil {
			return nil, err
		}
	}
	nowTSO, err := s.getGlobalTSO(ctx)
	if err != nil {
		return nil, err
	}
	now, _ := tsoutil.ParseTimestamp(nowTSO)
	serviceID := string(request.ServiceId)
	min, updated, err := s.gcSafePointManager.UpdateServiceGCSafePoint(serviceID, request.GetSafePoint(), request.GetTTL(), now)
	if err != nil {
		return nil, err
	}
	if updated {
		log.Info("update service GC safe point",
			zap.String("service-id", serviceID),
			zap.Int64("expire-at", now.Unix()+request.GetTTL()),
			zap.Uint64("safepoint", request.GetSafePoint()))
	}
	return &pdpb.UpdateServiceGCSafePointResponse{
		Header:       wrapHeader(),
		ServiceId:    []byte(min.ServiceID),
		TTL:          min.ExpiredAt - now.Unix(),
		MinSafePoint: min.SafePoint,
	}, nil
}

// GetOperator gets information about the operator belonging to the specify region.
func (s *GrpcServer) GetOperator(ctx context.Context, request *pdpb.GetOperatorRequest) (*pdpb.GetOperatorResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetOperatorResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetOperatorResponse{Header: notBootstrappedHeader()}, nil
	}

	if rc.IsServiceIndependent(constant.SchedulingServiceName) {
		forwardCli, err := s.updateSchedulingClient(ctx)
		if err != nil {
			return &pdpb.GetOperatorResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
		cli := forwardCli.getClient()
		if cli != nil {
			req := &schedulingpb.GetOperatorRequest{
				Header: &schedulingpb.RequestHeader{
					ClusterId: request.GetHeader().GetClusterId(),
					SenderId:  request.GetHeader().GetSenderId(),
				},
				RegionId: request.GetRegionId(),
			}
			resp, err := cli.GetOperator(ctx, req)
			if err != nil {
				errGetOperatorSend.Inc()
				// reset to let it be updated in the next request
				s.schedulingClient.CompareAndSwap(forwardCli, &schedulingClient{})
				return convertOperatorResponse(resp), err
			}
			return convertOperatorResponse(resp), nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetOperator(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetOperatorResponse), err
	}

	opController := rc.GetOperatorController()
	requestID := request.GetRegionId()
	r := opController.GetOperatorStatus(requestID)
	if r == nil {
		header := errorHeader(&pdpb.Error{
			Type:    pdpb.ErrorType_REGION_NOT_FOUND,
			Message: "Not Found",
		})
		return &pdpb.GetOperatorResponse{Header: header}, nil
	}

	return &pdpb.GetOperatorResponse{
		Header:   wrapHeader(),
		RegionId: requestID,
		Desc:     []byte(r.Desc()),
		Kind:     []byte(r.Kind().String()),
		Status:   r.Status,
	}, nil
}

// validateRequest checks if Server is leader and clusterID is matched.
func (s *GrpcServer) validateRequest(header *pdpb.RequestHeader) error {
	return s.validateRoleInRequest(context.TODO(), header, nil)
}

// validateRoleInRequest checks if Server is leader when disallow follower-handle and clusterID is matched.
// TODO: Call it in gRPC interceptor.
func (s *GrpcServer) validateRoleInRequest(ctx context.Context, header *pdpb.RequestHeader, allowFollower *bool) error {
	if s.IsClosed() {
		return ErrNotStarted
	}
	if !s.member.IsLeader() {
		if allowFollower == nil {
			return ErrNotLeader
		}
		if !grpcutil.IsFollowerHandleEnabled(ctx) {
			// TODO: change the error code
			return ErrFollowerHandlingNotAllowed
		}
		*allowFollower = true
	}
	if clusterID := keypath.ClusterID(); header.GetClusterId() != clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", clusterID, header.GetClusterId())
	}
	return nil
}

func wrapHeader() *pdpb.ResponseHeader {
	clusterID := keypath.ClusterID()
	if clusterID == 0 {
		return wrapErrorToHeader(pdpb.ErrorType_NOT_BOOTSTRAPPED, "cluster id is not ready")
	}
	return &pdpb.ResponseHeader{ClusterId: clusterID}
}

func wrapErrorToHeader(errorType pdpb.ErrorType, message string) *pdpb.ResponseHeader {
	return errorHeader(&pdpb.Error{
		Type:    errorType,
		Message: message,
	})
}

func errorHeader(err *pdpb.Error) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: keypath.ClusterID(),
		Error:     err,
	}
}

func notBootstrappedHeader() *pdpb.ResponseHeader {
	return errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}

func (s *GrpcServer) incompatibleVersion(tag string) *pdpb.ResponseHeader {
	msg := fmt.Sprintf("%s incompatible with current cluster version %s", tag, s.persistOptions.GetClusterVersion())
	return errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_INCOMPATIBLE_VERSION,
		Message: msg,
	})
}

func invalidValue(msg string) *pdpb.ResponseHeader {
	return errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_INVALID_VALUE,
		Message: msg,
	})
}

func regionNotFound() *pdpb.ResponseHeader {
	return errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_REGION_NOT_FOUND,
		Message: "region not found",
	})
}

func convertHeader(header *schedulingpb.ResponseHeader) *pdpb.ResponseHeader {
	switch header.GetError().GetType() {
	case schedulingpb.ErrorType_UNKNOWN:
		if strings.Contains(header.GetError().GetMessage(), "region not found") {
			return &pdpb.ResponseHeader{
				ClusterId: header.GetClusterId(),
				Error: &pdpb.Error{
					Type:    pdpb.ErrorType_REGION_NOT_FOUND,
					Message: header.GetError().GetMessage(),
				},
			}
		}
		return &pdpb.ResponseHeader{
			ClusterId: header.GetClusterId(),
			Error: &pdpb.Error{
				Type:    pdpb.ErrorType_UNKNOWN,
				Message: header.GetError().GetMessage(),
			},
		}
	default:
		return &pdpb.ResponseHeader{ClusterId: header.GetClusterId()}
	}
}

func convertSplitResponse(resp *schedulingpb.SplitRegionsResponse) *pdpb.SplitRegionsResponse {
	return &pdpb.SplitRegionsResponse{
		Header:             convertHeader(resp.GetHeader()),
		FinishedPercentage: resp.GetFinishedPercentage(),
	}
}

func convertScatterResponse(resp *schedulingpb.ScatterRegionsResponse) *pdpb.ScatterRegionResponse {
	return &pdpb.ScatterRegionResponse{
		Header:             convertHeader(resp.GetHeader()),
		FinishedPercentage: resp.GetFinishedPercentage(),
	}
}

func convertOperatorResponse(resp *schedulingpb.GetOperatorResponse) *pdpb.GetOperatorResponse {
	return &pdpb.GetOperatorResponse{
		Header:   convertHeader(resp.GetHeader()),
		RegionId: resp.GetRegionId(),
		Desc:     resp.GetDesc(),
		Kind:     resp.GetKind(),
		Status:   resp.GetStatus(),
	}
}

func convertAskSplitResponse(resp *schedulingpb.AskBatchSplitResponse) *pdpb.AskBatchSplitResponse {
	return &pdpb.AskBatchSplitResponse{
		Header: convertHeader(resp.GetHeader()),
		Ids:    resp.GetIds(),
	}
}

// Only used for the TestLocalAllocatorLeaderChange.
var mockLocalAllocatorLeaderChangeFlag = false

// SyncMaxTS will check whether MaxTS is the biggest one among all Local TSOs this PD is holding when skipCheck is set,
// and write it into all Local TSO Allocators then if it's indeed the biggest one.
func (s *GrpcServer) SyncMaxTS(_ context.Context, request *pdpb.SyncMaxTSRequest) (*pdpb.SyncMaxTSResponse, error) {
	// TODO: support local tso forward in api service mode in the future.
	if err := s.validateInternalRequest(request.GetHeader(), true); err != nil {
		return nil, err
	}
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.SyncMaxTSResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	tsoAllocatorManager := s.GetTSOAllocatorManager()
	// There is no dc-location found in this server, return err.
	if tsoAllocatorManager.GetClusterDCLocationsNumber() == 0 {
		return &pdpb.SyncMaxTSResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				"empty cluster dc-location found, checker may not work properly"),
		}, nil
	}
	// Get all Local TSO Allocator leaders
	allocatorLeaders, err := tsoAllocatorManager.GetHoldingLocalAllocatorLeaders()
	if err != nil {
		return &pdpb.SyncMaxTSResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}
	if !request.GetSkipCheck() {
		var maxLocalTS *pdpb.Timestamp
		syncedDCs := make([]string, 0, len(allocatorLeaders))
		for _, allocator := range allocatorLeaders {
			// No longer leader, just skip here because
			// the global allocator will check if all DCs are handled.
			if !allocator.IsAllocatorLeader() {
				continue
			}
			currentLocalTSO, err := allocator.GetCurrentTSO()
			if err != nil {
				return &pdpb.SyncMaxTSResponse{
					Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
				}, nil
			}
			if tsoutil.CompareTimestamp(currentLocalTSO, maxLocalTS) > 0 {
				maxLocalTS = currentLocalTSO
			}
			syncedDCs = append(syncedDCs, allocator.GetDCLocation())
		}

		failpoint.Inject("mockLocalAllocatorLeaderChange", func() {
			if !mockLocalAllocatorLeaderChangeFlag {
				maxLocalTS = nil
				request.MaxTs = nil
				mockLocalAllocatorLeaderChangeFlag = true
			}
		})

		if maxLocalTS == nil {
			return &pdpb.SyncMaxTSResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
					"local tso allocator leaders have changed during the sync, should retry"),
			}, nil
		}
		if request.GetMaxTs() == nil {
			return &pdpb.SyncMaxTSResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
					"empty maxTS in the request, should retry"),
			}, nil
		}
		// Found a bigger or equal maxLocalTS, return it directly.
		cmpResult := tsoutil.CompareTimestamp(maxLocalTS, request.GetMaxTs())
		if cmpResult >= 0 {
			// Found an equal maxLocalTS, plus 1 to logical part before returning it.
			// For example, we have a Global TSO t1 and a Local TSO t2, they have the
			// same physical and logical parts. After being differentiating with suffix,
			// there will be (t1.logical << suffixNum + 0) < (t2.logical << suffixNum + N),
			// where N is bigger than 0, which will cause a Global TSO fallback than the previous Local TSO.
			if cmpResult == 0 {
				maxLocalTS.Logical += 1
			}
			return &pdpb.SyncMaxTSResponse{
				Header:     wrapHeader(),
				MaxLocalTs: maxLocalTS,
				SyncedDcs:  syncedDCs,
			}, nil
		}
	}
	syncedDCs := make([]string, 0, len(allocatorLeaders))
	for _, allocator := range allocatorLeaders {
		if !allocator.IsAllocatorLeader() {
			continue
		}
		if err := allocator.WriteTSO(request.GetMaxTs()); err != nil {
			return &pdpb.SyncMaxTSResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
		syncedDCs = append(syncedDCs, allocator.GetDCLocation())
	}
	return &pdpb.SyncMaxTSResponse{
		Header:    wrapHeader(),
		SyncedDcs: syncedDCs,
	}, nil
}

// SplitRegions split regions by the given split keys
func (s *GrpcServer) SplitRegions(ctx context.Context, request *pdpb.SplitRegionsRequest) (*pdpb.SplitRegionsResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.SplitRegionsResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.SplitRegionsResponse{Header: notBootstrappedHeader()}, nil
	}

	if rc.IsServiceIndependent(constant.SchedulingServiceName) {
		forwardCli, err := s.updateSchedulingClient(ctx)
		if err != nil {
			return &pdpb.SplitRegionsResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
		cli := forwardCli.getClient()
		if cli != nil {
			req := &schedulingpb.SplitRegionsRequest{
				Header: &schedulingpb.RequestHeader{
					ClusterId: request.GetHeader().GetClusterId(),
					SenderId:  request.GetHeader().GetSenderId(),
				},
				SplitKeys:  request.GetSplitKeys(),
				RetryLimit: request.GetRetryLimit(),
			}
			resp, err := cli.SplitRegions(ctx, req)
			if err != nil {
				errSplitRegionsSend.Inc()
				// reset to let it be updated in the next request
				s.schedulingClient.CompareAndSwap(forwardCli, &schedulingClient{})
				return convertSplitResponse(resp), err
			}
			return convertSplitResponse(resp), nil
		}
	}

	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).SplitRegions(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.SplitRegionsResponse), err
	}

	finishedPercentage, newRegionIDs := rc.GetRegionSplitter().SplitRegions(ctx, request.GetSplitKeys(), int(request.GetRetryLimit()))
	return &pdpb.SplitRegionsResponse{
		Header:             wrapHeader(),
		RegionsId:          newRegionIDs,
		FinishedPercentage: uint64(finishedPercentage),
	}, nil
}

// SplitAndScatterRegions split regions by the given split keys, and scatter regions.
// Only regions which split successfully will be scattered.
// scatterFinishedPercentage indicates the percentage of successfully split regions that are scattered.
func (s *GrpcServer) SplitAndScatterRegions(ctx context.Context, request *pdpb.SplitAndScatterRegionsRequest) (*pdpb.SplitAndScatterRegionsResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.SplitAndScatterRegionsResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).SplitAndScatterRegions(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.SplitAndScatterRegionsResponse), err
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.SplitAndScatterRegionsResponse{Header: notBootstrappedHeader()}, nil
	}
	splitFinishedPercentage, newRegionIDs := rc.GetRegionSplitter().SplitRegions(ctx, request.GetSplitKeys(), int(request.GetRetryLimit()))
	scatterFinishedPercentage, err := scatterRegions(rc, newRegionIDs, request.GetGroup(), int(request.GetRetryLimit()), false)
	if err != nil {
		return nil, err
	}
	return &pdpb.SplitAndScatterRegionsResponse{
		Header:                    wrapHeader(),
		RegionsId:                 newRegionIDs,
		SplitFinishedPercentage:   uint64(splitFinishedPercentage),
		ScatterFinishedPercentage: uint64(scatterFinishedPercentage),
	}, nil
}

// scatterRegions add operators to scatter regions and return the processed percentage and error
func scatterRegions(cluster *cluster.RaftCluster, regionsID []uint64, group string, retryLimit int, skipStoreLimit bool) (int, error) {
	opsCount, failures, err := cluster.GetRegionScatterer().ScatterRegionsByID(regionsID, group, retryLimit, skipStoreLimit)
	if err != nil {
		return 0, err
	}
	percentage := 100
	if len(failures) > 0 {
		percentage = 100 - 100*len(failures)/(opsCount+len(failures))
		log.Debug("scatter regions", zap.Errors("failures", func() []error {
			r := make([]error, 0, len(failures))
			for _, err := range failures {
				r = append(r, err)
			}
			return r
		}()))
	}
	return percentage, nil
}

// Deprecated
func (*GrpcServer) GetDCLocationInfo(_ context.Context, _ *pdpb.GetDCLocationInfoRequest) (*pdpb.GetDCLocationInfoResponse, error) {
	return &pdpb.GetDCLocationInfoResponse{
		Header: wrapHeader(),
	}, nil
}

// validateInternalRequest checks if server is closed, which is used to validate
// the gRPC communication between PD servers internally.
func (s *GrpcServer) validateInternalRequest(header *pdpb.RequestHeader, onlyAllowLeader bool) error {
	if s.IsClosed() {
		return ErrNotStarted
	}
	// If onlyAllowLeader is true, check whether the sender is PD leader.
	if onlyAllowLeader {
		leaderID := s.GetLeader().GetMemberId()
		if leaderID != header.GetSenderId() {
			return status.Errorf(codes.FailedPrecondition, "%s, need %d but got %d", errs.MismatchLeaderErr, leaderID, header.GetSenderId())
		}
	}
	return nil
}

// for CDC compatibility, we need to initialize config path to `globalConfigPath`
const globalConfigPath = "/global/config/"

// StoreGlobalConfig store global config into etcd by transaction
// Since item value needs to support marshal of different struct types,
// it should be set to `Payload bytes` instead of `Value string`
func (s *GrpcServer) StoreGlobalConfig(_ context.Context, request *pdpb.StoreGlobalConfigRequest) (*pdpb.StoreGlobalConfigResponse, error) {
	if s.client == nil {
		return nil, ErrEtcdNotStarted
	}
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.StoreGlobalConfigResponse{
				Error: &pdpb.Error{
					Type:    pdpb.ErrorType_UNKNOWN,
					Message: err.Error(),
				},
			}, nil
		}
	}
	configPath := request.GetConfigPath()
	if configPath == "" {
		configPath = globalConfigPath
	}
	ops := make([]clientv3.Op, len(request.Changes))
	for i, item := range request.Changes {
		name := path.Join(configPath, item.GetName())
		switch item.GetKind() {
		case pdpb.EventType_PUT:
			// For CDC compatibility, we need to check the Value field firstly.
			value := item.GetValue()
			if value == "" {
				value = string(item.GetPayload())
			}
			ops[i] = clientv3.OpPut(name, value)
		case pdpb.EventType_DELETE:
			ops[i] = clientv3.OpDelete(name)
		}
	}
	res, err :=
		kv.NewSlowLogTxn(s.client).Then(ops...).Commit()
	if err != nil {
		return &pdpb.StoreGlobalConfigResponse{}, err
	}
	if !res.Succeeded {
		return &pdpb.StoreGlobalConfigResponse{}, errors.Errorf("failed to execute StoreGlobalConfig transaction")
	}
	return &pdpb.StoreGlobalConfigResponse{}, nil
}

// LoadGlobalConfig support 2 ways to load global config from etcd
// - `Names` iteratively get value from `ConfigPath/Name` but not care about revision
// - `ConfigPath` if `Names` is nil can get all values and revision of current path
func (s *GrpcServer) LoadGlobalConfig(ctx context.Context, request *pdpb.LoadGlobalConfigRequest) (*pdpb.LoadGlobalConfigResponse, error) {
	if s.client == nil {
		return nil, ErrEtcdNotStarted
	}
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return nil, err
		}
	}
	configPath := request.GetConfigPath()
	if configPath == "" {
		configPath = globalConfigPath
	}
	// Since item value needs to support marshal of different struct types,
	// it should be set to `Payload bytes` instead of `Value string`.
	if request.Names != nil {
		res := make([]*pdpb.GlobalConfigItem, len(request.Names))
		for i, name := range request.Names {
			r, err := s.client.Get(ctx, path.Join(configPath, name))
			if err != nil {
				res[i] = &pdpb.GlobalConfigItem{Name: name, Error: &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: err.Error()}}
			} else if len(r.Kvs) == 0 {
				msg := "key " + name + " not found"
				res[i] = &pdpb.GlobalConfigItem{Name: name, Error: &pdpb.Error{Type: pdpb.ErrorType_GLOBAL_CONFIG_NOT_FOUND, Message: msg}}
			} else {
				res[i] = &pdpb.GlobalConfigItem{Name: name, Payload: r.Kvs[0].Value, Kind: pdpb.EventType_PUT}
			}
		}
		return &pdpb.LoadGlobalConfigResponse{Items: res}, nil
	}
	r, err := s.client.Get(ctx, configPath, clientv3.WithPrefix())
	if err != nil {
		return &pdpb.LoadGlobalConfigResponse{}, err
	}
	res := make([]*pdpb.GlobalConfigItem, len(r.Kvs))
	for i, value := range r.Kvs {
		res[i] = &pdpb.GlobalConfigItem{Kind: pdpb.EventType_PUT, Name: string(value.Key), Payload: value.Value}
	}
	return &pdpb.LoadGlobalConfigResponse{Items: res, Revision: r.Header.GetRevision()}, nil
}

// WatchGlobalConfig will retry on recoverable errors forever until reconnected
// by Etcd.Watch() as long as the context has not been canceled or timed out.
// Watch on revision which greater than or equal to the required revision.
func (s *GrpcServer) WatchGlobalConfig(req *pdpb.WatchGlobalConfigRequest, server pdpb.PD_WatchGlobalConfigServer) error {
	if s.client == nil {
		return ErrEtcdNotStarted
	}
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return err
		}
	}
	ctx, cancel := context.WithCancel(server.Context())
	defer cancel()
	configPath := req.GetConfigPath()
	if configPath == "" {
		configPath = globalConfigPath
	}
	revision := req.GetRevision()
	// If the revision is compacted, will meet required revision has been compacted error.
	// - If required revision < CompactRevision, we need to reload all configs to avoid losing data.
	// - If required revision >= CompactRevision, just keep watching.
	// Use WithPrevKV() to get the previous key-value pair when get Delete Event.
	watchChan := s.client.Watch(ctx, configPath, clientv3.WithPrefix(), clientv3.WithRev(revision), clientv3.WithPrevKV())
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.Context().Done():
			return nil
		case res := <-watchChan:
			if res.Err() != nil {
				var resp pdpb.WatchGlobalConfigResponse
				if revision < res.CompactRevision {
					resp.Header = wrapErrorToHeader(pdpb.ErrorType_DATA_COMPACTED,
						fmt.Sprintf("required watch revision: %d is smaller than current compact/min revision %d.", revision, res.CompactRevision))
				} else {
					resp.Header = wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
						fmt.Sprintf("watch channel meet other error %s.", res.Err().Error()))
				}
				if err := server.Send(&resp); err != nil {
					return err
				}
				// Err() indicates that this WatchResponse holds a channel-closing error.
				return res.Err()
			}
			revision = res.Header.GetRevision()

			cfgs := make([]*pdpb.GlobalConfigItem, 0, len(res.Events))
			for _, e := range res.Events {
				// Since item value needs to support marshal of different struct types,
				// it should be set to `Payload bytes` instead of `Value string`.
				switch e.Type {
				case clientv3.EventTypePut:
					cfgs = append(cfgs, &pdpb.GlobalConfigItem{Name: string(e.Kv.Key), Payload: e.Kv.Value, Kind: pdpb.EventType(e.Type)})
				case clientv3.EventTypeDelete:
					if e.PrevKv != nil {
						cfgs = append(cfgs, &pdpb.GlobalConfigItem{Name: string(e.Kv.Key), Payload: e.PrevKv.Value, Kind: pdpb.EventType(e.Type)})
					} else {
						// Prev-kv is compacted means there must have been a delete event before this event,
						// which means that this is just a duplicated event, so we can just ignore it.
						log.Info("previous key-value pair has been compacted", zap.String("required-key", string(e.Kv.Key)))
					}
				}
			}

			if len(cfgs) > 0 {
				if err := server.Send(&pdpb.WatchGlobalConfigResponse{Changes: cfgs, Revision: res.Header.GetRevision()}); err != nil {
					return err
				}
			}
		}
	}
}

// Evict the leaders when the store is damaged. Damaged regions are emergency errors
// and requires user to manually remove the `evict-leader-scheduler` with pd-ctl
func (s *GrpcServer) handleDamagedStore(stats *pdpb.StoreStats) {
	// TODO: regions have no special process for the time being
	// and need to be removed in the future
	damagedRegions := stats.GetDamagedRegionsId()
	if len(damagedRegions) == 0 {
		return
	}

	for _, regionID := range stats.GetDamagedRegionsId() {
		// Remove peers to make sst recovery physically delete files in TiKV.
		err := s.GetHandler().AddRemovePeerOperator(regionID, stats.GetStoreId())
		if err != nil {
			log.Error("store damaged but can't add remove peer operator",
				zap.Uint64("region-id", regionID), zap.Uint64("store-id", stats.GetStoreId()), zap.String("error", err.Error()))
		} else {
			log.Info("added remove peer operator due to damaged region",
				zap.Uint64("region-id", regionID), zap.Uint64("store-id", stats.GetStoreId()))
		}
	}
}

// ReportMinResolvedTS implements gRPC PDServer.
func (s *GrpcServer) ReportMinResolvedTS(ctx context.Context, request *pdpb.ReportMinResolvedTsRequest) (*pdpb.ReportMinResolvedTsResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.ReportMinResolvedTsResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).ReportMinResolvedTS(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.ReportMinResolvedTsResponse), nil
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ReportMinResolvedTsResponse{Header: notBootstrappedHeader()}, nil
	}

	storeID := request.GetStoreId()
	minResolvedTS := request.GetMinResolvedTs()
	if err := rc.SetMinResolvedTS(storeID, minResolvedTS); err != nil {
		return nil, err
	}
	log.Debug("updated min resolved-ts",
		zap.Uint64("store", storeID),
		zap.Uint64("min resolved-ts", minResolvedTS))
	return &pdpb.ReportMinResolvedTsResponse{
		Header: wrapHeader(),
	}, nil
}

// SetExternalTimestamp implements gRPC PDServer.
func (s *GrpcServer) SetExternalTimestamp(ctx context.Context, request *pdpb.SetExternalTimestampRequest) (*pdpb.SetExternalTimestampResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.SetExternalTimestampResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).SetExternalTimestamp(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.SetExternalTimestampResponse), nil
	}

	nowTSO, err := s.getGlobalTSO(ctx)
	if err != nil {
		return nil, err
	}
	globalTS := tsoutil.GenerateTS(&nowTSO)
	externalTS := request.GetTimestamp()
	log.Debug("try to set external timestamp",
		zap.Uint64("external-ts", externalTS), zap.Uint64("global-ts", globalTS))
	if err := s.SetExternalTS(externalTS, globalTS); err != nil {
		return &pdpb.SetExternalTimestampResponse{Header: invalidValue(err.Error())}, nil
	}
	return &pdpb.SetExternalTimestampResponse{
		Header: wrapHeader(),
	}, nil
}

// GetExternalTimestamp implements gRPC PDServer.
func (s *GrpcServer) GetExternalTimestamp(ctx context.Context, request *pdpb.GetExternalTimestampRequest) (*pdpb.GetExternalTimestampResponse, error) {
	if s.GetServiceMiddlewarePersistOptions().IsGRPCRateLimitEnabled() {
		fName := currentFunction()
		limiter := s.GetGRPCRateLimiter()
		if done, err := limiter.Allow(fName); err == nil {
			defer done()
		} else {
			return &pdpb.GetExternalTimestampResponse{
				Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetExternalTimestamp(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetExternalTimestampResponse), nil
	}

	timestamp := s.GetExternalTS()
	return &pdpb.GetExternalTimestampResponse{
		Header:    wrapHeader(),
		Timestamp: timestamp,
	}, nil
}

func currentFunction() string {
	counter, _, _, _ := runtime.Caller(1)
	s := strings.Split(runtime.FuncForPC(counter).Name(), ".")
	return s[len(s)-1]
}
