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
	"context"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/server/versioninfo"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	heartbeatSendTimeout = 5 * time.Second

	// tso
	maxMergeTSORequests    = 10000
	defaultTSOProxyTimeout = 3 * time.Second

	// global config
	globalConfigPath = "/global/config/"
)

// gRPC errors
var (
	// ErrNotLeader is returned when current server is not the leader and not possible to process request.
	// TODO: work as proxy.
	ErrNotLeader            = status.Errorf(codes.Unavailable, "not leader")
	ErrNotStarted           = status.Errorf(codes.Unavailable, "server not started")
	ErrSendHeartbeatTimeout = status.Errorf(codes.DeadlineExceeded, "send heartbeat timeout")
	ErrEtcdNotStarted       = status.Errorf(codes.Unavailable, "server is started, but etcd not started")
)

// GrpcServer wraps Server to provide grpc service.
type GrpcServer struct {
	*Server
}

type forwardFn func(ctx context.Context, client *grpc.ClientConn) (interface{}, error)

func (s *GrpcServer) unaryMiddleware(ctx context.Context, header *pdpb.RequestHeader, fn forwardFn) (rsp interface{}, err error) {
	failpoint.Inject("customTimeout", func() {
		time.Sleep(5 * time.Second)
	})
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return fn(ctx, client)
	}
	if err := s.validateRequest(header); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *GrpcServer) wrapErrorToHeader(errorType pdpb.ErrorType, message string) *pdpb.ResponseHeader {
	return s.errorHeader(&pdpb.Error{
		Type:    errorType,
		Message: message,
	})
}

// GetMembers implements gRPC PDServer.
func (s *GrpcServer) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	// Here we purposely do not check the cluster ID because the client does not know the correct cluster ID
	// at startup and needs to get the cluster ID with the first request (i.e. GetMembers).
	if s.IsClosed() {
		return &pdpb.GetMembersResponse{
			Header: &pdpb.ResponseHeader{
				Error: &pdpb.Error{
					Type:    pdpb.ErrorType_UNKNOWN,
					Message: errs.ErrServerNotStarted.FastGenByArgs().Error(),
				},
			},
		}, nil
	}
	members, err := cluster.GetMembers(s.GetClient())
	if err != nil {
		return &pdpb.GetMembersResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
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

	tsoAllocatorManager := s.GetTSOAllocatorManager()
	tsoAllocatorLeaders, err := tsoAllocatorManager.GetLocalAllocatorLeaders()
	if err != nil {
		return &pdpb.GetMembersResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
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
		Header:              s.header(),
		Members:             members,
		Leader:              pdLeader,
		EtcdLeader:          etcdLeader,
		TsoAllocatorLeaders: tsoAllocatorLeaders,
	}, nil
}

// Tso implements gRPC PDServer.
func (s *GrpcServer) Tso(stream pdpb.PD_TsoServer) error {
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
		forwardedHost := getForwardedHost(streamCtx)
		if !s.isLocalRequest(forwardedHost) {
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
			return status.Error(codes.Unknown, err.Error())
		}
		tsoHandleDuration.Observe(time.Since(start).Seconds())
		response := &pdpb.TsoResponse{
			Header:    s.header(),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

type tsoRequest struct {
	forwardedHost string
	request       *pdpb.TsoRequest
	stream        pdpb.PD_TsoServer
}

func (s *GrpcServer) dispatchTSORequest(ctx context.Context, request *tsoRequest, forwardedHost string, doneCh <-chan struct{}, errCh chan<- error) {
	tsoRequestChInterface, loaded := s.tsoDispatcher.LoadOrStore(forwardedHost, make(chan *tsoRequest, maxMergeTSORequests))
	if !loaded {
		tsDeadlineCh := make(chan deadline, 1)
		go s.handleDispatcher(ctx, forwardedHost, tsoRequestChInterface.(chan *tsoRequest), tsDeadlineCh, doneCh, errCh)
		go watchTSDeadline(ctx, tsDeadlineCh)
	}
	tsoRequestChInterface.(chan *tsoRequest) <- request
}

func (s *GrpcServer) handleDispatcher(ctx context.Context, forwardedHost string, tsoRequestCh <-chan *tsoRequest, tsDeadlineCh chan<- deadline, doneCh <-chan struct{}, errCh chan<- error) {
	dispatcherCtx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()
	defer s.tsoDispatcher.Delete(forwardedHost)

	var (
		forwardStream pdpb.PD_TsoClient
		cancel        context.CancelFunc
	)
	client, err := s.getDelegateClient(ctx, forwardedHost)
	if err != nil {
		goto errHandling
	}
	log.Info("create tso forward stream", zap.String("forwarded-host", forwardedHost))
	forwardStream, cancel, err = s.createTsoForwardStream(client)
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

func (s *GrpcServer) processTSORequests(forwardStream pdpb.PD_TsoClient, requests []*tsoRequest) error {
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
	return s.finishTSORequest(requests, physical, firstLogical, suffixBits)
}

// Because of the suffix, we need to shift the count before we add it to the logical part.
func addLogical(logical, count int64, suffixBits uint32) int64 {
	return logical + count<<suffixBits
}

func (s *GrpcServer) finishTSORequest(requests []*tsoRequest, physical, firstLogical int64, suffixBits uint32) error {
	countSum := int64(0)
	for i := 0; i < len(requests); i++ {
		count := requests[i].request.GetCount()
		countSum += int64(count)
		response := &pdpb.TsoResponse{
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

// Bootstrap implements gRPC PDServer.
func (s *GrpcServer) Bootstrap(ctx context.Context, request *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).Bootstrap(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
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
			Header: s.errorHeader(err),
		}, nil
	}

	res, err := s.bootstrapCluster(request)
	if err != nil {
		return &pdpb.BootstrapResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	res.Header = s.header()
	return res, nil
}

// IsBootstrapped implements gRPC PDServer.
func (s *GrpcServer) IsBootstrapped(ctx context.Context, request *pdpb.IsBootstrappedRequest) (*pdpb.IsBootstrappedResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).IsBootstrapped(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.IsBootstrappedResponse), err
	}

	rc := s.GetRaftCluster()
	return &pdpb.IsBootstrappedResponse{
		Header:       s.header(),
		Bootstrapped: rc != nil,
	}, nil
}

// AllocID implements gRPC PDServer.
func (s *GrpcServer) AllocID(ctx context.Context, request *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).AllocID(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.AllocIDResponse), err
	}

	// We can use an allocator for all types ID allocation.
	id, err := s.idAllocator.Alloc()
	if err != nil {
		return &pdpb.AllocIDResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.AllocIDResponse{
		Header: s.header(),
		Id:     id,
	}, nil
}

// IsSnapshotRecovering implements gRPC PDServer.
func (s *GrpcServer) IsSnapshotRecovering(ctx context.Context, request *pdpb.IsSnapshotRecoveringRequest) (*pdpb.IsSnapshotRecoveringResponse, error) {
	// recovering mark is stored in etcd directly, there's no need to forward.
	marked, err := s.Server.IsSnapshotRecovering(ctx)
	if err != nil {
		return &pdpb.IsSnapshotRecoveringResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}
	return &pdpb.IsSnapshotRecoveringResponse{
		Header: s.header(),
		Marked: marked,
	}, nil
}

// GetStore implements gRPC PDServer.
func (s *GrpcServer) GetStore(ctx context.Context, request *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).GetStore(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetStoreResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetStoreResponse{Header: s.notBootstrappedHeader()}, nil
	}

	storeID := request.GetStoreId()
	store := rc.GetStore(storeID)
	if store == nil {
		return &pdpb.GetStoreResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				fmt.Sprintf("invalid store ID %d, not found", storeID)),
		}, nil
	}
	return &pdpb.GetStoreResponse{
		Header: s.header(),
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
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).PutStore(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.PutStoreResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.PutStoreResponse{Header: s.notBootstrappedHeader()}, nil
	}

	store := request.GetStore()
	if pberr := checkStore(rc, store.GetId()); pberr != nil {
		return &pdpb.PutStoreResponse{
			Header: s.errorHeader(pberr),
		}, nil
	}

	// NOTE: can be removed when placement rules feature is enabled by default.
	if !s.GetConfig().Replication.EnablePlacementRules && core.IsStoreContainLabel(store, core.EngineKey, core.EngineTiFlash) {
		return &pdpb.PutStoreResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				"placement rules is disabled"),
		}, nil
	}

	if err := rc.PutStore(store); err != nil {
		return &pdpb.PutStoreResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	log.Info("put store ok", zap.Stringer("store", store))
	CheckPDVersion(s.persistOptions)

	return &pdpb.PutStoreResponse{
		Header:            s.header(),
		ReplicationStatus: rc.GetReplicationMode().GetReplicationStatus(),
	}, nil
}

// GetAllStores implements gRPC PDServer.
func (s *GrpcServer) GetAllStores(ctx context.Context, request *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).GetAllStores(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetAllStoresResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetAllStoresResponse{Header: s.notBootstrappedHeader()}, nil
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
		Header: s.header(),
		Stores: stores,
	}, nil
}

// StoreHeartbeat implements gRPC PDServer.
func (s *GrpcServer) StoreHeartbeat(ctx context.Context, request *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).StoreHeartbeat(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.StoreHeartbeatResponse), err
	}

	if request.GetStats() == nil {
		return nil, errors.Errorf("invalid store heartbeat command, but %v", request)
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.StoreHeartbeatResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if pberr := checkStore(rc, request.GetStats().GetStoreId()); pberr != nil {
		return &pdpb.StoreHeartbeatResponse{
			Header: s.errorHeader(pberr),
		}, nil
	}
	storeID := request.GetStats().GetStoreId()
	store := rc.GetStore(storeID)
	if store == nil {
		return &pdpb.StoreHeartbeatResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				fmt.Sprintf("store %v not found", storeID)),
		}, nil
	}

	resp := &pdpb.StoreHeartbeatResponse{Header: s.header()}
	// Bypass stats handling if the store report for unsafe recover is not empty.
	if request.GetStoreReport() == nil {
		storeAddress := store.GetAddress()
		storeLabel := strconv.FormatUint(storeID, 10)
		start := time.Now()

		err := rc.HandleStoreHeartbeat(request, resp)
		if err != nil {
			return &pdpb.StoreHeartbeatResponse{
				Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
					err.Error()),
			}, nil
		}

		s.handleDamagedStore(request.GetStats())

		storeHeartbeatHandleDuration.WithLabelValues(storeAddress, storeLabel).Observe(time.Since(start).Seconds())
	}

	if status := request.GetDrAutosyncStatus(); status != nil {
		rc.GetReplicationMode().UpdateStoreDRStatus(request.GetStats().GetStoreId(), status)
	}

	resp.ReplicationStatus = rc.GetReplicationMode().GetReplicationStatus()
	resp.ClusterVersion = rc.GetClusterVersion()
	rc.GetUnsafeRecoveryController().HandleStoreHeartbeat(request, resp)

	return resp, nil
}

// bucketHeartbeatServer wraps PD_ReportBucketsServer to ensure when any error
// occurs on SendAndClose() or Recv(), both endpoints will be closed.
type bucketHeartbeatServer struct {
	stream pdpb.PD_ReportBucketsServer
	closed int32
}

func (b *bucketHeartbeatServer) Send(bucket *pdpb.ReportBucketsResponse) error {
	if atomic.LoadInt32(&b.closed) == 1 {
		return status.Errorf(codes.Canceled, "stream is closed")
	}
	done := make(chan error, 1)
	go func() {
		done <- b.stream.SendAndClose(bucket)
	}()
	select {
	case err := <-done:
		if err != nil {
			atomic.StoreInt32(&b.closed, 1)
		}
		return err
	case <-time.After(heartbeatSendTimeout):
		atomic.StoreInt32(&b.closed, 1)
		return ErrSendHeartbeatTimeout
	}
}

func (b *bucketHeartbeatServer) Recv() (*pdpb.ReportBucketsRequest, error) {
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

// heartbeatServer wraps PD_RegionHeartbeatServer to ensure when any error
// occurs on Send() or Recv(), both endpoints will be closed.
type heartbeatServer struct {
	stream pdpb.PD_RegionHeartbeatServer
	closed int32
}

func (s *heartbeatServer) Send(m *pdpb.RegionHeartbeatResponse) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.EOF
	}
	done := make(chan error, 1)
	go func() { done <- s.stream.Send(m) }()
	select {
	case err := <-done:
		if err != nil {
			atomic.StoreInt32(&s.closed, 1)
		}
		return errors.WithStack(err)
	case <-time.After(heartbeatSendTimeout):
		atomic.StoreInt32(&s.closed, 1)
		return ErrSendHeartbeatTimeout
	}
}

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
	for {
		request, err := server.Recv()
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
		forwardedHost := getForwardedHost(stream.Context())
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
				Header: s.notBootstrappedHeader(),
			}
			err := server.Send(resp)
			return errors.WithStack(err)
		}
		if err := s.validateRequest(request.GetHeader()); err != nil {
			return err
		}
		buckets := request.GetBuckets()
		if buckets == nil || len(buckets.Keys) == 0 {
			continue
		}
		store := rc.GetLeaderStoreByRegionID(buckets.GetRegionId())
		if store == nil {
			return errors.Errorf("the store of the bucket in region %v is not found ", buckets.GetRegionId())
		}
		storeLabel := strconv.FormatUint(store.GetID(), 10)
		storeAddress := store.GetAddress()
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
		server            = &heartbeatServer{stream: stream}
		flowRoundOption   = core.WithFlowRoundByDigit(s.persistOptions.GetPDServerConfig().FlowRoundByDigit)
		forwardStream     pdpb.PD_RegionHeartbeatClient
		cancel            context.CancelFunc
		lastForwardedHost string
		lastBind          time.Time
		errCh             chan error
	)
	defer func() {
		// cancel the forward stream
		if cancel != nil {
			cancel()
		}
	}()

	for {
		request, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		forwardedHost := getForwardedHost(stream.Context())
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
				forwardStream, cancel, err = s.createHeartbeatForwardStream(client)
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
				Header: s.notBootstrappedHeader(),
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
			flowRoundOption = core.WithFlowRoundByDigit(s.persistOptions.GetPDServerConfig().FlowRoundByDigit)
			lastBind = time.Now()
		}

		region := core.RegionFromHeartbeat(request, flowRoundOption)
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
	}
}

// GetRegion implements gRPC PDServer.
func (s *GrpcServer) GetRegion(ctx context.Context, request *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).GetRegion(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetRegionResponse), nil
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}
	region := rc.GetRegionByKey(request.GetRegionKey())
	if region == nil {
		return &pdpb.GetRegionResponse{Header: s.header()}, nil
	}
	var buckets *metapb.Buckets
	if rc.GetStoreConfig().IsEnableRegionBucket() && request.GetNeedBuckets() {
		buckets = region.GetBuckets()
	}
	return &pdpb.GetRegionResponse{
		Header:       s.header(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
		Buckets:      buckets,
	}, nil
}

// GetPrevRegion implements gRPC PDServer
func (s *GrpcServer) GetPrevRegion(ctx context.Context, request *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).GetPrevRegion(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetRegionResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}

	region := rc.GetPrevRegionByKey(request.GetRegionKey())
	if region == nil {
		return &pdpb.GetRegionResponse{Header: s.header()}, nil
	}
	var buckets *metapb.Buckets
	if rc.GetStoreConfig().IsEnableRegionBucket() && request.GetNeedBuckets() {
		buckets = region.GetBuckets()
	}
	return &pdpb.GetRegionResponse{
		Header:       s.header(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
		Buckets:      buckets,
	}, nil
}

// GetRegionByID implements gRPC PDServer.
func (s *GrpcServer) GetRegionByID(ctx context.Context, request *pdpb.GetRegionByIDRequest) (*pdpb.GetRegionResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).GetRegionByID(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetRegionResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}
	region := rc.GetRegion(request.GetRegionId())
	if region == nil {
		return &pdpb.GetRegionResponse{Header: s.header()}, nil
	}
	var buckets *metapb.Buckets
	if rc.GetStoreConfig().IsEnableRegionBucket() && request.GetNeedBuckets() {
		buckets = region.GetBuckets()
	}
	return &pdpb.GetRegionResponse{
		Header:       s.header(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
		Buckets:      buckets,
	}, nil
}

// ScanRegions implements gRPC PDServer.
func (s *GrpcServer) ScanRegions(ctx context.Context, request *pdpb.ScanRegionsRequest) (*pdpb.ScanRegionsResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).ScanRegions(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.ScanRegionsResponse), nil
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ScanRegionsResponse{Header: s.notBootstrappedHeader()}, nil
	}
	regions := rc.ScanRegions(request.GetStartKey(), request.GetEndKey(), int(request.GetLimit()))
	resp := &pdpb.ScanRegionsResponse{Header: s.header()}
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

// AskSplit implements gRPC PDServer.
func (s *GrpcServer) AskSplit(ctx context.Context, request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).AskSplit(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.AskSplitResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.AskSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	if request.GetRegion() == nil {
		return &pdpb.AskSplitResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_REGION_NOT_FOUND,
				"missing region for split"),
		}, nil
	}
	req := &pdpb.AskSplitRequest{
		Region: request.Region,
	}
	split, err := rc.HandleAskSplit(req)
	if err != nil {
		return &pdpb.AskSplitResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.AskSplitResponse{
		Header:      s.header(),
		NewRegionId: split.NewRegionId,
		NewPeerIds:  split.NewPeerIds,
	}, nil
}

// AskBatchSplit implements gRPC PDServer.
func (s *GrpcServer) AskBatchSplit(ctx context.Context, request *pdpb.AskBatchSplitRequest) (*pdpb.AskBatchSplitResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).AskBatchSplit(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.AskBatchSplitResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.AskBatchSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if !versioninfo.IsFeatureSupported(rc.GetOpts().GetClusterVersion(), versioninfo.BatchSplit) {
		return &pdpb.AskBatchSplitResponse{Header: s.incompatibleVersion("batch_split")}, nil
	}
	if request.GetRegion() == nil {
		return &pdpb.AskBatchSplitResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_REGION_NOT_FOUND,
				"missing region for split"),
		}, nil
	}
	req := &pdpb.AskBatchSplitRequest{
		Region:     request.Region,
		SplitCount: request.SplitCount,
	}
	split, err := rc.HandleAskBatchSplit(req)
	if err != nil {
		return &pdpb.AskBatchSplitResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.AskBatchSplitResponse{
		Header: s.header(),
		Ids:    split.Ids,
	}, nil
}

// ReportSplit implements gRPC PDServer.
func (s *GrpcServer) ReportSplit(ctx context.Context, request *pdpb.ReportSplitRequest) (*pdpb.ReportSplitResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).ReportSplit(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.ReportSplitResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ReportSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	_, err := rc.HandleReportSplit(request)
	if err != nil {
		return &pdpb.ReportSplitResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.ReportSplitResponse{
		Header: s.header(),
	}, nil
}

// ReportBatchSplit implements gRPC PDServer.
func (s *GrpcServer) ReportBatchSplit(ctx context.Context, request *pdpb.ReportBatchSplitRequest) (*pdpb.ReportBatchSplitResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).ReportBatchSplit(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.ReportBatchSplitResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ReportBatchSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	_, err := rc.HandleBatchReportSplit(request)
	if err != nil {
		return &pdpb.ReportBatchSplitResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				err.Error()),
		}, nil
	}

	return &pdpb.ReportBatchSplitResponse{
		Header: s.header(),
	}, nil
}

// GetClusterConfig implements gRPC PDServer.
func (s *GrpcServer) GetClusterConfig(ctx context.Context, request *pdpb.GetClusterConfigRequest) (*pdpb.GetClusterConfigResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).GetClusterConfig(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetClusterConfigResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetClusterConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}
	return &pdpb.GetClusterConfigResponse{
		Header:  s.header(),
		Cluster: rc.GetMetaCluster(),
	}, nil
}

// PutClusterConfig implements gRPC PDServer.
func (s *GrpcServer) PutClusterConfig(ctx context.Context, request *pdpb.PutClusterConfigRequest) (*pdpb.PutClusterConfigResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).PutClusterConfig(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.PutClusterConfigResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.PutClusterConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}
	conf := request.GetCluster()
	if err := rc.PutMetaCluster(conf); err != nil {
		return &pdpb.PutClusterConfigResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				err.Error()),
		}, nil
	}

	log.Info("put cluster config ok", zap.Reflect("config", conf))

	return &pdpb.PutClusterConfigResponse{
		Header: s.header(),
	}, nil
}

// ScatterRegion implements gRPC PDServer.
func (s *GrpcServer) ScatterRegion(ctx context.Context, request *pdpb.ScatterRegionRequest) (*pdpb.ScatterRegionResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).ScatterRegion(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.ScatterRegionResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ScatterRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if len(request.GetRegionsId()) > 0 {
		percentage, err := scatterRegions(rc, request.GetRegionsId(), request.GetGroup(), int(request.GetRetryLimit()))
		if err != nil {
			return nil, err
		}
		return &pdpb.ScatterRegionResponse{
			Header:             s.header(),
			FinishedPercentage: uint64(percentage),
		}, nil
	}
	// TODO: Deprecate it use `request.GetRegionsID`.
	//nolint
	region := rc.GetRegion(request.GetRegionId())
	if region == nil {
		if request.GetRegion() == nil {
			//nolint
			return &pdpb.ScatterRegionResponse{
				Header: s.wrapErrorToHeader(pdpb.ErrorType_REGION_NOT_FOUND,
					"region %d not found"),
			}, nil
		}
		region = core.NewRegionInfo(request.GetRegion(), request.GetLeader())
	}

	op, err := rc.GetRegionScatter().Scatter(region, request.GetGroup())
	if err != nil {
		return nil, err
	}
	if op != nil {
		rc.GetOperatorController().AddOperator(op)
	}

	return &pdpb.ScatterRegionResponse{
		Header:             s.header(),
		FinishedPercentage: 100,
	}, nil
}

// GetGCSafePoint implements gRPC PDServer.
func (s *GrpcServer) GetGCSafePoint(ctx context.Context, request *pdpb.GetGCSafePointRequest) (*pdpb.GetGCSafePointResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).GetGCSafePoint(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetGCSafePointResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}

	safePoint, err := s.gcSafePointManager.LoadGCSafePoint()
	if err != nil {
		return nil, err
	}

	return &pdpb.GetGCSafePointResponse{
		Header:    s.header(),
		SafePoint: safePoint,
	}, nil
}

// SyncRegions syncs the regions.
func (s *GrpcServer) SyncRegions(stream pdpb.PD_SyncRegionsServer) error {
	if s.IsClosed() || s.cluster == nil {
		return ErrNotStarted
	}
	ctx := s.cluster.Context()
	if ctx == nil {
		return ErrNotStarted
	}
	return s.cluster.GetRegionSyncer().Sync(ctx, stream)
}

// UpdateGCSafePoint implements gRPC PDServer.
func (s *GrpcServer) UpdateGCSafePoint(ctx context.Context, request *pdpb.UpdateGCSafePointRequest) (*pdpb.UpdateGCSafePointResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).UpdateGCSafePoint(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.UpdateGCSafePointResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.UpdateGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
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
		Header:       s.header(),
		NewSafePoint: newSafePoint,
	}, nil
}

// UpdateServiceGCSafePoint update the safepoint for specific service
func (s *GrpcServer) UpdateServiceGCSafePoint(ctx context.Context, request *pdpb.UpdateServiceGCSafePointRequest) (*pdpb.UpdateServiceGCSafePointResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).UpdateServiceGCSafePoint(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.UpdateServiceGCSafePointResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.UpdateServiceGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}
	var storage endpoint.GCSafePointStorage = s.storage
	if request.TTL <= 0 {
		if err := storage.RemoveServiceGCSafePoint(string(request.ServiceId)); err != nil {
			return nil, err
		}
	}

	nowTSO, err := s.tsoAllocatorManager.HandleTSORequest(tso.GlobalDCLocation, 1)
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
		Header:       s.header(),
		ServiceId:    []byte(min.ServiceID),
		TTL:          min.ExpiredAt - now.Unix(),
		MinSafePoint: min.SafePoint,
	}, nil
}

// GetOperator gets information about the operator belonging to the specify region.
func (s *GrpcServer) GetOperator(ctx context.Context, request *pdpb.GetOperatorRequest) (*pdpb.GetOperatorResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).GetOperator(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetOperatorResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetOperatorResponse{Header: s.notBootstrappedHeader()}, nil
	}

	opController := rc.GetOperatorController()
	requestID := request.GetRegionId()
	r := opController.GetOperatorStatus(requestID)
	if r == nil {
		header := s.errorHeader(&pdpb.Error{
			Type:    pdpb.ErrorType_REGION_NOT_FOUND,
			Message: "Not Found",
		})
		return &pdpb.GetOperatorResponse{Header: header}, nil
	}

	return &pdpb.GetOperatorResponse{
		Header:   s.header(),
		RegionId: requestID,
		Desc:     []byte(r.Desc()),
		Kind:     []byte(r.Kind().String()),
		Status:   r.Status,
	}, nil
}

// validateRequest checks if Server is leader and clusterID is matched.
// TODO: Call it in gRPC interceptor.
func (s *GrpcServer) validateRequest(header *pdpb.RequestHeader) error {
	if s.IsClosed() || !s.member.IsLeader() {
		return ErrNotLeader
	}
	if header.GetClusterId() != s.clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, header.GetClusterId())
	}
	return nil
}

func (s *GrpcServer) header() *pdpb.ResponseHeader {
	if s.clusterID == 0 {
		return s.wrapErrorToHeader(pdpb.ErrorType_NOT_BOOTSTRAPPED, "cluster id is not ready")
	}
	return &pdpb.ResponseHeader{ClusterId: s.clusterID}
}

func (s *GrpcServer) errorHeader(err *pdpb.Error) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

func (s *GrpcServer) notBootstrappedHeader() *pdpb.ResponseHeader {
	return s.errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}

func (s *GrpcServer) incompatibleVersion(tag string) *pdpb.ResponseHeader {
	msg := fmt.Sprintf("%s incompatible with current cluster version %s", tag, s.persistOptions.GetClusterVersion())
	return s.errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_INCOMPATIBLE_VERSION,
		Message: msg,
	})
}

func (s *GrpcServer) invalidValue(msg string) *pdpb.ResponseHeader {
	return s.errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_INVALID_VALUE,
		Message: msg,
	})
}

// Only used for the TestLocalAllocatorLeaderChange.
var mockLocalAllocatorLeaderChangeFlag = false

// SyncMaxTS will check whether MaxTS is the biggest one among all Local TSOs this PD is holding when skipCheck is set,
// and write it into all Local TSO Allocators then if it's indeed the biggest one.
func (s *GrpcServer) SyncMaxTS(_ context.Context, request *pdpb.SyncMaxTSRequest) (*pdpb.SyncMaxTSResponse, error) {
	if err := s.validateInternalRequest(request.GetHeader(), true); err != nil {
		return nil, err
	}
	tsoAllocatorManager := s.GetTSOAllocatorManager()
	// There is no dc-location found in this server, return err.
	if tsoAllocatorManager.GetClusterDCLocationsNumber() == 0 {
		return &pdpb.SyncMaxTSResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				"empty cluster dc-location found, checker may not work properly"),
		}, nil
	}
	// Get all Local TSO Allocator leaders
	allocatorLeaders, err := tsoAllocatorManager.GetHoldingLocalAllocatorLeaders()
	if err != nil {
		return &pdpb.SyncMaxTSResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
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
					Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
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
				Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
					"local tso allocator leaders have changed during the sync, should retry"),
			}, nil
		}
		if request.GetMaxTs() == nil {
			return &pdpb.SyncMaxTSResponse{
				Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
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
				Header:     s.header(),
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
				Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
			}, nil
		}
		syncedDCs = append(syncedDCs, allocator.GetDCLocation())
	}
	return &pdpb.SyncMaxTSResponse{
		Header:    s.header(),
		SyncedDcs: syncedDCs,
	}, nil
}

// SplitRegions split regions by the given split keys
func (s *GrpcServer) SplitRegions(ctx context.Context, request *pdpb.SplitRegionsRequest) (*pdpb.SplitRegionsResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).SplitRegions(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.SplitRegionsResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.SplitRegionsResponse{Header: s.notBootstrappedHeader()}, nil
	}
	finishedPercentage, newRegionIDs := rc.GetRegionSplitter().SplitRegions(ctx, request.GetSplitKeys(), int(request.GetRetryLimit()))
	return &pdpb.SplitRegionsResponse{
		Header:             s.header(),
		RegionsId:          newRegionIDs,
		FinishedPercentage: uint64(finishedPercentage),
	}, nil
}

// SplitAndScatterRegions split regions by the given split keys, and scatter regions.
// Only regions which splited successfully will be scattered.
// scatterFinishedPercentage indicates the percentage of successfully splited regions that are scattered.
func (s *GrpcServer) SplitAndScatterRegions(ctx context.Context, request *pdpb.SplitAndScatterRegionsRequest) (*pdpb.SplitAndScatterRegionsResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).SplitAndScatterRegions(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.SplitAndScatterRegionsResponse), err
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.SplitAndScatterRegionsResponse{Header: s.notBootstrappedHeader()}, nil
	}
	splitFinishedPercentage, newRegionIDs := rc.GetRegionSplitter().SplitRegions(ctx, request.GetSplitKeys(), int(request.GetRetryLimit()))
	scatterFinishedPercentage, err := scatterRegions(rc, newRegionIDs, request.GetGroup(), int(request.GetRetryLimit()))
	if err != nil {
		return nil, err
	}
	return &pdpb.SplitAndScatterRegionsResponse{
		Header:                    s.header(),
		RegionsId:                 newRegionIDs,
		SplitFinishedPercentage:   uint64(splitFinishedPercentage),
		ScatterFinishedPercentage: uint64(scatterFinishedPercentage),
	}, nil
}

// scatterRegions add operators to scatter regions and return the processed percentage and error
func scatterRegions(cluster *cluster.RaftCluster, regionsID []uint64, group string, retryLimit int) (int, error) {
	opsCount, failures, err := cluster.GetRegionScatter().ScatterRegionsByID(regionsID, group, retryLimit)
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

// GetDCLocationInfo gets the dc-location info of the given dc-location from PD leader's TSO allocator manager.
func (s *GrpcServer) GetDCLocationInfo(ctx context.Context, request *pdpb.GetDCLocationInfoRequest) (*pdpb.GetDCLocationInfoResponse, error) {
	var err error
	if err = s.validateInternalRequest(request.GetHeader(), false); err != nil {
		return nil, err
	}
	if !s.member.IsLeader() {
		return nil, ErrNotLeader
	}
	am := s.tsoAllocatorManager
	info, ok := am.GetDCLocationInfo(request.GetDcLocation())
	if !ok {
		am.ClusterDCLocationChecker()
		return &pdpb.GetDCLocationInfoResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
				fmt.Sprintf("dc-location %s is not found", request.GetDcLocation())),
		}, nil
	}
	resp := &pdpb.GetDCLocationInfoResponse{
		Header: s.header(),
		Suffix: info.Suffix,
	}
	// Because the number of suffix bits is changing dynamically according to the dc-location number,
	// there is a corner case may cause the Local TSO is not unique while member changing.
	// Example:
	//     t1: xxxxxxxxxxxxxxx1 | 11
	//     t2: xxxxxxxxxxxxxxx | 111
	// So we will force the newly added Local TSO Allocator to have a Global TSO synchronization
	// when it becomes the Local TSO Allocator leader.
	// Please take a look at https://github.com/tikv/pd/issues/3260 for more details.
	if resp.MaxTs, err = am.GetMaxLocalTSO(ctx); err != nil {
		return &pdpb.GetDCLocationInfoResponse{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}
	return resp, nil
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

func (s *GrpcServer) getDelegateClient(ctx context.Context, forwardedHost string) (*grpc.ClientConn, error) {
	client, ok := s.clientConns.Load(forwardedHost)
	if !ok {
		tlsConfig, err := s.GetTLSConfig().ToTLSConfig()
		if err != nil {
			return nil, err
		}
		cc, err := grpcutil.GetClientConn(ctx, forwardedHost, tlsConfig)
		if err != nil {
			return nil, err
		}
		client = cc
		s.clientConns.Store(forwardedHost, cc)
	}
	return client.(*grpc.ClientConn), nil
}

func getForwardedHost(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Debug("failed to get forwarding metadata")
	}
	if t, ok := md[grpcutil.ForwardMetadataKey]; ok {
		return t[0]
	}
	return ""
}

func (s *GrpcServer) isLocalRequest(forwardedHost string) bool {
	failpoint.Inject("useForwardRequest", func() {
		failpoint.Return(false)
	})
	if forwardedHost == "" {
		return true
	}
	memberAddrs := s.GetMember().Member().GetClientUrls()
	for _, addr := range memberAddrs {
		if addr == forwardedHost {
			return true
		}
	}
	return false
}

func (s *GrpcServer) createTsoForwardStream(client *grpc.ClientConn) (pdpb.PD_TsoClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go checkStream(ctx, cancel, done)
	forwardStream, err := pdpb.NewPDClient(client).Tso(ctx)
	done <- struct{}{}
	return forwardStream, cancel, err
}

func (s *GrpcServer) createHeartbeatForwardStream(client *grpc.ClientConn) (pdpb.PD_RegionHeartbeatClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go checkStream(ctx, cancel, done)
	forwardStream, err := pdpb.NewPDClient(client).RegionHeartbeat(ctx)
	done <- struct{}{}
	return forwardStream, cancel, err
}

func forwardRegionHeartbeatClientToServer(forwardStream pdpb.PD_RegionHeartbeatClient, server *heartbeatServer, errCh chan error) {
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

func (s *GrpcServer) createReportBucketsForwardStream(client *grpc.ClientConn) (pdpb.PD_ReportBucketsClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go checkStream(ctx, cancel, done)
	forwardStream, err := pdpb.NewPDClient(client).ReportBuckets(ctx)
	done <- struct{}{}
	return forwardStream, cancel, err
}

func forwardReportBucketClientToServer(forwardStream pdpb.PD_ReportBucketsClient, server *bucketHeartbeatServer, errCh chan error) {
	defer close(errCh)
	for {
		resp, err := forwardStream.CloseAndRecv()
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

// TODO: If goroutine here timeout when tso stream created successfully, we need to handle it correctly.
func checkStream(streamCtx context.Context, cancel context.CancelFunc, done chan struct{}) {
	select {
	case <-done:
		return
	case <-time.After(3 * time.Second):
		cancel()
	case <-streamCtx.Done():
	}
	<-done
}

// StoreGlobalConfig store global config into etcd by transaction
func (s *GrpcServer) StoreGlobalConfig(_ context.Context, request *pdpb.StoreGlobalConfigRequest) (*pdpb.StoreGlobalConfigResponse, error) {
	if s.client == nil {
		return nil, ErrEtcdNotStarted
	}
	ops := make([]clientv3.Op, len(request.Changes))
	for i, item := range request.Changes {
		name := globalConfigPath + item.GetName()
		value := item.GetValue()
		ops[i] = clientv3.OpPut(name, value)
	}
	res, err :=
		kv.NewSlowLogTxn(s.client).Then(ops...).Commit()
	if err != nil {
		return &pdpb.StoreGlobalConfigResponse{Error: &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: err.Error()}}, err
	}
	if !res.Succeeded {
		return &pdpb.StoreGlobalConfigResponse{Error: &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: "failed to execute StoreGlobalConfig transaction"}}, errors.Errorf("failed to execute StoreGlobalConfig transaction")
	}
	return &pdpb.StoreGlobalConfigResponse{}, err
}

// LoadGlobalConfig load global config from etcd
func (s *GrpcServer) LoadGlobalConfig(ctx context.Context, request *pdpb.LoadGlobalConfigRequest) (*pdpb.LoadGlobalConfigResponse, error) {
	if s.client == nil {
		return nil, ErrEtcdNotStarted
	}
	names := request.Names
	res := make([]*pdpb.GlobalConfigItem, len(names))
	for i, name := range names {
		r, err := s.client.Get(ctx, globalConfigPath+name)
		if err != nil {
			res[i] = &pdpb.GlobalConfigItem{Name: name, Error: &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: err.Error()}}
		} else if len(r.Kvs) == 0 {
			msg := "key " + name + " not found"
			res[i] = &pdpb.GlobalConfigItem{Name: name, Error: &pdpb.Error{Type: pdpb.ErrorType_GLOBAL_CONFIG_NOT_FOUND, Message: msg}}
		} else {
			res[i] = &pdpb.GlobalConfigItem{Name: name, Value: string(r.Kvs[0].Value)}
		}
	}
	return &pdpb.LoadGlobalConfigResponse{Items: res}, nil
}

// WatchGlobalConfig if the connection of WatchGlobalConfig is end
// or stoped by whatever reason
// just reconnect to it.
func (s *GrpcServer) WatchGlobalConfig(_ *pdpb.WatchGlobalConfigRequest, server pdpb.PD_WatchGlobalConfigServer) error {
	if s.client == nil {
		return ErrEtcdNotStarted
	}
	ctx, cancel := context.WithCancel(s.Context())
	defer cancel()
	err := s.sendAllGlobalConfig(ctx, server)
	if err != nil {
		return err
	}
	watchChan := s.client.Watch(ctx, globalConfigPath, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-watchChan:
			cfgs := make([]*pdpb.GlobalConfigItem, 0, len(res.Events))
			for _, e := range res.Events {
				if e.Type != clientv3.EventTypePut {
					continue
				}
				cfgs = append(cfgs, &pdpb.GlobalConfigItem{Name: string(e.Kv.Key), Value: string(e.Kv.Value)})
			}
			if len(cfgs) > 0 {
				err := server.Send(&pdpb.WatchGlobalConfigResponse{Changes: cfgs})
				if err != nil {
					return err
				}
			}
		}
	}
}

func (s *GrpcServer) sendAllGlobalConfig(ctx context.Context, server pdpb.PD_WatchGlobalConfigServer) error {
	configList, err := s.client.Get(ctx, globalConfigPath, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	ls := make([]*pdpb.GlobalConfigItem, configList.Count)
	for i, kv := range configList.Kvs {
		ls[i] = &pdpb.GlobalConfigItem{Name: string(kv.Key), Value: string(kv.Value)}
	}
	err = server.Send(&pdpb.WatchGlobalConfigResponse{Changes: ls})
	return err
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
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).ReportMinResolvedTS(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ReportMinResolvedTsResponse{Header: s.notBootstrappedHeader()}, nil
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
		Header: s.header(),
	}, nil
}

// SetExternalTimestamp implements gRPC PDServer.
func (s *GrpcServer) SetExternalTimestamp(ctx context.Context, request *pdpb.SetExternalTimestampRequest) (*pdpb.SetExternalTimestampResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).SetExternalTimestamp(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	timestamp := request.GetTimestamp()
	if err := s.SetExternalTS(timestamp); err != nil {
		return &pdpb.SetExternalTimestampResponse{Header: s.invalidValue(err.Error())}, nil
	}
	log.Debug("set external timestamp",
		zap.Uint64("timestamp", timestamp))
	return &pdpb.SetExternalTimestampResponse{
		Header: s.header(),
	}, nil
}

// GetExternalTimestamp implements gRPC PDServer.
func (s *GrpcServer) GetExternalTimestamp(ctx context.Context, request *pdpb.GetExternalTimestampRequest) (*pdpb.GetExternalTimestampResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).GetExternalTimestamp(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	timestamp := s.GetExternalTS()
	return &pdpb.GetExternalTimestampResponse{
		Header:    s.header(),
		Timestamp: timestamp,
	}, nil
}
