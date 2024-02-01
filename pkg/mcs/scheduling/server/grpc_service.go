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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gRPC errors
var (
	ErrNotStarted        = status.Errorf(codes.Unavailable, "server not started")
	ErrClusterMismatched = status.Errorf(codes.Unavailable, "cluster mismatched")
)

// SetUpRestHandler is a hook to sets up the REST service.
var SetUpRestHandler = func(srv *Service) (http.Handler, apiutil.APIServiceGroup) {
	return dummyRestService{}, apiutil.APIServiceGroup{}
}

type dummyRestService struct{}

func (d dummyRestService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("not implemented"))
}

// ConfigProvider is used to get scheduling config from the given
// `bs.server` without modifying its interface.
type ConfigProvider any

// Service is the scheduling grpc service.
type Service struct {
	*Server
}

// NewService creates a new scheduling service.
func NewService[T ConfigProvider](svr bs.Server) registry.RegistrableService {
	server, ok := svr.(*Server)
	if !ok {
		log.Fatal("create scheduling server failed")
	}
	return &Service{
		Server: server,
	}
}

// heartbeatServer wraps Scheduling_RegionHeartbeatServer to ensure when any error
// occurs on Send() or Recv(), both endpoints will be closed.
type heartbeatServer struct {
	stream schedulingpb.Scheduling_RegionHeartbeatServer
	closed int32
}

func (s *heartbeatServer) Send(m core.RegionHeartbeatResponse) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.EOF
	}
	done := make(chan error, 1)
	go func() {
		defer logutil.LogPanic()
		done <- s.stream.Send(m.(*schedulingpb.RegionHeartbeatResponse))
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			atomic.StoreInt32(&s.closed, 1)
		}
		return errors.WithStack(err)
	case <-timer.C:
		atomic.StoreInt32(&s.closed, 1)
		return status.Errorf(codes.DeadlineExceeded, "send heartbeat timeout")
	}
}

func (s *heartbeatServer) Recv() (*schedulingpb.RegionHeartbeatRequest, error) {
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

// RegionHeartbeat implements gRPC SchedulingServer.
func (s *Service) RegionHeartbeat(stream schedulingpb.Scheduling_RegionHeartbeatServer) error {
	var (
		server   = &heartbeatServer{stream: stream}
		cancel   context.CancelFunc
		lastBind time.Time
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

		c := s.GetCluster()
		if c == nil {
			resp := &schedulingpb.RegionHeartbeatResponse{Header: s.notBootstrappedHeader()}
			err := server.Send(resp)
			return errors.WithStack(err)
		}

		storeID := request.GetLeader().GetStoreId()
		store := c.GetStore(storeID)
		if store == nil {
			return errors.Errorf("invalid store ID %d, not found", storeID)
		}

		if time.Since(lastBind) > time.Minute {
			s.hbStreams.BindStream(storeID, server)
			lastBind = time.Now()
		}
		region := core.RegionFromHeartbeat(request)
		err = c.HandleRegionHeartbeat(region)
		if err != nil {
			// TODO: if we need to send the error back to API server.
			log.Error("failed handle region heartbeat", zap.Error(err))
			continue
		}
	}
}

// StoreHeartbeat implements gRPC SchedulingServer.
func (s *Service) StoreHeartbeat(ctx context.Context, request *schedulingpb.StoreHeartbeatRequest) (*schedulingpb.StoreHeartbeatResponse, error) {
	c := s.GetCluster()
	if c == nil {
		// TODO: add metrics
		log.Info("cluster isn't initialized")
		return &schedulingpb.StoreHeartbeatResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if c.GetStore(request.GetStats().GetStoreId()) == nil {
		s.metaWatcher.GetStoreWatcher().ForceLoad()
	}

	// TODO: add metrics
	if err := c.HandleStoreHeartbeat(request); err != nil {
		log.Error("handle store heartbeat failed", zap.Error(err))
	}
	return &schedulingpb.StoreHeartbeatResponse{Header: &schedulingpb.ResponseHeader{ClusterId: s.clusterID}}, nil
}

// SplitRegions split regions by the given split keys
func (s *Service) SplitRegions(ctx context.Context, request *schedulingpb.SplitRegionsRequest) (*schedulingpb.SplitRegionsResponse, error) {
	c := s.GetCluster()
	if c == nil {
		return &schedulingpb.SplitRegionsResponse{Header: s.notBootstrappedHeader()}, nil
	}
	finishedPercentage, newRegionIDs := c.GetRegionSplitter().SplitRegions(ctx, request.GetSplitKeys(), int(request.GetRetryLimit()))
	return &schedulingpb.SplitRegionsResponse{
		Header:             s.header(),
		RegionsId:          newRegionIDs,
		FinishedPercentage: uint64(finishedPercentage),
	}, nil
}

// ScatterRegions implements gRPC SchedulingServer.
func (s *Service) ScatterRegions(ctx context.Context, request *schedulingpb.ScatterRegionsRequest) (*schedulingpb.ScatterRegionsResponse, error) {
	c := s.GetCluster()
	if c == nil {
		return &schedulingpb.ScatterRegionsResponse{Header: s.notBootstrappedHeader()}, nil
	}

	opsCount, failures, err := c.GetRegionScatterer().ScatterRegionsByID(request.GetRegionsId(), request.GetGroup(), int(request.GetRetryLimit()), request.GetSkipStoreLimit())
	if err != nil {
		header := s.errorHeader(&schedulingpb.Error{
			Type:    schedulingpb.ErrorType_UNKNOWN,
			Message: err.Error(),
		})
		return &schedulingpb.ScatterRegionsResponse{Header: header}, nil
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
	return &schedulingpb.ScatterRegionsResponse{
		Header:             s.header(),
		FinishedPercentage: uint64(percentage),
	}, nil
}

// GetOperator gets information about the operator belonging to the specify region.
func (s *Service) GetOperator(ctx context.Context, request *schedulingpb.GetOperatorRequest) (*schedulingpb.GetOperatorResponse, error) {
	c := s.GetCluster()
	if c == nil {
		return &schedulingpb.GetOperatorResponse{Header: s.notBootstrappedHeader()}, nil
	}

	opController := c.GetCoordinator().GetOperatorController()
	requestID := request.GetRegionId()
	r := opController.GetOperatorStatus(requestID)
	if r == nil {
		header := s.errorHeader(&schedulingpb.Error{
			Type:    schedulingpb.ErrorType_UNKNOWN,
			Message: "region not found",
		})
		return &schedulingpb.GetOperatorResponse{Header: header}, nil
	}

	return &schedulingpb.GetOperatorResponse{
		Header:   s.header(),
		RegionId: requestID,
		Desc:     []byte(r.Desc()),
		Kind:     []byte(r.Kind().String()),
		Status:   r.Status,
	}, nil
}

// AskBatchSplit implements gRPC SchedulingServer.
func (s *Service) AskBatchSplit(ctx context.Context, request *schedulingpb.AskBatchSplitRequest) (*schedulingpb.AskBatchSplitResponse, error) {
	c := s.GetCluster()
	if c == nil {
		return &schedulingpb.AskBatchSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if request.GetRegion() == nil {
		return &schedulingpb.AskBatchSplitResponse{
			Header: s.wrapErrorToHeader(schedulingpb.ErrorType_UNKNOWN,
				"missing region for split"),
		}, nil
	}

	if c.persistConfig.IsSchedulingHalted() {
		return nil, errs.ErrSchedulingIsHalted.FastGenByArgs()
	}
	if !c.persistConfig.IsTikvRegionSplitEnabled() {
		return nil, errs.ErrSchedulerTiKVSplitDisabled.FastGenByArgs()
	}
	reqRegion := request.GetRegion()
	splitCount := request.GetSplitCount()
	err := c.ValidRegion(reqRegion)
	if err != nil {
		return nil, err
	}
	splitIDs := make([]*pdpb.SplitID, 0, splitCount)
	recordRegions := make([]uint64, 0, splitCount+1)

	for i := 0; i < int(splitCount); i++ {
		newRegionID, err := c.AllocID()
		if err != nil {
			return nil, errs.ErrSchedulerNotFound.FastGenByArgs()
		}

		peerIDs := make([]uint64, len(request.Region.Peers))
		for i := 0; i < len(peerIDs); i++ {
			if peerIDs[i], err = c.AllocID(); err != nil {
				return nil, err
			}
		}

		recordRegions = append(recordRegions, newRegionID)
		splitIDs = append(splitIDs, &pdpb.SplitID{
			NewRegionId: newRegionID,
			NewPeerIds:  peerIDs,
		})

		log.Info("alloc ids for region split", zap.Uint64("region-id", newRegionID), zap.Uint64s("peer-ids", peerIDs))
	}

	recordRegions = append(recordRegions, reqRegion.GetId())
	if versioninfo.IsFeatureSupported(c.persistConfig.GetClusterVersion(), versioninfo.RegionMerge) {
		// Disable merge the regions in a period of time.
		c.GetCoordinator().GetMergeChecker().RecordRegionSplit(recordRegions)
	}

	// If region splits during the scheduling process, regions with abnormal
	// status may be left, and these regions need to be checked with higher
	// priority.
	c.GetCoordinator().GetCheckerController().AddSuspectRegions(recordRegions...)

	return &schedulingpb.AskBatchSplitResponse{
		Header: s.header(),
		Ids:    splitIDs,
	}, nil
}

// RegisterGRPCService registers the service to gRPC server.
func (s *Service) RegisterGRPCService(g *grpc.Server) {
	schedulingpb.RegisterSchedulingServer(g, s)
}

// RegisterRESTHandler registers the service to REST server.
func (s *Service) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) {
	handler, group := SetUpRestHandler(s)
	apiutil.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}

func (s *Service) errorHeader(err *schedulingpb.Error) *schedulingpb.ResponseHeader {
	return &schedulingpb.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

func (s *Service) notBootstrappedHeader() *schedulingpb.ResponseHeader {
	return s.errorHeader(&schedulingpb.Error{
		Type:    schedulingpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not initialized",
	})
}

func (s *Service) header() *schedulingpb.ResponseHeader {
	if s.clusterID == 0 {
		return s.wrapErrorToHeader(schedulingpb.ErrorType_NOT_BOOTSTRAPPED, "cluster id is not ready")
	}
	return &schedulingpb.ResponseHeader{ClusterId: s.clusterID}
}

func (s *Service) wrapErrorToHeader(
	errorType schedulingpb.ErrorType, message string) *schedulingpb.ResponseHeader {
	return s.errorHeader(&schedulingpb.Error{Type: errorType, Message: message})
}
