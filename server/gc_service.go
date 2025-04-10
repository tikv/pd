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
	"math"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/gc"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tsoutil"
)

// UpdateGCSafePoint implements gRPC PDServer.
func (s *GrpcServer) UpdateGCSafePoint(ctx context.Context, request *pdpb.UpdateGCSafePointRequest) (*pdpb.UpdateGCSafePointResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
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
	oldSafePoint, _, err := s.gcStateManager.CompatibleUpdateGCSafePoint(newSafePoint)
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

// GetGCSafePoint implements gRPC PDServer.
func (s *GrpcServer) GetGCSafePoint(ctx context.Context, request *pdpb.GetGCSafePointRequest) (*pdpb.GetGCSafePointResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
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

	safePoint, err := s.gcStateManager.CompatibleLoadGCSafePoint()
	if err != nil {
		return nil, err
	}

	return &pdpb.GetGCSafePointResponse{
		Header:    wrapHeader(),
		SafePoint: safePoint,
	}, nil
}

// UpdateServiceGCSafePoint update the safepoint for specific service
func (s *GrpcServer) UpdateServiceGCSafePoint(ctx context.Context, request *pdpb.UpdateServiceGCSafePointRequest) (*pdpb.UpdateServiceGCSafePointResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
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
	nowTSO, err := s.getGlobalTSO(ctx)
	if err != nil {
		return nil, err
	}
	now, _ := tsoutil.ParseTimestamp(nowTSO)
	serviceID := string(request.ServiceId)
	min, updated, err := s.gcStateManager.CompatibleUpdateServiceGCSafePoint(serviceID, request.GetSafePoint(), request.GetTTL(), now)
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

// GetGCSafePointV2 return gc safe point for the given keyspace.
func (s *GrpcServer) GetGCSafePointV2(_ context.Context, _ *pdpb.GetGCSafePointV2Request) (*pdpb.GetGCSafePointV2Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetGCSafePointV2 is abandoned")
}

// UpdateGCSafePointV2 update gc safe point for the given keyspace.
func (s *GrpcServer) UpdateGCSafePointV2(_ context.Context, _ *pdpb.UpdateGCSafePointV2Request) (*pdpb.UpdateGCSafePointV2Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "UpdateGCSafePointV2 is abandoned")
}

// UpdateServiceSafePointV2 update service safe point for the given keyspace.
func (s *GrpcServer) UpdateServiceSafePointV2(_ context.Context, _ *pdpb.UpdateServiceSafePointV2Request) (*pdpb.UpdateServiceSafePointV2Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "UpdateServiceSafePointV2 is abandoned")
}

// WatchGCSafePointV2 watch keyspaces gc safe point changes.
func (s *GrpcServer) WatchGCSafePointV2(_ *pdpb.WatchGCSafePointV2Request, _ pdpb.PD_WatchGCSafePointV2Server) error {
	return status.Errorf(codes.Unimplemented, "WatchGCSafePointV2 is abandoned")
}

// GetAllGCSafePointV2 return all gc safe point v2.
func (s *GrpcServer) GetAllGCSafePointV2(_ context.Context, _ *pdpb.GetAllGCSafePointV2Request) (*pdpb.GetAllGCSafePointV2Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetAllGCSafePointV2 is abandoned")
}

func getKeyspaceID(keyspaceScope *pdpb.KeyspaceScope) uint32 {
	if keyspaceScope == nil {
		return constant.NullKeyspaceID
	}
	return keyspaceScope.GetKeyspaceId()
}

func gcBarrierToProto(b *endpoint.GCBarrier, now time.Time) *pdpb.GCBarrierInfo {
	if b == nil {
		return nil
	}

	// After rounding, the actual TTL might be not exactly the same as the specified value. Recalculate it anyway.
	// MaxInt64 represents that the expiration time is not specified and it never expires.
	var resultTTL int64 = math.MaxInt64
	if b.ExpirationTime != nil {
		resultTTL = int64(math.Floor(b.ExpirationTime.Sub(now).Seconds()))
		if resultTTL < 0 {
			resultTTL = 0
		}
	}

	return &pdpb.GCBarrierInfo{
		BarrierId:  b.BarrierID,
		BarrierTs:  b.BarrierTS,
		TtlSeconds: resultTTL,
	}
}

func gcStateToProto(gcState gc.GCState, now time.Time) *pdpb.GCState {
	gcBarriers := make([]*pdpb.GCBarrierInfo, 0, len(gcState.GCBarriers))
	for _, b := range gcState.GCBarriers {
		gcBarriers = append(gcBarriers, gcBarrierToProto(b, now))
	}
	return &pdpb.GCState{
		KeyspaceScope: &pdpb.KeyspaceScope{
			KeyspaceId: gcState.KeyspaceID,
		},
		TxnSafePoint: gcState.TxnSafePoint,
		GcSafePoint:  gcState.GCSafePoint,
		GcBarriers:   gcBarriers,
	}
}

func (s *GrpcServer) AdvanceGCSafePoint(ctx context.Context, request *pdpb.AdvanceGCSafePointRequest) (*pdpb.AdvanceGCSafePointResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).AdvanceGCSafePoint(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.AdvanceGCSafePointResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.AdvanceGCSafePointResponse{Header: notBootstrappedHeader()}, nil
	}
	oldGCSafePoint, newGCSafePoint, err := s.gcStateManager.AdvanceGCSafePoint(getKeyspaceID(request.GetKeyspaceScope()), request.GetTarget())
	if err != nil {
		return nil, err
	}
	return &pdpb.AdvanceGCSafePointResponse{
		Header:         wrapHeader(),
		OldGcSafePoint: oldGCSafePoint,
		NewGcSafePoint: newGCSafePoint,
	}, nil
}

func (s *GrpcServer) AdvanceTxnSafePoint(ctx context.Context, request *pdpb.AdvanceTxnSafePointRequest) (*pdpb.AdvanceTxnSafePointResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).AdvanceTxnSafePoint(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.AdvanceTxnSafePointResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.AdvanceTxnSafePointResponse{Header: notBootstrappedHeader()}, nil
	}

	res, err := s.gcStateManager.AdvanceTxnSafePoint(getKeyspaceID(request.GetKeyspaceScope()), request.GetTarget(), time.Now())
	if err != nil {
		return nil, err
	}
	return &pdpb.AdvanceTxnSafePointResponse{
		Header:             wrapHeader(),
		OldTxnSafePoint:    res.OldTxnSafePoint,
		NewTxnSafePoint:    res.NewTxnSafePoint,
		BlockerDescription: res.BlockerDescription,
	}, nil
}

func (s *GrpcServer) SetGCBarrier(ctx context.Context, request *pdpb.SetGCBarrierRequest) (*pdpb.SetGCBarrierResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).SetGCBarrier(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.SetGCBarrierResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.SetGCBarrierResponse{Header: notBootstrappedHeader()}, nil
	}

	now := time.Now()
	newBarrier, err := s.gcStateManager.SetGCBarrier(
		getKeyspaceID(request.GetKeyspaceScope()),
		request.GetBarrierId(),
		request.GetBarrierTs(),
		gc.SaturatingDuration(request.GetTtlSeconds(), time.Second),
		now)
	if err != nil {
		return nil, err
	}

	return &pdpb.SetGCBarrierResponse{
		Header:         wrapHeader(),
		NewBarrierInfo: gcBarrierToProto(newBarrier, now),
	}, nil
}

func (s *GrpcServer) DeleteGCBarrier(ctx context.Context, request *pdpb.DeleteGCBarrierRequest) (*pdpb.DeleteGCBarrierResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).DeleteGCBarrier(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.DeleteGCBarrierResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.DeleteGCBarrierResponse{Header: notBootstrappedHeader()}, nil
	}

	now := time.Now()

	deletedBarrier, err := s.gcStateManager.DeleteGCBarrier(getKeyspaceID(request.GetKeyspaceScope()), request.GetBarrierId())
	if err != nil {
		return nil, err
	}

	return &pdpb.DeleteGCBarrierResponse{
		Header:             wrapHeader(),
		DeletedBarrierInfo: gcBarrierToProto(deletedBarrier, now),
	}, nil
}

func (s *GrpcServer) GetGCState(ctx context.Context, request *pdpb.GetGCStateRequest) (*pdpb.GetGCStateResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetGCState(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetGCStateResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetGCStateResponse{Header: notBootstrappedHeader()}, nil
	}

	gcState, err := s.gcStateManager.GetGCState(getKeyspaceID(request.GetKeyspaceScope()))
	if err != nil {
		return nil, err
	}

	return &pdpb.GetGCStateResponse{
		Header:  wrapHeader(),
		GcState: gcStateToProto(gcState, time.Now()),
	}, nil
}

func (s *GrpcServer) GetAllKeyspacesGCStates(ctx context.Context, request *pdpb.GetAllKeyspacesGCStatesRequest) (*pdpb.GetAllKeyspacesGCStatesResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetAllKeyspacesGCStates(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetAllKeyspacesGCStatesResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetAllKeyspacesGCStatesResponse{Header: notBootstrappedHeader()}, nil
	}

	gcStates, err := s.gcStateManager.GetAllKeyspacesGCStates()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	gcStatesPb := make([]*pdpb.GCState, 0, len(gcStates))
	for _, gcState := range gcStates {
		gcStatesPb = append(gcStatesPb, gcStateToProto(gcState, now))
	}

	return &pdpb.GetAllKeyspacesGCStatesResponse{
		Header:   wrapHeader(),
		GcStates: gcStatesPb,
	}, nil
}
