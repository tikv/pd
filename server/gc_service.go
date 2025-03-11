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

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

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

func (s *GrpcServer) AdvanceGCSafePoint(ctx context.Context, pointRequest *pdpb.AdvanceGCSafePointRequest) (*pdpb.AdvanceGCSafePointResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *GrpcServer) AdvanceTxnSafePoint(ctx context.Context, pointRequest *pdpb.AdvanceTxnSafePointRequest) (*pdpb.AdvanceTxnSafePointResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *GrpcServer) SetGCBarrier(ctx context.Context, barrierRequest *pdpb.SetGCBarrierRequest) (*pdpb.SetGCBarrierResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *GrpcServer) DeleteGCBarrier(ctx context.Context, barrierRequest *pdpb.DeleteGCBarrierRequest) (*pdpb.DeleteGCBarrierResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *GrpcServer) GetGCState(ctx context.Context, stateRequest *pdpb.GetGCStateRequest) (*pdpb.GetGCStateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *GrpcServer) GetAllKeyspacesGCStates(ctx context.Context, statesRequest *pdpb.GetAllKeyspacesGCStatesRequest) (*pdpb.GetAllKeyspacesGCStatesResponse, error) {
	//TODO implement me
	panic("implement me")
}
