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
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// UpdateGCSafePoint implements gRPC PDServer.
//
// Deprecated: Use AdvanceGCSafePoint instead. Note that it's only for use of GC internal.
//
//nolint:staticcheck
func (s *GrpcServer) UpdateGCSafePoint(ctx context.Context, request *pdpb.UpdateGCSafePointRequest) (resp *pdpb.UpdateGCSafePointResponse, errRet error) {
	log.Warn("deprecated API UpdateGCSafePoint is called", zap.Uint64("req-safe-point", request.GetSafePoint()))
	defer func() {
		if errRet != nil {
			log.Error("deprecated API UpdateGCSafePoint encountered error", zap.Uint64("req-safe-point", request.GetSafePoint()), zap.Error(errRet))
		} else {
			log.Warn("deprecated API UpdateGCSafePoint returned", zap.Uint64("req-safe-point", request.GetSafePoint()), zap.Uint64("resp-new-safe-point", resp.GetNewSafePoint()))
		}
	}()

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
	oldSafePoint, _, err := s.gcStateManager.CompatibleUpdateGCSafePoint(constant.NullKeyspaceID, newSafePoint)
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
//
// Deprecated: Use GetGCState instead.
//
//nolint:staticcheck
func (s *GrpcServer) GetGCSafePoint(ctx context.Context, request *pdpb.GetGCSafePointRequest) (resp *pdpb.GetGCSafePointResponse, errRet error) {
	log.Warn("deprecated API GetGCSafePoint is called")
	defer func() {
		if errRet != nil {
			log.Error("deprecated API GetGCSafePoint encountered error", zap.Error(errRet))
		} else {
			log.Warn("deprecated API GetGCSafePoint returned", zap.Uint64("resp-safe-point", resp.GetSafePoint()))
		}
	}()

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

	safePoint, err := s.gcStateManager.CompatibleLoadGCSafePoint(constant.NullKeyspaceID)
	if err != nil {
		return nil, err
	}

	return &pdpb.GetGCSafePointResponse{
		Header:    wrapHeader(),
		SafePoint: safePoint,
	}, nil
}

// UpdateServiceGCSafePoint update the safepoint for specific service
//
// Deprecated: Use SetGCBarrier instead.
//
//nolint:staticcheck
func (s *GrpcServer) UpdateServiceGCSafePoint(ctx context.Context, request *pdpb.UpdateServiceGCSafePointRequest) (resp *pdpb.UpdateServiceGCSafePointResponse, errRet error) {
	log.Warn("deprecated API UpdateServiceGCSafePoint is called",
		zap.String("req-service-id", string(request.GetServiceId())),
		zap.Int64("req-ttl", request.GetTTL()),
		zap.Uint64("req-safe-point", request.GetSafePoint()))
	defer func() {
		if errRet != nil {
			log.Error("deprecated API UpdateServiceGCSafePoint encountered error",
				zap.String("req-service-id", string(request.GetServiceId())),
				zap.Int64("req-ttl", request.GetTTL()),
				zap.Uint64("req-safe-point", request.GetSafePoint()),
				zap.Error(errRet))
		} else {
			log.Warn("deprecated API UpdateServiceGCSafePoint returned",
				zap.String("req-service-id", string(request.GetServiceId())),
				zap.Int64("req-ttl", request.GetTTL()),
				zap.Uint64("req-safe-point", request.GetSafePoint()),
				zap.String("resp-service-id", string(resp.GetServiceId())),
				zap.Int64("resp-ttl", resp.GetTTL()),
				zap.Uint64("resp-min-safe-point", resp.GetMinSafePoint()))
		}
	}()

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
	min, updated, err := s.gcStateManager.CompatibleUpdateServiceGCSafePoint(constant.NullKeyspaceID, serviceID, request.GetSafePoint(), request.GetTTL(), now)
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
//
// Deprecated: Use GetGCState instead
//
//nolint:staticcheck
func (s *GrpcServer) GetGCSafePointV2(ctx context.Context, request *pdpb.GetGCSafePointV2Request) (resp *pdpb.GetGCSafePointV2Response, errRet error) {
	log.Warn("deprecated API GetGCSafePointV2 is called", zap.Uint32("keyspace-id", request.GetKeyspaceId()))
	defer func() {
		if errRet != nil {
			log.Error("deprecated API GetGCSafePointV2 encountered error", zap.Uint32("keyspace-id", request.GetKeyspaceId()), zap.Error(errRet))
		} else {
			log.Warn("deprecated API GetGCSafePointV2 returned", zap.Uint32("keyspace-id", request.GetKeyspaceId()), zap.Uint64("resp-safe-point", resp.GetSafePoint()))
		}
	}()

	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetGCSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetGCSafePointV2Response), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetGCSafePointV2Response{Header: notBootstrappedHeader()}, nil
	}

	safePoint, err := s.gcStateManager.CompatibleLoadGCSafePoint(request.GetKeyspaceId())

	if err != nil {
		return &pdpb.GetGCSafePointV2Response{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, err
	}

	return &pdpb.GetGCSafePointV2Response{
		Header:    wrapHeader(),
		SafePoint: safePoint,
	}, nil
}

// UpdateGCSafePointV2 update gc safe point for the given keyspace.
//
// Deprecated: Use AdvanceGCSafePoint instead. Note that it's only for use of GC internal.
//
//nolint:staticcheck
func (s *GrpcServer) UpdateGCSafePointV2(ctx context.Context, request *pdpb.UpdateGCSafePointV2Request) (resp *pdpb.UpdateGCSafePointV2Response, errRet error) {
	log.Warn("deprecated API UpdateGCSafePointV2 is called", zap.Uint32("keyspace-id", request.GetKeyspaceId()), zap.Uint64("req-safe-point", request.GetSafePoint()))
	defer func() {
		if errRet != nil {
			log.Error("deprecated API UpdateGCSafePointV2 encountered error", zap.Uint32("keyspace-id", request.GetKeyspaceId()), zap.Uint64("req-safe-point", request.GetSafePoint()), zap.Error(errRet))
		} else {
			log.Warn("deprecated API UpdateGCSafePointV2 returned", zap.Uint32("keyspace-id", request.GetKeyspaceId()), zap.Uint64("req-safe-point", request.GetSafePoint()), zap.Uint64("resp-new-safe-point", resp.GetNewSafePoint()))
		}
	}()

	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).UpdateGCSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.UpdateGCSafePointV2Response), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.UpdateGCSafePointV2Response{Header: notBootstrappedHeader()}, nil
	}

	newSafePoint := request.GetSafePoint()
	oldSafePoint, _, err := s.gcStateManager.CompatibleUpdateGCSafePoint(request.GetKeyspaceId(), newSafePoint)
	if err != nil {
		return nil, err
	}

	if newSafePoint > oldSafePoint {
		log.Info("updated gc safe point",
			zap.Uint64("safe-point", newSafePoint),
			zap.Uint32("keyspace-id", request.GetKeyspaceId()))
	} else if newSafePoint < oldSafePoint {
		log.Warn("trying to update gc safe point",
			zap.Uint64("old-safe-point", oldSafePoint),
			zap.Uint64("new-safe-point", newSafePoint),
			zap.Uint32("keyspace-id", request.GetKeyspaceId()))
		newSafePoint = oldSafePoint
	}

	return &pdpb.UpdateGCSafePointV2Response{
		Header:       wrapHeader(),
		NewSafePoint: newSafePoint,
	}, nil
}

// UpdateServiceSafePointV2 update service safe point for the given keyspace.
//
// Deprecated: Use SetGCBarrier instead.
//
//nolint:staticcheck
func (s *GrpcServer) UpdateServiceSafePointV2(ctx context.Context, request *pdpb.UpdateServiceSafePointV2Request) (resp *pdpb.UpdateServiceSafePointV2Response, errRet error) {
	log.Warn("deprecated API UpdateServiceSafePointV2 is called",
		zap.Uint32("keyspace-id", request.GetKeyspaceId()),
		zap.String("req-service-id", string(request.GetServiceId())),
		zap.Int64("req-ttl", request.GetTtl()),
		zap.Uint64("req-safe-point", request.GetSafePoint()))
	defer func() {
		if errRet != nil {
			log.Error("deprecated API UpdateServiceSafePointV2 encountered error",
				zap.Uint32("keyspace-id", request.GetKeyspaceId()),
				zap.String("req-service-id", string(request.GetServiceId())),
				zap.Int64("req-ttl", request.GetTtl()),
				zap.Uint64("req-safe-point", request.GetSafePoint()),
				zap.Error(errRet))
		} else {
			log.Warn("deprecated API UpdateServiceSafePointV2 returned",
				zap.Uint32("keyspace-id", request.GetKeyspaceId()),
				zap.String("req-service-id", string(request.GetServiceId())),
				zap.Int64("req-ttl", request.GetTtl()),
				zap.Uint64("req-safe-point", request.GetSafePoint()),
				zap.String("resp-service-id", string(resp.GetServiceId())),
				zap.Int64("resp-ttl", resp.GetTtl()),
				zap.Uint64("resp-min-safe-point", resp.GetMinSafePoint()))
		}
	}()

	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).UpdateServiceSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.UpdateServiceSafePointV2Response), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.UpdateServiceSafePointV2Response{Header: notBootstrappedHeader()}, nil
	}

	nowTSO, err := s.getGlobalTSO(ctx)
	if err != nil {
		return nil, err
	}
	now, _ := tsoutil.ParseTimestamp(nowTSO)

	serviceID := string(request.ServiceId)
	min, _, err := s.gcStateManager.CompatibleUpdateServiceGCSafePoint(request.GetKeyspaceId(), serviceID, request.GetSafePoint(), request.GetTtl(), now)

	if err != nil {
		return nil, err
	}
	return &pdpb.UpdateServiceSafePointV2Response{
		Header:       wrapHeader(),
		ServiceId:    []byte(min.ServiceID),
		Ttl:          min.ExpiredAt - now.Unix(),
		MinSafePoint: min.SafePoint,
	}, nil
}

// WatchGCSafePointV2 watch keyspaces gc safe point changes.
//
// Deprecated: Poll GetAllKeyspacesGCStates instead.
//
//nolint:staticcheck
func (*GrpcServer) WatchGCSafePointV2(_ *pdpb.WatchGCSafePointV2Request, _ pdpb.PD_WatchGCSafePointV2Server) error {
	log.Error("removed API WatchGCSafePointV2 is called")
	return status.Errorf(codes.Unimplemented, "WatchGCSafePointV2 is obsolete. Poll GetAllKeyspacesGCStates instead if necessary")
}

// GetAllGCSafePointV2 return all gc safe point v2.
//
// Deprecated: Use GetAllKeyspacesGCStates instead.
//
//nolint:staticcheck
func (s *GrpcServer) GetAllGCSafePointV2(ctx context.Context, request *pdpb.GetAllGCSafePointV2Request) (resp *pdpb.GetAllGCSafePointV2Response, errRet error) {
	log.Warn("deprecated API GetAllGCSafePointV2 is called")
	defer func() {
		if errRet != nil {
			log.Error("deprecated API GetAllGCSafePointV2 encountered error", zap.Error(errRet))
		} else {
			log.Warn("deprecated API GetAllGCSafePointV2 returned", zap.Stringers("resp-safe-point", resp.GetGcSafePoints()), zap.Int64("resp-revision", resp.GetRevision()))
		}
	}()

	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetAllGCSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetAllGCSafePointV2Response), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetAllGCSafePointV2Response{Header: notBootstrappedHeader()}, nil
	}

	gcStates, err := s.gcStateManager.GetAllKeyspacesGCStates(ctx)
	if err != nil {
		return &pdpb.GetAllGCSafePointV2Response{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	gcSafePoints := make([]*pdpb.GCSafePointV2, 0, len(gcStates))
	for _, gcState := range gcStates {
		if gcState.KeyspaceID == constant.NullKeyspaceID {
			// The original GetAllGCSafePointV2 API doesn't return the null keyspace. To keep the behavior, we
			// still exclude it.
			continue
		}
		gcSafePoints = append(gcSafePoints, &pdpb.GCSafePointV2{
			KeyspaceId:  gcState.KeyspaceID,
			GcSafePoint: gcState.GCSafePoint,
		})
	}

	return &pdpb.GetAllGCSafePointV2Response{
		Header:       wrapHeader(),
		GcSafePoints: gcSafePoints,
		Revision:     0,
	}, nil
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
		resultTTL = int64(max(math.Floor(b.ExpirationTime.Sub(now).Seconds()), 0))
	}

	return &pdpb.GCBarrierInfo{
		BarrierId:  b.BarrierID,
		BarrierTs:  b.BarrierTS,
		TtlSeconds: resultTTL,
	}
}

func globalGCBarrierToProto(b *endpoint.GlobalGCBarrier, now time.Time) *pdpb.GlobalGCBarrierInfo {
	if b == nil {
		return nil
	}

	// After rounding, the actual TTL might be not exactly the same as the specified value. Recalculate it anyway.
	// MaxInt64 represents that the expiration time is not specified and it never expires.
	var resultTTL int64 = math.MaxInt64
	if b.ExpirationTime.Time != nil {
		resultTTL = int64(max(math.Floor(b.ExpirationTime.Time.Sub(now).Seconds()), 0))
	}

	return &pdpb.GlobalGCBarrierInfo{
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
		IsKeyspaceLevelGc: gcState.IsKeyspaceLevel,
		TxnSafePoint:      gcState.TxnSafePoint,
		GcSafePoint:       gcState.GCSafePoint,
		GcBarriers:        gcBarriers,
	}
}

// AdvanceGCSafePoint tries to advance the GC safe point.
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
		return &pdpb.AdvanceGCSafePointResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.AdvanceGCSafePointResponse{
		Header:         wrapHeader(),
		OldGcSafePoint: oldGCSafePoint,
		NewGcSafePoint: newGCSafePoint,
	}, nil
}

// AdvanceTxnSafePoint tries to advance the transaction safe point.
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
		return &pdpb.AdvanceTxnSafePointResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.AdvanceTxnSafePointResponse{
		Header:             wrapHeader(),
		OldTxnSafePoint:    res.OldTxnSafePoint,
		NewTxnSafePoint:    res.NewTxnSafePoint,
		BlockerDescription: res.BlockerDescription,
	}, nil
}

// SetGCBarrier sets a GC barrier.
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
	keyspaceID := getKeyspaceID(request.GetKeyspaceScope())
	barrierID := request.GetBarrierId()
	barrierTS := request.GetBarrierTs()
	ttl := typeutil.SaturatingStdDurationFromSeconds(request.GetTtlSeconds())
	newBarrier, err := s.gcStateManager.SetGCBarrier(keyspaceID, barrierID, barrierTS, ttl, now)
	if err != nil {
		return &pdpb.SetGCBarrierResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.SetGCBarrierResponse{
		Header:         wrapHeader(),
		NewBarrierInfo: gcBarrierToProto(newBarrier, now),
	}, nil
}

// DeleteGCBarrier deletes a GC barrier.
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
		return &pdpb.DeleteGCBarrierResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.DeleteGCBarrierResponse{
		Header:             wrapHeader(),
		DeletedBarrierInfo: gcBarrierToProto(deletedBarrier, now),
	}, nil
}

// GetGCState gets the GC state.
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
		return &pdpb.GetGCStateResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.GetGCStateResponse{
		Header:  wrapHeader(),
		GcState: gcStateToProto(gcState, time.Now()),
	}, nil
}

// GetAllKeyspacesGCStates gets the GC states of all keyspaces.
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

	gcStates, err := s.gcStateManager.GetAllKeyspacesGCStates(ctx)
	if err != nil {
		return &pdpb.GetAllKeyspacesGCStatesResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
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

// SetGlobalGCBarrier sets a global GC barrier.
func (s *GrpcServer) SetGlobalGCBarrier(ctx context.Context, request *pdpb.SetGlobalGCBarrierRequest) (*pdpb.SetGlobalGCBarrierResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).SetGlobalGCBarrier(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.SetGlobalGCBarrierResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.SetGlobalGCBarrierResponse{Header: notBootstrappedHeader()}, nil
	}

	now := time.Now()
	barrierID := request.GetBarrierId()
	barrierTS := request.GetBarrierTs()
	ttl := typeutil.SaturatingStdDurationFromSeconds(request.GetTtlSeconds())
	newBarrier, err := s.gcStateManager.SetGlobalGCBarrier(ctx, barrierID, barrierTS, ttl, now)
	if err != nil {
		return &pdpb.SetGlobalGCBarrierResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.SetGlobalGCBarrierResponse{
		Header:         wrapHeader(),
		NewBarrierInfo: globalGCBarrierToProto(newBarrier, now),
	}, nil
}

// DeleteGlobalGCBarrier deletes a GC barrier.
func (s *GrpcServer) DeleteGlobalGCBarrier(ctx context.Context, request *pdpb.DeleteGlobalGCBarrierRequest) (*pdpb.DeleteGlobalGCBarrierResponse, error) {
	done, err := s.rateLimitCheck()
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer done()
	}
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).DeleteGlobalGCBarrier(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.DeleteGlobalGCBarrierResponse), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.DeleteGlobalGCBarrierResponse{Header: notBootstrappedHeader()}, nil
	}

	now := time.Now()

	barrierID := request.GetBarrierId()
	deletedBarrier, err := s.gcStateManager.DeleteGlobalGCBarrier(ctx, barrierID)
	if err != nil {
		return &pdpb.DeleteGlobalGCBarrierResponse{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, nil
	}

	return &pdpb.DeleteGlobalGCBarrierResponse{
		Header:             wrapHeader(),
		DeletedBarrierInfo: globalGCBarrierToProto(deletedBarrier, now),
	}, nil
}
