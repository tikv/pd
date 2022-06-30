// Copyright 2022 TiKV Project Authors.
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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"time"

	"github.com/pingcap/kvproto/pkg/gcpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/server/tso"
	"go.uber.org/zap"
)

// GcServer wraps Server to provide garbage collection service.
type GcServer struct {
	*Server
}

func (s *GcServer) header() *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{ClusterId: s.clusterID}
}

func (s *GcServer) errorHeader(err *pdpb.Error) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

func (s *GcServer) notBootstrappedHeader() *pdpb.ResponseHeader {
	return s.errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}

// ListGCSafePoints sends key spaces that have gc safe point to a grpc client.
func (s *GcServer) ListGCSafePoints(ctx context.Context, request *gcpb.ListGCSafePointsRequest) (*gcpb.ListGCSafePointsResponse, error) {
	rc := s.GetRaftCluster()
	if rc == nil {
		return &gcpb.ListGCSafePointsResponse{Header: s.notBootstrappedHeader()}, nil
	}

	safePoints, err := s.keyspaceSafePointManager.ListGCSafePoints()
	if err != nil {
		return nil, err
	}

	resp := &gcpb.ListGCSafePointsResponse{
		Header:     s.header(),
		SafePoints: safePoints,
	}
	return resp, nil
}

// UpdateGCSafePoint used by gc_worker to update their gc safe points.
func (s *GcServer) UpdateGCSafePoint(ctx context.Context, request *gcpb.UpdateGCSafePointRequest) (*gcpb.UpdateGCSafePointResponse, error) {
	rc := s.GetRaftCluster()
	if rc == nil {
		return &gcpb.UpdateGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}

	spaceID := request.GetSpaceId()
	newSafePoint := request.GetSafePoint()

	oldSafePoint, err := s.keyspaceSafePointManager.UpdateGCSafePoint(spaceID, newSafePoint)
	if err != nil {
		return nil, err
	}

	if newSafePoint > oldSafePoint {
		log.Info("updated keyspace gc safe point",
			zap.Uint32("keyspace", spaceID),
			zap.Uint64("safe-point", newSafePoint))
	} else if newSafePoint < oldSafePoint {
		log.Warn("trying to update keyspace gc safe point",
			zap.Uint32("keyspace", spaceID),
			zap.Uint64("old-safe-point", oldSafePoint),
			zap.Uint64("new-safe-point", newSafePoint))
		newSafePoint = oldSafePoint
	}

	return &gcpb.UpdateGCSafePointResponse{
		Header:       s.header(),
		NewSafePoint: newSafePoint,
	}, nil
}

func (s *GcServer) getNow() (time.Time, error) {
	nowTSO, err := s.tsoAllocatorManager.HandleTSORequest(tso.GlobalDCLocation, 1)
	if err != nil {
		return time.Time{}, err
	}
	now, _ := tsoutil.ParseTimestamp(nowTSO)
	return now, err
}

// UpdateServiceSafePoint for services like CDC/BR/Lightning to update gc safe points in PD.
func (s *GcServer) UpdateServiceSafePoint(ctx context.Context, request *gcpb.UpdateServiceSafePointRequest) (*gcpb.UpdateServiceSafePointResponse, error) {
	rc := s.GetRaftCluster()
	if rc == nil {
		return &gcpb.UpdateServiceSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}

	spaceID := request.GetSpaceId()
	serviceID := string(request.GetServiceId())
	ttl := request.GetTTL()
	requestSafePoint := request.GetSafePoint()

	// a less than 0 ttl means to remove the safe point
	if ttl <= 0 {
		if err := s.keyspaceSafePointManager.RemoveServiceSafePoint(spaceID, serviceID); err != nil {
			return nil, err
		}
	}

	now, err := s.getNow()
	if err != nil {
		return nil, err
	}
	minSafePoint, updated, err := s.keyspaceSafePointManager.UpdateServiceSafePoint(spaceID, serviceID, requestSafePoint, ttl, now)
	if err != nil {
		return nil, err
	}
	if updated {
		log.Info("update service GC safe point",
			zap.Uint32("keyspace", spaceID),
			zap.String("service-id", serviceID),
			zap.Int64("expire-at", now.Unix()+ttl),
			zap.Uint64("safepoint", requestSafePoint))
	}

	return &gcpb.UpdateServiceSafePointResponse{
		Header:       s.header(),
		ServiceId:    []byte(minSafePoint.ServiceID),
		TTL:          minSafePoint.ExpiredAt - now.Unix(),
		MinSafePoint: minSafePoint.SafePoint,
	}, nil
}
