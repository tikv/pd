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
	"math"

	"github.com/pingcap/kvproto/pkg/gcpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/tso"
	"go.uber.org/zap"
)

type GcServer struct {
	*Server
}

func (s *GcServer) header() *gcpb.ResponseHeader {
	return &gcpb.ResponseHeader{ClusterId: s.clusterID}
}

func (s *GcServer) errorHeader(err *gcpb.Error) *gcpb.ResponseHeader {
	return &gcpb.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

func (s *GcServer) notBootstrappedHeader() *gcpb.ResponseHeader {
	return s.errorHeader(&gcpb.Error{
		Type:    gcpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}

func (s *GcServer) revisionMismatchHeader(requestRevision, currentRevision int64) *gcpb.ResponseHeader {
	return s.errorHeader(&gcpb.Error{
		Type:    gcpb.ErrorType_REVISION_MISMATCH,
		Message: fmt.Sprintf("revision mismatch, requested revision %v but current revision %v", requestRevision, currentRevision),
	})
}

func (s *GcServer) safePointRollbackHeader(requestSafePoint, requiredSafePoint uint64) *gcpb.ResponseHeader {
	return s.errorHeader(&gcpb.Error{
		Type:    gcpb.ErrorType_SAFEPOINT_ROLLBACK,
		Message: fmt.Sprintf("safe point rollback, requested safe point %v is less than required safe point %v", requestSafePoint, requiredSafePoint),
	})
}

// GetAllServiceGroups return all service group ids
func (s *GcServer) GetAllServiceGroups(ctx context.Context, request *gcpb.GetAllServiceGroupsRequest) (*gcpb.GetAllServiceGroupsResponse, error) {

	rc := s.GetRaftCluster()
	if rc == nil {
		return &gcpb.GetAllServiceGroupsResponse{Header: s.notBootstrappedHeader()}, nil
	}

	var storage endpoint.GCSafePointStorage = s.storage
	serviceGroupList, err := storage.LoadAllServiceGroups()
	if err != nil {
		return nil, err
	}

	return &gcpb.GetAllServiceGroupsResponse{
		Header:         s.header(),
		ServiceGroupId: serviceGroupList,
	}, nil
}

// GetMinServiceSafePointByServiceGroup returns given service group's min service safe point
func (s *GcServer) GetMinServiceSafePointByServiceGroup(ctx context.Context, request *gcpb.GetMinServiceSafePointByServiceGroupRequest) (*gcpb.GetMinServiceSafePointByServiceGroupResponse, error) {

	rc := s.GetRaftCluster()
	if rc == nil {
		return &gcpb.GetMinServiceSafePointByServiceGroupResponse{Header: s.notBootstrappedHeader()}, nil
	}

	var storage endpoint.GCSafePointStorage = s.storage
	serviceGroupID := string(request.ServiceGroupId)
	nowTSO, err := s.tsoAllocatorManager.HandleTSORequest(tso.GlobalDCLocation, 1)
	if err != nil {
		return nil, err
	}
	now, _ := tsoutil.ParseTimestamp(nowTSO)
	min, err := storage.LoadMinServiceSafePointByServiceGroup(serviceGroupID, now)
	if err != nil {
		return nil, err
	}
	var returnSafePoint uint64
	if min != nil {
		returnSafePoint = min.SafePoint
	}
	// perform a get operation on a non-existing key to obtain current etcd revision number from response header
	rsp, _ := s.client.Get(ctx, "NA")
	currentRevision := rsp.Header.GetRevision()
	return &gcpb.GetMinServiceSafePointByServiceGroupResponse{
		Header:    s.header(),
		SafePoint: returnSafePoint,
		Revision:  currentRevision,
	}, nil
}

// UpdateGCSafePointByServiceGroup used by gc_worker to update their gc safe points
func (s *GcServer) UpdateGCSafePointByServiceGroup(ctx context.Context, request *gcpb.UpdateGCSafePointByServiceGroupRequest) (*gcpb.UpdateGCSafePointByServiceGroupResponse, error) {
	s.serviceGroupSafePointLock.Lock()
	defer s.serviceGroupSafePointLock.Unlock()

	rc := s.GetRaftCluster()
	if rc == nil {
		return &gcpb.UpdateGCSafePointByServiceGroupResponse{Header: s.notBootstrappedHeader()}, nil
	}

	var storage endpoint.GCSafePointStorage = s.storage

	// check if revision changed since last min calculation
	rsp, _ := s.client.Get(ctx, "NA")
	currentRevision := rsp.Header.GetRevision()
	requestRevision := request.GetRevision()
	if currentRevision != requestRevision {
		return &gcpb.UpdateGCSafePointByServiceGroupResponse{
			Header:       s.revisionMismatchHeader(requestRevision, currentRevision),
			Succeeded:    false,
			NewSafePoint: 0,
		}, nil
	}
	serviceGroupID := string(request.ServiceGroupId)
	newSafePoint := &endpoint.GCSafePoint{
		ServiceGroupID: serviceGroupID,
		SafePoint:      request.SafePoint,
	}

	prev, err := storage.LoadGCWorkerSafePoint(serviceGroupID)
	if err != nil {
		return nil, err
	}
	// if no previous safepoint, treat it as 0
	var oldSafePoint uint64 = 0
	if prev != nil {
		oldSafePoint = prev.SafePoint
	}

	response := &gcpb.UpdateGCSafePointByServiceGroupResponse{}

	// fail to store due to safe point rollback
	if newSafePoint.SafePoint < oldSafePoint {
		log.Warn("trying to update gc_worker safe point",
			zap.String("service-group-id", serviceGroupID),
			zap.Uint64("old-safe-point", request.SafePoint),
			zap.Uint64("new-safe-point", newSafePoint.SafePoint))
		response.Header = s.safePointRollbackHeader(newSafePoint.SafePoint, oldSafePoint)
		response.Succeeded = false
		response.NewSafePoint = oldSafePoint
		return response, nil
	}

	// save the safe point to storage
	if err := storage.SaveGCWorkerSafePoint(newSafePoint); err != nil {
		return nil, err
	}
	response.Header = s.header()
	response.Succeeded = true
	response.NewSafePoint = newSafePoint.SafePoint
	log.Info("updated gc_worker safe point",
		zap.String("service-group-id", serviceGroupID),
		zap.Uint64("safe-point", newSafePoint.SafePoint))
	return response, nil
}

// UpdateServiceSafePointByServiceGroup for services like CDC/BR/Lightning to update gc safe points in PD
func (s *GcServer) UpdateServiceSafePointByServiceGroup(ctx context.Context, request *gcpb.UpdateServiceSafePointByServiceGroupRequest) (*gcpb.UpdateServiceSafePointByServiceGroupResponse, error) {
	s.serviceGroupSafePointLock.Lock()
	defer s.serviceGroupSafePointLock.Unlock()

	rc := s.GetRaftCluster()
	if rc == nil {
		return &gcpb.UpdateServiceSafePointByServiceGroupResponse{Header: s.notBootstrappedHeader()}, nil
	}

	var storage endpoint.GCSafePointStorage = s.storage
	serviceGroupID := string(request.ServiceGroupId)
	serviceID := string(request.ServiceId)
	// a less than 0 ttl means to remove the safe point, immediately return after the deletion request
	if request.TTL <= 0 {
		if err := storage.RemoveServiceSafePointByServiceGroup(serviceGroupID, serviceID); err != nil {
			return nil, err
		}
		return &gcpb.UpdateServiceSafePointByServiceGroupResponse{
			Header:    s.header(),
			Succeeded: true,
		}, nil
	}

	nowTSO, err := s.tsoAllocatorManager.HandleTSORequest(tso.GlobalDCLocation, 1)
	if err != nil {
		return nil, err
	}
	now, _ := tsoutil.ParseTimestamp(nowTSO)

	sspOld, err := storage.LoadServiceSafePoint(serviceGroupID, serviceID)
	if err != nil {
		return nil, err
	}
	gcsp, err := storage.LoadGCWorkerSafePoint(serviceGroupID)
	if err != nil {
		return nil, err
	}

	response := &gcpb.UpdateServiceSafePointByServiceGroupResponse{}
	// safePointLowerBound is the minimum request.SafePoint for update request to succeed
	// it is oldServiceSafePoint if oldServiceSafePoint exists, else gcSafePoint if it exists
	// otherwise it's set to 0, indicate all safePoint accepted
	var safePointLowerBound uint64 = 0
	if gcsp != nil {
		safePointLowerBound = gcsp.SafePoint
		response.GcSafePoint = gcsp.SafePoint
	}
	if sspOld != nil {
		safePointLowerBound = sspOld.SafePoint
		response.OldSafePoint = sspOld.SafePoint
	}

	// request.SafePoint smaller than safePointLowerBound, we have a safePointRollBack
	if request.SafePoint < safePointLowerBound {
		response.Header = s.safePointRollbackHeader(request.SafePoint, safePointLowerBound)
		response.Succeeded = false
		return response, nil
	}

	response.Succeeded = true
	response.NewSafePoint = request.SafePoint
	ssp := &endpoint.ServiceSafePoint{
		ServiceID: serviceID,
		ExpiredAt: now.Unix() + request.TTL,
		SafePoint: request.SafePoint,
	}
	// handles overflow
	if math.MaxInt64-now.Unix() <= request.TTL {
		ssp.ExpiredAt = math.MaxInt64
	}
	if err := storage.SaveServiceSafePointByServiceGroup(serviceGroupID, ssp); err != nil {
		return nil, err
	}
	log.Info("update service safe point by service group",
		zap.String("service-group-id", serviceGroupID),
		zap.String("service-id", ssp.ServiceID),
		zap.Int64("expire-at", ssp.ExpiredAt),
		zap.Uint64("safepoint", ssp.SafePoint))
	return response, nil
}

// GetAllServiceGroupGCSafePoints returns all service group's gc safe point
func (s *GcServer) GetAllServiceGroupGCSafePoints(ctx context.Context, request *gcpb.GetAllServiceGroupGCSafePointsRequest) (*gcpb.GetAllServiceGroupGCSafePointsResponse, error) {

	rc := s.GetRaftCluster()
	if rc == nil {
		return &gcpb.GetAllServiceGroupGCSafePointsResponse{Header: s.notBootstrappedHeader()}, nil
	}

	var storage endpoint.GCSafePointStorage = s.storage
	serviceIDs, gcSafePoints, err := storage.LoadAllServiceGroupGCSafePoints()
	if err != nil {
		return nil, err
	}

	safePoints := make([]*gcpb.ServiceGroupSafePoint, 0, 2)
	for i := range serviceIDs {
		safePoints = append(safePoints, &gcpb.ServiceGroupSafePoint{
			ServiceGroupId: serviceIDs[i],
			SafePoint:      gcSafePoints[i],
		})
	}
	return &gcpb.GetAllServiceGroupGCSafePointsResponse{
		Header:     s.header(),
		SafePoints: safePoints,
	}, nil
}
