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
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/gcpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/tso"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GcServer wraps GrpcServer to provide garbage collection service.
type GcServer struct {
	*GrpcServer
}

// UpdateGCSafePoint used by gc_worker to update their gc safe points.
func (s *GcServer) UpdateGCSafePoint(ctx context.Context, request *gcpb.UpdateGCSafePointRequest) (*gcpb.UpdateGCSafePointResponse, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return gcpb.NewGCClient(client).UpdateGCSafePoint(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*gcpb.UpdateGCSafePointResponse), err
	}
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
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return gcpb.NewGCClient(client).UpdateServiceSafePoint(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request.GetHeader(), fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*gcpb.UpdateServiceSafePointResponse), err
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return &gcpb.UpdateServiceSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}

	spaceID := request.GetSpaceId()
	serviceID := string(request.GetServiceId())
	ttl := request.GetTTL()
	requestSafePoint := request.GetSafePoint()

	// A less than 0 ttl means to remove the safe point.
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

// WatchGCSafePoints captures and sends keyspace GC Safe Point changes to the client via gRPC stream.
// Note: It sends all existing GC Safe Points as it's first package to the client.
func (s *GcServer) WatchGCSafePoints(request *gcpb.WatchGCSafePointsRequest, stream gcpb.GC_WatchGCSafePointsServer) error {
	if s.IsClosed() {
		return ErrNotStarted
	}
	if request.GetHeader().GetClusterId() != s.clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, request.Header.GetClusterId())
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return stream.Send(&gcpb.WatchGCSafePointsResponse{Header: s.notBootstrappedHeader()})
	}

	ctx, cancel := context.WithCancel(s.Context())
	defer cancel()

	err := s.sendAllGCSafePoints(stream)
	if err != nil {
		return err
	}

	watchChan := s.client.Watch(ctx, path.Join(s.rootPath, endpoint.KeyspaceSafePointPrefix()), clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-watchChan:
			spEvents := make([]*gcpb.SafePointEvent, 0, len(res.Events))
			for _, event := range res.Events {
				spEvent, err := s.parseEvent(event)
				if err != nil {
					return err
				}
				if spEvent == nil {
					continue
				}
				spEvents = append(spEvents, spEvent)
			}
			if len(spEvents) > 0 {
				if err = stream.Send(&gcpb.WatchGCSafePointsResponse{Header: s.header(), Events: spEvents}); err != nil {
					return err
				}
			}
		}
	}
}

// parseEvent parse a clientV3 event to a safe point event.
func (s *GcServer) parseEvent(event *clientv3.Event) (*gcpb.SafePointEvent, error) {
	eventKey := string(event.Kv.Key)
	eventVal := string(event.Kv.Value)

	prefix := path.Join(s.rootPath, endpoint.KeyspaceSafePointPrefix())
	suffix := endpoint.KeyspaceGCSafePointSuffix()
	// Skip service safe point.
	if !strings.HasSuffix(eventKey, suffix) {
		return nil, nil
	}

	spaceIDStr := strings.TrimPrefix(eventKey, prefix)
	spaceIDStr = strings.TrimSuffix(spaceIDStr, suffix)
	spaceID, err := strconv.ParseUint(spaceIDStr, 10, 32)
	if err != nil {
		return nil, err
	}
	value, err := strconv.ParseUint(eventVal, 16, 64)
	if err != nil {
		return nil, err
	}
	spEvent := &gcpb.SafePointEvent{
		SpaceId:   uint32(spaceID),
		SafePoint: value,
	}
	switch event.Type {
	case clientv3.EventTypePut:
		spEvent.Type = gcpb.EventType_PUT
	case clientv3.EventTypeDelete:
		spEvent.Type = gcpb.EventType_DELETE
	default:
		// Skip unknown watch event.
		return nil, nil
	}
	return spEvent, nil
}

func (s *GcServer) sendAllGCSafePoints(stream gcpb.GC_WatchGCSafePointsServer) error {
	gcSafePoints, err := s.keyspaceSafePointManager.ListGCSafePoints()
	if err != nil {
		return err
	}
	events := make([]*gcpb.SafePointEvent, len(gcSafePoints))
	for i, gcSafePoint := range gcSafePoints {
		events[i] = &gcpb.SafePointEvent{
			SpaceId:   gcSafePoint.SpaceId,
			SafePoint: gcSafePoint.SafePoint,
			Type:      gcpb.EventType_PUT,
		}
	}
	return stream.Send(&gcpb.WatchGCSafePointsResponse{
		Header: s.header(),
		Events: events,
	})
}
