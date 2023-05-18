package server

import (
	"context"
	"encoding/json"
	"math"
	"path"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func (s *GrpcServer) GetGCSafePointV2(ctx context.Context, request *pdpb.GetGCSafePointV2Request) (*pdpb.GetGCSafePointV2Response, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).GetGCSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetGCSafePointV2Response), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetGCSafePointV2Response{Header: s.notBootstrappedHeader()}, nil
	}

	safePoint, err := s.safePointV2Manager.LoadGCSafePoint(request.GetKeyspaceId())

	if err != nil {
		return &pdpb.GetGCSafePointV2Response{
			Header: s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, err
	}

	return &pdpb.GetGCSafePointV2Response{
		Header:    s.header(),
		SafePoint: safePoint.SafePoint,
	}, nil
}

func (s *GrpcServer) UpdateGCSafePointV2(ctx context.Context, request *pdpb.UpdateGCSafePointV2Request) (*pdpb.UpdateGCSafePointV2Response, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).UpdateGCSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.UpdateGCSafePointV2Response), err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.UpdateGCSafePointV2Response{Header: s.notBootstrappedHeader()}, nil
	}

	newSafePoint := request.GetSafePoint()
	oldSafePoint, err := s.safePointV2Manager.UpdateGCSafePoint(&endpoint.GCSafePointV2{
		KeyspaceID: request.KeyspaceId,
		SafePoint:  request.SafePoint,
	})
	if err != nil {
		return nil, err
	}
	if newSafePoint > oldSafePoint.SafePoint {
		log.Info("updated gc safe point",
			zap.Uint64("safe-point", newSafePoint),
			zap.Uint32("keyspace-id", request.GetKeyspaceId()))
	} else if newSafePoint < oldSafePoint.SafePoint {
		log.Warn("trying to update gc safe point",
			zap.Uint64("old-safe-point", oldSafePoint.SafePoint),
			zap.Uint64("new-safe-point", newSafePoint),
			zap.Uint32("keyspace-id", request.GetKeyspaceId()))
		newSafePoint = oldSafePoint.SafePoint
	}

	return &pdpb.UpdateGCSafePointV2Response{
		Header:       s.header(),
		NewSafePoint: newSafePoint,
	}, nil
}

func (s *GrpcServer) UpdateServiceSafePointV2(ctx context.Context, request *pdpb.UpdateServiceSafePointV2Request) (*pdpb.UpdateServiceSafePointV2Response, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (interface{}, error) {
		return pdpb.NewPDClient(client).UpdateServiceSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.UpdateServiceSafePointV2Response), err
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.UpdateServiceSafePointV2Response{Header: s.notBootstrappedHeader()}, nil
	}

	nowTSO, err := s.tsoAllocatorManager.HandleRequest(tso.GlobalDCLocation, 1)
	if err != nil {
		return nil, err
	}
	physicalTime, _ := tsoutil.ParseTimestamp(nowTSO)
	now := physicalTime.Unix()

	var minServiceSafePoint *endpoint.ServiceSafePointV2
	if request.Ttl < 0 {
		minServiceSafePoint, err = s.safePointV2Manager.RemoveServiceSafePoint(request.GetKeyspaceId(), string(request.GetServiceId()), now)
	} else {
		serviceSafePoint := &endpoint.ServiceSafePointV2{
			KeyspaceID: request.GetKeyspaceId(),
			ServiceID:  string(request.GetServiceId()),
			ExpiredAt:  now + request.GetTtl(),
			SafePoint:  request.GetSafePoint(),
		}
		// Fix possible overflow.
		if math.MaxInt64-now <= request.GetTtl() {
			serviceSafePoint.ExpiredAt = math.MaxInt64
		}
		minServiceSafePoint, err = s.safePointV2Manager.UpdateServiceSafePoint(serviceSafePoint, now)
	}
	if err != nil {
		return nil, err
	}
	return &pdpb.UpdateServiceSafePointV2Response{
		Header:       s.header(),
		ServiceId:    []byte(minServiceSafePoint.ServiceID),
		Ttl:          minServiceSafePoint.ExpiredAt - now,
		MinSafePoint: minServiceSafePoint.SafePoint,
	}, nil
}

func (s *GrpcServer) WatchGCSafePointV2(request *pdpb.WatchGCSafePointV2Request, stream pdpb.PD_WatchGCSafePointV2Server) error {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return err
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return stream.Send(&pdpb.WatchGCSafePointV2Response{Header: s.notBootstrappedHeader()})
	}

	ctx, cancel := context.WithCancel(s.Context())
	defer cancel()

	err := s.sendAllGCSafePoints(ctx, stream)
	if err != nil {
		return err
	}
	watchChan := s.client.Watch(ctx, path.Join(s.rootPath, endpoint.GCSafePointV2Prefix()), clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-watchChan:
			safePointEvents := make([]*pdpb.SafePointEvent, 0, len(res.Events))
			for _, event := range res.Events {
				gcSafePoint := &endpoint.GCSafePointV2{}
				if err = json.Unmarshal(event.Kv.Value, gcSafePoint); err != nil {
					return err
				}
				safePointEvents = append(safePointEvents, &pdpb.SafePointEvent{
					KeyspaceId: gcSafePoint.KeyspaceID,
					SafePoint:  gcSafePoint.SafePoint,
					Type:       pdpb.EventType(event.Type),
				})
			}
			if len(safePointEvents) > 0 {
				if err = stream.Send(&pdpb.WatchGCSafePointV2Response{Header: s.header(), Events: safePointEvents}); err != nil {
					return err
				}
			}
		}
	}
}

func (s *GrpcServer) sendAllGCSafePoints(ctx context.Context, stream pdpb.PD_WatchGCSafePointV2Server) error {
	getResp, err := s.client.Get(ctx, path.Join(s.rootPath, endpoint.GCSafePointV2Prefix()), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	safePointEvents := make([]*pdpb.SafePointEvent, 0, len(getResp.Kvs))
	for i, kv := range getResp.Kvs {
		gcSafePoint := &endpoint.GCSafePointV2{}
		if err = json.Unmarshal(kv.Value, gcSafePoint); err != nil {
			return err
		}
		safePointEvents[i] = &pdpb.SafePointEvent{
			KeyspaceId: gcSafePoint.KeyspaceID,
			SafePoint:  gcSafePoint.SafePoint,
			Type:       pdpb.EventType_PUT,
		}
	}
	return stream.Send(&pdpb.WatchGCSafePointV2Response{Header: s.header(), Events: safePointEvents})
}
