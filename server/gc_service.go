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
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/tsoutil"
)

// GetGCSafePointV2 return gc safe point for the given keyspace.
func (s *GrpcServer) GetGCSafePointV2(ctx context.Context, request *pdpb.GetGCSafePointV2Request) (*pdpb.GetGCSafePointV2Response, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetGCSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetGCSafePointV2Response), err
	}

	safePoint, err := s.safePointV2Manager.LoadGCSafePoint(request.GetKeyspaceId())

	if err != nil {
		return &pdpb.GetGCSafePointV2Response{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, err
	}

	return &pdpb.GetGCSafePointV2Response{
		Header:    wrapHeader(),
		SafePoint: safePoint.SafePoint,
	}, nil
}

// UpdateGCSafePointV2 update gc safe point for the given keyspace.
func (s *GrpcServer) UpdateGCSafePointV2(ctx context.Context, request *pdpb.UpdateGCSafePointV2Request) (*pdpb.UpdateGCSafePointV2Response, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).UpdateGCSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.UpdateGCSafePointV2Response), err
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
		Header:       wrapHeader(),
		NewSafePoint: newSafePoint,
	}, nil
}

// UpdateServiceSafePointV2 update service safe point for the given keyspace.
func (s *GrpcServer) UpdateServiceSafePointV2(ctx context.Context, request *pdpb.UpdateServiceSafePointV2Request) (*pdpb.UpdateServiceSafePointV2Response, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).UpdateServiceSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.UpdateServiceSafePointV2Response), err
	}

	nowTSO, err := s.getGlobalTSO(ctx)
	if err != nil {
		return nil, err
	}
	now, _ := tsoutil.ParseTimestamp(nowTSO)

	var minServiceSafePoint *endpoint.ServiceSafePointV2
	if request.Ttl < 0 {
		minServiceSafePoint, err = s.safePointV2Manager.RemoveServiceSafePoint(request.GetKeyspaceId(), string(request.GetServiceId()), now)
	} else {
		serviceSafePoint := &endpoint.ServiceSafePointV2{
			KeyspaceID: request.GetKeyspaceId(),
			ServiceID:  string(request.GetServiceId()),
			ExpiredAt:  now.Unix() + request.GetTtl(),
			SafePoint:  request.GetSafePoint(),
		}
		// Fix possible overflow.
		if math.MaxInt64-now.Unix() <= request.GetTtl() {
			serviceSafePoint.ExpiredAt = math.MaxInt64
		}
		minServiceSafePoint, err = s.safePointV2Manager.UpdateServiceSafePoint(serviceSafePoint, now)
	}
	if err != nil {
		return nil, err
	}
	return &pdpb.UpdateServiceSafePointV2Response{
		Header:       wrapHeader(),
		ServiceId:    []byte(minServiceSafePoint.ServiceID),
		Ttl:          minServiceSafePoint.ExpiredAt - now.Unix(),
		MinSafePoint: minServiceSafePoint.SafePoint,
	}, nil
}

// WatchGCSafePointV2 watch keyspaces gc safe point changes.
func (s *GrpcServer) WatchGCSafePointV2(request *pdpb.WatchGCSafePointV2Request, stream pdpb.PD_WatchGCSafePointV2Server) error {
	ctx, cancel := context.WithCancel(s.Context())
	defer cancel()
	revision := request.GetRevision()
	// If the revision is compacted, will meet required revision has been compacted error.
	// - If required revision < CompactRevision, we need to reload all configs to avoid losing data.
	// - If required revision >= CompactRevision, just keep watching.
	// Use WithPrevKV() to get the previous key-value pair when get Delete Event.
	watchChan := s.client.Watch(ctx, path.Join(s.rootPath, keypath.GCSafePointV2Prefix()), clientv3.WithRev(revision), clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-watchChan:
			if res.Err() != nil {
				var resp pdpb.WatchGCSafePointV2Response
				if revision < res.CompactRevision {
					resp.Header = wrapErrorToHeader(pdpb.ErrorType_DATA_COMPACTED,
						fmt.Sprintf("required watch revision: %d is smaller than current compact/min revision %d.", revision, res.CompactRevision))
				} else {
					resp.Header = wrapErrorToHeader(pdpb.ErrorType_UNKNOWN,
						fmt.Sprintf("watch channel meet other error %s.", res.Err().Error()))
				}
				if err := stream.Send(&resp); err != nil {
					return err
				}
				// Err() indicates that this WatchResponse holds a channel-closing error.
				return res.Err()
			}
			revision = res.Header.GetRevision()

			safePointEvents := make([]*pdpb.SafePointEvent, 0, len(res.Events))
			for _, event := range res.Events {
				gcSafePoint := &endpoint.GCSafePointV2{}
				if err := json.Unmarshal(event.Kv.Value, gcSafePoint); err != nil {
					return err
				}
				safePointEvents = append(safePointEvents, &pdpb.SafePointEvent{
					KeyspaceId: gcSafePoint.KeyspaceID,
					SafePoint:  gcSafePoint.SafePoint,
					Type:       pdpb.EventType(event.Type),
				})
			}
			if len(safePointEvents) > 0 {
				if err := stream.Send(&pdpb.WatchGCSafePointV2Response{Header: wrapHeader(), Events: safePointEvents, Revision: res.Header.GetRevision()}); err != nil {
					return err
				}
			}
		}
	}
}

// GetAllGCSafePointV2 return all gc safe point v2.
func (s *GrpcServer) GetAllGCSafePointV2(ctx context.Context, request *pdpb.GetAllGCSafePointV2Request) (*pdpb.GetAllGCSafePointV2Response, error) {
	fn := func(ctx context.Context, client *grpc.ClientConn) (any, error) {
		return pdpb.NewPDClient(client).GetAllGCSafePointV2(ctx, request)
	}
	if rsp, err := s.unaryMiddleware(ctx, request, fn); err != nil {
		return nil, err
	} else if rsp != nil {
		return rsp.(*pdpb.GetAllGCSafePointV2Response), err
	}

	startkey := keypath.GCSafePointV2Prefix()
	endkey := clientv3.GetPrefixRangeEnd(startkey)
	_, values, revision, err := s.loadRangeFromEtcd(startkey, endkey)

	gcSafePoints := make([]*pdpb.GCSafePointV2, 0, len(values))
	for _, value := range values {
		jsonGcSafePoint := &endpoint.GCSafePointV2{}
		if err = json.Unmarshal([]byte(value), jsonGcSafePoint); err != nil {
			return nil, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
		}
		gcSafePoint := &pdpb.GCSafePointV2{
			KeyspaceId:  jsonGcSafePoint.KeyspaceID,
			GcSafePoint: jsonGcSafePoint.SafePoint,
		}
		log.Debug("get all gc safe point v2",
			zap.Uint32("keyspace-id", jsonGcSafePoint.KeyspaceID),
			zap.Uint64("gc-safe-point", jsonGcSafePoint.SafePoint))
		gcSafePoints = append(gcSafePoints, gcSafePoint)
	}

	if err != nil {
		return &pdpb.GetAllGCSafePointV2Response{
			Header: wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error()),
		}, err
	}

	return &pdpb.GetAllGCSafePointV2Response{
		Header:       wrapHeader(),
		GcSafePoints: gcSafePoints,
		Revision:     revision,
	}, nil
}

func (s *GrpcServer) loadRangeFromEtcd(startKey, endKey string) ([]string, []string, int64, error) {
	startKey = strings.Join([]string{s.rootPath, startKey}, "/")
	var opOption []clientv3.OpOption
	if endKey == "\x00" {
		opOption = append(opOption, clientv3.WithPrefix())
	} else {
		endKey = strings.Join([]string{s.rootPath, endKey}, "/")
		opOption = append(opOption, clientv3.WithRange(endKey))
	}
	resp, err := etcdutil.EtcdKVGet(s.client, startKey, opOption...)
	if err != nil {
		return nil, nil, 0, err
	}
	keys := make([]string, 0, len(resp.Kvs))
	values := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		keys = append(keys, strings.TrimPrefix(strings.TrimPrefix(string(item.Key), s.rootPath), "/"))
		values = append(values, string(item.Value))
	}
	return keys, values, resp.Header.Revision, nil
}
