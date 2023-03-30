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
	"fmt"
	"net/http"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// errNotLeader is returned when current server is not the leader.
	errNotLeader = status.Errorf(codes.Unavailable, "not leader")
)

var _ meta_storagepb.MetaStorageServer = (*Service)(nil)

// SetUpRestHandler is a hook to sets up the REST service.
var SetUpRestHandler = func(srv *Service) (http.Handler, apiutil.APIServiceGroup) {
	return dummyRestService{}, apiutil.APIServiceGroup{}
}

type dummyRestService struct{}

func (d dummyRestService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("not implemented"))
}

// Service is the gRPC service for meta storage.
type Service struct {
	ctx     context.Context
	manager *Manager
	// settings
}

// NewService creates a new meta storage service.
func NewService[T ClusterIDProvider](svr bs.Server) registry.RegistrableService {
	return &Service{
		ctx:     svr.Context(),
		manager: NewManager[T](svr),
	}
}

// RegisterGRPCService registers the service to gRPC server.
func (s *Service) RegisterGRPCService(g *grpc.Server) {
	meta_storagepb.RegisterMetaStorageServer(g, s)
}

// RegisterRESTHandler registers the service to REST server.
func (s *Service) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) {
	handler, group := SetUpRestHandler(s)
	apiutil.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}

func (s *Service) checkServing() error {
	if s.manager == nil || s.manager.srv == nil || !s.manager.srv.IsServing() {
		return errNotLeader
	}
	return nil
}

// Watch watches the key with a given prefix and revision.
func (s *Service) Watch(req *meta_storagepb.WatchRequest, server meta_storagepb.MetaStorage_WatchServer) error {
	if err := s.checkServing(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	options := []clientv3.OpOption{}
	key := string(req.GetKey())
	var startRevision int64
	if endKey := req.GetRangeEnd(); endKey != nil {
		options = append(options, clientv3.WithRange(string(endKey)))
	}
	if startRevision = req.GetStartRevision(); startRevision != 0 {
		options = append(options, clientv3.WithRev(startRevision))
	}
	if prevKv := req.GetPrevKv(); prevKv {
		options = append(options, clientv3.WithPrevKV())
	}
	cli := s.manager.GetClient()
	watchChan := cli.Watch(ctx, key, options...)
	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-watchChan:
			if res.Err() != nil {
				var resp meta_storagepb.WatchResponse
				if startRevision < res.CompactRevision {
					resp.Header = s.wrapErrorAndRevision(res.Header.GetRevision(), meta_storagepb.ErrorType_DATA_COMPACTED,
						fmt.Sprintf("required watch revision: %d is smaller than current compact/min revision %d.", startRevision, res.CompactRevision))
					resp.CompactRevision = res.CompactRevision
				} else {
					resp.Header = s.wrapErrorAndRevision(res.Header.GetRevision(), meta_storagepb.ErrorType_UNKNOWN,
						fmt.Sprintf("watch channel meet other error %s.", res.Err().Error()))
				}
				if err := server.Send(&resp); err != nil {
					return err
				}
				// Err() indicates that this WatchResponse holds a channel-closing error.
				return res.Err()
			}

			events := make([]*meta_storagepb.Event, 0, len(res.Events))
			for _, e := range res.Events {
				event := &meta_storagepb.Event{Kv: &meta_storagepb.KeyValue{Key: e.Kv.Key, Value: e.Kv.Value}, Type: meta_storagepb.Event_EventType(e.Type)}
				if e.PrevKv != nil {
					event.PrevKv = &meta_storagepb.KeyValue{Key: e.PrevKv.Key, Value: e.PrevKv.Value}
				}
				events = append(events, event)
			}
			if len(events) > 0 {
				if err := server.Send(&meta_storagepb.WatchResponse{
					Header: &meta_storagepb.ResponseHeader{ClusterId: s.manager.ClusterID(), Revision: res.Header.GetRevision()},
					Events: events, CompactRevision: res.CompactRevision}); err != nil {
					return err
				}
			}
		}
	}
}

// Get gets the key-value pair with a given key.
func (s *Service) Get(ctx context.Context, req *meta_storagepb.GetRequest) (*meta_storagepb.GetResponse, error) {
	if err := s.checkServing(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	options := []clientv3.OpOption{}
	key := string(req.GetKey())
	if endKey := req.GetRangeEnd(); endKey != nil {
		options = append(options, clientv3.WithRange(string(endKey)))
	}
	if rev := req.GetRevision(); rev != 0 {
		options = append(options, clientv3.WithRev(rev))
	}
	if limit := req.GetLimit(); limit != 0 {
		options = append(options, clientv3.WithLimit(limit))
	}
	cli := s.manager.GetClient()
	res, err := cli.Get(ctx, key, options...)
	if err != nil {
		return &meta_storagepb.GetResponse{Header: s.wrapErrorAndRevision(res.Header.GetRevision(), meta_storagepb.ErrorType_UNKNOWN, err.Error())}, nil
	}
	resp := &meta_storagepb.GetResponse{
		Header: &meta_storagepb.ResponseHeader{ClusterId: s.manager.ClusterID(), Revision: res.Header.GetRevision()},
		Count:  res.Count,
		More:   res.More,
	}
	for _, kv := range res.Kvs {
		resp.Kvs = append(resp.Kvs, &meta_storagepb.KeyValue{Key: kv.Key, Value: kv.Value})
	}

	return resp, nil
}

// Put puts the key-value pair into meta storage.
func (s *Service) Put(ctx context.Context, req *meta_storagepb.PutRequest) (*meta_storagepb.PutResponse, error) {
	if err := s.checkServing(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	options := []clientv3.OpOption{}
	key := string(req.GetKey())
	value := string(req.GetValue())
	if lease := clientv3.LeaseID(req.GetLease()); lease != 0 {
		options = append(options, clientv3.WithLease(lease))
	}
	if prevKv := req.GetPrevKv(); prevKv {
		options = append(options, clientv3.WithPrevKV())
	}

	cli := s.manager.GetClient()
	res, err := cli.Put(ctx, key, value, options...)
	if err != nil {
		return &meta_storagepb.PutResponse{Header: s.wrapErrorAndRevision(res.Header.GetRevision(), meta_storagepb.ErrorType_UNKNOWN, err.Error())}, nil
	}

	resp := &meta_storagepb.PutResponse{
		Header: &meta_storagepb.ResponseHeader{ClusterId: s.manager.ClusterID(), Revision: res.Header.GetRevision()},
	}
	if res.PrevKv != nil {
		resp.PrevKv = &meta_storagepb.KeyValue{Key: res.PrevKv.Key, Value: res.PrevKv.Value}
	}
	return resp, nil
}

func (s *Service) wrapErrorAndRevision(revision int64, errorType meta_storagepb.ErrorType, message string) *meta_storagepb.ResponseHeader {
	return s.errorHeader(revision, &meta_storagepb.Error{
		Type:    errorType,
		Message: message,
	})
}

func (s *Service) errorHeader(revision int64, err *meta_storagepb.Error) *meta_storagepb.ResponseHeader {
	return &meta_storagepb.ResponseHeader{
		ClusterId: s.manager.ClusterID(),
		Revision:  revision,
		Error:     err,
	}
}
