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

	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"go.etcd.io/etcd/clientv3"
)

// MetaStorageServer wraps GrpcServer to provide meta storage service.
type MetaStorageServer struct {
	*GrpcServer
}

// Watch watches the key with a given prefix and revision.
func (s *MetaStorageServer) Watch(req *meta_storagepb.WatchRequest, server meta_storagepb.MetaStorage_WatchServer) error {
	ctx, cancel := context.WithCancel(s.Context())
	defer cancel()
	key := string(req.GetKey())
	endKey := string(req.GetRangeEnd())
	startRevision := req.GetStartRevision()
	watchChan := s.client.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithRange(endKey), clientv3.WithRev(startRevision), clientv3.WithPrevKV())
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
					Header: &meta_storagepb.ResponseHeader{Revision: res.Header.GetRevision(), ClusterId: s.clusterID},
					Events: events, CompactRevision: res.CompactRevision}); err != nil {
					return err
				}
			}
		}
	}
}

// Get gets the key-value pair with a given key.
func (s *MetaStorageServer) Get(ctx context.Context, req *meta_storagepb.GetRequest) (*meta_storagepb.GetResponse, error) {
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
	res, err := s.client.Get(ctx, key, options...)
	if err != nil {
		return &meta_storagepb.GetResponse{Header: s.wrapErrorAndRevision(res.Header.GetRevision(), meta_storagepb.ErrorType_UNKNOWN, err.Error())}, nil
	}
	resp := &meta_storagepb.GetResponse{
		Header: &meta_storagepb.ResponseHeader{ClusterId: s.clusterID, Revision: res.Header.GetRevision()},
		Count:  res.Count,
		More:   res.More,
	}
	for _, kv := range res.Kvs {
		resp.Kvs = append(resp.Kvs, &meta_storagepb.KeyValue{Key: kv.Key, Value: kv.Value})
	}

	return resp, nil
}

// Put puts the key-value pair into meta storage.
func (s *MetaStorageServer) Put(ctx context.Context, req *meta_storagepb.PutRequest) (*meta_storagepb.PutResponse, error) {
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

	res, err := s.client.Put(ctx, key, value, options...)
	if err != nil {
		return &meta_storagepb.PutResponse{Header: s.wrapErrorAndRevision(res.Header.GetRevision(), meta_storagepb.ErrorType_UNKNOWN, err.Error())}, nil
	}

	resp := &meta_storagepb.PutResponse{
		Header: &meta_storagepb.ResponseHeader{ClusterId: s.clusterID, Revision: res.Header.GetRevision()},
	}
	if res.PrevKv != nil {
		resp.PrevKv = &meta_storagepb.KeyValue{Key: res.PrevKv.Key, Value: res.PrevKv.Value}
	}
	return resp, nil
}

func (s *MetaStorageServer) wrapErrorAndRevision(revision int64, errorType meta_storagepb.ErrorType, message string) *meta_storagepb.ResponseHeader {
	return s.errorHeader(revision, &meta_storagepb.Error{
		Type:    errorType,
		Message: message,
	})
}

func (s *MetaStorageServer) errorHeader(revision int64, err *meta_storagepb.Error) *meta_storagepb.ResponseHeader {
	return &meta_storagepb.ResponseHeader{
		ClusterId: s.clusterID,
		Revision:  revision,
		Error:     err,
	}
}
