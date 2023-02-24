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

	"github.com/pingcap/kvproto/pkg/etcdpb"
	"go.etcd.io/etcd/clientv3"
)

// EtcdServer wraps GrpcServer to provide etcd service.
type EtcdServer struct {
	*GrpcServer
}

// Watch watches the key with a given prefix and revision.
func (s *EtcdServer) Watch(req *etcdpb.WatchRequest, server etcdpb.Etcd_WatchServer) error {
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
				var resp etcdpb.WatchResponse
				if startRevision < res.CompactRevision {
					resp.Header = s.wrapErrorAndRevision(res.Header.GetRevision(), etcdpb.ErrorType_DATA_COMPACTED,
						fmt.Sprintf("required watch revision: %d is smaller than current compact/min revision %d.", startRevision, res.CompactRevision))
					resp.CompactRevision = res.CompactRevision
				} else {
					resp.Header = s.wrapErrorAndRevision(res.Header.GetRevision(), etcdpb.ErrorType_UNKNOWN,
						fmt.Sprintf("watch channel meet other error %s.", res.Err().Error()))
				}
				if err := server.Send(&resp); err != nil {
					return err
				}
				// Err() indicates that this WatchResponse holds a channel-closing error.
				return res.Err()
			}

			events := make([]*etcdpb.Event, 0, len(res.Events))
			for _, e := range res.Events {
				event := &etcdpb.Event{Kv: &etcdpb.KeyValue{Key: e.Kv.Key, Value: e.Kv.Value}, Type: etcdpb.Event_EventType(e.Type)}
				if e.PrevKv != nil {
					event.PrevKv = &etcdpb.KeyValue{Key: e.PrevKv.Key, Value: e.PrevKv.Value}
				}
				events = append(events, event)
			}
			if len(events) > 0 {
				if err := server.Send(&etcdpb.WatchResponse{
					Header: &etcdpb.ResponseHeader{Revision: res.Header.GetRevision(), ClusterId: s.clusterID},
					Events: events, CompactRevision: res.CompactRevision}); err != nil {
					return err
				}
			}
		}
	}
}

// Get gets the key-value pair with a given key.
func (s *EtcdServer) Get(ctx context.Context, req *etcdpb.GetRequest) (*etcdpb.GetResponse, error) {
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
		return &etcdpb.GetResponse{Header: s.wrapErrorAndRevision(res.Header.GetRevision(), etcdpb.ErrorType_UNKNOWN, err.Error())}, nil
	}
	resp := &etcdpb.GetResponse{
		Header: &etcdpb.ResponseHeader{ClusterId: s.clusterID, Revision: res.Header.GetRevision()},
		Count:  res.Count,
		More:   res.More,
	}
	for _, kv := range res.Kvs {
		resp.Kvs = append(resp.Kvs, &etcdpb.KeyValue{Key: kv.Key, Value: kv.Value})
	}

	return resp, nil
}

// Put puts the key-value pair into etcd.
func (s *EtcdServer) Put(ctx context.Context, req *etcdpb.PutRequest) (*etcdpb.PutResponse, error) {
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
		return &etcdpb.PutResponse{Header: s.wrapErrorAndRevision(res.Header.GetRevision(), etcdpb.ErrorType_UNKNOWN, err.Error())}, nil
	}

	resp := &etcdpb.PutResponse{
		Header: &etcdpb.ResponseHeader{ClusterId: s.clusterID, Revision: res.Header.GetRevision()},
	}
	if res.PrevKv != nil {
		resp.PrevKv = &etcdpb.KeyValue{Key: res.PrevKv.Key, Value: res.PrevKv.Value}
	}
	return resp, nil
}

func (s *EtcdServer) wrapErrorAndRevision(revision int64, errorType etcdpb.ErrorType, message string) *etcdpb.ResponseHeader {
	return s.etcdErrorHeader(revision, &etcdpb.Error{
		Type:    errorType,
		Message: message,
	})
}

func (s *EtcdServer) etcdErrorHeader(revision int64, err *etcdpb.Error) *etcdpb.ResponseHeader {
	return &etcdpb.ResponseHeader{
		ClusterId: s.clusterID,
		Revision:  revision,
		Error:     err,
	}
}
