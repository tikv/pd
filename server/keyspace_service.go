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
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// KeyspaceServer wraps GrpcServer to provide keyspace service.
type KeyspaceServer struct {
	*GrpcServer
}

// getErrorHeader returns corresponding ResponseHeader based on err.
func (s *KeyspaceServer) getErrorHeader(err error) *pdpb.ResponseHeader {
	switch err {
	case keyspace.ErrKeyspaceExists:
		return s.wrapErrorToHeader(pdpb.ErrorType_DUPLICATED_ENTRY, err.Error())
	case keyspace.ErrKeyspaceNotFound:
		return s.wrapErrorToHeader(pdpb.ErrorType_ENTRY_NOT_FOUND, err.Error())
	default:
		return s.wrapErrorToHeader(pdpb.ErrorType_UNKNOWN, err.Error())
	}
}

// LoadKeyspace load and return target keyspace metadata.
// Request must specify keyspace name.
// On Error, keyspaceMeta in response will be nil,
// error information will be encoded in response header with corresponding error type.
func (s *KeyspaceServer) LoadKeyspace(_ context.Context, request *keyspacepb.LoadKeyspaceRequest) (*keyspacepb.LoadKeyspaceResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	manager := s.GetKeyspaceManager()
	meta, err := manager.LoadKeyspace(request.GetName())
	if err != nil {
		return &keyspacepb.LoadKeyspaceResponse{Header: s.getErrorHeader(err)}, nil
	}
	return &keyspacepb.LoadKeyspaceResponse{
		Header:   s.header(),
		Keyspace: meta,
	}, nil
}

// WatchKeyspaces captures and sends keyspace metadata changes to the client via gRPC stream.
// Note: It sends all existing keyspaces as it's first package to the client.
func (s *KeyspaceServer) WatchKeyspaces(request *keyspacepb.WatchKeyspacesRequest, stream keyspacepb.Keyspace_WatchKeyspacesServer) error {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(s.Context())
	defer cancel()
	startKey := path.Join(s.rootPath, endpoint.KeyspaceMetaPrefix()) + "/"

	keyspaces := make([]*keyspacepb.KeyspaceMeta, 0)
	putFn := func(kv *mvccpb.KeyValue) error {
		meta := &keyspacepb.KeyspaceMeta{}
		if err := proto.Unmarshal(kv.Value, meta); err != nil {
			defer cancel() // cancel context to stop watcher
			return err
		}
		keyspaces = append(keyspaces, meta)
		return nil
	}
	deleteFn := func(*mvccpb.KeyValue) error {
		return nil
	}
	postEventsFn := func([]*clientv3.Event) error {
		if len(keyspaces) == 0 {
			return nil
		}
		defer func() {
			keyspaces = keyspaces[:0]
		}()
		err := stream.Send(&keyspacepb.WatchKeyspacesResponse{
			Header:    s.header(),
			Keyspaces: keyspaces})
		if err != nil {
			defer cancel() // cancel context to stop watcher
			return err
		}
		return nil
	}

	watcher := etcdutil.NewLoopWatcher(
		ctx,
		&s.serverLoopWg,
		s.client,
		"keyspace-server-watcher",
		startKey,
		func([]*clientv3.Event) error { return nil },
		putFn,
		deleteFn,
		postEventsFn,
		true, /* withPrefix */
	)
	watcher.StartWatchLoop()
	if err := watcher.WaitLoad(); err != nil {
		cancel() // cancel context to stop watcher
		return err
	}

	<-ctx.Done() // wait for context done
	return nil
}

// UpdateKeyspaceState updates the state of keyspace specified in the request.
func (s *KeyspaceServer) UpdateKeyspaceState(_ context.Context, request *keyspacepb.UpdateKeyspaceStateRequest) (*keyspacepb.UpdateKeyspaceStateResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	manager := s.GetKeyspaceManager()
	meta, err := manager.UpdateKeyspaceStateByID(request.GetId(), request.GetState(), time.Now().Unix())
	if err != nil {
		return &keyspacepb.UpdateKeyspaceStateResponse{Header: s.getErrorHeader(err)}, nil
	}
	return &keyspacepb.UpdateKeyspaceStateResponse{
		Header:   s.header(),
		Keyspace: meta,
	}, nil
}

// GetAllKeyspaces get all keyspace's metadata.
func (s *KeyspaceServer) GetAllKeyspaces(_ context.Context, request *keyspacepb.GetAllKeyspacesRequest) (*keyspacepb.GetAllKeyspacesResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	manager := s.GetKeyspaceManager()
	keyspaces, err := manager.LoadRangeKeyspace(request.StartId, int(request.Limit))
	if err != nil {
		return &keyspacepb.GetAllKeyspacesResponse{Header: s.getErrorHeader(err)}, nil
	}

	return &keyspacepb.GetAllKeyspacesResponse{
		Header:    s.header(),
		Keyspaces: keyspaces,
	}, nil
}
