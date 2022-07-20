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
	"github.com/gogo/protobuf/proto"
	"github.com/tikv/pd/server/keyspace"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/storage/endpoint"
	"go.etcd.io/etcd/clientv3"
)

// KeyspaceServer wraps Server to provide keyspace service.
type KeyspaceServer struct {
	*Server
}

func (s *KeyspaceServer) header() *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{ClusterId: s.clusterID}
}

func (s *KeyspaceServer) errorHeader(err *pdpb.Error) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

func (s *KeyspaceServer) notBootstrappedHeader() *pdpb.ResponseHeader {
	return s.errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}

// getErrorHeader returns corresponding ResponseHeader based on err.
func (s *KeyspaceServer) getErrorHeader(err error) *pdpb.ResponseHeader {
	switch err {
	case keyspace.ErrKeyspaceExists:
		return s.errorHeader(&pdpb.Error{
			Type:    pdpb.ErrorType_DUPLICATED_ENTRY,
			Message: err.Error(),
		})
	case keyspace.ErrKeyspaceNotFound:
		return s.errorHeader(&pdpb.Error{
			Type:    pdpb.ErrorType_ENTRY_NOT_FOUND,
			Message: err.Error(),
		})
	default:
		return s.errorHeader(&pdpb.Error{
			Type:    pdpb.ErrorType_UNKNOWN,
			Message: err.Error(),
		})
	}
}

func (s *KeyspaceServer) UpdateKeyspaceConfig(ctx context.Context, request *keyspacepb.UpdateKeyspaceConfigRequest) (*keyspacepb.UpdateKeyspaceConfigResponse, error) {
	rc := s.GetRaftCluster()
	if rc == nil {
		return &keyspacepb.UpdateKeyspaceConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}

	manager := s.keyspaceManager
	updatedMeta, err := manager.UpdateKeyspaceConfig(request.Name, request.Mutations)
	if err != nil {
		return &keyspacepb.UpdateKeyspaceConfigResponse{Header: s.getErrorHeader(err)}, err
	}

	return &keyspacepb.UpdateKeyspaceConfigResponse{
		Header:   s.header(),
		Keyspace: updatedMeta,
	}, nil
}

func (s *KeyspaceServer) LoadKeyspace(ctx context.Context, request *keyspacepb.LoadKeyspaceRequest) (*keyspacepb.LoadKeyspaceResponse, error) {
	rc := s.GetRaftCluster()
	if rc == nil {
		return &keyspacepb.LoadKeyspaceResponse{Header: s.notBootstrappedHeader()}, nil
	}

	manager := s.keyspaceManager
	meta, err := manager.LoadKeyspace(request.Name)
	if err != nil {
		return &keyspacepb.LoadKeyspaceResponse{Header: s.getErrorHeader(err)}, err
	}
	return &keyspacepb.LoadKeyspaceResponse{
		Header:   s.header(),
		Keyspace: meta,
	}, nil
}

func (s *KeyspaceServer) WatchKeyspaces(_ *keyspacepb.WatchKeyspacesRequest, stream keyspacepb.Keyspace_WatchKeyspacesServer) error {
	rc := s.GetRaftCluster()
	if rc == nil {
		return stream.Send(&keyspacepb.WatchKeyspacesResponse{Header: s.notBootstrappedHeader()})
	}

	ctx, cancel := context.WithCancel(s.Context())
	defer cancel()

	err := s.sendAllKeyspaceMeta(ctx, stream)
	if err != nil {
		return err
	}
	watchChan := s.client.Watch(ctx, endpoint.KeyspaceMetaPrefix(), clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-watchChan:
			keyspaces := make([]*keyspacepb.KeyspaceMeta, 0, len(res.Events))
			for _, event := range res.Events {
				if event.Type != clientv3.EventTypePut {
					continue
				}
				meta := &keyspacepb.KeyspaceMeta{}
				if err = proto.Unmarshal(event.Kv.Value, meta); err != nil {
					return err
				}
				keyspaces = append(keyspaces, meta)
			}
			if len(keyspaces) > 0 {
				if err = stream.Send(&keyspacepb.WatchKeyspacesResponse{Header: s.header(), Keyspaces: keyspaces}); err != nil {
					return err
				}
			}
		}
	}
}

func (s *KeyspaceServer) sendAllKeyspaceMeta(ctx context.Context, stream keyspacepb.Keyspace_WatchKeyspacesServer) error {
	getResp, err := s.client.Get(ctx, endpoint.KeyspaceMetaPrefix(), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	metas := make([]*keyspacepb.KeyspaceMeta, getResp.Count)
	for i, kv := range getResp.Kvs {
		if err = proto.Unmarshal(kv.Value, metas[i]); err != nil {
			return err
		}
	}
	return stream.Send(&keyspacepb.WatchKeyspacesResponse{Header: s.header(), Keyspaces: metas})
}
