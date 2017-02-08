// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/pdpb2"
	"golang.org/x/net/context"
)

// GetPDMembers implements gRPC PDServer.
func (s *Server) GetPDMembers(context.Context, *pdpb2.GetPDMembersRequest) (*pdpb2.GetPDMembersResponse, error) {
	members, err := GetPDMembers(s.GetClient())
	if err != nil {
		return nil, errors.Trace(err)
	}
	members2 := make([]*pdpb2.Member, len(members))
	for i := range members {
		members2[i] = &pdpb2.Member{
			Name:       members[i].GetName(),
			MemberId:   s.clusterID,
			PeerUrls:   members[i].PeerUrls,
			ClientUrls: members[i].ClientUrls,
		}
	}
	return &pdpb2.GetPDMembersResponse{
		Header:  &pdpb2.ResponseHeader{ClusterId: s.clusterID},
		Members: members2,
	}, nil
}

// Tso implements gRPC PDServer.
func (s *Server) Tso(ctx context.Context, req *pdpb2.TsoRequest) (*pdpb2.TsoResponse, error) {
	if !s.IsLeader() {
		// TODO: work as proxy.
		return nil, errors.New("not leader")
	}

	count := req.GetCount()
	ts, err := s.getRespTS(count)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &pdpb2.TsoResponse{
		Header: &pdpb2.ResponseHeader{ClusterId: s.clusterID},
		Timestamp: &pdpb2.Timestamp{
			Physical: ts.GetPhysical(),
			Logical:  ts.GetLogical(),
		},
		Count: count,
	}, nil
}

// Bootstrap implements gRPC PDServer.
func (s *Server) Bootstrap(context.Context, *pdpb2.BootstrapRequest) (*pdpb2.BootstrapResponse, error) {
	panic("not implemented")
}

// IsBootstrapped implements gRPC PDServer.
func (s *Server) IsBootstrapped(context.Context, *pdpb2.IsBootstrappedRequest) (*pdpb2.IsBootstrappedResponse, error) {
	panic("not implemented")
}

// AllocID implements gRPC PDServer.
func (s *Server) AllocID(context.Context, *pdpb2.AllocIDRequest) (*pdpb2.AllocIDResponse, error) {
	panic("not implemented")
}

// GetStore implements gRPC PDServer.
func (s *Server) GetStore(ctx context.Context, req *pdpb2.GetStoreRequest) (*pdpb2.GetStoreResponse, error) {
	if !s.IsLeader() {
		// TODO: work as proxy.
		return nil, errors.New("not leader")
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return nil, errors.Trace(errNotBootstrapped)
	}
	store, _, err := cluster.GetStore(req.GetStoreId())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &pdpb2.GetStoreResponse{
		Header: &pdpb2.ResponseHeader{ClusterId: s.clusterID},
		Store:  store,
	}, nil
}

// PutStore implements gRPC PDServer.
func (s *Server) PutStore(context.Context, *pdpb2.PutStoreRequest) (*pdpb2.PutStoreResponse, error) {
	panic("not implemented")
}

// StoreHeartbeat implements gRPC PDServer.
func (s *Server) StoreHeartbeat(context.Context, *pdpb2.StoreHeartbeatRequest) (*pdpb2.StoreHeartbeatResponse, error) {
	panic("not implemented")
}

// RegionHeartbeat implements gRPC PDServer.
func (s *Server) RegionHeartbeat(context.Context, *pdpb2.RegionHeartbeatRequest) (*pdpb2.RegionHeartbeatResponse, error) {
	panic("not implemented")
}

// GetRegion implements gRPC PDServer.
func (s *Server) GetRegion(ctx context.Context, req *pdpb2.GetRegionRequest) (*pdpb2.GetRegionResponse, error) {
	if !s.IsLeader() {
		// TODO: work as proxy.
		return nil, errors.New("not leader")
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return nil, errors.Trace(errNotBootstrapped)
	}
	region, leader := cluster.getRegion(req.GetRegionKey())
	return &pdpb2.GetRegionResponse{
		Header: &pdpb2.ResponseHeader{ClusterId: s.clusterID},
		Region: region,
		Leader: leader,
	}, nil
}

// GetRegionByID implements gRPC PDServer.
func (s *Server) GetRegionByID(context.Context, *pdpb2.GetRegionByIDRequest) (*pdpb2.GetRegionResponse, error) {
	panic("not implemented")
}

// AskSplit implements gRPC PDServer.
func (s *Server) AskSplit(context.Context, *pdpb2.AskSplitRequest) (*pdpb2.AskSplitResponse, error) {
	panic("not implemented")
}

// ReportSplit implements gRPC PDServer.
func (s *Server) ReportSplit(context.Context, *pdpb2.ReportSplitRequest) (*pdpb2.ReportSplitResponse, error) {
	panic("not implemented")
}

// GetClusterConfig implements gRPC PDServer.
func (s *Server) GetClusterConfig(context.Context, *pdpb2.GetClusterConfigRequest) (*pdpb2.GetClusterConfigResponse, error) {
	panic("not implemented")
}

// PutClusterConfig implements gRPC PDServer.
func (s *Server) PutClusterConfig(context.Context, *pdpb2.PutClusterConfigRequest) (*pdpb2.PutClusterConfigResponse, error) {
	panic("not implemented")
}
