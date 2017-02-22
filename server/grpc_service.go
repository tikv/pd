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
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/pdpb2"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// notLeaderError is returned when current server is not the leader and not possible to process request.
// TODO: work as proxy.
var notLeaderError = grpc.Errorf(codes.Unavailable, "not leader")

// GetPDMembers implements gRPC PDServer.
func (s *Server) GetPDMembers(context.Context, *pdpb2.GetPDMembersRequest) (*pdpb2.GetPDMembersResponse, error) {
	members, err := GetPDMembers(s.GetClient())
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, err.Error())
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
		Header:  s.header(),
		Members: members2,
	}, nil
}

// Tso implements gRPC PDServer.
func (s *Server) Tso(stream pdpb2.PD_TsoServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}
		if !s.IsLeader() {
			return notLeaderError
		}
		if err := s.validateHeader(request.GetHeader()); err != nil {
			return errors.Trace(err)
		}
		count := request.GetCount()
		ts, err := s.getRespTS(count)
		if err != nil {
			return grpc.Errorf(codes.Unknown, err.Error())
		}
		response := &pdpb2.TsoResponse{
			Header: s.header(),
			Timestamp: &pdpb2.Timestamp{
				Physical: ts.GetPhysical(),
				Logical:  ts.GetLogical(),
			},
			Count: count,
		}
		if err := stream.Send(response); err != nil {
			return errors.Trace(err)
		}
	}
}

// Bootstrap implements gRPC PDServer.
func (s *Server) Bootstrap(ctx context.Context, request *pdpb2.BootstrapRequest) (*pdpb2.BootstrapResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	if cluster != nil {
		err := &pdpb2.Error{
			Type:    pdpb2.ErrorType_NOT_BOOTSTRAPPED,
			Message: "cluster is already bootstrapped",
		}
		return &pdpb2.BootstrapResponse{
			Header: s.errorHeader(err),
		}, nil
	}
	req := &pdpb.BootstrapRequest{
		Store:  request.Store,
		Region: request.Region,
	}
	if _, err := s.bootstrapCluster(req); err != nil {
		return nil, grpc.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb2.BootstrapResponse{
		Header: s.header(),
	}, nil
}

// IsBootstrapped implements gRPC PDServer.
func (s *Server) IsBootstrapped(ctx context.Context, request *pdpb2.IsBootstrappedRequest) (*pdpb2.IsBootstrappedResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	return &pdpb2.IsBootstrappedResponse{
		Header:       s.header(),
		Bootstrapped: cluster != nil,
	}, nil
}

// AllocID implements gRPC PDServer.
func (s *Server) AllocID(ctx context.Context, request *pdpb2.AllocIDRequest) (*pdpb2.AllocIDResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	// We can use an allocator for all types ID allocation.
	id, err := s.idAlloc.Alloc()
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb2.AllocIDResponse{
		Header: s.header(),
		Id:     id,
	}, nil
}

// GetStore implements gRPC PDServer.
func (s *Server) GetStore(ctx context.Context, request *pdpb2.GetStoreRequest) (*pdpb2.GetStoreResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb2.GetStoreResponse{Header: s.notBootstrappedHeader()}, nil
	}

	store, _, err := cluster.GetStore(request.GetStoreId())
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, err.Error())
	}
	return &pdpb2.GetStoreResponse{
		Header: s.header(),
		Store:  store,
	}, nil
}

// checkStore2 returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
// Copied from server/command.go
func checkStore2(cluster *RaftCluster, storeID uint64) *pdpb2.Error {
	store, _, err := cluster.GetStore(storeID)
	if err == nil && store != nil {
		if store.GetState() == metapb.StoreState_Tombstone {
			return &pdpb2.Error{
				Type:    pdpb2.ErrorType_STORE_TOMBSTONE,
				Message: "store is tombstone",
			}
		}
	}
	return nil
}

// PutStore implements gRPC PDServer.
func (s *Server) PutStore(ctx context.Context, request *pdpb2.PutStoreRequest) (*pdpb2.PutStoreResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb2.PutStoreResponse{Header: s.notBootstrappedHeader()}, nil
	}

	store := request.GetStore()
	if pberr := checkStore2(cluster, store.GetId()); pberr != nil {
		return &pdpb2.PutStoreResponse{
			Header: s.errorHeader(pberr),
		}, nil
	}

	if err := cluster.putStore(store); err != nil {
		return nil, grpc.Errorf(codes.Unknown, err.Error())
	}

	log.Infof("put store ok - %v", store)

	return &pdpb2.PutStoreResponse{
		Header: s.header(),
	}, nil
}

// StoreHeartbeat implements gRPC PDServer.
func (s *Server) StoreHeartbeat(ctx context.Context, request *pdpb2.StoreHeartbeatRequest) (*pdpb2.StoreHeartbeatResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	if request.GetStats() == nil {
		return nil, errors.Errorf("invalid store heartbeat command, but %v", request)
	}
	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb2.StoreHeartbeatResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if pberr := checkStore2(cluster, request.GetStats().GetStoreId()); pberr != nil {
		return &pdpb2.StoreHeartbeatResponse{
			Header: s.errorHeader(pberr),
		}, nil
	}

	stats := &pdpb.StoreStats{
		StoreId:            request.Stats.StoreId,
		Capacity:           request.Stats.Capacity,
		Available:          request.Stats.Available,
		RegionCount:        request.Stats.RegionCount,
		SendingSnapCount:   request.Stats.SendingSnapCount,
		ReceivingSnapCount: request.Stats.ReceivingSnapCount,
		StartTime:          request.Stats.StartTime,
		ApplyingSnapCount:  request.Stats.ApplyingSnapCount,
		IsBusy:             request.Stats.IsBusy,
	}

	err := cluster.cachedCluster.handleStoreHeartbeat(stats)
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb2.StoreHeartbeatResponse{
		Header: s.header(),
	}, nil
}

func toPDPB2PeerStats(stats []*pdpb2.PeerStats) []*pdpb.PeerStats {
	ret := make([]*pdpb.PeerStats, 0, len(stats))
	for _, s := range stats {
		n := &pdpb.PeerStats{
			Peer: s.Peer,
		}
		if s.DownSeconds != 0 {
			n.DownSeconds = proto.Uint64(s.DownSeconds)
		}
		ret = append(ret, n)
	}
	return ret
}

// RegionHeartbeat implements gRPC PDServer.
func (s *Server) RegionHeartbeat(ctx context.Context, request *pdpb2.RegionHeartbeatRequest) (*pdpb2.RegionHeartbeatResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb2.RegionHeartbeatResponse{Header: s.notBootstrappedHeader()}, nil
	}

	resp := &pdpb2.RegionHeartbeatResponse{}
	region := newRegionInfo(request.GetRegion(), request.GetLeader())
	region.DownPeers = toPDPB2PeerStats(request.GetDownPeers())
	region.PendingPeers = request.GetPendingPeers()
	if region.GetId() == 0 {
		pberr := &pdpb2.Error{
			Type:    pdpb2.ErrorType_UNKNOWN,
			Message: fmt.Sprintf("invalid request region, %v", request),
		}
		resp.Header = s.errorHeader(pberr)

		return resp, nil
	}
	if region.Leader == nil {
		pberr := &pdpb2.Error{
			Type:    pdpb2.ErrorType_UNKNOWN,
			Message: fmt.Sprintf("invalid request leader, %v", request),
		}
		resp.Header = s.errorHeader(pberr)

		return resp, nil
	}

	err := cluster.cachedCluster.handleRegionHeartbeat(region)
	if err != nil {
		pberr := &pdpb2.Error{
			Type:    pdpb2.ErrorType_UNKNOWN,
			Message: errors.Trace(err).Error(),
		}
		resp.Header = s.errorHeader(pberr)

		return resp, nil
	}

	res, err := cluster.handleRegionHeartbeat(region)
	if err != nil {
		pberr := &pdpb2.Error{
			Type:    pdpb2.ErrorType_UNKNOWN,
			Message: errors.Trace(err).Error(),
		}
		resp.Header = s.errorHeader(pberr)

		return resp, nil
	}

	resp.Header = s.header()
	if res != nil {
		if res.ChangePeer != nil {
			resp.ChangePeer = &pdpb2.ChangePeer{
				Peer:       res.ChangePeer.Peer,
				ChangeType: pdpb2.ConfChangeType(res.ChangePeer.GetChangeType()),
			}
		}
		if res.TransferLeader != nil {
			resp.TransferLeader = &pdpb2.TransferLeader{Peer: res.TransferLeader.Peer}
		}
	}

	return resp, nil
}

// GetRegion implements gRPC PDServer.
func (s *Server) GetRegion(ctx context.Context, request *pdpb2.GetRegionRequest) (*pdpb2.GetRegionResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb2.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}
	region, leader := cluster.getRegion(request.GetRegionKey())
	return &pdpb2.GetRegionResponse{
		Header: s.header(),
		Region: region,
		Leader: leader,
	}, nil
}

// GetRegionByID implements gRPC PDServer.
func (s *Server) GetRegionByID(ctx context.Context, request *pdpb2.GetRegionByIDRequest) (*pdpb2.GetRegionResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb2.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}
	id := request.GetRegionId()
	region, leader := cluster.GetRegionByID(id)
	return &pdpb2.GetRegionResponse{
		Header: s.header(),
		Region: region,
		Leader: leader,
	}, nil
}

// AskSplit implements gRPC PDServer.
func (s *Server) AskSplit(ctx context.Context, request *pdpb2.AskSplitRequest) (*pdpb2.AskSplitResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb2.AskSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	if request.GetRegion().GetStartKey() == nil {
		return nil, errors.New("missing region start key for split")
	}
	req := &pdpb.AskSplitRequest{
		Region: request.Region,
	}
	split, err := cluster.handleAskSplit(req)
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb2.AskSplitResponse{
		Header:      s.header(),
		NewRegionId: split.NewRegionId,
		NewPeerIds:  split.NewPeerIds,
	}, nil
}

// ReportSplit implements gRPC PDServer.
func (s *Server) ReportSplit(ctx context.Context, request *pdpb2.ReportSplitRequest) (*pdpb2.ReportSplitResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb2.ReportSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	req := &pdpb.ReportSplitRequest{
		Left:  request.Left,
		Right: request.Right,
	}
	_, err := cluster.handleReportSplit(req)
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb2.ReportSplitResponse{
		Header: s.header(),
	}, nil
}

// GetClusterConfig implements gRPC PDServer.
func (s *Server) GetClusterConfig(ctx context.Context, request *pdpb2.GetClusterConfigRequest) (*pdpb2.GetClusterConfigResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb2.GetClusterConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}
	return &pdpb2.GetClusterConfigResponse{
		Header:  s.header(),
		Cluster: cluster.GetConfig(),
	}, nil
}

// PutClusterConfig implements gRPC PDServer.
func (s *Server) PutClusterConfig(ctx context.Context, request *pdpb2.PutClusterConfigRequest) (*pdpb2.PutClusterConfigResponse, error) {
	if !s.IsLeader() {
		return nil, notLeaderError
	}
	if err := s.validateHeader(request.GetHeader()); err != nil {
		return nil, errors.Trace(err)
	}

	cluster := s.GetRaftCluster()
	if cluster == nil {
		return &pdpb2.PutClusterConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}
	conf := request.GetCluster()
	if err := cluster.putConfig(conf); err != nil {
		return nil, grpc.Errorf(codes.Unknown, err.Error())
	}

	log.Infof("put cluster config ok - %v", conf)

	return &pdpb2.PutClusterConfigResponse{
		Header: s.header(),
	}, nil
}

func (s *Server) validateHeader(header *pdpb2.RequestHeader) error {
	if header.GetClusterId() != s.clusterID {
		return grpc.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, header.GetClusterId())
	}
	return nil
}

func (s *Server) header() *pdpb2.ResponseHeader {
	return &pdpb2.ResponseHeader{ClusterId: s.clusterID}
}

func (s *Server) errorHeader(err *pdpb2.Error) *pdpb2.ResponseHeader {
	return &pdpb2.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

func (s *Server) notBootstrappedHeader() *pdpb2.ResponseHeader {
	return s.errorHeader(&pdpb2.Error{
		Type:    pdpb2.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}
