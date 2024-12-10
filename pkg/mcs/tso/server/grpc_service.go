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
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gRPC errors
var (
	ErrNotStarted        = status.Errorf(codes.Unavailable, "server not started")
	ErrClusterMismatched = status.Errorf(codes.Unavailable, "cluster mismatched")
)

var _ tsopb.TSOServer = (*Service)(nil)

// SetUpRestHandler is a hook to sets up the REST service.
var SetUpRestHandler = func(*Service) (http.Handler, apiutil.APIServiceGroup) {
	return dummyRestService{}, apiutil.APIServiceGroup{}
}

type dummyRestService struct{}

func (dummyRestService) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("not implemented"))
}

// ConfigProvider is used to get tso config from the given
// `bs.server` without modifying its interface.
type ConfigProvider any

// Service is the TSO grpc service.
type Service struct {
	*Server
}

// NewService creates a new TSO service.
func NewService[T ConfigProvider](svr bs.Server) registry.RegistrableService {
	server, ok := svr.(*Server)
	if !ok {
		log.Fatal("create tso server failed")
	}
	return &Service{
		Server: server,
	}
}

// RegisterGRPCService registers the service to gRPC server.
func (s *Service) RegisterGRPCService(g *grpc.Server) {
	tsopb.RegisterTSOServer(g, s)
}

// RegisterRESTHandler registers the service to REST server.
func (s *Service) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) error {
	handler, group := SetUpRestHandler(s)
	return apiutil.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}

// Tso returns a stream of timestamps
func (s *Service) Tso(stream tsopb.TSO_TsoServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		start := time.Now()
		// TSO uses leader lease to determine validity. No need to check leader here.
		if s.IsClosed() {
			return status.Errorf(codes.Unknown, "server not started")
		}
		header := request.GetHeader()
		clusterID := header.GetClusterId()
		if clusterID != keypath.ClusterID() {
			return status.Errorf(
				codes.FailedPrecondition, "mismatch cluster id, need %d but got %d",
				keypath.ClusterID(), clusterID)
		}
		keyspaceID := header.GetKeyspaceId()
		keyspaceGroupID := header.GetKeyspaceGroupId()
		count := request.GetCount()
		ts, keyspaceGroupBelongTo, err := s.keyspaceGroupManager.HandleTSORequest(
			ctx,
			keyspaceID, keyspaceGroupID,
			count)
		if err != nil {
			return status.Error(codes.Unknown, err.Error())
		}
		keyspaceGroupIDStr := strconv.FormatUint(uint64(keyspaceGroupID), 10)
		tsoHandleDuration.WithLabelValues(keyspaceGroupIDStr).Observe(time.Since(start).Seconds())
		response := &tsopb.TsoResponse{
			Header:    wrapHeader(keyspaceGroupBelongTo),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

// FindGroupByKeyspaceID returns the keyspace group that the keyspace belongs to.
func (s *Service) FindGroupByKeyspaceID(
	_ context.Context, request *tsopb.FindGroupByKeyspaceIDRequest,
) (*tsopb.FindGroupByKeyspaceIDResponse, error) {
	respKeyspaceGroup := request.GetHeader().GetKeyspaceGroupId()
	if errorType, err := s.validRequest(request.GetHeader()); err != nil {
		return &tsopb.FindGroupByKeyspaceIDResponse{
			Header: wrapErrorToHeader(errorType, err.Error(), respKeyspaceGroup),
		}, nil
	}

	keyspaceID := request.GetKeyspaceId()
	am, keyspaceGroup, keyspaceGroupID, err := s.keyspaceGroupManager.FindGroupByKeyspaceID(keyspaceID)
	if err != nil {
		return &tsopb.FindGroupByKeyspaceIDResponse{
			Header: wrapErrorToHeader(tsopb.ErrorType_UNKNOWN, err.Error(), keyspaceGroupID),
		}, nil
	}
	if keyspaceGroup == nil {
		return &tsopb.FindGroupByKeyspaceIDResponse{
			Header: wrapErrorToHeader(
				tsopb.ErrorType_UNKNOWN, "keyspace group not found", keyspaceGroupID),
		}, nil
	}

	members := make([]*tsopb.KeyspaceGroupMember, 0, len(keyspaceGroup.Members))
	for _, member := range keyspaceGroup.Members {
		members = append(members, &tsopb.KeyspaceGroupMember{
			Address: member.Address,
			// TODO: watch the keyspace groups' primary serving address changes
			// to get the latest primary serving addresses of all keyspace groups.
			IsPrimary: member.IsAddressEquivalent(am.GetLeaderAddr()),
		})
	}

	var splitState *tsopb.SplitState
	if keyspaceGroup.SplitState != nil {
		splitState = &tsopb.SplitState{
			SplitSource: keyspaceGroup.SplitState.SplitSource,
		}
	}

	return &tsopb.FindGroupByKeyspaceIDResponse{
		Header: wrapHeader(keyspaceGroupID),
		KeyspaceGroup: &tsopb.KeyspaceGroup{
			Id:         keyspaceGroupID,
			UserKind:   keyspaceGroup.UserKind,
			SplitState: splitState,
			Members:    members,
		},
	}, nil
}

// GetMinTS gets the minimum timestamp across all keyspace groups served by the TSO server
// who receives and handles the request.
func (s *Service) GetMinTS(
	_ context.Context, request *tsopb.GetMinTSRequest,
) (*tsopb.GetMinTSResponse, error) {
	respKeyspaceGroup := request.GetHeader().GetKeyspaceGroupId()
	if errorType, err := s.validRequest(request.GetHeader()); err != nil {
		return &tsopb.GetMinTSResponse{
			Header: wrapErrorToHeader(errorType, err.Error(), respKeyspaceGroup),
		}, nil
	}

	minTS, kgAskedCount, kgTotalCount, err := s.keyspaceGroupManager.GetMinTS()
	if err != nil {
		return &tsopb.GetMinTSResponse{
			Header: wrapErrorToHeader(
				tsopb.ErrorType_UNKNOWN, err.Error(), respKeyspaceGroup),
			Timestamp:             &minTS,
			KeyspaceGroupsServing: kgAskedCount,
			KeyspaceGroupsTotal:   kgTotalCount,
		}, nil
	}

	return &tsopb.GetMinTSResponse{
		Header:                wrapHeader(respKeyspaceGroup),
		Timestamp:             &minTS,
		KeyspaceGroupsServing: kgAskedCount,
		KeyspaceGroupsTotal:   kgTotalCount,
	}, nil
}

func (s *Service) validRequest(header *tsopb.RequestHeader) (tsopb.ErrorType, error) {
	if s.IsClosed() || s.keyspaceGroupManager == nil {
		return tsopb.ErrorType_NOT_BOOTSTRAPPED, ErrNotStarted
	}
	if header == nil || header.GetClusterId() != keypath.ClusterID() {
		return tsopb.ErrorType_CLUSTER_MISMATCHED, ErrClusterMismatched
	}
	return tsopb.ErrorType_OK, nil
}

func wrapHeader(keyspaceGroupBelongTo uint32) *tsopb.ResponseHeader {
	if keypath.ClusterID() == 0 {
		return wrapErrorToHeader(
			tsopb.ErrorType_NOT_BOOTSTRAPPED, "cluster id is not ready", keyspaceGroupBelongTo)
	}
	return &tsopb.ResponseHeader{ClusterId: keypath.ClusterID(), KeyspaceGroupId: keyspaceGroupBelongTo}
}

func wrapErrorToHeader(
	errorType tsopb.ErrorType, message string, keyspaceGroupBelongTo uint32,
) *tsopb.ResponseHeader {
	return errorHeader(&tsopb.Error{Type: errorType, Message: message}, keyspaceGroupBelongTo)
}

func errorHeader(err *tsopb.Error, keyspaceGroupBelongTo uint32) *tsopb.ResponseHeader {
	return &tsopb.ResponseHeader{
		ClusterId:       keypath.ClusterID(),
		Error:           err,
		KeyspaceGroupId: keyspaceGroupBelongTo,
	}
}
