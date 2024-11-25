// Copyright 2024 TiKV Project Authors.
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
	"runtime"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"google.golang.org/grpc"
)

type request interface {
	GetHeader() *pdpb.RequestHeader
}

type forwardFn func(ctx context.Context, client *grpc.ClientConn, request any) (any, error)

var forwardFns = map[string]forwardFn{
	"GetMinTS": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).GetMinTS(ctx, request.(*pdpb.GetMinTSRequest))
	},
	"Bootstrap": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).Bootstrap(ctx, request.(*pdpb.BootstrapRequest))
	},
	"IsBootstrapped": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).IsBootstrapped(ctx, request.(*pdpb.IsBootstrappedRequest))
	},
	"AllocID": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).AllocID(ctx, request.(*pdpb.AllocIDRequest))
	},
	"GetStore": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).GetStore(ctx, request.(*pdpb.GetStoreRequest))
	},
	"PutStore": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).PutStore(ctx, request.(*pdpb.PutStoreRequest))
	},
	"GetAllStores": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).GetAllStores(ctx, request.(*pdpb.GetAllStoresRequest))
	},
	"StoreHeartbeat": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).StoreHeartbeat(ctx, request.(*pdpb.StoreHeartbeatRequest))
	},
	"AskSplit": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).AskSplit(ctx, request.(*pdpb.AskSplitRequest))
	},
	"AskBatchSplit": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).AskBatchSplit(ctx, request.(*pdpb.AskBatchSplitRequest))
	},
	"ReportSplit": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).ReportSplit(ctx, request.(*pdpb.ReportSplitRequest))
	},
	"ReportBatchSplit": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).ReportBatchSplit(ctx, request.(*pdpb.ReportBatchSplitRequest))
	},
	"GetClusterConfig": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).GetClusterConfig(ctx, request.(*pdpb.GetClusterConfigRequest))
	},
	"PutClusterConfig": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).PutClusterConfig(ctx, request.(*pdpb.PutClusterConfigRequest))
	},
	"ScatterRegion": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).ScatterRegion(ctx, request.(*pdpb.ScatterRegionRequest))
	},
	"GetGCSafePoint": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).GetGCSafePoint(ctx, request.(*pdpb.GetGCSafePointRequest))
	},
	"UpdateGCSafePoint": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).UpdateGCSafePoint(ctx, request.(*pdpb.UpdateGCSafePointRequest))
	},
	"UpdateServiceGCSafePoint": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).UpdateServiceGCSafePoint(ctx, request.(*pdpb.UpdateServiceGCSafePointRequest))
	},
	"GetOperator": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).GetOperator(ctx, request.(*pdpb.GetOperatorRequest))
	},
	"SplitRegions": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).SplitRegions(ctx, request.(*pdpb.SplitRegionsRequest))
	},
	"SplitAndScatterRegions": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).SplitAndScatterRegions(ctx, request.(*pdpb.SplitAndScatterRegionsRequest))
	},
	"ReportMinResolvedTS": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).ReportMinResolvedTS(ctx, request.(*pdpb.ReportMinResolvedTsRequest))
	},
	"SetExternalTimestamp": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).SetExternalTimestamp(ctx, request.(*pdpb.SetExternalTimestampRequest))
	},
	"GetExternalTimestamp": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).GetExternalTimestamp(ctx, request.(*pdpb.GetExternalTimestampRequest))
	},

	"GetGCSafePointV2": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).GetGCSafePointV2(ctx, request.(*pdpb.GetGCSafePointV2Request))
	},
	"UpdateGCSafePointV2": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).UpdateGCSafePointV2(ctx, request.(*pdpb.UpdateGCSafePointV2Request))
	},
	"UpdateServiceSafePointV2": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).UpdateServiceSafePointV2(ctx, request.(*pdpb.UpdateServiceSafePointV2Request))
	},
	"GetAllGCSafePointV2": func(ctx context.Context, client *grpc.ClientConn, request any) (any, error) {
		return pdpb.NewPDClient(client).GetAllGCSafePointV2(ctx, request.(*pdpb.GetAllGCSafePointV2Request))
	},
}

func (s *GrpcServer) unaryMiddleware(ctx context.Context, req request, methodName string) (rsp any, err error) {
	return s.unaryFollowerMiddleware(ctx, req, forwardFns[methodName], nil)
}

// unaryFollowerMiddleware adds the check of followers enable compared to unaryMiddleware.
func (s *GrpcServer) unaryFollowerMiddleware(ctx context.Context, req request, fn forwardFn, allowFollower *bool) (rsp any, err error) {
	failpoint.Inject("customTimeout", func() {
		time.Sleep(5 * time.Second)
	})
	forwardedHost := grpcutil.GetForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return fn(ctx, client, req)
	}
	if err := s.validateRoleInRequest(ctx, req.GetHeader(), allowFollower); err != nil {
		return nil, err
	}
	return nil, nil
}

func currentFunction() string {
	counter, _, _, _ := runtime.Caller(1)
	s := strings.Split(runtime.FuncForPC(counter).Name(), ".")
	return s[len(s)-1]
}
