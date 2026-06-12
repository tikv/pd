// Copyright 2026 TiKV Project Authors.
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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestNewSchedulingAskBatchSplitRequestPreservesReason(t *testing.T) {
	re := require.New(t)
	req := newSchedulingAskBatchSplitRequest(&pdpb.AskBatchSplitRequest{
		Header:     &pdpb.RequestHeader{ClusterId: 1, SenderId: 2},
		Region:     &metapb.Region{Id: 100},
		SplitCount: 3,
		Reason:     pdpb.SplitReason_LOAD,
	})
	re.Equal(uint64(1), req.GetHeader().GetClusterId())
	re.Equal(uint64(2), req.GetHeader().GetSenderId())
	re.Equal(uint64(100), req.GetRegion().GetId())
	re.Equal(uint32(3), req.GetSplitCount())
	re.Equal(pdpb.SplitReason_LOAD, req.GetReason())
}

func TestRegionReadRaftClusterGate(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	cfg := config.NewConfig()
	serviceMiddlewareCfg := config.NewServiceMiddlewareConfig()
	serviceMiddlewareCfg.GRPCRateLimitConfig.EnableRateLimit = false
	svr := &Server{
		cfg:                             cfg,
		persistOptions:                  config.NewPersistOptions(cfg),
		serviceMiddlewareCfg:            serviceMiddlewareCfg,
		serviceMiddlewarePersistOptions: config.NewServiceMiddlewarePersistOptions(serviceMiddlewareCfg),
		member:                          &member.Member{},
		ctx:                             ctx,
	}
	atomic.StoreInt64(&svr.isRunning, 1)

	basicCluster := core.NewBasicCluster()
	region := core.NewTestRegionInfo(1, 1, []byte("a"), []byte("z"))
	basicCluster.PutRegion(region)
	svr.cluster = cluster.NewRaftCluster(
		ctx,
		svr.GetMember(),
		basicCluster,
		storage.NewStorageWithMemoryBackend(),
		nil,
		nil,
		nil,
		nil,
	)

	// The root index may already contain the region while startup is still
	// loading. Region read APIs must wait for the explicit region-read gate
	// instead of returning a false empty/not-found result.
	directResp, err := grpcutil.GetRegionByID(basicCluster, &pdpb.GetRegionByIDRequest{
		RegionId: region.GetID(),
	}, false)
	re.NoError(err)
	re.Equal(region.GetID(), directResp.GetRegion().GetId())

	re.Nil(svr.GetRegionReadRaftCluster())
	grpcServer := &GrpcServer{Server: svr}
	rc, header := grpcServer.getRaftCluster(false)
	re.Nil(rc)
	re.Equal(pdpb.ErrorType_NOT_BOOTSTRAPPED, header.GetError().GetType())
}

func TestConvertSchedulingHeaderPreservesError(t *testing.T) {
	testCases := []struct {
		name string
		in   *schedulingpb.Error
		want *pdpb.Error
	}{
		{
			name: "ok",
		},
		{
			name: "not bootstrapped",
			in:   &schedulingpb.Error{Type: schedulingpb.ErrorType_NOT_BOOTSTRAPPED, Message: "cluster is not initialized"},
			want: &pdpb.Error{Type: pdpb.ErrorType_NOT_BOOTSTRAPPED, Message: "cluster is not initialized"},
		},
		{
			name: "already bootstrapped",
			in:   &schedulingpb.Error{Type: schedulingpb.ErrorType_ALREADY_BOOTSTRAPPED, Message: "cluster is already bootstrapped"},
			want: &pdpb.Error{Type: pdpb.ErrorType_ALREADY_BOOTSTRAPPED, Message: "cluster is already bootstrapped"},
		},
		{
			name: "invalid value",
			in:   &schedulingpb.Error{Type: schedulingpb.ErrorType_INVALID_VALUE, Message: "bad request"},
			want: &pdpb.Error{Type: pdpb.ErrorType_INVALID_VALUE, Message: "bad request"},
		},
		{
			name: "region not found",
			in:   &schedulingpb.Error{Type: schedulingpb.ErrorType_UNKNOWN, Message: "region not found"},
			want: &pdpb.Error{Type: pdpb.ErrorType_REGION_NOT_FOUND, Message: "region not found"},
		},
		{
			name: "unknown",
			in:   &schedulingpb.Error{Type: schedulingpb.ErrorType_CLUSTER_MISMATCHED, Message: "cluster mismatch"},
			want: &pdpb.Error{Type: pdpb.ErrorType_UNKNOWN, Message: "cluster mismatch"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			header := convertHeader(&schedulingpb.ResponseHeader{
				ClusterId: 1,
				Error:     testCase.in,
			})
			re.Equal(uint64(1), header.GetClusterId())
			re.Equal(testCase.want, header.GetError())
		})
	}
}
