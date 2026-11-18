// Copyright 2016 TiKV Project Authors.
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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestHandleAskBatchSplitRejectsExpiredAffinity(t *testing.T) {
	re := require.New(t)
	cluster, _ := newSplitScatterTestCluster(t)
	cluster.opt.SetAffinityScheduleLimit(4)
	var err error
	cluster.affinityManager, err = affinity.NewManager(t.Context(), cluster.storage, cluster, cluster.opt, cluster.regionLabeler)
	re.NoError(err)
	cluster.hbstreams = cluster.GetCoordinator().GetHeartbeatStreams()
	region := cluster.GetRegion(100)
	re.NoError(cluster.affinityManager.CreateAffinityGroups([]affinity.GroupKeyRanges{{
		GroupID: "g1", KeyRanges: []keyutil.KeyRange{{StartKey: region.GetStartKey(), EndKey: region.GetEndKey()}},
	}}))
	_, err = cluster.affinityManager.UpdateAffinityGroupPeers("g1", 1, []uint64{1, 2, 3})
	re.NoError(err)
	// The group keeps its target after expiration, so the split is still rejected.
	cluster.affinityManager.ExpireAffinityGroup("g1")
	_, err = cluster.HandleAskBatchSplit(&pdpb.AskBatchSplitRequest{
		Region: region.GetMeta(), SplitCount: 1, Reason: pdpb.SplitReason_LOAD,
	})
	re.ErrorContains(err, "cannot split affinity region")
	re.Equal(1, cluster.hbstreams.MsgLength())
}

func TestReportSplit(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	left := &metapb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	right := &metapb.Region{Id: 2, StartKey: []byte("b"), EndKey: []byte("c")}
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: left, Right: right})
	re.NoError(err)
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: right, Right: left})
	re.Error(err)
}

func TestReportBatchSplit(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	regions := []*metapb.Region{
		{Id: 1, StartKey: []byte(""), EndKey: []byte("a")},
		{Id: 2, StartKey: []byte("a"), EndKey: []byte("b")},
		{Id: 3, StartKey: []byte("b"), EndKey: []byte("c")},
		{Id: 3, StartKey: []byte("c"), EndKey: []byte("")},
	}
	_, err = cluster.HandleBatchReportSplit(&pdpb.ReportBatchSplitRequest{Regions: regions})
	re.NoError(err)
}
