// Copyright 2019 TiKV Project Authors.
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

package pdctl

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/tests"
)

// ExecuteCommand is used for test purpose.
func ExecuteCommand(root *cobra.Command, args ...string) (output []byte, err error) {
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetArgs(args)
	err = root.Execute()
	return buf.Bytes(), err
}

// CheckStoresInfo is used to check the test results.
// CheckStoresInfo will not check Store.State because this field has been omitted pd-ctl output
func CheckStoresInfo(re *require.Assertions, stores []*api.StoreInfo, want []*api.StoreInfo) {
	re.Len(stores, len(want))
	mapWant := make(map[uint64]*api.StoreInfo)
	for _, s := range want {
		if _, ok := mapWant[s.Store.Id]; !ok {
			mapWant[s.Store.Id] = s
		}
	}
	for _, s := range stores {
		obtained := typeutil.DeepClone(s.Store.Store, core.StoreFactory)
		expected := typeutil.DeepClone(mapWant[obtained.Id].Store.Store, core.StoreFactory)
		// Ignore state
		obtained.State, expected.State = 0, 0
		obtained.NodeState, expected.NodeState = 0, 0
		// Ignore lastHeartbeat
		obtained.LastHeartbeat, expected.LastHeartbeat = 0, 0
		re.Equal(expected, obtained)

		obtainedStateName := s.Store.StateName
		expectedStateName := mapWant[obtained.Id].Store.StateName
		re.Equal(expectedStateName, obtainedStateName)
	}
}

// CheckRegionInfo is used to check the test results.
func CheckRegionInfo(re *require.Assertions, output *api.RegionInfo, expected *core.RegionInfo) {
	region := api.NewAPIRegionInfo(expected)
	output.Adjust()
	re.Equal(region, output)
}

// CheckRegionsInfo is used to check the test results.
func CheckRegionsInfo(re *require.Assertions, output *api.RegionsInfo, expected []*core.RegionInfo) {
	re.Len(expected, output.Count)
	got := output.Regions
	sort.Slice(got, func(i, j int) bool {
		return got[i].ID < got[j].ID
	})
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].GetID() < expected[j].GetID()
	})
	for i, region := range expected {
		CheckRegionInfo(re, &got[i], region)
	}
}

// MustPutStore is used for test purpose.
func MustPutStore(re *require.Assertions, svr *server.Server, store *metapb.Store) {
	store.Address = fmt.Sprintf("tikv%d", store.GetId())
	if len(store.Version) == 0 {
		store.Version = versioninfo.MinSupportedVersion(versioninfo.Version2_0).String()
	}
	grpcServer := &server.GrpcServer{Server: svr}
	_, err := grpcServer.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
		Store:  store,
	})
	re.NoError(err)

	storeInfo := grpcServer.GetRaftCluster().GetStore(store.GetId())
	newStore := storeInfo.Clone(core.SetStoreStats(&pdpb.StoreStats{
		Capacity:  uint64(10 * units.GiB),
		UsedSize:  uint64(9 * units.GiB),
		Available: uint64(1 * units.GiB),
	}))
	grpcServer.GetRaftCluster().GetBasicCluster().PutStore(newStore)
}

// MustPutRegion is used for test purpose.
func MustPutRegion(re *require.Assertions, cluster *tests.TestCluster, regionID, storeID uint64, start, end []byte, opts ...core.RegionCreateOption) *core.RegionInfo {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: storeID,
	}
	metaRegion := &metapb.Region{
		Id:          regionID,
		StartKey:    start,
		EndKey:      end,
		Peers:       []*metapb.Peer{leader},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	opts = append(opts, core.SetSource(core.Heartbeat))
	r := core.NewRegionInfo(metaRegion, leader, opts...)
	err := cluster.HandleRegionHeartbeat(r)
	re.NoError(err)
	return r
}
