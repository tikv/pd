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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/assertutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/versioninfo"
	"github.com/tikv/pd/tests"
)

// ExecuteCommand is used for test purpose.
func ExecuteCommand(root *cobra.Command, args ...string) (output []byte, err error) {
	buf := new(bytes.Buffer)
	root.SetOutput(buf)
	root.SetArgs(args)
	err = root.Execute()
	return buf.Bytes(), err
}

// CheckStoresInfo is used to check the test results.
// CheckStoresInfo will not check Store.State because this feild has been omitted pdctl output
func CheckStoresInfo(c *check.C, stores []*api.StoreInfo, want []*api.StoreInfo) {
	c.Assert(len(stores), check.Equals, len(want))
	mapWant := make(map[uint64]*api.StoreInfo)
	for _, s := range want {
		if _, ok := mapWant[s.Store.Id]; !ok {
			mapWant[s.Store.Id] = s
		}
	}
	for _, s := range stores {
		obtained := proto.Clone(s.Store.Store).(*metapb.Store)
		expected := proto.Clone(mapWant[obtained.Id].Store.Store).(*metapb.Store)
		// Ignore state
		obtained.State, expected.State = 0, 0
		obtained.NodeState, expected.NodeState = 0, 0
		// Ignore lastHeartbeat
		obtained.LastHeartbeat, expected.LastHeartbeat = 0, 0
		c.Assert(obtained, check.DeepEquals, expected)

		obtainedStateName := s.Store.StateName
		expectedStateName := mapWant[obtained.Id].Store.StateName
		c.Assert(obtainedStateName, check.Equals, expectedStateName)
	}
}

// CheckRegionInfo is used to check the test results.
func CheckRegionInfo(c *check.C, output *api.RegionInfo, expected *core.RegionInfo) {
	region := api.NewRegionInfo(expected)
	output.Adjust()
	c.Assert(output, check.DeepEquals, region)
}

// CheckRegionsInfo is used to check the test results.
func CheckRegionsInfo(c *check.C, output *api.RegionsInfo, expected []*core.RegionInfo) {
	c.Assert(output.Count, check.Equals, len(expected))
	got := output.Regions
	sort.Slice(got, func(i, j int) bool {
		return got[i].ID < got[j].ID
	})
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].GetID() < expected[j].GetID()
	})
	for i, region := range expected {
		CheckRegionInfo(c, &got[i], region)
	}
}

// MustPutStore is used for test purpose.
func MustPutStore(c *check.C, svr *server.Server, store *metapb.Store) {
	store.Address = fmt.Sprintf("tikv%d", store.GetId())
	if len(store.Version) == 0 {
		store.Version = versioninfo.MinSupportedVersion(versioninfo.Version2_0).String()
	}
	grpcServer := &server.GrpcServer{Server: svr}
	_, err := grpcServer.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
		Store:  store,
	})
	c.Assert(err, check.IsNil)
}

// MustPutRegion is used for test purpose.
func MustPutRegion(c *check.C, cluster *tests.TestCluster, regionID, storeID uint64, start, end []byte, opts ...core.RegionCreateOption) *core.RegionInfo {
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
	r := core.NewRegionInfo(metaRegion, leader, opts...)
	err := cluster.HandleRegionHeartbeat(r)
	c.Assert(err, check.IsNil)
	return r
}

func checkerWithNilAssert(c *check.C) *assertutil.Checker {
	checker := assertutil.NewChecker(c.FailNow)
	checker.IsNil = func(obtained interface{}) {
		c.Assert(obtained, check.IsNil)
	}
	return checker
}
