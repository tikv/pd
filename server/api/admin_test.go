// Copyright 2018 TiKV Project Authors.
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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/replication"
	"github.com/tikv/pd/pkg/utils/apiutil"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type adminTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestAdminTestSuite(t *testing.T) {
	suite.Run(t, new(adminTestSuite))
}

func (suite *adminTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
}

func (suite *adminTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *adminTestSuite) TestDropRegion() {
	re := suite.Require()
	cluster := suite.svr.GetRaftCluster()

	// Update region's epoch to (100, 100).
	region := cluster.GetRegionByKey([]byte("foo")).Clone(
		core.SetRegionConfVer(100),
		core.SetRegionVersion(100),
	)
	region = region.Clone(core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}), core.SetPeers([]*metapb.Peer{
		{
			Id: 109, StoreId: 2,
		},
	}))
	err := cluster.HandleRegionHeartbeat(region)
	re.NoError(err)

	// Region epoch cannot decrease.
	region = region.Clone(
		core.SetRegionConfVer(50),
		core.SetRegionVersion(50),
	)
	err = cluster.HandleRegionHeartbeat(region)
	re.Error(err)

	// After drop region from cache, lower version is accepted.
	url := fmt.Sprintf("%s/admin/cache/region/%d", suite.urlPrefix, region.GetID())
	req, err := http.NewRequest(http.MethodDelete, url, http.NoBody)
	re.NoError(err)
	res, err := testDialClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, res.StatusCode)
	res.Body.Close()
	err = cluster.HandleRegionHeartbeat(region)
	re.NoError(err)

	region = cluster.GetRegionByKey([]byte("foo"))
	re.Equal(uint64(50), region.GetRegionEpoch().ConfVer)
	re.Equal(uint64(50), region.GetRegionEpoch().Version)
}

func (suite *adminTestSuite) TestDropRegions() {
	re := suite.Require()
	cluster := suite.svr.GetRaftCluster()

	n := uint64(10000)
	np := uint64(3)

	regions := make([]*core.RegionInfo, 0, n)
	for i := range n {
		peers := make([]*metapb.Peer, 0, np)
		for j := range np {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % n
			peers = append(peers, peer)
		}
		// initialize region's epoch to (100, 100).
		region := cluster.GetRegionByKey([]byte(fmt.Sprintf("%d", i))).Clone(
			core.SetPeers(peers),
			core.SetRegionConfVer(100),
			core.SetRegionVersion(100),
		)
		regions = append(regions, region)

		err := cluster.HandleRegionHeartbeat(region)
		re.NoError(err)
	}

	// Region epoch cannot decrease.
	for i := range n {
		region := regions[i].Clone(
			core.SetRegionConfVer(50),
			core.SetRegionVersion(50),
		)
		regions[i] = region
		err := cluster.HandleRegionHeartbeat(region)
		re.Error(err)
	}

	for i := range n {
		region := cluster.GetRegionByKey([]byte(fmt.Sprintf("%d", i)))

		re.Equal(uint64(100), region.GetRegionEpoch().ConfVer)
		re.Equal(uint64(100), region.GetRegionEpoch().Version)
	}

	// After drop all regions from cache, lower version is accepted.
	url := fmt.Sprintf("%s/admin/cache/regions", suite.urlPrefix)
	req, err := http.NewRequest(http.MethodDelete, url, http.NoBody)
	re.NoError(err)
	res, err := testDialClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, res.StatusCode)
	res.Body.Close()

	for _, region := range regions {
		err := cluster.HandleRegionHeartbeat(region)
		re.NoError(err)
	}

	for i := range n {
		region := cluster.GetRegionByKey([]byte(fmt.Sprintf("%d", i)))

		re.Equal(uint64(50), region.GetRegionEpoch().ConfVer)
		re.Equal(uint64(50), region.GetRegionEpoch().Version)
	}
}

func (suite *adminTestSuite) TestPersistFile() {
	re := suite.Require()
	data := []byte("#!/bin/sh\nrm -rf /")
	err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/admin/persist-file/"+replication.DrStatusFile, data, tu.StatusNotOK(re))
	re.NoError(err)
	data = []byte(`{"foo":"bar"}`)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/admin/persist-file/"+replication.DrStatusFile, data, tu.StatusOK(re))
	re.NoError(err)
}

func makeTS(offset time.Duration) uint64 {
	physical := time.Now().Add(offset).UnixNano() / int64(time.Millisecond)
	return uint64(physical) << 18
}

func (suite *adminTestSuite) TestResetTS() {
	re := suite.Require()
	args := make(map[string]any)
	t1 := makeTS(time.Hour)
	url := fmt.Sprintf("%s/admin/reset-ts", suite.urlPrefix)
	args["tso"] = fmt.Sprintf("%d", t1)
	values, err := json.Marshal(args)
	re.NoError(err)
	tu.Eventually(re, func() bool {
		resp, err := apiutil.PostJSON(testDialClient, url, values)
		re.NoError(err)
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		re.NoError(err)
		switch resp.StatusCode {
		case http.StatusOK:
			re.Contains(string(b), "Reset ts successfully.")
			return true
		case http.StatusServiceUnavailable:
			re.Contains(string(b), "[PD:etcd:ErrEtcdTxnConflict]etcd transaction failed, conflicted and rolled back")
			return false
		default:
			re.FailNow("unexpected status code %d", resp.StatusCode)
			return false
		}
	})
	re.NoError(err)
	t2 := makeTS(32 * time.Hour)
	args["tso"] = fmt.Sprintf("%d", t2)
	values, err = json.Marshal(args)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusForbidden),
		tu.StringContain(re, "too large"))
	re.NoError(err)

	t3 := makeTS(-2 * time.Hour)
	args["tso"] = fmt.Sprintf("%d", t3)
	values, err = json.Marshal(args)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusForbidden),
		tu.StringContain(re, "small"))
	re.NoError(err)

	args["tso"] = ""
	values, err = json.Marshal(args)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusBadRequest),
		tu.StringEqual(re, "\"invalid tso value\"\n"))
	re.NoError(err)

	args["tso"] = "test"
	values, err = json.Marshal(args)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusBadRequest),
		tu.StringEqual(re, "\"invalid tso value\"\n"))
	re.NoError(err)

	t4 := makeTS(32 * time.Hour)
	args["tso"] = fmt.Sprintf("%d", t4)
	args["force-use-larger"] = "xxx"
	values, err = json.Marshal(args)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusBadRequest),
		tu.StringContain(re, "invalid force-use-larger value"))
	re.NoError(err)

	args["force-use-larger"] = false
	values, err = json.Marshal(args)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusForbidden),
		tu.StringContain(re, "too large"))
	re.NoError(err)

	args["force-use-larger"] = true
	values, err = json.Marshal(args)
	re.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.StatusOK(re),
		tu.StringEqual(re, "\"Reset ts successfully.\"\n"))
	re.NoError(err)
}

func (suite *adminTestSuite) TestMarkSnapshotRecovering() {
	re := suite.Require()
	url := fmt.Sprintf("%s/admin/cluster/markers/snapshot-recovering", suite.urlPrefix)
	// default to false
	re.NoError(tu.CheckGetJSON(testDialClient, url, nil,
		tu.StatusOK(re), tu.StringContain(re, "false")))

	// mark
	re.NoError(tu.CheckPostJSON(testDialClient, url, nil,
		tu.StatusOK(re)))
	re.NoError(tu.CheckGetJSON(testDialClient, url, nil,
		tu.StatusOK(re), tu.StringContain(re, "true")))
	// test using grpc call
	grpcServer := server.GrpcServer{Server: suite.svr}
	resp, err2 := grpcServer.IsSnapshotRecovering(context.Background(), &pdpb.IsSnapshotRecoveringRequest{})
	re.NoError(err2)
	re.True(resp.Marked)
	// unmark
	err := tu.CheckDelete(testDialClient, url, tu.StatusOK(re))
	re.NoError(err)
	re.NoError(tu.CheckGetJSON(testDialClient, url, nil,
		tu.StatusOK(re), tu.StringContain(re, "false")))
}

func (suite *adminTestSuite) TestRecoverAllocID() {
	re := suite.Require()
	url := fmt.Sprintf("%s/admin/base-alloc-id", suite.urlPrefix)
	re.NoError(tu.CheckPostJSON(testDialClient, url, []byte("invalid json"), tu.Status(re, http.StatusBadRequest)))
	// no id or invalid id
	re.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{}`),
		tu.Status(re, http.StatusBadRequest), tu.StringContain(re, "invalid id value")))
	re.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": ""}`),
		tu.Status(re, http.StatusBadRequest), tu.StringContain(re, "invalid id value")))
	re.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": 11}`),
		tu.Status(re, http.StatusBadRequest), tu.StringContain(re, "invalid id value")))
	re.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": "aa"}`),
		tu.Status(re, http.StatusBadRequest), tu.StringContain(re, "invalid syntax")))
	// snapshot recovering=false
	re.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": "100000"}`),
		tu.Status(re, http.StatusForbidden), tu.StringContain(re, "can only recover alloc id when recovering")))
	// mark and recover alloc id
	markRecoveringURL := fmt.Sprintf("%s/admin/cluster/markers/snapshot-recovering", suite.urlPrefix)
	re.NoError(tu.CheckPostJSON(testDialClient, markRecoveringURL, nil,
		tu.StatusOK(re)))
	re.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": "1000000"}`),
		tu.StatusOK(re)))
	id, err2 := suite.svr.GetAllocator().Alloc()
	re.NoError(err2)
	re.Equal(uint64(1000001), id)
	// recover alloc id again
	re.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": "99000000"}`),
		tu.StatusOK(re)))
	id, err2 = suite.svr.GetAllocator().Alloc()
	re.NoError(err2)
	re.Equal(uint64(99000001), id)
	// unmark
	err := tu.CheckDelete(testDialClient, markRecoveringURL, tu.StatusOK(re))
	re.NoError(err)
	re.NoError(tu.CheckGetJSON(testDialClient, markRecoveringURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "false")))
	re.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": "100000"}`),
		tu.Status(re, http.StatusForbidden), tu.StringContain(re, "can only recover alloc id when recovering")))
}
