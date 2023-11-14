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
	suite.NoError(err)

	// Region epoch cannot decrease.
	region = region.Clone(
		core.SetRegionConfVer(50),
		core.SetRegionVersion(50),
	)
	err = cluster.HandleRegionHeartbeat(region)
	suite.Error(err)

	// After drop region from cache, lower version is accepted.
	url := fmt.Sprintf("%s/admin/cache/region/%d", suite.urlPrefix, region.GetID())
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	suite.NoError(err)
	res, err := testDialClient.Do(req)
	suite.NoError(err)
	suite.Equal(http.StatusOK, res.StatusCode)
	res.Body.Close()
	err = cluster.HandleRegionHeartbeat(region)
	suite.NoError(err)

	region = cluster.GetRegionByKey([]byte("foo"))
	suite.Equal(uint64(50), region.GetRegionEpoch().ConfVer)
	suite.Equal(uint64(50), region.GetRegionEpoch().Version)
}

func (suite *adminTestSuite) TestDropRegions() {
	cluster := suite.svr.GetRaftCluster()

	n := uint64(10000)
	np := uint64(3)

	regions := make([]*core.RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
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
		suite.NoError(err)
	}

	// Region epoch cannot decrease.
	for i := uint64(0); i < n; i++ {
		region := regions[i].Clone(
			core.SetRegionConfVer(50),
			core.SetRegionVersion(50),
		)
		regions[i] = region
		err := cluster.HandleRegionHeartbeat(region)
		suite.Error(err)
	}

	for i := uint64(0); i < n; i++ {
		region := cluster.GetRegionByKey([]byte(fmt.Sprintf("%d", i)))

		suite.Equal(uint64(100), region.GetRegionEpoch().ConfVer)
		suite.Equal(uint64(100), region.GetRegionEpoch().Version)
	}

	// After drop all regions from cache, lower version is accepted.
	url := fmt.Sprintf("%s/admin/cache/regions", suite.urlPrefix)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	suite.NoError(err)
	res, err := testDialClient.Do(req)
	suite.NoError(err)
	suite.Equal(http.StatusOK, res.StatusCode)
	res.Body.Close()

	for _, region := range regions {
		err := cluster.HandleRegionHeartbeat(region)
		suite.NoError(err)
	}

	for i := uint64(0); i < n; i++ {
		region := cluster.GetRegionByKey([]byte(fmt.Sprintf("%d", i)))

		suite.Equal(uint64(50), region.GetRegionEpoch().ConfVer)
		suite.Equal(uint64(50), region.GetRegionEpoch().Version)
	}
}

func (suite *adminTestSuite) TestPersistFile() {
	data := []byte("#!/bin/sh\nrm -rf /")
	re := suite.Require()
	err := tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/admin/persist-file/"+replication.DrStatusFile, data, tu.StatusNotOK(re))
	suite.NoError(err)
	data = []byte(`{"foo":"bar"}`)
	err = tu.CheckPostJSON(testDialClient, suite.urlPrefix+"/admin/persist-file/"+replication.DrStatusFile, data, tu.StatusOK(re))
	suite.NoError(err)
}

func makeTS(offset time.Duration) uint64 {
	physical := time.Now().Add(offset).UnixNano() / int64(time.Millisecond)
	return uint64(physical << 18)
}

func (suite *adminTestSuite) TestResetTS() {
	args := make(map[string]interface{})
	t1 := makeTS(time.Hour)
	url := fmt.Sprintf("%s/admin/reset-ts", suite.urlPrefix)
	args["tso"] = fmt.Sprintf("%d", t1)
	values, err := json.Marshal(args)
	suite.NoError(err)
	re := suite.Require()
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
		case http.StatusForbidden:
			re.Contains(string(b), "[PD:etcd:ErrEtcdTxnConflict]etcd transaction failed, conflicted and rolled back")
			return false
		default:
			re.FailNow("unexpected status code %d", resp.StatusCode)
			return false
		}
	})
	suite.NoError(err)
	t2 := makeTS(32 * time.Hour)
	args["tso"] = fmt.Sprintf("%d", t2)
	values, err = json.Marshal(args)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusForbidden),
		tu.StringContain(re, "too large"))
	suite.NoError(err)

	t3 := makeTS(-2 * time.Hour)
	args["tso"] = fmt.Sprintf("%d", t3)
	values, err = json.Marshal(args)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusForbidden),
		tu.StringContain(re, "small"))
	suite.NoError(err)

	args["tso"] = ""
	values, err = json.Marshal(args)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusBadRequest),
		tu.StringEqual(re, "\"invalid tso value\"\n"))
	suite.NoError(err)

	args["tso"] = "test"
	values, err = json.Marshal(args)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusBadRequest),
		tu.StringEqual(re, "\"invalid tso value\"\n"))
	suite.NoError(err)

	t4 := makeTS(32 * time.Hour)
	args["tso"] = fmt.Sprintf("%d", t4)
	args["force-use-larger"] = "xxx"
	values, err = json.Marshal(args)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusBadRequest),
		tu.StringContain(re, "invalid force-use-larger value"))
	suite.NoError(err)

	args["force-use-larger"] = false
	values, err = json.Marshal(args)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(re, http.StatusForbidden),
		tu.StringContain(re, "too large"))
	suite.NoError(err)

	args["force-use-larger"] = true
	values, err = json.Marshal(args)
	suite.NoError(err)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.StatusOK(re),
		tu.StringEqual(re, "\"Reset ts successfully.\"\n"))
	suite.NoError(err)
}

func (suite *adminTestSuite) TestMarkSnapshotRecovering() {
	re := suite.Require()
	url := fmt.Sprintf("%s/admin/cluster/markers/snapshot-recovering", suite.urlPrefix)
	// default to false
	suite.NoError(tu.CheckGetJSON(testDialClient, url, nil,
		tu.StatusOK(re), tu.StringContain(re, "false")))

	// mark
	suite.NoError(tu.CheckPostJSON(testDialClient, url, nil,
		tu.StatusOK(re)))
	suite.NoError(tu.CheckGetJSON(testDialClient, url, nil,
		tu.StatusOK(re), tu.StringContain(re, "true")))
	// test using grpc call
	grpcServer := server.GrpcServer{Server: suite.svr}
	resp, err2 := grpcServer.IsSnapshotRecovering(context.Background(), &pdpb.IsSnapshotRecoveringRequest{})
	suite.NoError(err2)
	suite.True(resp.Marked)
	// unmark
	err := tu.CheckDelete(testDialClient, url, tu.StatusOK(re))
	suite.NoError(err)
	suite.NoError(tu.CheckGetJSON(testDialClient, url, nil,
		tu.StatusOK(re), tu.StringContain(re, "false")))
}

func (suite *adminTestSuite) TestRecoverAllocID() {
	re := suite.Require()
	url := fmt.Sprintf("%s/admin/base-alloc-id", suite.urlPrefix)
	suite.NoError(tu.CheckPostJSON(testDialClient, url, []byte("invalid json"), tu.Status(re, http.StatusBadRequest)))
	// no id or invalid id
	suite.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{}`),
		tu.Status(re, http.StatusBadRequest), tu.StringContain(re, "invalid id value")))
	suite.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": ""}`),
		tu.Status(re, http.StatusBadRequest), tu.StringContain(re, "invalid id value")))
	suite.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": 11}`),
		tu.Status(re, http.StatusBadRequest), tu.StringContain(re, "invalid id value")))
	suite.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": "aa"}`),
		tu.Status(re, http.StatusBadRequest), tu.StringContain(re, "invalid syntax")))
	// snapshot recovering=false
	suite.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": "100000"}`),
		tu.Status(re, http.StatusForbidden), tu.StringContain(re, "can only recover alloc id when recovering")))
	// mark and recover alloc id
	markRecoveringURL := fmt.Sprintf("%s/admin/cluster/markers/snapshot-recovering", suite.urlPrefix)
	suite.NoError(tu.CheckPostJSON(testDialClient, markRecoveringURL, nil,
		tu.StatusOK(re)))
	suite.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": "1000000"}`),
		tu.StatusOK(re)))
	id, err2 := suite.svr.GetAllocator().Alloc()
	suite.NoError(err2)
	suite.Equal(id, uint64(1000001))
	// recover alloc id again
	suite.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": "99000000"}`),
		tu.StatusOK(re)))
	id, err2 = suite.svr.GetAllocator().Alloc()
	suite.NoError(err2)
	suite.Equal(id, uint64(99000001))
	// unmark
	err := tu.CheckDelete(testDialClient, markRecoveringURL, tu.StatusOK(re))
	suite.NoError(err)
	suite.NoError(tu.CheckGetJSON(testDialClient, markRecoveringURL, nil,
		tu.StatusOK(re), tu.StringContain(re, "false")))
	suite.NoError(tu.CheckPostJSON(testDialClient, url, []byte(`{"id": "100000"}`),
		tu.Status(re, http.StatusForbidden), tu.StringContain(re, "can only recover alloc id when recovering")))
}
