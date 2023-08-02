// Copyright 2022 TiKV Project Authors.
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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
)

type minResolvedTSTestSuite struct {
	suite.Suite
	svr             *server.Server
	cleanup         testutil.CleanupFunc
	url             string
	defaultInterval time.Duration
}

func TestMinResolvedTSTestSuite(t *testing.T) {
	suite.Run(t, new(minResolvedTSTestSuite))
}

func (suite *minResolvedTSTestSuite) SetupSuite() {
	suite.defaultInterval = time.Millisecond
	cluster.DefaultMinResolvedTSPersistenceInterval = suite.defaultInterval
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.url = fmt.Sprintf("%s%s/api/v1/min-resolved-ts", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	r1 := core.NewTestRegionInfo(7, 1, []byte("a"), []byte("b"))
	mustRegionHeartbeat(re, suite.svr, r1)
	r2 := core.NewTestRegionInfo(8, 1, []byte("b"), []byte("c"))
	mustRegionHeartbeat(re, suite.svr, r2)
}

func (suite *minResolvedTSTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *minResolvedTSTestSuite) TestMinResolvedTS() {
	// case1: default run job
	interval := suite.svr.GetRaftCluster().GetPDServerConfig().MinResolvedTSPersistenceInterval
	suite.checkMinResolvedTS(&minResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case2: stop run job
	zero := typeutil.Duration{Duration: 0}
	suite.setMinResolvedTSPersistenceInterval(zero)
	suite.checkMinResolvedTS(&minResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      false,
		PersistInterval: zero,
	})
	// case3: start run job
	interval = typeutil.Duration{Duration: suite.defaultInterval}
	suite.setMinResolvedTSPersistenceInterval(interval)
	suite.Eventually(func() bool {
		return interval == suite.svr.GetRaftCluster().GetPDServerConfig().MinResolvedTSPersistenceInterval
	}, time.Second*10, time.Millisecond*20)
	suite.checkMinResolvedTS(&minResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case4: set min resolved ts
	rc := suite.svr.GetRaftCluster()
	ts := uint64(233)
	rc.SetMinResolvedTS(1, ts)
	suite.checkMinResolvedTS(&minResolvedTS{
		MinResolvedTS:   ts,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case5: stop persist and return last persist value when interval is 0
	interval = typeutil.Duration{Duration: 0}
	suite.setMinResolvedTSPersistenceInterval(interval)
	suite.checkMinResolvedTS(&minResolvedTS{
		MinResolvedTS:   ts,
		IsRealTime:      false,
		PersistInterval: interval,
	})
	rc.SetMinResolvedTS(1, ts+1)
	suite.checkMinResolvedTS(&minResolvedTS{
		MinResolvedTS:   ts, // last persist value
		IsRealTime:      false,
		PersistInterval: interval,
	})
}

func (suite *minResolvedTSTestSuite) TestMinResolvedTSByStores() {
	// default run job
	interval := typeutil.Duration{Duration: suite.defaultInterval}
	suite.setMinResolvedTSPersistenceInterval(interval)
	suite.Eventually(func() bool {
		return interval == suite.svr.GetRaftCluster().GetPDServerConfig().MinResolvedTSPersistenceInterval
	}, time.Second*10, time.Millisecond*20)
	// set min resolved ts
	rc := suite.svr.GetRaftCluster()
	ts := uint64(233)
	rc.SetMinResolvedTS(1, ts)
	storeIDs := []string{"1"}
	suite.checkMinResolvedTSByStores(&minResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      true,
		PersistInterval: interval,
		StoreMinResolvedTS: map[uint64]uint64{
			1: ts,
		},
	}, storeIDs)
}

func (suite *minResolvedTSTestSuite) setMinResolvedTSPersistenceInterval(duration typeutil.Duration) {
	cfg := suite.svr.GetRaftCluster().GetPDServerConfig().Clone()
	cfg.MinResolvedTSPersistenceInterval = duration
	suite.svr.GetRaftCluster().SetPDServerConfig(cfg)
}

func (suite *minResolvedTSTestSuite) checkMinResolvedTS(expect *minResolvedTS) {
	suite.Eventually(func() bool {
		res, err := testDialClient.Get(suite.url)
		suite.NoError(err)
		defer res.Body.Close()
		listResp := &minResolvedTS{}
		err = apiutil.ReadJSON(res.Body, listResp)
		suite.NoError(err)
		return reflect.DeepEqual(expect, listResp)
	}, time.Second*10, time.Millisecond*20)
}

func (suite *minResolvedTSTestSuite) checkMinResolvedTSByStores(expect *minResolvedTS, storeIDs []string) {
	suite.Eventually(func() bool {
		data, _ := json.Marshal(storeIDs)
		req, _ := http.NewRequest(http.MethodGet, suite.url, bytes.NewBuffer(data))
		res, err := testDialClient.Do(req)
		suite.NoError(err)
		defer res.Body.Close()
		listResp := &minResolvedTS{}
		err = apiutil.ReadJSON(res.Body, listResp)
		suite.NoError(err)
		return reflect.DeepEqual(expect, listResp)
	}, time.Second*10, time.Millisecond*20)
}
