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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

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
	storesNum       int
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
	suite.storesNum = 3
	for i := 1; i <= suite.storesNum; i++ {
		id := uint64(i)
		mustPutStore(re, suite.svr, id, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
		r := core.NewTestRegionInfo(id, id, []byte(fmt.Sprintf("%da", id)), []byte(fmt.Sprintf("%db", id)))
		mustRegionHeartbeat(re, suite.svr, r)
	}
}

func (suite *minResolvedTSTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *minResolvedTSTestSuite) TestMinResolvedTS() {
	re := suite.Require()
	// case1: default run job
	interval := suite.svr.GetRaftCluster().GetPDServerConfig().MinResolvedTSPersistenceInterval
	suite.checkMinResolvedTS(re, &minResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case2: stop run job
	zero := typeutil.Duration{Duration: 0}
	suite.setMinResolvedTSPersistenceInterval(zero)
	suite.checkMinResolvedTS(re, &minResolvedTS{
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
	suite.checkMinResolvedTS(re, &minResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case4: set min resolved ts
	ts := uint64(233)
	suite.setAllStoresMinResolvedTS(ts)
	suite.checkMinResolvedTS(re, &minResolvedTS{
		MinResolvedTS:   ts,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case5: stop persist and return last persist value when interval is 0
	interval = typeutil.Duration{Duration: 0}
	suite.setMinResolvedTSPersistenceInterval(interval)
	suite.checkMinResolvedTS(re, &minResolvedTS{
		MinResolvedTS:   ts,
		IsRealTime:      false,
		PersistInterval: interval,
	})
	suite.setAllStoresMinResolvedTS(ts)
	suite.checkMinResolvedTS(re, &minResolvedTS{
		MinResolvedTS:   ts, // last persist value
		IsRealTime:      false,
		PersistInterval: interval,
	})
}

func (suite *minResolvedTSTestSuite) TestMinResolvedTSByStores() {
	re := suite.Require()
	// run job.
	interval := typeutil.Duration{Duration: suite.defaultInterval}
	suite.setMinResolvedTSPersistenceInterval(interval)
	suite.Eventually(func() bool {
		return interval == suite.svr.GetRaftCluster().GetPDServerConfig().MinResolvedTSPersistenceInterval
	}, time.Second*10, time.Millisecond*20)
	// set min resolved ts.
	rc := suite.svr.GetRaftCluster()
	ts := uint64(233)

	// scope is `cluster`
	testStoresID := make([]string, 0)
	testMap := make(map[uint64]uint64)
	for i := 1; i <= suite.storesNum; i++ {
		storeID := uint64(i)
		testTS := ts + storeID
		testMap[storeID] = testTS
		rc.SetMinResolvedTS(storeID, testTS)

		testStoresID = append(testStoresID, strconv.Itoa(i))
	}
	suite.checkMinResolvedTSByStores(re, &minResolvedTS{
		MinResolvedTS:       234,
		IsRealTime:          true,
		PersistInterval:     interval,
		StoresMinResolvedTS: testMap,
	}, "cluster")

	// set all stores min resolved ts.
	testStoresIDStr := strings.Join(testStoresID, ",")
	suite.checkMinResolvedTSByStores(re, &minResolvedTS{
		MinResolvedTS:       234,
		IsRealTime:          true,
		PersistInterval:     interval,
		StoresMinResolvedTS: testMap,
	}, testStoresIDStr)

	// remove last store for test.
	testStoresID = testStoresID[:len(testStoresID)-1]
	testStoresIDStr = strings.Join(testStoresID, ",")
	delete(testMap, uint64(suite.storesNum))
	suite.checkMinResolvedTSByStores(re, &minResolvedTS{
		MinResolvedTS:       234,
		IsRealTime:          true,
		PersistInterval:     interval,
		StoresMinResolvedTS: testMap,
	}, testStoresIDStr)
}

func (suite *minResolvedTSTestSuite) setMinResolvedTSPersistenceInterval(duration typeutil.Duration) {
	cfg := suite.svr.GetRaftCluster().GetPDServerConfig().Clone()
	cfg.MinResolvedTSPersistenceInterval = duration
	suite.svr.GetRaftCluster().SetPDServerConfig(cfg)
}

func (suite *minResolvedTSTestSuite) setAllStoresMinResolvedTS(ts uint64) {
	rc := suite.svr.GetRaftCluster()
	for i := 1; i <= suite.storesNum; i++ {
		rc.SetMinResolvedTS(uint64(i), ts)
	}
}

func (suite *minResolvedTSTestSuite) checkMinResolvedTS(re *require.Assertions, expect *minResolvedTS) {
	suite.Eventually(func() bool {
		res, err := testDialClient.Get(suite.url)
		re.NoError(err)
		defer res.Body.Close()
		listResp := &minResolvedTS{}
		err = apiutil.ReadJSON(res.Body, listResp)
		re.NoError(err)
		re.Nil(listResp.StoresMinResolvedTS)
		return reflect.DeepEqual(expect, listResp)
	}, time.Second*10, time.Millisecond*20)
}

func (suite *minResolvedTSTestSuite) checkMinResolvedTSByStores(re *require.Assertions, expect *minResolvedTS, scope string) {
	suite.Eventually(func() bool {
		url := fmt.Sprintf("%s?scope=%s", suite.url, scope)
		res, err := testDialClient.Get(url)
		re.NoError(err)
		defer res.Body.Close()
		listResp := &minResolvedTS{}
		err = apiutil.ReadJSON(res.Body, listResp)
		re.NoError(err)
		return reflect.DeepEqual(expect, listResp)
	}, time.Second*10, time.Millisecond*20)
}
