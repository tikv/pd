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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
)

type minWatermarkTestSuite struct {
	suite.Suite
	svr             *server.Server
	cleanup         testutil.CleanupFunc
	url             string
	defaultInterval time.Duration
	storesNum       int
}

func TestMinWatermarkTestSuite(t *testing.T) {
	suite.Run(t, new(minWatermarkTestSuite))
}

func (suite *minWatermarkTestSuite) SetupSuite() {
	suite.defaultInterval = time.Millisecond
	cluster.DefaultMinWatermarkPersistenceInterval = suite.defaultInterval
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

func (suite *minWatermarkTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *minWatermarkTestSuite) TestMinWatermark() {
	re := suite.Require()
	// case1: default run job
	interval := suite.svr.GetRaftCluster().GetPDServerConfig().MinWatermarkPersistenceInterval
	suite.checkMinWatermark(re, &minWatermark{
		MinWatermark:   0,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case2: stop run job
	zero := typeutil.Duration{Duration: 0}
	suite.setMinWatermarkPersistenceInterval(zero)
	suite.checkMinWatermark(re, &minWatermark{
		MinWatermark:   0,
		IsRealTime:      false,
		PersistInterval: zero,
	})
	// case3: start run job
	interval = typeutil.Duration{Duration: suite.defaultInterval}
	suite.setMinWatermarkPersistenceInterval(interval)
	suite.Eventually(func() bool {
		return interval == suite.svr.GetRaftCluster().GetPDServerConfig().MinWatermarkPersistenceInterval
	}, time.Second*10, time.Millisecond*20)
	suite.checkMinWatermark(re, &minWatermark{
		MinWatermark:   0,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case4: set min watermark
	ts := uint64(233)
	suite.setAllStoresMinWatermark(ts)
	suite.checkMinWatermark(re, &minWatermark{
		MinWatermark:   ts,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case5: stop persist and return last persist value when interval is 0
	interval = typeutil.Duration{Duration: 0}
	suite.setMinWatermarkPersistenceInterval(interval)
	suite.checkMinWatermark(re, &minWatermark{
		MinWatermark:   ts,
		IsRealTime:      false,
		PersistInterval: interval,
	})
	suite.setAllStoresMinWatermark(ts)
	suite.checkMinWatermark(re, &minWatermark{
		MinWatermark:   ts, // last persist value
		IsRealTime:      false,
		PersistInterval: interval,
	})
}

func (suite *minWatermarkTestSuite) TestMinWatermarkByStores() {
	re := suite.Require()
	// run job.
	interval := typeutil.Duration{Duration: suite.defaultInterval}
	suite.setMinWatermarkPersistenceInterval(interval)
	suite.Eventually(func() bool {
		return interval == suite.svr.GetRaftCluster().GetPDServerConfig().MinWatermarkPersistenceInterval
	}, time.Second*10, time.Millisecond*20)
	// set min watermark.
	rc := suite.svr.GetRaftCluster()
	ts := uint64(233)

	// scope is `cluster`
	testStoresID := make([]string, 0)
	testMap := make(map[uint64]uint64)
	for i := 1; i <= suite.storesNum; i++ {
		storeID := uint64(i)
		testTS := ts + storeID
		testMap[storeID] = testTS
		rc.SetMinWatermark(storeID, testTS)

		testStoresID = append(testStoresID, strconv.Itoa(i))
	}
	suite.checkMinWatermarkByStores(re, &minWatermark{
		MinWatermark:       234,
		IsRealTime:          true,
		PersistInterval:     interval,
		StoresMinWatermark: testMap,
	}, "cluster")

	// set all stores min watermark.
	testStoresIDStr := strings.Join(testStoresID, ",")
	suite.checkMinWatermarkByStores(re, &minWatermark{
		MinWatermark:       234,
		IsRealTime:          true,
		PersistInterval:     interval,
		StoresMinWatermark: testMap,
	}, testStoresIDStr)

	// remove last store for test.
	testStoresID = testStoresID[:len(testStoresID)-1]
	testStoresIDStr = strings.Join(testStoresID, ",")
	delete(testMap, uint64(suite.storesNum))
	suite.checkMinWatermarkByStores(re, &minWatermark{
		MinWatermark:       234,
		IsRealTime:          true,
		PersistInterval:     interval,
		StoresMinWatermark: testMap,
	}, testStoresIDStr)
}

func (suite *minWatermarkTestSuite) setMinWatermarkPersistenceInterval(duration typeutil.Duration) {
	cfg := suite.svr.GetRaftCluster().GetPDServerConfig().Clone()
	cfg.MinWatermarkPersistenceInterval = duration
	suite.svr.GetRaftCluster().SetPDServerConfig(cfg)
}

func (suite *minWatermarkTestSuite) setAllStoresMinWatermark(ts uint64) {
	rc := suite.svr.GetRaftCluster()
	for i := 1; i <= suite.storesNum; i++ {
		rc.SetMinWatermark(uint64(i), ts)
	}
}

func (suite *minWatermarkTestSuite) checkMinWatermark(re *require.Assertions, expect *minWatermark) {
	suite.Eventually(func() bool {
		res, err := testDialClient.Get(suite.url)
		re.NoError(err)
		defer res.Body.Close()
		listResp := &minWatermark{}
		err = apiutil.ReadJSON(res.Body, listResp)
		re.NoError(err)
		re.Nil(listResp.StoresMinWatermark)
		return reflect.DeepEqual(expect, listResp)
	}, time.Second*10, time.Millisecond*20)
}

func (suite *minWatermarkTestSuite) checkMinWatermarkByStores(re *require.Assertions, expect *minWatermark, scope string) {
	suite.Eventually(func() bool {
		url := fmt.Sprintf("%s?scope=%s", suite.url, scope)
		res, err := testDialClient.Get(url)
		re.NoError(err)
		defer res.Body.Close()
		listResp := &minWatermark{}
		err = apiutil.ReadJSON(res.Body, listResp)
		re.NoError(err)
		return reflect.DeepEqual(expect, listResp)
	}, time.Second*10, time.Millisecond*20)
}
