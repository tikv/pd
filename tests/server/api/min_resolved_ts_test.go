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

	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/tests"
)

type minResolvedTSTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestMinResolvedTSTestSuite(t *testing.T) {
	suite.Run(t, new(minResolvedTSTestSuite))
}

func (suite *minResolvedTSTestSuite) SetupSuite() {
	cluster.DefaultMinResolvedTSPersistenceInterval = time.Millisecond
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *minResolvedTSTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *minResolvedTSTestSuite) TestMinResolvedTS() {
	suite.env.RunTestInNonMicroserviceEnv(suite.checkMinResolvedTS)
	suite.env.RunTestInNonMicroserviceEnv(suite.checkMinResolvedTSByStores)
}

func (suite *minResolvedTSTestSuite) checkMinResolvedTS(cluster *tests.TestCluster) {
	re := suite.Require()

	for i := 1; i <= 3; i++ {
		id := uint64(i)
		tests.MustPutStore(re, cluster, &metapb.Store{
			Id:            id,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		})
		tests.MustPutRegion(re, cluster, id, id, []byte(fmt.Sprintf("%da", id)), []byte(fmt.Sprintf("%db", id)))
	}

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1/min-resolved-ts"

	// case1: default run job
	interval := leader.GetRaftCluster().GetPDServerConfig().MinResolvedTSPersistenceInterval
	checkMinResolvedTS(re, urlPrefix, &api.MinResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case2: stop run job
	zero := typeutil.Duration{Duration: 0}
	setMinResolvedTSPersistenceInterval(leader, zero)
	checkMinResolvedTS(re, urlPrefix, &api.MinResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      false,
		PersistInterval: zero,
	})
	// case3: start run job
	interval = typeutil.Duration{Duration: time.Millisecond}
	setMinResolvedTSPersistenceInterval(leader, interval)
	suite.Eventually(func() bool {
		return interval == leader.GetRaftCluster().GetPDServerConfig().MinResolvedTSPersistenceInterval
	}, time.Second*10, time.Millisecond*20)
	checkMinResolvedTS(re, urlPrefix, &api.MinResolvedTS{
		MinResolvedTS:   0,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case4: set min resolved ts
	ts := uint64(233)
	err := setAllStoresMinResolvedTS(leader, ts)
	re.NoError(err)
	checkMinResolvedTS(re, urlPrefix, &api.MinResolvedTS{
		MinResolvedTS:   ts,
		IsRealTime:      true,
		PersistInterval: interval,
	})
	// case5: stop persist and return last persist value when interval is 0
	interval = typeutil.Duration{Duration: 0}
	setMinResolvedTSPersistenceInterval(leader, interval)
	checkMinResolvedTS(re, urlPrefix, &api.MinResolvedTS{
		MinResolvedTS:   ts,
		IsRealTime:      false,
		PersistInterval: interval,
	})
	err = setAllStoresMinResolvedTS(leader, ts)
	re.NoError(err)
	checkMinResolvedTS(re, urlPrefix, &api.MinResolvedTS{
		MinResolvedTS:   ts, // last persist value
		IsRealTime:      false,
		PersistInterval: interval,
	})
}

func (suite *minResolvedTSTestSuite) checkMinResolvedTSByStores(cluster *tests.TestCluster) {
	re := suite.Require()

	leader := cluster.GetLeaderServer()
	urlPrefix := leader.GetAddr() + "/pd/api/v1/min-resolved-ts"
	// run job.
	interval := typeutil.Duration{Duration: time.Millisecond}
	setMinResolvedTSPersistenceInterval(leader, interval)
	suite.Eventually(func() bool {
		return interval == leader.GetRaftCluster().GetPDServerConfig().MinResolvedTSPersistenceInterval
	}, time.Second*10, time.Millisecond*20)
	// set min resolved ts.
	rc := leader.GetRaftCluster()
	ts := uint64(233)

	// scope is `cluster`
	testStoresID := make([]string, 0)
	testMap := make(map[uint64]uint64)
	for i := 1; i <= 3; i++ {
		storeID := uint64(i)
		testTS := ts + storeID
		testMap[storeID] = testTS
		err := rc.SetMinResolvedTS(storeID, testTS)
		re.NoError(err)

		testStoresID = append(testStoresID, strconv.Itoa(i))
	}
	checkMinResolvedTSByStores(re, urlPrefix, &api.MinResolvedTS{
		MinResolvedTS:       234,
		IsRealTime:          true,
		PersistInterval:     interval,
		StoresMinResolvedTS: testMap,
	}, "cluster")

	// set all stores min resolved ts.
	testStoresIDStr := strings.Join(testStoresID, ",")
	checkMinResolvedTSByStores(re, urlPrefix, &api.MinResolvedTS{
		MinResolvedTS:       234,
		IsRealTime:          true,
		PersistInterval:     interval,
		StoresMinResolvedTS: testMap,
	}, testStoresIDStr)

	// remove last store for test.
	testStoresID = testStoresID[:len(testStoresID)-1]
	testStoresIDStr = strings.Join(testStoresID, ",")
	delete(testMap, uint64(3))
	checkMinResolvedTSByStores(re, urlPrefix, &api.MinResolvedTS{
		MinResolvedTS:       234,
		IsRealTime:          true,
		PersistInterval:     interval,
		StoresMinResolvedTS: testMap,
	}, testStoresIDStr)
}

func setMinResolvedTSPersistenceInterval(svr *tests.TestServer, duration typeutil.Duration) {
	cfg := svr.GetRaftCluster().GetPDServerConfig().Clone()
	cfg.MinResolvedTSPersistenceInterval = duration
	svr.GetRaftCluster().SetPDServerConfig(cfg)
}

func setAllStoresMinResolvedTS(svr *tests.TestServer, ts uint64) error {
	rc := svr.GetRaftCluster()
	for i := 1; i <= 3; i++ {
		err := rc.SetMinResolvedTS(uint64(i), ts)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkMinResolvedTS(re *require.Assertions, url string, expect *api.MinResolvedTS) {
	re.Eventually(func() bool {
		res, err := tests.TestDialClient.Get(url)
		re.NoError(err)
		defer res.Body.Close()
		listResp := &api.MinResolvedTS{}
		err = apiutil.ReadJSON(res.Body, listResp)
		re.NoError(err)
		re.Nil(listResp.StoresMinResolvedTS)
		return reflect.DeepEqual(expect, listResp)
	}, time.Second*10, time.Millisecond*20)
}

func checkMinResolvedTSByStores(re *require.Assertions, url string, expect *api.MinResolvedTS, scope string) {
	re.Eventually(func() bool {
		url := fmt.Sprintf("%s?scope=%s", url, scope)
		res, err := tests.TestDialClient.Get(url)
		re.NoError(err)
		defer res.Body.Close()
		listResp := &api.MinResolvedTS{}
		err = apiutil.ReadJSON(res.Body, listResp)
		re.NoError(err)
		return reflect.DeepEqual(expect, listResp)
	}, time.Second*10, time.Millisecond*20)
}
