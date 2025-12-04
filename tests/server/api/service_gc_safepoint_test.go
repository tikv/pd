// Copyright 2020 TiKV Project Authors.
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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/tests"
)

type serviceGCSafepointTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestServiceGCSafepointTestSuite(t *testing.T) {
	suite.Run(t, new(serviceGCSafepointTestSuite))
}

func (suite *serviceGCSafepointTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *serviceGCSafepointTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *serviceGCSafepointTestSuite) TestServiceGCSafepoint() {
	suite.env.RunTest(suite.checkServiceGCSafepoint)
}

func (suite *serviceGCSafepointTestSuite) checkServiceGCSafepoint(cluster *tests.TestCluster) {
	re := suite.Require()

	tests.MustPutStore(re, cluster, &metapb.Store{
		Id:        1,
		Address:   "mock://tikv-1:1",
		State:     metapb.StoreState_Up,
		NodeState: metapb.NodeState_Serving,
	})

	leader := cluster.GetLeaderServer()
	sspURL := leader.GetAddr() + "/pd/api/v1/gc/safepoint"

	gcStateManager := leader.GetServer().GetGCStateManager()
	now := time.Now().Truncate(time.Second)
	list := &api.ListServiceGCSafepoint{
		ServiceGCSafepoints: []*endpoint.ServiceSafePoint{
			{
				ServiceID:  "a",
				ExpiredAt:  now.Unix() + 10,
				SafePoint:  10,
				KeyspaceID: constant.NullKeyspaceID,
			},
			{
				ServiceID:  "b",
				ExpiredAt:  now.Unix() + 10,
				SafePoint:  20,
				KeyspaceID: constant.NullKeyspaceID,
			},
			{
				ServiceID:  "c",
				ExpiredAt:  now.Unix() + 10,
				SafePoint:  30,
				KeyspaceID: constant.NullKeyspaceID,
			},
			{
				ServiceID:  "gc_worker",
				ExpiredAt:  math.MaxInt64,
				SafePoint:  1,
				KeyspaceID: constant.NullKeyspaceID,
			},
		},
		GCSafePoint:           1,
		MinServiceGcSafepoint: 1,
	}
	// Skip writing the "gc_worker" one.
	for _, ssp := range list.ServiceGCSafepoints[:3] {
		_, _, err := gcStateManager.CompatibleUpdateServiceGCSafePoint(constant.NullKeyspaceID, ssp.ServiceID, ssp.SafePoint, 10, now)
		re.NoError(err)
	}
	_, err := gcStateManager.AdvanceTxnSafePoint(constant.NullKeyspaceID, 1, now)
	re.NoError(err)
	_, _, err = gcStateManager.AdvanceGCSafePoint(constant.NullKeyspaceID, 1)
	re.NoError(err)

	res, err := tests.TestDialClient.Get(sspURL)
	re.NoError(err)
	defer res.Body.Close()
	listResp := &api.ListServiceGCSafepoint{}
	err = apiutil.ReadJSON(res.Body, listResp)
	re.NoError(err)
	re.Equal(list, listResp)

	err = testutil.CheckDelete(tests.TestDialClient, sspURL+"/a", testutil.StatusOK(re))
	re.NoError(err)

	state, err := gcStateManager.GetGCState(constant.NullKeyspaceID)
	re.NoError(err)
	left := state.GCBarriers
	leftSsps := make([]*endpoint.ServiceSafePoint, 0, len(left))
	for _, barrier := range left {
		leftSsps = append(leftSsps, barrier.ToServiceSafePoint(constant.NullKeyspaceID))
	}
	// Exclude the gc_worker as it's not included in GetGCState's result.
	re.Equal(list.ServiceGCSafepoints[1:3], leftSsps)
}
