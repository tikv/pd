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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

type serviceGCSafepointTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   testutil.CleanupFunc
	urlPrefix string
}

func TestServiceGCSafepointTestSuite(t *testing.T) {
	suite.Run(t, new(serviceGCSafepointTestSuite))
}

func (suite *serviceGCSafepointTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
}

func (suite *serviceGCSafepointTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *serviceGCSafepointTestSuite) TestServiceGCSafepoint() {
	sspURL := suite.urlPrefix + "/gc/safepoint"

	storage := suite.svr.GetStorage()
	list := &ListServiceGCSafepoint{
		ServiceGCSafepoints: []*endpoint.ServiceSafePoint{
			{
				ServiceID: "a",
				ExpiredAt: time.Now().Unix() + 10,
				SafePoint: 1,
			},
			{
				ServiceID: "b",
				ExpiredAt: time.Now().Unix() + 10,
				SafePoint: 2,
			},
			{
				ServiceID: "c",
				ExpiredAt: time.Now().Unix() + 10,
				SafePoint: 3,
			},
		},
		GCSafePoint: 1,
	}
	for _, ssp := range list.ServiceGCSafepoints {
		err := storage.SaveServiceGCSafePoint(ssp)
		suite.NoError(err)
	}
	storage.SaveGCSafePoint(1)

	res, err := testDialClient.Get(sspURL)
	suite.NoError(err)
	defer res.Body.Close()
	listResp := &ListServiceGCSafepoint{}
	err = apiutil.ReadJSON(res.Body, listResp)
	suite.NoError(err)
	suite.Equal(list, listResp)

	err = testutil.CheckDelete(testDialClient, sspURL+"/a", testutil.StatusOK(suite.Require()))
	suite.NoError(err)

	left, err := storage.LoadAllServiceGCSafePoints()
	suite.NoError(err)
	suite.Equal(list.ServiceGCSafepoints[1:], left)
}
