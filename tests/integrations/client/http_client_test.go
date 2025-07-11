// Copyright 2023 TiKV Project Authors.
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

package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/tests"
)

const ttlConfigPrefix = "/config/ttl"

type httpClientTestSuite struct {
	suite.Suite
	ctx        context.Context
	cancelFunc context.CancelFunc
	cluster    *tests.TestCluster
}

func TestHTTPClientTestSuite(t *testing.T) {
	suite.Run(t, &httpClientTestSuite{})
}

func (suite *httpClientTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	suite.ctx, suite.cancelFunc = context.WithCancel(context.Background())

	cluster, err := tests.NewTestCluster(suite.ctx, 2)
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	leader := cluster.WaitLeader()
	re.NotEmpty(leader)
	leaderServer := cluster.GetLeaderServer()

	err = leaderServer.BootstrapCluster()
	// Add 2 more stores to the cluster.
	for i := 2; i <= 4; i++ {
		tests.MustPutStore(re, cluster, &metapb.Store{
			Id:            uint64(i),
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		})
	}
	re.NoError(err)
	for _, region := range []*core.RegionInfo{
		core.NewTestRegionInfo(10, 1, []byte("a1"), []byte("a2")),
		core.NewTestRegionInfo(11, 1, []byte("a2"), []byte("a3")),
	} {
		err := leaderServer.GetRaftCluster().HandleRegionHeartbeat(region)
		re.NoError(err)
	}
	suite.cluster = cluster
}

func (suite *httpClientTestSuite) TearDownSuite() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	suite.cancelFunc()
	suite.cluster.Destroy()
}

func (suite *httpClientTestSuite) TestTTLConfigPersist() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	configKey := "schedule.max-pending-peer-count"

	testCases := []struct {
		inputValue        any
		expectedEtcdValue string
	}{
		{
			inputValue:        uint64(10000000),
			expectedEtcdValue: "10000000",
		},
		{
			inputValue:        int(20000000),
			expectedEtcdValue: "20000000",
		},
		{
			inputValue:        float64(2147483647),
			expectedEtcdValue: "2147483647",
		},
		{
			inputValue:        float64(1234567890.0),
			expectedEtcdValue: "1234567890",
		},
		{
			inputValue:        float64(0.0),
			expectedEtcdValue: "0",
		},
		{
			inputValue:        float64(987.65),
			expectedEtcdValue: "987.65",
		},
		{
			inputValue:        int(-1),
			expectedEtcdValue: "-1",
		},
		{
			inputValue:        int32(-2147483647),
			expectedEtcdValue: "-2147483647",
		},
	}

	for _, tc := range testCases {
		newConfig := map[string]any{
			configKey: tc.inputValue,
		}
		err := suite.setConfig(ctx, newConfig, 10000)
		re.NoError(err)
		resp, err := suite.cluster.GetEtcdClient().Get(ctx, ttlConfigPrefix+"/"+configKey)
		re.NoError(err)
		re.Len(resp.Kvs, 1)
		re.Equal([]byte(tc.expectedEtcdValue), resp.Kvs[0].Value)
		err = suite.setConfig(ctx, newConfig, 0)
		re.NoError(err)
	}
}

func (suite *httpClientTestSuite) setConfig(ctx context.Context, config map[string]any, ttlSeconds int) error {
	leader := suite.cluster.GetLeaderServer()
	re := suite.Require()
	re.NotNil(leader)
	addr := leader.GetConfig().AdvertiseClientUrls
	apiURL := fmt.Sprintf("%s/pd/api/v1/config?ttlSecond=%d", addr, ttlSeconds)

	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("setConfig request failed with status %s and could not read body: %w", resp.Status, readErr)
		}
		return fmt.Errorf("setConfig request failed with status %s: %s", resp.Status, string(bodyBytes))
	}

	return nil
}
