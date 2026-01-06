// Copyright 2025 TiKV Project Authors.
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

package scheduling

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/client/testutil"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type keyspaceTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	// The PD cluster.
	cluster *tests.TestCluster
	// pdLeaderServer is the leader server of the PD cluster.
	pdLeaderServer *tests.TestServer
}

var (
	labelPrefix     = "/pd/api/v1/config/region-label/rule"
	initKeyspaceNum = 10
)

func TestKeyspace(t *testing.T) {
	suite.Run(t, &keyspaceTestSuite{})
}

func (suite *keyspaceTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	var err error
	skipWait := func(conf *config.Config, _ string) {
		conf.Keyspace.WaitRegionSplit = false
	}
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1, skipWait)
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	re.NoError(suite.pdLeaderServer.BootstrapCluster())

	testConfig := map[string]string{
		"config1": "100",
		"config2": "200",
	}
	for id := range initKeyspaceNum {
		name := fmt.Sprintf("test_keyspace_%d", id)
		createRequest := &handlers.CreateKeyspaceParams{
			Name:   name,
			Config: testConfig,
		}

		address := suite.pdLeaderServer.GetAddr()
		data, _ := json.Marshal(createRequest)
		resp, err := http.DefaultClient.Post(address+"/pd/api/v2/keyspaces", "application/json", bytes.NewBuffer(data))
		re.NoError(err)
		re.Equal(http.StatusOK, resp.StatusCode)
		re.NoError(resp.Body.Close())
	}
}

func (suite *keyspaceTestSuite) TearDownSuite() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *keyspaceTestSuite) resignLeader() string {
	re := suite.Require()
	re.NoError(suite.cluster.ResignLeader())
	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	return suite.pdLeaderServer.GetAddr()
}

func mustGetAllRegionRules(re *require.Assertions, address string) []*labeler.LabelRule {
	var rules []*labeler.LabelRule
	testutil.Eventually(re, func() bool {
		resp, err := http.DefaultClient.Get(address + labelPrefix + "s")
		if err != nil || resp.StatusCode != http.StatusOK {
			return false
		}
		data, err := io.ReadAll(resp.Body)
		re.NoError(err)
		err = json.Unmarshal(data, &rules)
		re.NoError(err)
		re.NoError(resp.Body.Close())
		return len(rules) > 0
	})

	return rules
}

func (suite *keyspaceTestSuite) TestKeyspaceCRUDWithoutSchedulingService() {
	re := suite.Require()
	address := suite.pdLeaderServer.GetAddr()
	labelPrefix := "/pd/api/v1/config/region-label/rule"

	// get rules
	rules := mustGetAllRegionRules(re, address)
	re.NotEmpty(rules)

	// triger reload rules
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/labeler/pauseLoadRules", `return("10m")`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/labeler/pauseLoadRules"))
	}()
	address = suite.resignLeader()

	// get rule
	resp, err := http.DefaultClient.Get(address + labelPrefix + "/0")
	re.NoError(err)
	re.Equal(http.StatusNotFound, resp.StatusCode)
	re.NoError(resp.Body.Close())

	// batch get rules
	req, err := http.NewRequest(http.MethodGet, address+"/pd/api/v1/config/region-label/rules/ids", bytes.NewBuffer([]byte(`["rule1", "rule3"]`)))
	re.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusInternalServerError, resp.StatusCode)
	re.NoError(resp.Body.Close())

	// delete rule
	req, err = http.NewRequest(http.MethodDelete, address+labelPrefix+"/0", nil)
	re.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusInternalServerError, resp.StatusCode)
	re.NoError(resp.Body.Close())

	// update
	rule := rules[0]
	rule.Index = 1
	data, err := json.Marshal(rule)
	re.NoError(err)
	resp, err = http.DefaultClient.Post(address+labelPrefix, "application/json", bytes.NewBuffer(data))
	re.NoError(err)
	re.Equal(http.StatusInternalServerError, resp.StatusCode)
	re.NoError(resp.Body.Close())

	// patch
	patch := labeler.LabelRulePatch{
		SetRules: []*labeler.LabelRule{
			{ID: "0", Labels: []labeler.RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: map[string]string{"start_key": "", "end_key": ""}},
		},
		DeleteRules: []string{"rule1"},
	}
	data, _ = json.Marshal(patch)
	req, err = http.NewRequest(http.MethodPatch, address+labelPrefix+"s", bytes.NewBuffer(data))
	re.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusInternalServerError, resp.StatusCode)
	re.NoError(resp.Body.Close())
}

func (suite *keyspaceTestSuite) TestCreateKeyspace() {
	re := suite.Require()
	successNames := make([]string, 0)
	failedNames := make([]string, 0)
	testConfig := map[string]string{
		"config1": "100",
		"config2": "200",
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/labeler/pauseLoadRules", `return("1s")`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/labeler/pauseLoadRules"))
	}()
	address := suite.pdLeaderServer.GetAddr()
	wg := sync.WaitGroup{}
	for id := 10; id < 20; id++ {
		name := fmt.Sprintf("test_keyspace_%d", id)
		createRequest := &handlers.CreateKeyspaceParams{
			Name:   name,
			Config: testConfig,
		}

		data, _ := json.Marshal(createRequest)
		resp, err := http.DefaultClient.Post(address+"/pd/api/v2/keyspaces", "application/json", bytes.NewBuffer(data))
		if err != nil || resp.StatusCode != http.StatusOK {
			failedNames = append(failedNames, name)
		} else {
			successNames = append(successNames, name)
		}
		time.Sleep(500 * time.Microsecond)
		re.NoError(resp.Body.Close())
		if id == 15 {
			wg.Add(1)
			go func() {
				suite.resignLeader()
				wg.Done()
			}()
		}
	}
	wg.Wait()
	re.NotEmpty(failedNames)
	re.NotEmpty(successNames)
	rules := mustGetAllRegionRules(re, address)
	re.Len(rules, len(successNames)+initKeyspaceNum+1)
}

func TestKeyspaceSchedulingExist(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	disableFallback := func(conf *config.Config, _ string) {
		conf.MicroService.EnableSchedulingFallback = false
		conf.Keyspace.WaitRegionSplit = false
	}
	cluster, err := tests.NewTestAPICluster(ctx, 1, disableFallback)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	pdLeader := cluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())

	testutil.Eventually(re, func() bool {
		return pdLeader.GetRaftCluster() != nil
	})
	testConfig := map[string]string{
		"config1": "100",
		"config2": "200",
	}
	for id := range initKeyspaceNum {
		name := fmt.Sprintf("test_keyspace_%d", id)
		createRequest := &handlers.CreateKeyspaceParams{
			Name:   name,
			Config: testConfig,
		}

		address := pdLeader.GetAddr()
		data, _ := json.Marshal(createRequest)
		resp, err := http.DefaultClient.Post(address+"/pd/api/v2/keyspaces", "application/json", bytes.NewBuffer(data))
		re.NoError(err)
		re.Equal(http.StatusOK, resp.StatusCode)
		re.NoError(resp.Body.Close())
	}

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/labeler/pauseLoadRules", `return("10m")`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/labeler/pauseLoadRules"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
	}()
	re.NoError(cluster.ResignLeader())

	leaderName = cluster.WaitLeader()
	re.NotEmpty(leaderName)

	tc, err := tests.NewTestSchedulingCluster(ctx, 1, cluster)
	re.NoError(err)

	// get all rules
	pdLeader = cluster.GetServer(leaderName)
	address := pdLeader.GetAddr()
	rules := mustGetAllRegionRules(re, address)
	re.Len(rules, 11)
	// get rule
	rule := rules[1]
	url := address + labelPrefix + "/" + rule.ID
	resp, err := http.DefaultClient.Get(url)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	re.NoError(resp.Body.Close())

	// batch get rules
	req, err := http.NewRequest(http.MethodGet, address+"/pd/api/v1/config/region-label/rules/ids", bytes.NewBuffer([]byte(`["rule1", "rule3"]`)))
	re.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	re.NoError(resp.Body.Close())

	tc.Destroy()
	cluster.Destroy()
}
