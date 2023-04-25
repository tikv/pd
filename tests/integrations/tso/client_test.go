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

package tso

import (
	"context"
	"math"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/testutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

type tsoClientTestSuite struct {
	suite.Suite
	legacy bool

	ctx    context.Context
	cancel context.CancelFunc
	// The PD cluster.
	cluster *tests.TestCluster
	// The TSO service in microservice mode.
	tsoCluster *mcs.TestTSOCluster

	backendEndpoints string

	client pd.TSOClient
}

func TestLegacyTSOClient(t *testing.T) {
	suite.Run(t, &tsoClientTestSuite{
		legacy: true,
	})
}

func TestMicroserviceTSOClient(t *testing.T) {
	suite.Run(t, &tsoClientTestSuite{
		legacy: false,
	})
}

func (suite *tsoClientTestSuite) SetupSuite() {
	re := suite.Require()

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	if suite.legacy {
		suite.cluster, err = tests.NewTestCluster(suite.ctx, serverCount)
	} else {
		suite.cluster, err = tests.NewTestAPICluster(suite.ctx, serverCount)
	}
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	pdLeader := suite.cluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())
	suite.backendEndpoints = pdLeader.GetAddr()
	if suite.legacy {
		suite.client, err = pd.NewClientWithContext(suite.ctx, strings.Split(suite.backendEndpoints, ","), pd.SecurityOption{})
		re.NoError(err)
	} else {
		suite.tsoCluster, err = mcs.NewTestTSOCluster(suite.ctx, 3, suite.backendEndpoints)
		re.NoError(err)
		suite.client = mcs.SetupClientWithKeyspace(suite.ctx, re, strings.Split(suite.backendEndpoints, ","))
	}
}

func (suite *tsoClientTestSuite) TearDownSuite() {
	suite.cancel()
	if !suite.legacy {
		suite.tsoCluster.Destroy()
	}
	suite.cluster.Destroy()
}

func (suite *tsoClientTestSuite) TestGetTS() {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			var lastTS uint64
			for i := 0; i < tsoRequestRound; i++ {
				physical, logical, err := suite.client.GetTS(suite.ctx)
				suite.NoError(err)
				ts := tsoutil.ComposeTS(physical, logical)
				suite.Less(lastTS, ts)
				lastTS = ts
			}
		}()
	}
	wg.Wait()
}

func (suite *tsoClientTestSuite) TestGetTSAsync() {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			tsFutures := make([]pd.TSFuture, tsoRequestRound)
			for i := range tsFutures {
				tsFutures[i] = suite.client.GetTSAsync(suite.ctx)
			}
			var lastTS uint64 = math.MaxUint64
			for i := len(tsFutures) - 1; i >= 0; i-- {
				physical, logical, err := tsFutures[i].Wait()
				suite.NoError(err)
				ts := tsoutil.ComposeTS(physical, logical)
				suite.Greater(lastTS, ts)
				lastTS = ts
			}
		}()
	}
	wg.Wait()
}

// More details can be found in this issue: https://github.com/tikv/pd/issues/4884
func (suite *tsoClientTestSuite) TestUpdateAfterResetTSO() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	testutil.Eventually(re, func() bool {
		_, _, err := suite.client.GetTS(ctx)
		return err == nil
	})
	// Transfer leader to trigger the TSO resetting.
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/updateAfterResetTSO", "return(true)"))
	oldLeaderName := suite.cluster.WaitLeader()
	err := suite.cluster.GetServer(oldLeaderName).ResignLeader()
	re.NoError(err)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/updateAfterResetTSO"))
	newLeaderName := suite.cluster.WaitLeader()
	re.NotEqual(oldLeaderName, newLeaderName)
	// Request a new TSO.
	testutil.Eventually(re, func() bool {
		_, _, err := suite.client.GetTS(ctx)
		return err == nil
	})
	// Transfer leader back.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp", `return(true)`))
	err = suite.cluster.GetServer(newLeaderName).ResignLeader()
	re.NoError(err)
	// Should NOT panic here.
	testutil.Eventually(re, func() bool {
		_, _, err := suite.client.GetTS(ctx)
		return err == nil
	})
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp"))
}

func (suite *tsoClientTestSuite) TestRandomResignLeader() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	defer re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))

	ctx, cancel := context.WithCancel(suite.ctx)
	var wg sync.WaitGroup
	checkTSO(ctx, re, &wg, suite.backendEndpoints)
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := r.Intn(2) + 1
		time.Sleep(time.Duration(n) * time.Second)
		if !suite.legacy {
			suite.tsoCluster.ResignPrimary()
			suite.tsoCluster.WaitForDefaultPrimaryServing(re)
		} else {
			err := suite.cluster.ResignLeader()
			re.NoError(err)
			suite.cluster.WaitLeader()
		}
		time.Sleep(time.Duration(n) * time.Second)
		cancel()
	}()
	wg.Wait()
}

func (suite *tsoClientTestSuite) TestRandomShutdown() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	defer re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))

	ctx, cancel := context.WithCancel(suite.ctx)
	var wg sync.WaitGroup
	checkTSO(ctx, re, &wg, suite.backendEndpoints)
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := r.Intn(2) + 1
		time.Sleep(time.Duration(n) * time.Second)
		if !suite.legacy {
			suite.tsoCluster.WaitForDefaultPrimaryServing(re).Close()
		} else {
			suite.cluster.GetServer(suite.cluster.GetLeader()).GetServer().Close()
		}
		time.Sleep(time.Duration(n) * time.Second)
		cancel()
	}()
	wg.Wait()
	suite.TearDownSuite()
	suite.SetupSuite()
}

// When we upgrade the PD cluster, there may be a period of time that the old and new PDs are running at the same time.
func TestMixedTSODeployment(t *testing.T) {
	re := require.New(t)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	defer re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/skipUpdateServiceMode", "return(true)"))
	defer re.NoError(failpoint.Enable("github.com/tikv/pd/client/skipUpdateServiceMode", "return(true)"))

	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cancel()
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderServer := cluster.GetServer(cluster.WaitLeader())
	backendEndpoints := leaderServer.GetAddr()

	apiSvr, err := cluster.JoinAPIServer(ctx)
	re.NoError(err)
	err = apiSvr.Run()
	re.NoError(err)

	_, cleanup := mcs.StartSingleTSOTestServer(ctx, re, backendEndpoints, tempurl.Alloc())
	defer cleanup()

	ctx1, cancel1 := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	checkTSO(ctx1, re, &wg, backendEndpoints)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2; i++ {
			n := r.Intn(2) + 1
			time.Sleep(time.Duration(n) * time.Second)
			leaderServer.ResignLeader()
			leaderServer = cluster.GetServer(cluster.WaitLeader())
		}
		cancel1()
	}()
	wg.Wait()
}

func checkTSO(ctx context.Context, re *require.Assertions, wg *sync.WaitGroup, backendEndpoints string) {
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			cli := mcs.SetupClientWithKeyspace(ctx, re, strings.Split(backendEndpoints, ","))
			var ts, lastTS uint64
			for {
				select {
				case <-ctx.Done():
					// Make sure the lastTS is not empty
					re.NotEmpty(lastTS)
					return
				default:
				}
				physical, logical, err := cli.GetTS(ctx)
				// omit the error check since there are many kinds of errors
				if err != nil {
					continue
				}
				ts = tsoutil.ComposeTS(physical, logical)
				re.Less(lastTS, ts)
				lastTS = ts
			}
		}()
	}
}
