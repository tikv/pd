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
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
)

type tsoClientTestSuite struct {
	suite.Suite
	legacy bool

	ctx    context.Context
	cancel context.CancelFunc
	// The PD cluster.
	cluster *tests.TestCluster
	// The TSO service in microservice mode.
	tsoServer        *tso.Server
	tsoServerCleanup func()

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
		suite.tsoServer, suite.tsoServerCleanup = mcs.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		suite.client = mcs.SetupClientWithKeyspace(suite.ctx, re, strings.Split(suite.backendEndpoints, ","))
	}
}

func (suite *tsoClientTestSuite) TearDownSuite() {
	suite.cancel()
	if !suite.legacy {
		suite.tsoServerCleanup()
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

func (suite *tsoClientTestSuite) TestRandomTransferLeader() {
	re := suite.Require()

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	defer re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	ctx, cancel := context.WithCancel(suite.ctx)
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber + 1)
	go func() {
		defer wg.Done()
		n := r.Intn(2) + 1
		time.Sleep(time.Duration(n) * time.Second)
		err := suite.cluster.ResignLeader()
		re.NoError(err)
		suite.cluster.WaitLeader()
		cancel()
	}()

	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			client := mcs.SetupClientWithKeyspace(suite.ctx, re, strings.Split(suite.backendEndpoints, ","))
			var ts, lastTS uint64
			for {
				physical, logical, err := client.GetTS(suite.ctx)
				if err != nil {
					re.ErrorContains(err, "not leader")
				} else {
					ts = tsoutil.ComposeTS(physical, logical)
					re.Less(lastTS, ts)
					lastTS = ts
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}()
	}
	wg.Wait()
}

func (suite *tsoClientTestSuite) TestRandomShutdown() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	defer re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))

	tsoSvr, cleanup := mcs.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	defer cleanup()

	ctx, cancel := context.WithCancel(suite.ctx)
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber + 1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		n := r.Intn(2) + 1
		time.Sleep(time.Duration(n) * time.Second)
		if !suite.legacy {
			// random close one of the tso servers
			if r.Intn(2) == 0 {
				tsoSvr.Close()
			} else {
				suite.tsoServer.Close()
			}
		} else {
			// close pd leader server
			suite.cluster.GetServer(suite.cluster.GetLeader()).GetServer().Close()
		}
		cancel()
	}()

	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			client := mcs.SetupClientWithKeyspace(suite.ctx, re, strings.Split(suite.backendEndpoints, ","))
			var ts, lastTS uint64
			for {
				physical, logical, err := client.GetTS(suite.ctx)
				if err != nil {
					re.Regexp("(server not started|closed unexpectedly)", err.Error())
				} else {
					ts = tsoutil.ComposeTS(physical, logical)
					re.Less(lastTS, ts)
					lastTS = ts
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}()
	}
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
	wg.Add(tsoRequestConcurrencyNumber + 1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < 2; i++ {
			n := r.Intn(2) + 1
			time.Sleep(time.Duration(n) * time.Second)
			leaderServer.ResignLeader()
			leaderServer = cluster.GetServer(cluster.WaitLeader())
		}
		cancel1()
	}()

	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			cli := mcs.SetupClientWithKeyspace(ctx, re, strings.Split(backendEndpoints, ","))
			var ts, lastTS uint64
			for {
				physical, logical, err := cli.GetTS(ctx)
				if err != nil {
					re.Regexp("(context canceled|not leader|not the pd or local tso allocator leader)", err.Error())
				} else {
					ts = tsoutil.ComposeTS(physical, logical)
					re.Less(lastTS, ts)
					lastTS = ts
				}
				select {
				case <-ctx1.Done():
					return
				default:
				}
			}
		}()
	}
	wg.Wait()
}
