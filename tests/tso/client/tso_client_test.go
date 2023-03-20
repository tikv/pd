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

package tso_client_test

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/mcs"
)

const (
	tsoRequestConcurrencyNumber = 5
	tsoRequestRound             = 30
)

type tsoClient interface {
	GetTS(ctx context.Context) (int64, int64, error)
	GetTSAsync(ctx context.Context) pd.TSFuture
}

type tsoClientTestSuite struct {
	suite.Suite
	legacy bool

	ctx    context.Context
	cancel context.CancelFunc
	// The PD cluster
	cluster *tests.TestCluster
	// The TSO service in microservice mode
	tsoServer        *tso.Server
	tsoServerCleanup func()

	client tsoClient
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
	switch suite.legacy {
	case true:
		suite.cluster, err = tests.NewTestCluster(suite.ctx, 1)
		re.NoError(err)
		err = suite.cluster.RunInitialServers()
		re.NoError(err)
		leaderName := suite.cluster.WaitLeader()
		pdLeader := suite.cluster.GetServer(leaderName)
		backendEndpoints := pdLeader.GetAddr()
		suite.client, err = pd.NewClientWithContext(suite.ctx, strings.Split(backendEndpoints, ","), pd.SecurityOption{})
		re.NoError(err)
	case false:
		suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
		re.NoError(err)
		err = suite.cluster.RunInitialServers()
		re.NoError(err)
		leaderName := suite.cluster.WaitLeader()
		pdLeader := suite.cluster.GetServer(leaderName)
		backendEndpoints := pdLeader.GetAddr()
		suite.tsoServer, suite.tsoServerCleanup = mcs.StartSingleTSOTestServer(suite.ctx, re, backendEndpoints)
		suite.client = mcs.SetupTSOClient(suite.ctx, re, strings.Split(backendEndpoints, ","))
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
