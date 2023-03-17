// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	tsosvr "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/mcs"
)

const (
	tsoRequestConcurrencyNumber = 1
	tsoRequestRound             = 30
)

type tsoServiceTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
	tsoSvr1          *tsosvr.Server
	tsoSvrCleanup1   func()
}

func TestTSOServiceTestSuite(t *testing.T) {
	suite.Run(t, new(tsoServiceTestSuite))
}

func (suite *tsoServiceTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()

	suite.tsoSvr1, suite.tsoSvrCleanup1 = mcs.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints)
}

func (suite *tsoServiceTestSuite) TearDownSuite() {
	suite.tsoSvrCleanup1()
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *tsoServiceTestSuite) TestTSOServerRegister() {
	re := suite.Require()

	endpoints := strings.Split(suite.backendEndpoints, ",")
	cli1 := mcs.SetupTSOClient(suite.ctx, re, endpoints)
	cli2 := mcs.SetupTSOClient(suite.ctx, re, endpoints)

	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			var lastTS uint64
			for i := 0; i < tsoRequestRound; i++ {
				physical, logical, err := cli1.GetTS(context.Background())
				re.NoError(err)
				ts := tsoutil.ComposeTS(physical, logical)
				re.Less(lastTS, ts)
				lastTS = ts
				physical, logical, err = cli2.GetTS(context.Background())
				re.NoError(err)
				ts = tsoutil.ComposeTS(physical, logical)
				re.Less(lastTS, ts)
				lastTS = ts
			}
		}()
	}
	wg.Wait()
}
