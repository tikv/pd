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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

type etcdTestSuite struct {
	suite.Suite
	cfgs    []*config.Config
	servers []*server.Server
	clean   func()
}

func TestEtcdTestSuite(t *testing.T) {
	suite.Run(t, new(etcdTestSuite))
}

func (suite *etcdTestSuite) SetupSuite() {
	suite.cfgs, suite.servers, suite.clean = mustNewCluster(suite.Require(), 3, func(cfg *config.Config) {
		cfg.EnableLocalTSO = true
		cfg.Labels = map[string]string{
			config.ZoneLabel: "dc-1",
		}
	})
}

func (suite *etcdTestSuite) TearDownSuite() {
	suite.clean()
}

func (suite *etcdTestSuite) TestEtcdReady() {
	for _, cfg := range suite.cfgs {
		url := cfg.ClientUrls + apiPrefix + "/api/v1/ready"

		result := etcdReadiness{}
		suite.Eventually(func() bool {
			err := tu.ReadGetJSON(suite.Require(), testDialClient, url, &result)
			suite.T().Logf("response: '%+v'", result)
			return err == nil
		}, time.Second, time.Millisecond*50)
	}
}
