// Copyright 2026 TiKV Project Authors.
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

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
	apiv2handlers "github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

type safePointTestSuite struct {
	suite.Suite
	cleanup func()
	cluster *tests.TestCluster
	server  *tests.TestServer
}

func TestSafePointTestSuite(t *testing.T) {
	suite.Run(t, new(safePointTestSuite))
}

func (suite *safePointTestSuite) SetupTest() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 1)
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	re.NoError(suite.server.BootstrapCluster())
}

func (suite *safePointTestSuite) TearDownTest() {
	suite.cleanup()
	suite.cluster.Destroy()
}

func (suite *safePointTestSuite) TestLoadGCSafePoint() {
	re := suite.Require()
	gcStateManager := suite.server.GetServer().GetGCStateManager()
	keyspaceID := keyspace.GetBootstrapKeyspaceID()
	gcKeyspaceID := constant.NullKeyspaceID
	if kerneltype.IsNextGen() {
		gcKeyspaceID = keyspaceID
	}
	_, err := gcStateManager.AdvanceTxnSafePoint(gcKeyspaceID, 200, time.Now())
	re.NoError(err)
	_, _, err = gcStateManager.AdvanceGCSafePoint(gcKeyspaceID, 200)
	re.NoError(err)

	sp := fmt.Sprintf("/gc/safepoint/%d", keyspaceID)
	resp, err := tests.TestDialClient.Get(suite.server.GetAddr() + v2Prefix + sp)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusOK, resp.StatusCode)

	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	safePoint := &apiv2handlers.GCSafePoint{}
	re.NoError(json.Unmarshal(data, safePoint))
	re.Equal(keyspaceID, safePoint.KeyspaceID)
	re.Equal(uint64(200), safePoint.SafePoint)
}
