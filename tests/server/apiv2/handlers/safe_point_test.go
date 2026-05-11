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

package handlers_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/keyspace"
	"github.com/tikv/pd/tests"
)

const gcSafePointPrefix = "/pd/api/v2/gc/safepoint"

type safePointTestSuite struct {
	keyspaceTestSuite
}

func TestSafePointTestSuite(t *testing.T) {
	suite.Run(t, new(safePointTestSuite))
}

func (suite *safePointTestSuite) TestLoadGCSafePoint() {
	re := suite.Require()
	storage := suite.server.GetServer().GetStorage()
	re.NoError(storage.SaveGCSafePoint(100))

	defaultSafePoint := mustLoadGCSafePoint(re, suite.server, keyspace.DefaultKeyspaceID)
	re.Equal(uint32(keyspace.DefaultKeyspaceID), defaultSafePoint.KeyspaceID)
	re.Equal(uint64(100), defaultSafePoint.SafePoint)

	keyspaces := mustMakeTestKeyspaces(re, suite.server, 1)
	keyspaceID := keyspaces[0].Id
	re.NoError(storage.SaveKeyspaceGCSafePoint(strconv.FormatUint(uint64(keyspaceID), 10), 200))

	keyspaceSafePoint := mustLoadGCSafePoint(re, suite.server, keyspaceID)
	re.Equal(keyspaceID, keyspaceSafePoint.KeyspaceID)
	re.Equal(uint64(200), keyspaceSafePoint.SafePoint)
}

func mustLoadGCSafePoint(re *require.Assertions, server *tests.TestServer, keyspaceID uint32) *handlers.GCSafePoint {
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+gcSafePointPrefix+"/"+strconv.FormatUint(uint64(keyspaceID), 10), nil)
	re.NoError(err)
	httpResp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	re.Equal(http.StatusOK, httpResp.StatusCode)

	data, err := io.ReadAll(httpResp.Body)
	re.NoError(err)
	resp := &handlers.GCSafePoint{}
	re.NoError(json.Unmarshal(data, resp))
	return resp
}
