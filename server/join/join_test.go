// Copyright 2016 TiKV Project Authors.
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

package join

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
<<<<<<< HEAD:server/join/join_test.go
	"github.com/tikv/pd/pkg/utils/assertutil"
=======
	"go.uber.org/goleak"

	scheserver "github.com/tikv/pd/pkg/mcs/scheduling/server"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
>>>>>>> 51b1226c30 (mcs/scheduling: clean primary resources on exit (#10645)):pkg/mcs/scheduling/server/apis/v1/api_test.go
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

<<<<<<< HEAD:server/join/join_test.go
// A PD joins itself.
func TestPDJoinsItself(t *testing.T) {
	re := require.New(t)
	cfg := server.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	defer testutil.CleanServer(cfg.DataDir)
	cfg.Join = cfg.AdvertiseClientUrls
	re.Error(PrepareJoinCluster(cfg))
=======
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestGetAllStoresReturnsNotBootstrappedWhenBasicClusterMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	re := require.New(t)
	resp := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(resp)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/stores", nil)
	ctx.Set(multiservicesapi.ServiceContextKey, &scheserver.Server{})

	getAllStores(ctx)

	re.Equal(http.StatusInternalServerError, resp.Code)
	re.Contains(resp.Body.String(), "not bootstrapped")
>>>>>>> 51b1226c30 (mcs/scheduling: clean primary resources on exit (#10645)):pkg/mcs/scheduling/server/apis/v1/api_test.go
}
