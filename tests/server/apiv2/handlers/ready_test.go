// Copyright 2024 TiKV Project Authors.
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
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

func TestReadyAPI(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	leader := cluster.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	url := leader.GetConfig().ClientUrls + v2Prefix + "/ready"
	// check ready status when region is not loaded for leader
	failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow", `return("`+leader.GetAddr()+`")`)
	checkReadyAPI(re, url, false)
	// check ready status when region is loaded for leader
	failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow")
	checkReadyAPI(re, url, true)
	// check ready status when region is not loaded for follower
	followerServer := cluster.GetServer(cluster.GetFollower())
	url = followerServer.GetConfig().ClientUrls + v2Prefix + "/ready"
	failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow", `return("`+followerServer.GetAddr()+`")`)
	checkReadyAPI(re, url, true)
	checkReadyAPI(re, url, false, apiutil.PDAllowFollowerHandleHeader)
	// check ready status when region is loaded for follower
	failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow")
	checkReadyAPI(re, url, true)
	checkReadyAPI(re, url, true, apiutil.PDAllowFollowerHandleHeader)
}

func checkReadyAPI(re *require.Assertions, url string, isReady bool, headers ...string) {
	expectCode := http.StatusOK
	if !isReady {
		expectCode = http.StatusInternalServerError
	}
	// check ready status
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	re.NoError(err)
	if len(headers) > 0 {
		req.Header.Add(headers[0], "true")
	}
	resp, err := tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.Empty(buf)
	re.Equal(expectCode, resp.StatusCode)
	// check ready status with verbose
	req, err = http.NewRequest(http.MethodGet, url+"?verbose", http.NoBody)
	re.NoError(err)
	if len(headers) > 0 {
		req.Header.Add(headers[0], "true")
	}
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err = io.ReadAll(resp.Body)
	re.NoError(err)
	r := &handlers.ReadyStatus{}
	re.NoError(json.Unmarshal(buf, &r))
	re.Equal(expectCode, resp.StatusCode)
	re.Equal(isReady, r.RegionLoaded)
}
