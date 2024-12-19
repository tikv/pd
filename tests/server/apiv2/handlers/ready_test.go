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

	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

func TestReady(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	server := cluster.GetLeaderServer()
	re.NoError(server.BootstrapCluster())
	url := server.GetConfig().ClientUrls + v2Prefix + "/ready"
	failpoint.Enable("github.com/tikv/pd/pkg/storage/loadRegionSlow", `return()`)
	checkReady(re, url, false)
	failpoint.Disable("github.com/tikv/pd/pkg/storage/loadRegionSlow")
	checkReady(re, url, true)
}

func checkReady(re *require.Assertions, url string, isReady bool) {
	expectCode := http.StatusOK
	if !isReady {
		expectCode = http.StatusInternalServerError
	}
	resp, err := tests.TestDialClient.Get(url)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.Empty(buf)
	re.Equal(expectCode, resp.StatusCode)
	r := &handlers.ReadyStatus{}
	if isReady {
		r.RegionLoaded = true
	}
	data, err := json.Marshal(r)
	re.NoError(err)
	err = tu.CheckGetJSON(tests.TestDialClient, url+"?verbose", data,
		tu.Status(re, expectCode))
	re.NoError(err)
}
