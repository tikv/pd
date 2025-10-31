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
	leaderURL := leader.GetConfig().ClientUrls + v2Prefix + "/ready"
	followerServer := cluster.GetServer(cluster.GetFollower())
	followerURL := followerServer.GetConfig().ClientUrls + v2Prefix + "/ready"
	// check leader ready status before bootstrap
	checkReadyAPI(re, leaderURL, http.StatusOK, false, false)
	re.NoError(leader.BootstrapCluster())

	// check ready status after bootstrap
	checkReadyAPI(re, leaderURL, http.StatusOK, true, true)
	checkReadyAPI(re, followerURL, http.StatusOK, true, true)

	// check ready status when region is not loaded for leader
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow", `return("`+leader.GetAddr()+`")`))
	checkReadyAPI(re, leaderURL, http.StatusInternalServerError, true, false)
	checkReadyAPI(re, followerURL, http.StatusOK, true, true)

	// check ready status when region is not loaded for follower
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow", `return("`+followerServer.GetAddr()+`")`))
	checkReadyAPI(re, leaderURL, http.StatusOK, true, true)
	checkReadyAPI(re, followerURL, http.StatusInternalServerError, true, false)

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow"))
}

func checkReadyAPI(re *require.Assertions, url string, expectCode int, expectBootstrapped bool, expectRegionLoaded bool) {
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	re.NoError(err)
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
	resp, err = tests.TestDialClient.Do(req)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err = io.ReadAll(resp.Body)
	re.NoError(err)
	r := &handlers.ReadyStatus{}
	re.NoError(json.Unmarshal(buf, &r))
	re.Equal(expectCode, resp.StatusCode)
	re.Equal(expectBootstrapped, r.IsBootstrapped)
	re.Equal(expectRegionLoaded, r.RegionLoaded)
}

func TestReadyCheckAPI(t *testing.T) {
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

	leaderURL := leader.GetConfig().ClientUrls + v2Prefix
	follower := cluster.GetServer(cluster.GetFollower())
	followerURL := follower.GetConfig().ClientUrls + v2Prefix

	testCases := []struct {
		name                  string
		targetURL             string // to use to set leader or follower
		path                  string
		setup                 func()
		cleanup               func()
		expectCode            int
		expectBodyContains    []string
		expectBodyNotContains []string
	}{
		// --- /livez tests ---
		{
			name:               "livez: healthy",
			targetURL:          leaderURL,
			path:               "/livez",
			expectCode:         http.StatusOK,
			expectBodyContains: []string{"ok\n"},
		},
		{
			name:               "livez: healthy with trailing slash",
			targetURL:          leaderURL,
			path:               "/livez/",
			expectCode:         http.StatusOK,
			expectBodyContains: []string{"ok\n"},
		},
		{
			name:               "livez: verbose healthy",
			targetURL:          leaderURL,
			path:               "/livez?verbose",
			expectCode:         http.StatusOK,
			expectBodyContains: []string{"[+]etcd-serializable-read ok", "ok\n"},
		},
		{
			name:      "livez: unhealthy due to serializable read error",
			targetURL: leaderURL,
			path:      "/livez",
			setup: func() {
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdSerializableReadError", `return("`+leader.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdSerializableReadError"))
			},
			expectCode:         http.StatusServiceUnavailable,
			expectBodyContains: []string{"[-]etcd-serializable-read failed: injected serializable read error"},
		},

		// --- /readyz tests ---
		{
			name:               "readyz: verbose healthy",
			targetURL:          leaderURL,
			path:               "/readyz?verbose",
			expectCode:         http.StatusOK,
			expectBodyContains: []string{"[+]leader-promotion ok", "[+]etcd-data-corruption ok", "ok\n"},
		},
		{
			name:      "readyz: unhealthy when regions are not loaded (leader-promotion fails)",
			targetURL: leaderURL,
			path:      "/readyz?verbose",
			setup: func() {
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow", `return("`+leader.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/loadRegionSlow"))
			},
			expectCode:         http.StatusServiceUnavailable,
			expectBodyContains: []string{"[-]leader-promotion failed: regions not loaded"},
		},
		{
			name:      "readyz: unhealthy when etcd data corruption alarm is active",
			targetURL: leaderURL,
			path:      "/readyz?verbose",
			setup: func() {
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdDataCorruptionAlarm", `return("`+leader.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdDataCorruptionAlarm"))
			},
			expectCode:         http.StatusServiceUnavailable,
			expectBodyContains: []string{"[-]etcd-data-corruption failed: injected data corruption alarm"},
		},
		{
			name:      "readyz: unhealthy when member is a learner",
			targetURL: leaderURL,
			path:      "/readyz?verbose",
			setup: func() {
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner", `return("`+leader.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner"))
			},
			expectCode:         http.StatusServiceUnavailable,
			expectBodyContains: []string{"[-]etcd-non-learner failed: injected learner state"},
		},

		// --- /readyz and exclude tests ---
		{
			name:      "readyz: reports multiple failures",
			targetURL: leaderURL,
			path:      "/readyz",
			setup: func() {
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdLinearizableReadError", `return("`+leader.GetAddr()+`")`))
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner", `return("`+leader.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdLinearizableReadError"))
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner"))
			},
			expectCode:         http.StatusServiceUnavailable,
			expectBodyContains: []string{"[-]etcd-linearizable-read failed", "[-]etcd-non-learner failed"},
		},
		{
			name:      "readyz: becomes healthy after excluding all multiple failures",
			targetURL: leaderURL,
			path:      "/readyz?exclude=etcd-linearizable-read&exclude=etcd-non-learner",
			setup: func() {
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdLinearizableReadError", `return("`+leader.GetAddr()+`")`))
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner", `return("`+leader.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdLinearizableReadError"))
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner"))
			},
			expectCode:            http.StatusOK,
			expectBodyContains:    []string{"ok\n"},
			expectBodyNotContains: []string{"etcd-linearizable-read", "etcd-non-learner"},
		},
		{
			name:      "readyz: remains unhealthy when excluding only one of two failures",
			targetURL: leaderURL,
			path:      "/readyz?exclude=etcd-non-learner",
			setup: func() {
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdLinearizableReadError", `return("`+leader.GetAddr()+`")`))
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner", `return("`+leader.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdLinearizableReadError"))
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner"))
			},
			expectCode:            http.StatusServiceUnavailable,
			expectBodyContains:    []string{"[-]etcd-linearizable-read failed"},
			expectBodyNotContains: []string{"etcd-non-learner"},
		},
		{
			name:      "subpath: /readyz/etcd-non-learner is unhealthy",
			targetURL: leaderURL,
			path:      "/readyz/etcd-non-learner",
			setup: func() {
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner", `return("`+leader.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner"))
			},
			expectCode:         http.StatusServiceUnavailable,
			expectBodyContains: []string{"[-]etcd-non-learner failed: injected learner state"},
		},
		{
			name:      "readyz: remains unhealthy when excluding a non-existent check",
			targetURL: leaderURL,
			path:      "/readyz?exclude=non-existent-check",
			setup: func() {
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdLinearizableReadError", `return("`+leader.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdLinearizableReadError"))
			},
			expectCode:         http.StatusServiceUnavailable,
			expectBodyContains: []string{"[-]etcd-linearizable-read failed"},
		},
		// --- subpath specific tests ---
		{
			name:      "subpath: individual check works independently",
			targetURL: leaderURL,
			path:      "/readyz/leader-promotion?verbose",
			setup: func() {
				// inject unrelated failure
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner", `return("`+leader.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdIsLearner"))
			},
			expectCode:         http.StatusOK,
			expectBodyContains: []string{"[+]leader-promotion ok", "ok\n"},
		},
		// --- follower specific tests ---
		{
			name:      "readyz on follower: unhealthy when local serializable read fails",
			targetURL: followerURL,
			path:      "/readyz?verbose",
			setup: func() {
				// inject error only on follower
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdSerializableReadError", `return("`+follower.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdSerializableReadError"))
			},
			expectCode:         http.StatusServiceUnavailable,
			expectBodyContains: []string{"[-]etcd-serializable-read failed"},
		},
		{
			name:      "readyz on leader: remains healthy when follower fails",
			targetURL: leaderURL,
			path:      "/readyz",
			setup: func() {
				// inject error only on follower
				re.NoError(failpoint.Enable("github.com/tikv/pd/server/apiv2/handlers/etcdSerializableReadError", `return("`+follower.GetAddr()+`")`))
			},
			cleanup: func() {
				re.NoError(failpoint.Disable("github.com/tikv/pd/server/apiv2/handlers/etcdSerializableReadError"))
			},
			expectCode: http.StatusOK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			if tc.setup != nil {
				tc.setup()
			}
			if tc.cleanup != nil {
				defer tc.cleanup()
			}

			req, err := http.NewRequest(http.MethodGet, tc.targetURL+tc.path, http.NoBody)
			re.NoError(err)
			resp, err := tests.TestDialClient.Do(req)
			re.NoError(err)
			defer resp.Body.Close()

			re.Equal(tc.expectCode, resp.StatusCode, "http status code not match")

			body, err := io.ReadAll(resp.Body)
			re.NoError(err)
			bodyStr := string(body)

			for _, contain := range tc.expectBodyContains {
				re.Contains(bodyStr, contain)
			}
			for _, notContain := range tc.expectBodyNotContains {
				re.NotContains(bodyStr, notContain)
			}
		})
	}
}
