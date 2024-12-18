// Copyright 2018 TiKV Project Authors.
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
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

func checkSliceResponse(re *require.Assertions, body []byte, cfgs []*config.Config, unhealthy string) {
	var got []Health
	re.NoError(json.Unmarshal(body, &got))
	re.Len(cfgs, len(got))

	for _, h := range got {
		for _, cfg := range cfgs {
			if h.Name != cfg.Name {
				continue
			}
			relaxEqualStings(re, h.ClientUrls, strings.Split(cfg.ClientUrls, ","))
		}
		if h.Name == unhealthy {
			re.False(h.Health)
			continue
		}
		re.True(h.Health)
	}
}

func TestHealthSlice(t *testing.T) {
	re := require.New(t)
	cfgs, svrs, clean := mustNewCluster(re, 3)
	defer clean()
	var leader, follower *server.Server

	for _, svr := range svrs {
		if !svr.IsClosed() && svr.GetMember().IsLeader() {
			leader = svr
		} else {
			follower = svr
		}
	}
	mustBootstrapCluster(re, leader)
	addr := leader.GetConfig().ClientUrls + apiPrefix + "/api/v1/health"
	follower.Close()
	resp, err := testDialClient.Get(addr)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	re.NoError(err)
	checkSliceResponse(re, buf, cfgs, follower.GetConfig().Name)
}

func TestReady(t *testing.T) {
	re := require.New(t)
	_, svrs, clean := mustNewCluster(re, 1)
	defer clean()
	mustBootstrapCluster(re, svrs[0])
	url := svrs[0].GetConfig().ClientUrls + apiPrefix + "/api/v1/ready"
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
	resp, err := testDialClient.Get(url)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	re.NoError(err)
	re.Empty(buf)
	re.Equal(expectCode, resp.StatusCode)
	r := &ReadyStatus{}
	if isReady {
		r.RegionLoaded = true
	}
	data, err := json.Marshal(r)
	re.NoError(err)
	err = tu.CheckGetJSON(testDialClient, url+"?verbose", data,
		tu.Status(re, expectCode))
	re.NoError(err)
}
