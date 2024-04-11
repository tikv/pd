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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
	var leader, follow *server.Server

	for _, svr := range svrs {
		if !svr.IsClosed() && svr.GetMember().IsLeader() {
			leader = svr
		} else {
			follow = svr
		}
	}
	mustBootstrapCluster(re, leader)
	addr := leader.GetConfig().ClientUrls + apiPrefix + "/api/v1/health"
	follow.Close()
	resp, err := testDialClient.Get(addr)
	re.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	re.NoError(err)
	checkSliceResponse(re, buf, cfgs, follow.GetConfig().Name)
}
