// Copyright 2021 TiKV Project Authors.
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

package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	cmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const pdControlCallerID = "pd-http-client"

func TestSendAndGetComponent(t *testing.T) {
	re := require.New(t)
	handler := func(context.Context, *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
		mux := http.NewServeMux()
		mux.HandleFunc("/pd/api/v1/cluster", func(w http.ResponseWriter, r *http.Request) {
			cluster := &metapb.Cluster{
				Id: 0,
			}
			var rd render.Render
			rd.JSON(w, http.StatusOK, cluster)
		})
		mux.HandleFunc("/pd/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
			callerID := apiutil.GetCallerIDOnHTTP(r)
			for k := range r.Header {
				log.Info("header", zap.String("key", k))
			}
			log.Info("caller id", zap.String("caller-id", callerID))
			re.Equal(pdControlCallerID, callerID)
			healths := []api.Health{
				{
					Name: "test",
				},
			}
			healthsBytes, err := json.Marshal(healths)
			re.NoError(err)
			w.Write(healthsBytes)
		})
		info := apiutil.APIServiceGroup{
			IsCore: true,
		}
		return mux, info, nil
	}
	cfg := server.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	ctx, cancel := context.WithCancel(context.Background())
	svr, err := server.CreateServer(ctx, cfg, nil, handler)
	re.NoError(err)
	err = svr.Run()
	re.NoError(err)
	pdAddr := svr.GetAddr()
	defer func() {
		cancel()
		svr.Close()
		testutil.CleanServer(svr.GetConfig().DataDir)
	}()

	cmd := cmd.GetRootCmd()
	args := []string{"-u", pdAddr, "health"}
	output, err := ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(`[
  {
    "name": "test",
    "member_id": 0,
    "client_urls": null,
    "health": false
  }
]
`, string(output))
}
