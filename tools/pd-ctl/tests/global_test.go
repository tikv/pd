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
	"fmt"
	"net/http"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	cmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/pdctl/command"
)

func TestSendAndGetComponent(t *testing.T) {
	re := require.New(t)
	handler := func(context.Context, *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
		mux := http.NewServeMux()
		// check pd http sdk api
		mux.HandleFunc("/pd/api/v1/cluster", func(w http.ResponseWriter, r *http.Request) {
			callerID := apiutil.GetCallerIDOnHTTP(r)
			re.Equal(command.PDControlCallerID, callerID)
			cluster := &metapb.Cluster{Id: 1}
			clusterBytes, err := json.Marshal(cluster)
			re.NoError(err)
			w.Write(clusterBytes)
		})
		// check http client api
		// TODO: remove this comment after replacing dialClient with the PD HTTP client completely.
		mux.HandleFunc("/pd/api/v1/stores", func(w http.ResponseWriter, r *http.Request) {
			callerID := apiutil.GetCallerIDOnHTTP(r)
			re.Equal(command.PDControlCallerID, callerID)
			fmt.Fprint(w, callerID)
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
	args := []string{"-u", pdAddr, "cluster"}
	output, err := ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(fmt.Sprintf("%s\n", `{
  "id": 1
}`), string(output))

	args = []string{"-u", pdAddr, "store"}
	output, err = ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Equal(fmt.Sprintf("%s\n", command.PDControlCallerID), string(output))
}
