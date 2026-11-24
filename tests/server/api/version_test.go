// Copyright 2019 TiKV Project Authors.
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
	"context"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/tests"
)

func TestGetVersion(t *testing.T) {
	re := require.New(t)

	temp, err := os.CreateTemp(t.TempDir(), "stdout")
	re.NoError(err)
	fname := temp.Name()
	old := os.Stdout
	t.Cleanup(func() {
		os.Stdout = old
		re.NoError(temp.Close())
	})
	os.Stdout = temp

	ctx := t.Context()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	var requestErr error
	var responseCode int
	var requested bool
	addr := cluster.GetServer("pd1").GetAddr() + api.APIPrefix + "/api/v1/version"
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/server/memberNil", func() {
		requested = true
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, addr, http.NoBody)
		if err != nil {
			requestErr = err
			return
		}
		resp, err := tests.TestDialClient.Do(req)
		if err != nil {
			requestErr = err
			return
		}
		defer resp.Body.Close()
		responseCode = resp.StatusCode
		_, requestErr = io.ReadAll(resp.Body)
	}))
	defer func() { re.NoError(failpoint.Disable("github.com/tikv/pd/server/memberNil")) }()
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	re.True(requested, "the request must run before member initialization")
	re.NoError(requestErr)
	re.Equal(http.StatusOK, responseCode)

	out, err := os.ReadFile(fname)
	re.NoError(err)
	re.NotContains(string(out), "PANIC")
}
