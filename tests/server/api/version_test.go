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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/tests"
)

func TestGetVersion(t *testing.T) {
	re := require.New(t)

	fname := filepath.Join(os.TempDir(), "stdout")
	old := os.Stdout
	temp, err := os.Create(fname)
	re.NoError(err)
	os.Stdout = temp

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/memberNil", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/memberNil"))
	}()

	reqCh := make(chan struct{})
	go func() {
		<-reqCh
		time.Sleep(200 * time.Millisecond)
		leader := cluster.GetLeaderServer()
		if leader == nil {
			return
		}
		addr := leader.GetAddr() + api.APIPrefix + "/api/v1/version"
		resp, err := tests.TestDialClient.Get(addr)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
	}()

	reqCh <- struct{}{}
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()

	out, err := os.ReadFile(fname)
	re.NoError(err)
	re.NotContains(string(out), "PANIC")

	// clean up
	temp.Close()
	os.Stdout = old
	os.RemoveAll(fname)
}
