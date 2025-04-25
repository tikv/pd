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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

func TestGRPCGateway(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leaderServer := cluster.GetLeaderServer()

	addr := leaderServer.GetAddr() + "/v3/kv/put"
	putKey := map[string]string{"key": "Zm9v", "value": "YmFy"}
	v, _ := json.Marshal(putKey)
	err = tu.CheckPostJSON(testDialClient, addr, v, tu.StatusOK(re))
	re.NoError(err)
	addr = leaderServer.GetAddr() + "/v3/kv/range"
	getKey := map[string]string{"key": "Zm9v"}
	v, _ = json.Marshal(getKey)
	err = tu.CheckPostJSON(testDialClient, addr, v, tu.StatusOK(re), tu.StringContain(re, "Zm9v"))
	re.NoError(err)
}
