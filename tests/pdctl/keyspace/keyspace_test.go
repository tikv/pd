// Copyright 2023 TiKV Project Authors.
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

package keyspace_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/testutil"
	api "github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func TestKeyspace(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	keyspaces := make([]string, 0)
	for i := 1; i < 10; i++ {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := tests.NewTestAPICluster(ctx, 3, func(conf *config.Config, serverName string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	ttc, err := tests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer ttc.Destroy()
	cmd := pdctlCmd.GetRootCmd()

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	defaultKeyspaceGroupID := fmt.Sprintf("%d", utils.DefaultKeyspaceGroupID)

	var k api.KeyspaceMeta
	keyspaceName := "keyspace_1"
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace", keyspaceName}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &k))
		return k.GetName() == keyspaceName
	})
	re.Equal(uint32(1), k.GetId())
	re.Equal(defaultKeyspaceGroupID, k.Config[keyspace.TSOKeyspaceGroupIDKey])

	// split keyspace group.
	newGroupID := "2"
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "split", "0", newGroupID, "1"}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	// check keyspace group in keyspace whether changed.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace", keyspaceName}
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &k))
		return newGroupID == k.Config[keyspace.TSOKeyspaceGroupIDKey]
	})

	// test error name
	args := []string{"-u", pdAddr, "keyspace", "error_name"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Fail")
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}
