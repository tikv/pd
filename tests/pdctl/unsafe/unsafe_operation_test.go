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

package unsafe_operation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func TestRemoveFailedStores(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	err = cluster.GetLeaderServer().BootstrapCluster()
	re.NoError(err)
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()
	defer cluster.Destroy()

	args := []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "1,2,3"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "1,2,3", "--timeout", "3600"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "1,2,3", "--timeout", "abc"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.Error(err)
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "show"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "history"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
}
