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
	"testing"

	"github.com/stretchr/testify/require"

	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func TestRemoveFailedStores(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	cluster, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	err = cluster.GetLeaderServer().BootstrapCluster()
	re.NoError(err)
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	// Store 1 is up and connected and stores 2 and 3 don't exist, so the server
	// rejects these requests and the error is returned to the caller. Which
	// store gets reported depends on map iteration order.
	args := []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "1,2,3"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.ErrorContains(err, "ErrUnsafeRecoveryInvalidInput")
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "1,2,3", "--timeout", "3600"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.ErrorContains(err, "ErrUnsafeRecoveryInvalidInput")
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "1,2,3", "--plan-execution-timeout", "600", "--disable-paranoid-check"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.ErrorContains(err, "ErrUnsafeRecoveryInvalidInput")
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "1", "2"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.ErrorContains(err, "more arguments than expected")
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "1,2,3", "--timeout", "abc"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.Error(err)
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "abort"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "show"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	// "history" is not a subcommand, so it is parsed as a store id.
	args = []string{"-u", pdAddr, "unsafe", "remove-failed-stores", "history"}
	_, err = tests.ExecuteCommand(cmd, args...)
	re.ErrorContains(err, "invalid store id")
}
